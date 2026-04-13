// zephyros_test.go: Safety net tests for SPSC → MPSC conversion
//
// Copyright (c) 2025 AGILira
// Series: an AGLIra fragment
// SPDX-License-Identifier: MPL-2.0

package zephyros

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestWrite_BasicFunctionality tests the core Write operation (SPSC baseline)
func TestWrite_BasicFunctionality(t *testing.T) {
	var processed []int
	processor := func(item *int) {
		processed = append(processed, *item)
	}

	notus, err := NewBuilder[int](4).
		WithProcessor(processor).
		Build()

	if err != nil {
		t.Fatalf("Failed to build notus: %v", err)
	}
	defer notus.Close()

	// Test successful write
	success := notus.Write(func(slot *int) {
		*slot = 42
	})

	if !success {
		t.Error("Write should succeed on empty buffer")
	}

	// Verify using stats
	stats := notus.Stats()
	if stats["writer_position"] != 1 {
		t.Errorf("Expected writer position 1, got %d", stats["writer_position"])
	}
}

// TestWrite_ClosedBuffer tests writing to closed buffer
func TestWrite_ClosedBuffer(t *testing.T) {
	processor := func(item *int) {}

	notus, err := NewBuilder[int](4).
		WithProcessor(processor).
		Build()

	if err != nil {
		t.Fatalf("Failed to build notus: %v", err)
	}

	// Close the buffer
	notus.Close()

	// Attempt write - should fail
	success := notus.Write(func(slot *int) {
		*slot = 42
	})

	if success {
		t.Error("Write should fail on closed buffer")
	}
}

// TestProcessBatch_BasicFunctionality tests ProcessBatch operation
func TestProcessBatch_BasicFunctionality(t *testing.T) {
	var processed []int
	processor := func(item *int) {
		processed = append(processed, *item)
	}

	notus, err := NewBuilder[int](8).
		WithProcessor(processor).
		WithBatchSize(4).
		Build()

	if err != nil {
		t.Fatalf("Failed to build notus: %v", err)
	}
	defer notus.Close()

	// Write some items
	for i := 0; i < 3; i++ {
		notus.Write(func(slot *int) {
			*slot = i
		})
	}

	// Flush to ensure writes are visible
	notus.Flush()

	// Process the items
	count := notus.ProcessBatch()

	if count != 3 {
		t.Errorf("Expected 3 processed items, got %d", count)
	}

	if len(processed) != 3 {
		t.Errorf("Expected 3 items in processed slice, got %d", len(processed))
	}

	// Verify ordering
	for i, val := range processed {
		if val != i {
			t.Errorf("Expected processed[%d]=%d, got %d", i, i, val)
		}
	}
}

// TestConcurrent_SPSCBaseline tests single producer single consumer baseline
func TestConcurrent_SPSCBaseline(t *testing.T) {
	var processed int64
	processor := func(item *int) {
		atomic.AddInt64(&processed, 1)
	}

	zephyros, err := NewBuilder[int](1024).
		WithProcessor(processor).
		WithBatchSize(32).
		Build()

	if err != nil {
		t.Fatalf("Failed to build zephyros: %v", err)
	}
	defer zephyros.Close()

	const itemCount = 10000
	start := time.Now()

	// Writer goroutine
	go func() {
		for i := 0; i < itemCount; i++ {
			for !zephyros.Write(func(slot *int) { *slot = i }) {
				// Retry on backpressure
			}
		}
		zephyros.Flush()
	}()

	// Reader goroutine
	go func() {
		timeout := 5 * time.Second
		for atomic.LoadInt64(&processed) < itemCount && time.Since(start) < timeout {
			count := zephyros.ProcessBatch()
			if count == 0 {
				time.Sleep(time.Microsecond)
			}
		}
	}()

	// Wait for completion
	timeout := 5 * time.Second
	for atomic.LoadInt64(&processed) < itemCount && time.Since(start) < timeout {
		time.Sleep(time.Millisecond)
	}

	actualProcessed := atomic.LoadInt64(&processed)
	if actualProcessed != itemCount {
		t.Errorf("Expected %d processed items, got %d", itemCount, actualProcessed)
	}

	stats := zephyros.Stats()
	t.Logf("SPSC Baseline: Processed %d items, buffered: %d", actualProcessed, stats["items_buffered"])
}

// TestMPSC_DualProducers tests two concurrent producers each owning a
// dedicated ring (Anemoi invariant: one producer per ring).
//
// WHY ThreadedZephyros: raw Zephyros is SPSC — exactly one producer goroutine
// per ring. Multi-producer correctness requires ThreadedZephyros which maps
// each producer to its own ring. Using a single ring for two goroutines is
// a data race if the ring ever wraps (sequence + capacity = same slot).
func TestMPSC_DualProducers(t *testing.T) {
	var processedCount int64
	processor := func(item *int) { atomic.AddInt64(&processedCount, 1) }

	tz, err := NewThreadedBuilder[int](256, 2).
		WithProcessor(processor).
		WithBatchSize(16).
		Build()
	if err != nil {
		t.Fatalf("build ThreadedZephyros: %v", err)
	}
	<-tz.LoopProcess() // wait for both consumer goroutines to be live
	defer tz.Close()

	const itemsPerProducer = 100
	var wg sync.WaitGroup

	// Create writers before launching goroutines (SPSC enforcement).
	mpscW0 := tz.NewSafeWriter(0)
	mpscW1 := tz.NewSafeWriter(1)
	mpscWriters := [2]*SafeWriter[int]{mpscW0, mpscW1}

	// Each goroutine writes exclusively to its own ring (ringID = producerID).
	wg.Add(2)
	for producer := 0; producer < 2; producer++ {
		go func(w *SafeWriter[int]) {
			defer wg.Done()
			for i := 0; i < itemsPerProducer; i++ {
				ringID := w.GetRingID()
				val := ringID*itemsPerProducer + i
				for !w.Write(func(slot *int) { *slot = val }) {
					// Ring full; yield and retry rather than busy-loop.
					runtime.Gosched()
				}
			}
		}(mpscWriters[producer])
	}
	wg.Wait()

	// Drain: wait until all written items have been processed.
	deadline := time.Now().Add(5 * time.Second)
	for atomic.LoadInt64(&processedCount) < int64(itemsPerProducer*2) && time.Now().Before(deadline) {
		runtime.Gosched()
	}

	if got := int(atomic.LoadInt64(&processedCount)); got != itemsPerProducer*2 {
		t.Errorf("expected %d items processed, got %d", itemsPerProducer*2, got)
	} else {
		t.Logf("MPSC: %d items processed via 2 producers / 2 rings", got)
	}
}

// TestBackpressure_BufferFull tests backpressure handling
func TestBackpressure_BufferFull(t *testing.T) {
	processor := func(item *int) {
		// Don't process anything - cause backpressure
	}

	zephyros, err := NewBuilder[int](4).
		WithProcessor(processor).
		Build()

	if err != nil {
		t.Fatalf("Failed to build zephyros: %v", err)
	}
	defer zephyros.Close()

	// Fill the buffer with timeout to prevent infinite loops
	successCount := 0
	start := time.Now()
	timeout := 100 * time.Millisecond

	for i := 0; i < 10 && time.Since(start) < timeout; i++ {
		done := make(chan bool, 1)
		go func(val int) {
			success := zephyros.Write(func(slot *int) {
				*slot = val
			})
			done <- success
		}(i)

		select {
		case success := <-done:
			if success {
				successCount++
			}
		case <-time.After(10 * time.Millisecond):
			// Write timed out due to backpressure - expected behavior
			goto timeout_reached
		}
	}

timeout_reached:
	// Should succeed for initial writes, then backpressure kicks in
	t.Logf("Successfully wrote %d items with capacity 4 before backpressure", successCount)

	// At least some writes should succeed before backpressure
	if successCount == 0 {
		t.Error("Expected at least some writes to succeed before backpressure")
	}
}

// TestStats_Accuracy tests Stats reporting accuracy
func TestStats_Accuracy(t *testing.T) {
	processor := func(item *int) {}

	zephyros, err := NewBuilder[int](64).
		WithProcessor(processor).
		Build()

	if err != nil {
		t.Fatalf("Failed to build zephyros: %v", err)
	}
	defer zephyros.Close()

	// Initial stats
	stats := zephyros.Stats()
	if stats["writer_position"] != 0 || stats["reader_position"] != 0 {
		t.Errorf("Expected initial positions 0, got writer=%d reader=%d",
			stats["writer_position"], stats["reader_position"])
	}

	// Write and verify stats
	zephyros.Write(func(slot *int) { *slot = 1 })
	zephyros.Write(func(slot *int) { *slot = 2 })
	zephyros.Flush()

	stats = zephyros.Stats()
	if stats["items_buffered"] != 2 {
		t.Errorf("Expected 2 buffered items, got %d", stats["items_buffered"])
	}
}

// BenchmarkMPSC_SingleProducer single producer with consumer
func BenchmarkMPSC_SingleProducer(b *testing.B) {
	var processed int64
	processor := func(item *int) {
		atomic.AddInt64(&processed, 1)
	}

	zephyros, err := NewBuilder[int](8192).
		WithProcessor(processor).
		WithBatchSize(64).
		Build()

	if err != nil {
		b.Fatalf("Failed to build zephyros: %v", err)
	}
	defer zephyros.Close()

	// Start consumer
	go func() {
		for atomic.LoadInt64(&processed) < int64(b.N) {
			zephyros.ProcessBatch()
		}
	}()

	b.ResetTimer()
	// Single producer benchmark
	for i := 0; i < b.N; i++ {
		for !zephyros.Write(func(slot *int) {
			*slot = i
		}) {
			// Retry on backpressure
		}
	}

	// Wait for processing completion
	for atomic.LoadInt64(&processed) < int64(b.N) {
		time.Sleep(time.Microsecond)
	}
}

// BenchmarkMPSC_WriteOnly demonstrates single producer performance (CORRECT MPSC usage)
func BenchmarkMPSC_WriteOnly(b *testing.B) {
	processor := func(item *int) {}

	// Use huge buffer to avoid backpressure completely
	zephyros, err := NewBuilder[int](1048576). // 1M slots
							WithProcessor(processor).
							Build()

	if err != nil {
		b.Fatalf("Failed to build zephyros: %v", err)
	}
	defer zephyros.Close()

	// Start consumer to prevent buffer fill
	go zephyros.LoopProcess()

	b.ResetTimer()
	// CORRECT MPSC: Single producer per ring (optimal performance)
	for i := 0; i < b.N; i++ {
		success := zephyros.Write(func(slot *int) {
			*slot = 42
		})
		if !success {
			b.Fatal("Write failed - should not happen with huge buffer")
		}
	}
}

// BenchmarkMPSC_ClaimOnly measures pure sequence claiming contention
func BenchmarkMPSC_ClaimOnly(b *testing.B) {
	processor := func(item *int) {}

	zephyros, err := NewBuilder[int](1048576).
		WithProcessor(processor).
		Build()

	if err != nil {
		b.Fatalf("Failed to build zephyros: %v", err)
	}
	defer zephyros.Close()

	b.ResetTimer()
	// Just measure the atomic Add operation contention
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			zephyros.writerCursor.Add(1)
		}
	})
}

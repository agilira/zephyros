// zephyros.go: Unit tests for ultra-high performance MPSC lock-free ring buffer
//
// Copyright (c) 2025 AGILira
// Series: an AGLIra fragment
// SPDX-License-Identifier: MPL-2.0

package zephyros

import (
	"sync/atomic"
	"testing"
	"time"
)

// TestZephyros_Flush tests the Flush method
func TestZephyros_Flush(t *testing.T) {
	processed := int64(0)
	processor := func(item *int) {
		atomic.AddInt64(&processed, 1)
	}

	zephyros, err := NewBuilder[int](64).
		WithProcessor(processor).
		WithBatchSize(16).
		Build()

	if err != nil {
		t.Fatalf("Failed to build: %v", err)
	}
	defer zephyros.Close()

	// Write some items
	for i := 0; i < 5; i++ {
		success := zephyros.Write(func(slot *int) {
			*slot = i
		})
		if !success {
			t.Errorf("Write %d should succeed", i)
		}
	}

	// Test Flush method - should complete without error
	// Note: In MPSC, Flush is a no-op as writes are automatically committed
	zephyros.Flush()

	t.Logf("Flush unit test passed - method executed without error")
}

// TestZephyros_TryProcessBatch tests the TryProcessBatch method
func TestZephyros_TryProcessBatch(t *testing.T) {
	processed := int64(0)
	processor := func(item *int) {
		atomic.AddInt64(&processed, 1)
	}

	zephyros, err := NewBuilder[int](64).
		WithProcessor(processor).
		WithBatchSize(16).
		Build()

	if err != nil {
		t.Fatalf("Failed to build: %v", err)
	}
	defer zephyros.Close()

	// Test TryProcessBatch with empty buffer
	count := zephyros.TryProcessBatch()
	if count != 0 {
		t.Errorf("Expected 0 items processed from empty buffer, got %d", count)
	}

	// Write some items
	itemsWritten := 0
	for i := 0; i < 10; i++ {
		success := zephyros.Write(func(slot *int) {
			*slot = i
		})
		if success {
			itemsWritten++
		}
	}

	if itemsWritten == 0 {
		t.Fatal("Should have written at least some items")
	}

	// Test TryProcessBatch with items
	count = zephyros.TryProcessBatch()
	if count == 0 {
		t.Error("Expected some items to be processed")
	}

	t.Logf("TryProcessBatch unit test: processed %d items", count)
}

// TestZephyros_LoopProcess tests the LoopProcess method
func TestZephyros_LoopProcess(t *testing.T) {
	processed := int64(0)
	processor := func(item *int) {
		atomic.AddInt64(&processed, 1)
	}

	zephyros, err := NewBuilder[int](64).
		WithProcessor(processor).
		WithBatchSize(16).
		Build()

	if err != nil {
		t.Fatalf("Failed to build: %v", err)
	}

	// Start LoopProcess in background goroutine
	go zephyros.LoopProcess()

	// Write some items
	itemsWritten := 0
	for i := 0; i < 20; i++ {
		success := zephyros.Write(func(slot *int) {
			*slot = i
		})
		if success {
			itemsWritten++
		}
	}

	if itemsWritten == 0 {
		t.Fatal("Should have written at least some items")
	}

	// Wait for background processing
	time.Sleep(time.Millisecond * 100)

	// Stop LoopProcess by closing
	zephyros.Close()

	// Check that some processing occurred
	processedCount := atomic.LoadInt64(&processed)
	if processedCount == 0 {
		t.Error("LoopProcess should have processed some items")
	}

	t.Logf("LoopProcess unit test: processed %d items", processedCount)
}

// TestZephyros_Write_BufferFull tests Write behavior when buffer is full
func TestZephyros_Write_BufferFull(t *testing.T) {
	// Use very small buffer for easy testing
	processed := int64(0)
	processor := func(item *int) {
		atomic.AddInt64(&processed, 1)
		// Slow processing to fill buffer
		time.Sleep(time.Millisecond)
	}

	zephyros, err := NewBuilder[int](4). // Very small buffer
						WithProcessor(processor).
						WithBatchSize(1).
						Build()

	if err != nil {
		t.Fatalf("Failed to build: %v", err)
	}
	defer zephyros.Close()

	// Fill buffer beyond capacity
	successCount := 0
	failCount := 0

	for i := 0; i < 10; i++ {
		success := zephyros.Write(func(slot *int) {
			*slot = i
		})
		if success {
			successCount++
		} else {
			failCount++
		}
	}

	// Should have some successes and some failures due to buffer full
	if successCount == 0 {
		t.Error("Should have had at least some successful writes")
	}

	if failCount == 0 {
		t.Error("Should have had some failed writes due to buffer full")
	}

	t.Logf("Buffer full test: %d successful, %d failed writes", successCount, failCount)
}

// TestZephyros_Write_ClosedBuffer tests Write behavior on closed buffer
func TestZephyros_Write_ClosedBuffer(t *testing.T) {
	processor := func(item *int) {}

	zephyros, err := NewBuilder[int](64).
		WithProcessor(processor).
		WithBatchSize(16).
		Build()

	if err != nil {
		t.Fatalf("Failed to build: %v", err)
	}

	// Close the buffer first
	zephyros.Close()

	// Attempt write to closed buffer - should fail
	success := zephyros.Write(func(slot *int) {
		*slot = 42
	})

	if success {
		t.Error("Write to closed buffer should fail")
	}

	t.Logf("Closed buffer write test passed")
}

// TestZephyros_ProcessBatch_Coverage tests ProcessBatch method for coverage
func TestZephyros_ProcessBatch_Coverage(t *testing.T) {
	processed := int64(0)
	processor := func(item *int) {
		atomic.AddInt64(&processed, 1)
	}

	zephyros, err := NewBuilder[int](64).
		WithProcessor(processor).
		WithBatchSize(8).
		Build()

	if err != nil {
		t.Fatalf("Failed to build: %v", err)
	}
	defer zephyros.Close()

	// Write items
	for i := 0; i < 15; i++ {
		zephyros.Write(func(slot *int) {
			*slot = i
		})
	}

	// Process batch
	count := zephyros.ProcessBatch()
	if count == 0 {
		t.Error("ProcessBatch should have processed items")
	}

	t.Logf("ProcessBatch coverage test: processed %d items", count)
}

// TestZephyros_Stats_Coverage tests Stats method for coverage
func TestZephyros_Stats_Coverage(t *testing.T) {
	processor := func(item *int) {}

	zephyros, err := NewBuilder[int](128).
		WithProcessor(processor).
		WithBatchSize(32).
		Build()

	if err != nil {
		t.Fatalf("Failed to build: %v", err)
	}
	defer zephyros.Close()

	// Write some items
	for i := 0; i < 10; i++ {
		zephyros.Write(func(slot *int) {
			*slot = i
		})
	}

	// Get stats
	stats := zephyros.Stats()
	if stats == nil {
		t.Error("Stats should not be nil")
	}

	// Check expected fields
	if _, exists := stats["buffer_size"]; !exists {
		t.Error("Stats should contain 'buffer_size' field")
	}

	if _, exists := stats["writer_position"]; !exists {
		t.Error("Stats should contain 'writer_position' field")
	}

	if _, exists := stats["reader_position"]; !exists {
		t.Error("Stats should contain 'reader_position' field")
	}

	if _, exists := stats["items_buffered"]; !exists {
		t.Error("Stats should contain 'items_buffered' field")
	}

	if _, exists := stats["closed"]; !exists {
		t.Error("Stats should contain 'closed' field")
	}

	t.Logf("Stats coverage test passed: %v", stats)
}

// TestZephyros_LoopProcess_EdgeCases tests LoopProcess edge cases for better coverage
func TestZephyros_LoopProcess_EdgeCases(t *testing.T) {
	processed := int64(0)
	processor := func(item *int) {
		atomic.AddInt64(&processed, 1)
		time.Sleep(time.Microsecond) // Small delay
	}

	zephyros, err := NewBuilder[int](128).
		WithProcessor(processor).
		WithBatchSize(32).
		Build()

	if err != nil {
		t.Fatalf("Failed to build: %v", err)
	}

	// Test LoopProcess with concurrent writes
	go zephyros.LoopProcess()

	// Write items concurrently to test different LoopProcess paths
	done := make(chan bool, 3)

	// Writer 1
	go func() {
		for i := 0; i < 30; i++ {
			zephyros.Write(func(slot *int) { *slot = i })
			time.Sleep(time.Microsecond * 10)
		}
		done <- true
	}()

	// Writer 2
	go func() {
		for i := 100; i < 130; i++ {
			zephyros.Write(func(slot *int) { *slot = i })
			time.Sleep(time.Microsecond * 15)
		}
		done <- true
	}()

	// Writer 3
	go func() {
		for i := 200; i < 220; i++ {
			zephyros.Write(func(slot *int) { *slot = i })
			time.Sleep(time.Microsecond * 5)
		}
		done <- true
	}()

	// Wait for all writers
	for i := 0; i < 3; i++ {
		<-done
	}

	// Allow processing to complete
	time.Sleep(time.Millisecond * 100)
	zephyros.Close()

	processedCount := atomic.LoadInt64(&processed)
	if processedCount == 0 {
		t.Error("LoopProcess should have processed items")
	}

	t.Logf("LoopProcess edge cases: processed %d items", processedCount)
}

// TestZephyros_Write_ConcurrentStress tests concurrent write throughput using the
// correct Anemoi pattern: one producer goroutine per ring via ThreadedZephyros.
//
// WHY rewritten: the previous version put N goroutines on a single Zephyros ring,
// violating the Anemoi invariant (one producer per ring). That caused a data race
// on the buffer slot when two goroutines claimed sequences that mapped to the same
// physical slot after a ring wrap-around. The race was latent in the original code
// and exposed by the new CPU-friendly backoff (which makes the consumer sleep
// longer when idle, giving producers more time to lap a small ring).
//
// The correct API for N concurrent producers is ThreadedZephyros: each producer
// gets a dedicated ring with zero contention.
func TestZephyros_Write_ConcurrentStress(t *testing.T) {
	var processed int64
	processor := func(item *int) {
		atomic.AddInt64(&processed, 1)
	}

	// Use ThreadedZephyros: one ring per producer -- the Anemoi invariant.
	numWriters := 10
	itemsPerWriter := 50
	totalItems := int64(numWriters * itemsPerWriter)

	tz, err := NewThreadedBuilder[int](256, numWriters).
		WithProcessor(processor).
		WithBatchSize(64).
		Build()
	if err != nil {
		t.Fatalf("Failed to build ThreadedZephyros: %v", err)
	}
	defer tz.Close()

	<-tz.LoopProcess()

	// Create one SafeWriter per ring before goroutines start.
	stressWriters := make([]*SafeWriter[int], numWriters)
	for id := 0; id < numWriters; id++ {
		stressWriters[id] = tz.NewSafeWriter(id)
	}

	done := make(chan int, numWriters)
	for producerID := 0; producerID < numWriters; producerID++ {
		go func(w *SafeWriter[int]) {
			count := 0
			for i := 0; i < itemsPerWriter; i++ {
				ok := w.Write(func(slot *int) {
					ringID := w.GetRingID()
					*slot = ringID*1000 + i
				})
				if ok {
					count++
				}
			}
			done <- count
		}(stressWriters[producerID])
	}

	total := 0
	for i := 0; i < numWriters; i++ {
		total += <-done
	}
	t.Logf("Total successful writes: %d / %d", total, totalItems)

	// Drain: wait until the consumer has processed everything that was written.
	deadline := time.Now().Add(5 * time.Second)
	for atomic.LoadInt64(&processed) < int64(total) {
		if time.Now().After(deadline) {
			t.Fatalf("Timeout: processed %d but wrote %d", atomic.LoadInt64(&processed), total)
		}
		time.Sleep(time.Millisecond)
	}

	if atomic.LoadInt64(&processed) != int64(total) {
		t.Errorf("Expected %d processed items, got %d", total, atomic.LoadInt64(&processed))
	}
	t.Logf("Write concurrent stress: %d items written and processed", total)
}

// TestZephyros_ProcessBatch_EdgeCases tests ProcessBatch edge cases
func TestZephyros_ProcessBatch_EdgeCases(t *testing.T) {
	processed := int64(0)
	processor := func(item *int) {
		atomic.AddInt64(&processed, 1)
	}

	zephyros, err := NewBuilder[int](64).
		WithProcessor(processor).
		WithBatchSize(8).
		Build()

	if err != nil {
		t.Fatalf("Failed to build: %v", err)
	}
	defer zephyros.Close()

	// Test ProcessBatch with different buffer states

	// 1. Empty buffer
	count1 := zephyros.ProcessBatch()
	if count1 != 0 {
		t.Errorf("Empty buffer should process 0 items, got %d", count1)
	}

	// 2. Single item
	zephyros.Write(func(slot *int) { *slot = 1 })
	count2 := zephyros.ProcessBatch()
	if count2 == 0 {
		t.Error("Should process at least 1 item")
	}

	// 3. Multiple items
	for i := 0; i < 15; i++ {
		zephyros.Write(func(slot *int) { *slot = i + 10 })
	}
	count3 := zephyros.ProcessBatch()
	if count3 == 0 {
		t.Error("Should process multiple items")
	}

	// 4. Full batch
	for i := 0; i < 20; i++ {
		zephyros.Write(func(slot *int) { *slot = i + 100 })
	}
	count4 := zephyros.ProcessBatch()
	if count4 == 0 {
		t.Error("Should process full batch")
	}

	t.Logf("ProcessBatch edge cases: processed batches of %d, %d, %d, %d items", count1, count2, count3, count4)
}

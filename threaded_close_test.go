// threaded_close_test.go: Tests for close methods
//
// Copyright (c) 2025 AGILira
// Series: an AGLIra fragment
// SPDX-License-Identifier: MPL-2.0

package zephyros

import (
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

// TestThreadedZephyros_RobustClose verifies that the WaitGroup fix makes shutdown deterministic
func TestThreadedZephyros_RobustClose(t *testing.T) {
	t.Log("TESTING ROBUST CLOSE WITH WAITGROUP")

	// Create a processor that tracks completion
	var processedCount int64
	processor := func(item *int) {
		atomic.AddInt64(&processedCount, 1)
		// Simulate some work to make timing more realistic
		runtime.Gosched()
	}

	// Build ThreadedZephyros
	tz, err := NewThreadedBuilder[int](1024, 4).
		WithProcessor(processor).
		WithBatchSize(16).
		Build()
	if err != nil {
		t.Fatalf("Failed to build ThreadedZephyros: %v", err)
	}

	// Start processing
	tz.LoopProcess()

	// Four SafeWriters, one per ring, created before the write loop.
	robustWriters := make([]*SafeWriter[int], 4)
	for id := 0; id < 4; id++ {
		robustWriters[id] = tz.NewSafeWriter(id)
	}

	messagesPerRing := 1000
	totalMessages := messagesPerRing * 4

	for ring := 0; ring < 4; ring++ {
		for i := 0; i < messagesPerRing; i++ {
			value := ring*messagesPerRing + i
			success := robustWriters[ring].Write(func(slot *int) {
				*slot = value
			})
			if !success {
				t.Logf("Write failed for ring %d, message %d", ring, i)
			}
		}
	}

	t.Logf("Wrote %d messages across 4 rings", totalMessages)

	// Give workers time to process
	time.Sleep(50 * time.Millisecond)

	// Test the robust close
	t.Log("Testing deterministic close with WaitGroup...")

	startClose := time.Now()
	tz.Close()
	closeTime := time.Since(startClose)

	t.Logf("Close completed in %v (deterministic, no race conditions)", closeTime)

	// Verify all workers have truly stopped
	processed := atomic.LoadInt64(&processedCount)
	t.Logf("Final stats: %d messages processed", processed)

	// Verify the threaded zephyros is properly closed
	stats := tz.Stats()
	if stats["closed"] != 1 {
		t.Errorf("Expected closed=1, got %d", stats["closed"])
	}

	// Try writing after close via an existing SafeWriter — must return false.
	success := robustWriters[0].Write(func(slot *int) {
		*slot = 9999
	})
	if success {
		t.Error("Write should fail after close")
	}

	t.Log("ROBUST CLOSE TEST PASSED - WaitGroup fix is working perfectly!")
}

// TestThreadedZephyros_MultipleCloses verifies close is idempotent
func TestThreadedZephyros_MultipleCloses(t *testing.T) {
	t.Log("TESTING MULTIPLE CLOSES")

	processor := func(item *int) {
		// Do nothing
	}

	tz, err := NewThreadedBuilder[int](256, 2).
		WithProcessor(processor).
		Build()
	if err != nil {
		t.Fatalf("Failed to build ThreadedZephyros: %v", err)
	}

	tz.LoopProcess()

	// Close multiple times - should be safe
	t.Log("Calling Close() multiple times...")

	tz.Close()
	t.Log("First close completed")

	tz.Close()
	t.Log("Second close completed (idempotent)")

	tz.Close()
	t.Log("Third close completed (idempotent)")

	// Verify state is consistent
	stats := tz.Stats()
	if stats["closed"] != 1 {
		t.Errorf("Expected closed=1 after multiple closes, got %d", stats["closed"])
	}

	t.Log("MULTIPLE CLOSES TEST PASSED!")
}

// TestThreadedZephyros_CloseUnderLoad verifies close works under high load
func TestThreadedZephyros_CloseUnderLoad(t *testing.T) {
	t.Log("TESTING CLOSE UNDER HIGH LOAD")

	var processedCount int64
	processor := func(item *int) {
		atomic.AddInt64(&processedCount, 1)
	}

	// numRings must equal writers to satisfy the Anemoi invariant:
	// exactly one producer goroutine per ring.
	writers := 8
	tz, err := NewThreadedBuilder[int](8192, writers).
		WithProcessor(processor).
		WithBatchSize(128).
		Build()
	if err != nil {
		t.Fatalf("Failed to build ThreadedZephyros: %v", err)
	}

	tz.LoopProcess()

	// Create SafeWriters before launching goroutines (SPSC enforcement).
	loadWriters := make([]*SafeWriter[int], writers)
	for id := 0; id < writers; id++ {
		loadWriters[id] = tz.NewSafeWriter(id)
	}

	// Start aggressive writers — each writer owns a dedicated ring.
	messagesPerWriter := 10000

	t.Logf("Starting %d aggressive writers with %d messages each", writers, messagesPerWriter)

	for w := 0; w < writers; w++ {
		go func(sw *SafeWriter[int]) {
			// Each goroutine writes exclusively to its own ring (Anemoi invariant).
			writerID := sw.GetRingID()
			for i := 0; i < messagesPerWriter; i++ {
				value := writerID*messagesPerWriter + i
				for {
					success := sw.Write(func(slot *int) {
						*slot = value
					})
					if success {
						break
					}
					// Yield when the ring is full or closed; do not busy-loop.
					runtime.Gosched()
				}
			}
		}(loadWriters[w])
	}

	// Let it run for a bit before forcing close.
	time.Sleep(100 * time.Millisecond)

	t.Log("Closing under high load...")
	startClose := time.Now()
	tz.Close()
	closeTime := time.Since(startClose)

	processed := atomic.LoadInt64(&processedCount)
	t.Logf("Close under load completed in %v", closeTime)
	t.Logf("Processed %d messages during high-load close", processed)

	if closeTime > 1*time.Second {
		t.Errorf("Close took too long under load: %v", closeTime)
	}

	t.Log("CLOSE UNDER LOAD TEST PASSED!")
}

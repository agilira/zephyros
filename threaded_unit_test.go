// threaded_unit_test.go: Unit tests for multi-threaded MPSC lock-free ring buffer
//
// Copyright (c) 2025 AGILira
// Series: an AGLIra fragment
// SPDX-License-Identifier: MPL-2.0

package zephyros

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// TestThreadedBuilder_WithWorkers tests the WithWorkers method in isolation
func TestThreadedBuilder_WithWorkers(t *testing.T) {
	processor := func(item *int) {}

	// Test WithWorkers with positive value
	builder := NewThreadedBuilder[int](64, 2).
		WithProcessor(processor).
		WithBatchSize(16).
		WithWorkers(3)

	if builder.numWorkers != 3 {
		t.Errorf("Expected numWorkers=3, got %d", builder.numWorkers)
	}

	// Test WithWorkers with zero (should default to NumCPU)
	builder2 := NewThreadedBuilder[int](64, 2).
		WithProcessor(processor).
		WithBatchSize(16).
		WithWorkers(0)

	if builder2.numWorkers <= 0 {
		t.Error("WithWorkers(0) should set to runtime.NumCPU()")
	}

	// Test WithWorkers with negative (should default to NumCPU)
	builder3 := NewThreadedBuilder[int](64, 2).
		WithProcessor(processor).
		WithBatchSize(16).
		WithWorkers(-5)

	if builder3.numWorkers <= 0 {
		t.Error("WithWorkers(-5) should set to runtime.NumCPU()")
	}

	t.Logf("WithWorkers unit test passed")
}

// TestThreadedZephyros_Write tests writing via a SafeWriter
func TestThreadedZephyros_Write(t *testing.T) {
	processed := int64(0)
	processor := func(item *int) {
		atomic.AddInt64(&processed, 1)
	}

	threaded, err := NewThreadedBuilder[int](64, 2).
		WithProcessor(processor).
		WithBatchSize(16).
		Build()

	if err != nil {
		t.Fatalf("Failed to build: %v", err)
	}
	defer threaded.Close()

	// Each ring gets exactly one SafeWriter (SPSC enforcement at creation time).
	w0 := threaded.NewSafeWriter(0)
	success := w0.Write(func(slot *int) {
		*slot = 42
	})
	if !success {
		t.Error("Write via SafeWriter(0) should succeed")
	}

	w1 := threaded.NewSafeWriter(1)
	success = w1.Write(func(slot *int) {
		*slot = 43
	})
	if !success {
		t.Error("Write via SafeWriter(1) should succeed")
	}

	t.Logf("ThreadedZephyros Write unit test passed")
}

// TestThreadedZephyros_Write_InvalidThreadID verifies that an invalid ring ID is
// rejected at SafeWriter creation time, not silently at Write time.
// WHY: moving validation to creation makes the error impossible to miss and
// eliminates the "write succeeded but went nowhere" failure mode.
func TestThreadedZephyros_Write_InvalidThreadID(t *testing.T) {
	processor := func(item *int) {}

	threaded, err := NewThreadedBuilder[int](64, 2).
		WithProcessor(processor).
		WithBatchSize(16).
		Build()

	if err != nil {
		t.Fatalf("Failed to build: %v", err)
	}
	defer threaded.Close()

	// Invalid ring ID must be rejected at writer creation, not at write time.
	writer, werr := threaded.NewSafeWriterWithError(99)
	if werr == nil {
		t.Error("NewSafeWriterWithError with invalid ID must return error")
	}
	if !errors.Is(werr, ErrInvalidRingID) {
		t.Errorf("Expected ErrInvalidRingID, got %v", werr)
	}
	if writer != nil {
		t.Error("NewSafeWriterWithError with invalid ID must return nil writer")
	}

	t.Logf("Invalid ring ID correctly rejected at writer creation time")
}

// TestThreadedZephyros_GetWriterRing verifies NewSafeWriter for all valid ring IDs.
func TestThreadedZephyros_GetWriterRing(t *testing.T) {
	processor := func(item *int) {}

	threaded, err := NewThreadedBuilder[int](64, 3).
		WithProcessor(processor).
		WithBatchSize(16).
		Build()

	if err != nil {
		t.Fatalf("Failed to build: %v", err)
	}
	defer threaded.Close()

	// Each valid ring ID must yield a non-nil SafeWriter with the correct ring index.
	for i := 0; i < 3; i++ {
		writer := threaded.NewSafeWriter(i)
		if writer == nil {
			t.Errorf("NewSafeWriter(%d) should not return nil", i)
		}
		if writer.GetRingID() != i {
			t.Errorf("NewSafeWriter(%d) returned writer for ring %d", i, writer.GetRingID())
		}
	}

	t.Logf("NewSafeWriter valid-ID coverage test passed")
}

// TestThreadedZephyros_GetWriterRing_InvalidID verifies that assigning a ring
// a second time panics immediately. Double-assignment violates the SPSC
// invariant and must be a hard, visible error.
func TestThreadedZephyros_GetWriterRing_InvalidID(t *testing.T) {
	processor := func(item *int) {}

	threaded, err := NewThreadedBuilder[int](64, 2).
		WithProcessor(processor).
		WithBatchSize(16).
		Build()

	if err != nil {
		t.Fatalf("Failed to build: %v", err)
	}
	defer threaded.Close()

	defer func() {
		if r := recover(); r == nil {
			t.Error("Double-assignment of a ring must panic; no panic received")
		} else {
			t.Logf("Double-assignment correctly panicked: %v", r)
		}
	}()

	// First assignment succeeds.
	_ = threaded.NewSafeWriter(0)
	// Second assignment to the same ring must panic.
	threaded.NewSafeWriter(0)
}

// TestSafeWriter_Creation tests NewSafeWriter method
func TestSafeWriter_Creation(t *testing.T) {
	processor := func(item *int) {}

	threaded, err := NewThreadedBuilder[int](64, 2).
		WithProcessor(processor).
		WithBatchSize(16).
		Build()

	if err != nil {
		t.Fatalf("Failed to build: %v", err)
	}
	defer threaded.Close()

	// Test creating SafeWriter for valid ring IDs
	writer0 := threaded.NewSafeWriter(0)
	if writer0 == nil {
		t.Error("NewSafeWriter(0) should not return nil")
	}

	writer1 := threaded.NewSafeWriter(1)
	if writer1 == nil {
		t.Error("NewSafeWriter(1) should not return nil")
	}

	t.Logf("SafeWriter creation test passed")
}

// TestSafeWriter_InvalidRingID tests panic behavior for invalid ring IDs
func TestSafeWriter_InvalidRingID(t *testing.T) {
	processor := func(item *int) {}

	threaded, err := NewThreadedBuilder[int](64, 2).
		WithProcessor(processor).
		WithBatchSize(16).
		Build()

	if err != nil {
		t.Fatalf("Failed to build: %v", err)
	}
	defer threaded.Close()

	// Test panic recovery for invalid ring ID
	defer func() {
		if r := recover(); r != nil {
			expectedMsg := "CRITICAL BUG: invalid ringID 99 (valid range: 0-1)"
			if r.(string) == expectedMsg {
				t.Logf("Expected panic for invalid ring ID: %s", r)
			} else {
				t.Errorf("Unexpected panic message: %v", r)
			}
		} else {
			t.Error("Expected panic for invalid ring ID")
		}
	}()

	// This should panic
	threaded.NewSafeWriter(99)

	t.Logf("SafeWriter invalid ring ID panic test passed")
}

// TestSafeWriter_GetRingID tests GetRingID method
func TestSafeWriter_GetRingID(t *testing.T) {
	processor := func(item *int) {}

	threaded, err := NewThreadedBuilder[int](64, 3).
		WithProcessor(processor).
		WithBatchSize(16).
		Build()

	if err != nil {
		t.Fatalf("Failed to build: %v", err)
	}
	defer threaded.Close()

	// Test GetRingID for different writers
	writer0 := threaded.NewSafeWriter(0)
	if writer0.GetRingID() != 0 {
		t.Errorf("Expected ring ID 0, got %d", writer0.GetRingID())
	}

	writer2 := threaded.NewSafeWriter(2)
	if writer2.GetRingID() != 2 {
		t.Errorf("Expected ring ID 2, got %d", writer2.GetRingID())
	}

	t.Logf("SafeWriter GetRingID test passed")
}

// TestSafeWriter_Write tests SafeWriter Write method
func TestSafeWriter_Write(t *testing.T) {
	processed := int64(0)
	processor := func(item *int) {
		atomic.AddInt64(&processed, 1)
	}

	threaded, err := NewThreadedBuilder[int](64, 2).
		WithProcessor(processor).
		WithBatchSize(16).
		Build()

	if err != nil {
		t.Fatalf("Failed to build: %v", err)
	}
	defer threaded.Close()

	writer := threaded.NewSafeWriter(0)

	// Test writing through SafeWriter
	success := writer.Write(func(slot *int) {
		*slot = 42
	})

	if !success {
		t.Error("SafeWriter Write should succeed")
	}

	t.Logf("SafeWriter Write test passed")
}

// TestThreadedZephyros_Stats tests Stats method
func TestThreadedZephyros_Stats(t *testing.T) {
	processor := func(item *int) {}

	threaded, err := NewThreadedBuilder[int](64, 2).
		WithProcessor(processor).
		WithBatchSize(16).
		WithWorkers(4).
		Build()

	if err != nil {
		t.Fatalf("Failed to build: %v", err)
	}
	defer threaded.Close()

	stats := threaded.Stats()

	// Check expected stats
	if stats["num_rings"] != 2 {
		t.Errorf("Expected num_rings=2, got %d", stats["num_rings"])
	}

	if stats["num_workers"] != 4 {
		t.Errorf("Expected num_workers=4, got %d", stats["num_workers"])
	}

	if stats["closed"] != 0 {
		t.Errorf("Expected closed=0, got %d", stats["closed"])
	}

	t.Logf("ThreadedZephyros Stats test passed")
}

// TestThreadedZephyros_LoopProcessAndClose tests LoopProcess and Close methods
func TestThreadedZephyros_LoopProcessAndClose(t *testing.T) {
	processed := int64(0)
	processor := func(item *int) {
		atomic.AddInt64(&processed, 1)
	}

	threaded, err := NewThreadedBuilder[int](64, 2).
		WithProcessor(processor).
		WithBatchSize(16).
		Build()

	if err != nil {
		t.Fatalf("Failed to build: %v", err)
	}

	// Start processing (should be non-blocking now)
	threaded.LoopProcess()

	// Write some data
	writer := threaded.NewSafeWriter(0)
	success := writer.Write(func(slot *int) {
		*slot = 42
	})

	if !success {
		t.Error("Write should succeed")
	}

	// Give a bit of time for processing
	time.Sleep(10 * time.Millisecond)

	// Close should work without hanging
	threaded.Close()

	// Verify stats after close
	stats := threaded.Stats()
	if stats["closed"] != 1 {
		t.Errorf("Expected closed=1, got %d", stats["closed"])
	}

	t.Logf("ThreadedZephyros LoopProcess and Close test passed")
}

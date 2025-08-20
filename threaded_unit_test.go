// threaded_unit_test.go: Unit tests for multi-threaded MPSC lock-free ring buffer
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

	t.Logf("✅ WithWorkers unit test passed")
}

// TestThreadedZephyros_Write tests the Write method with thread ID validation
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

	// Test valid thread ID 0
	success := threaded.Write(0, func(slot *int) {
		*slot = 42
	})

	if !success {
		t.Error("Write with valid thread ID 0 should succeed")
	}

	// Test valid thread ID 1
	success = threaded.Write(1, func(slot *int) {
		*slot = 43
	})

	if !success {
		t.Error("Write with valid thread ID 1 should succeed")
	}

	t.Logf("✅ ThreadedZephyros Write unit test passed")
}

// TestThreadedZephyros_Write_InvalidThreadID tests panic behavior for invalid thread IDs
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

	// Test invalid ring ID - should return false, not panic
	success := threaded.Write(99, func(slot *int) {
		*slot = 99
	})
	if success {
		t.Error("Write with invalid thread ID should return false")
	}

	t.Logf("✅ Write with invalid thread ID correctly returned false")
}

// TestThreadedZephyros_GetWriterRing tests GetWriterRing method coverage
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

	// Test getting each ring
	for i := 0; i < 3; i++ {
		ring := threaded.GetWriterRing(i)
		if ring == nil {
			t.Errorf("GetWriterRing(%d) should not return nil", i)
		}
	}

	t.Logf("✅ GetWriterRing unit test passed")
}

// TestThreadedZephyros_GetWriterRing_InvalidID tests panic behavior for invalid ring IDs
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

	// Test that invalid ring ID panics (this is by design)
	defer func() {
		if r := recover(); r != nil {
			t.Logf("✅ Expected panic for invalid ring ID: %v", r)
		} else {
			t.Error("Expected panic for invalid ring ID, but didn't panic")
		}
	}()

	// This should panic
	threaded.GetWriterRing(99)
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

	t.Logf("✅ SafeWriter creation test passed")
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
				t.Logf("✅ Expected panic for invalid ring ID: %s", r)
			} else {
				t.Errorf("Unexpected panic message: %v", r)
			}
		} else {
			t.Error("Expected panic for invalid ring ID")
		}
	}()

	// This should panic
	threaded.NewSafeWriter(99)

	t.Logf("✅ SafeWriter invalid ring ID panic test passed")
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

	t.Logf("✅ SafeWriter GetRingID test passed")
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

	t.Logf("✅ SafeWriter Write test passed")
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

	t.Logf("✅ ThreadedZephyros Stats test passed")
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
	success := threaded.Write(0, func(slot *int) {
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

	t.Logf("✅ ThreadedZephyros LoopProcess and Close test passed")
}

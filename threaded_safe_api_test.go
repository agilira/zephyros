// threaded_safe_api_test.go: Tests for safe API methods
//
// Copyright (c) 2025 AGILira
// Series: an AGLIra fragment
// SPDX-License-Identifier: MPL-2.0

package zephyros

import (
	"errors"
	"testing"
)

// TestSafeAPI verifies the new safe API methods work correctly
func TestSafeAPI_SafeGetWriterRing(t *testing.T) {
	t.Log("🔒 TESTING SAFE API - SafeGetWriterRing")

	processor := func(item *int) {
		// Do nothing
	}

	tz, err := NewThreadedBuilder[int](256, 4).
		WithProcessor(processor).
		Build()
	if err != nil {
		t.Fatalf("Failed to build ThreadedZephyros: %v", err)
	}
	defer tz.Close()

	// Test valid thread IDs
	for i := 0; i < 4; i++ {
		ring, err := tz.SafeGetWriterRing(i)
		if err != nil {
			t.Errorf("SafeGetWriterRing(%d) returned error: %v", i, err)
		}
		if ring == nil {
			t.Errorf("SafeGetWriterRing(%d) returned nil ring", i)
		}
	}

	// Test invalid thread IDs
	invalidIDs := []int{-1, 4, 5, 100}
	for _, id := range invalidIDs {
		ring, err := tz.SafeGetWriterRing(id)
		if err == nil {
			t.Errorf("SafeGetWriterRing(%d) should return error", id)
		}
		if !errors.Is(err, ErrInvalidThreadID) {
			t.Errorf("SafeGetWriterRing(%d) should return ErrInvalidThreadID, got %v", id, err)
		}
		if ring != nil {
			t.Errorf("SafeGetWriterRing(%d) should return nil ring on error", id)
		}
	}

	t.Log("✅ SafeGetWriterRing test passed")
}

func TestSafeAPI_NewSafeWriterWithError(t *testing.T) {
	t.Log("🔒 TESTING SAFE API - NewSafeWriterWithError")

	processor := func(item *int) {
		// Do nothing
	}

	tz, err := NewThreadedBuilder[int](256, 4).
		WithProcessor(processor).
		Build()
	if err != nil {
		t.Fatalf("Failed to build ThreadedZephyros: %v", err)
	}
	defer tz.Close()

	// Test valid ring IDs
	for i := 0; i < 4; i++ {
		writer, err := tz.NewSafeWriterWithError(i)
		if err != nil {
			t.Errorf("NewSafeWriterWithError(%d) returned error: %v", i, err)
		}
		if writer == nil {
			t.Errorf("NewSafeWriterWithError(%d) returned nil writer", i)
		}
		if writer != nil && writer.GetRingID() != i {
			t.Errorf("NewSafeWriterWithError(%d) returned wrong ring ID: %d", i, writer.GetRingID())
		}
	}

	// Test invalid ring IDs
	invalidIDs := []int{-1, 4, 5, 100}
	for _, id := range invalidIDs {
		writer, err := tz.NewSafeWriterWithError(id)
		if err == nil {
			t.Errorf("NewSafeWriterWithError(%d) should return error", id)
		}
		if !errors.Is(err, ErrInvalidRingID) {
			t.Errorf("NewSafeWriterWithError(%d) should return ErrInvalidRingID, got %v", id, err)
		}
		if writer != nil {
			t.Errorf("NewSafeWriterWithError(%d) should return nil writer on error", id)
		}
	}

	t.Log("✅ NewSafeWriterWithError test passed")
}

// TestDualAPI verifies both fast and safe APIs work consistently
func TestDualAPI_Consistency(t *testing.T) {
	t.Log("⚡ TESTING DUAL API CONSISTENCY")

	processor := func(item *int) {
		// Do nothing
	}

	tz, err := NewThreadedBuilder[int](256, 4).
		WithProcessor(processor).
		Build()
	if err != nil {
		t.Fatalf("Failed to build ThreadedZephyros: %v", err)
	}
	defer tz.Close()

	// Test that fast and safe APIs return the same ring for valid IDs
	for i := 0; i < 4; i++ {
		fastRing := tz.GetWriterRing(i)
		safeRing, err := tz.SafeGetWriterRing(i)

		if err != nil {
			t.Errorf("SafeGetWriterRing(%d) returned error: %v", i, err)
		}

		if fastRing != safeRing {
			t.Errorf("Fast and safe APIs returned different rings for ID %d", i)
		}
	}

	// Test that fast and safe writer APIs are consistent
	for i := 0; i < 4; i++ {
		fastWriter := tz.NewSafeWriter(i)
		safeWriter, err := tz.NewSafeWriterWithError(i)

		if err != nil {
			t.Errorf("NewSafeWriterWithError(%d) returned error: %v", i, err)
		}

		if fastWriter.GetRingID() != safeWriter.GetRingID() {
			t.Errorf("Fast and safe writer APIs returned different ring IDs for ID %d", i)
		}
	}

	t.Log("✅ Dual API consistency test passed")
}

// TestFastAPI_StillPanics verifies the fast API still panics as expected
func TestFastAPI_StillPanics(t *testing.T) {
	t.Log("💥 TESTING FAST API STILL PANICS")

	processor := func(item *int) {
		// Do nothing
	}

	tz, err := NewThreadedBuilder[int](256, 4).
		WithProcessor(processor).
		Build()
	if err != nil {
		t.Fatalf("Failed to build ThreadedZephyros: %v", err)
	}
	defer tz.Close()

	// Test GetWriterRing panic
	defer func() {
		if r := recover(); r == nil {
			t.Error("GetWriterRing with invalid ID should panic")
		} else {
			t.Log("✅ GetWriterRing correctly panicked:", r)
		}
	}()

	_ = tz.GetWriterRing(100) // Should panic
}

func TestFastAPI_NewSafeWriterPanics(t *testing.T) {
	t.Log("💥 TESTING NewSafeWriter STILL PANICS")

	processor := func(item *int) {
		// Do nothing
	}

	tz, err := NewThreadedBuilder[int](256, 4).
		WithProcessor(processor).
		Build()
	if err != nil {
		t.Fatalf("Failed to build ThreadedZephyros: %v", err)
	}
	defer tz.Close()

	// Test NewSafeWriter panic
	defer func() {
		if r := recover(); r == nil {
			t.Error("NewSafeWriter with invalid ID should panic")
		} else {
			t.Log("✅ NewSafeWriter correctly panicked:", r)
		}
	}()

	_ = tz.NewSafeWriter(100) // Should panic
}

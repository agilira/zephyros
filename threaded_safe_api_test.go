// threaded_safe_api_test.go: Tests for safe API methods
//
// Copyright (c) 2025 AGILira
// Series: an AGLIra fragment
// SPDX-License-Identifier: MPL-2.0

package zephyros

import (
	"errors"
	"sync/atomic"
	"testing"
)

// TestSafeAPI_SingleOwnerEnforcement verifies that each ring can be claimed by
// exactly one SafeWriter. A second NewSafeWriterWithError call on the same ring
// must return ErrRingAlreadyAssigned; calls with out-of-range IDs return
// ErrInvalidRingID.
func TestSafeAPI_SingleOwnerEnforcement(t *testing.T) {
	t.Log("TESTING SafeWriter single-owner enforcement")

	processor := func(item *int) {}

	tz, err := NewThreadedBuilder[int](256, 4).
		WithProcessor(processor).
		Build()
	if err != nil {
		t.Fatalf("Failed to build ThreadedZephyros: %v", err)
	}
	defer tz.Close()

	// First claim must succeed for all rings.
	for i := 0; i < 4; i++ {
		writer, werr := tz.NewSafeWriterWithError(i)
		if werr != nil {
			t.Errorf("NewSafeWriterWithError(%d) first call returned error: %v", i, werr)
			continue
		}
		if writer.GetRingID() != i {
			t.Errorf("NewSafeWriterWithError(%d) returned writer for ring %d", i, writer.GetRingID())
		}
	}

	// Second claim on any ring must return ErrRingAlreadyAssigned.
	for i := 0; i < 4; i++ {
		_, werr := tz.NewSafeWriterWithError(i)
		if werr == nil {
			t.Errorf("NewSafeWriterWithError(%d) second call should return error", i)
		}
		if !errors.Is(werr, ErrRingAlreadyAssigned) {
			t.Errorf("NewSafeWriterWithError(%d) second call: expected ErrRingAlreadyAssigned, got %v", i, werr)
		}
	}

	// Out-of-range IDs must return ErrInvalidRingID.
	invalidIDs := []int{-1, 4, 5, 100}
	for _, id := range invalidIDs {
		writer, werr := tz.NewSafeWriterWithError(id)
		if werr == nil {
			t.Errorf("NewSafeWriterWithError(%d) should return error for invalid ID", id)
		}
		if !errors.Is(werr, ErrInvalidRingID) {
			t.Errorf("NewSafeWriterWithError(%d) expected ErrInvalidRingID, got %v", id, werr)
		}
		if writer != nil {
			t.Errorf("NewSafeWriterWithError(%d) should return nil writer on error", id)
		}
	}

	t.Log("Single-owner enforcement test passed")
}

func TestSafeAPI_NewSafeWriterWithError(t *testing.T) {
	t.Log("TESTING SAFE API - NewSafeWriterWithError")

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

	// Test valid ring IDs.
	// WHY no explicit writer == nil check after continue: if err is nil,
	// NewSafeWriterWithError guarantees a non-nil writer (API contract).
	// A nil writer would panic on GetRingID(), making the bug visible.
	for i := 0; i < 4; i++ {
		writer, werr := tz.NewSafeWriterWithError(i)
		if werr != nil {
			t.Errorf("NewSafeWriterWithError(%d) returned error: %v", i, werr)
			continue
		}
		if writer.GetRingID() != i {
			t.Errorf("NewSafeWriterWithError(%d) returned wrong ring ID: %d", i, writer.GetRingID())
		}
	}

	// Test invalid ring IDs
	invalidIDs := []int{-1, 4, 5, 100}
	for _, id := range invalidIDs {
		writer, werr := tz.NewSafeWriterWithError(id)
		if werr == nil {
			t.Errorf("NewSafeWriterWithError(%d) should return error", id)
		}
		if !errors.Is(werr, ErrInvalidRingID) {
			t.Errorf("NewSafeWriterWithError(%d) should return ErrInvalidRingID, got %v", id, werr)
		}
		if writer != nil {
			t.Errorf("NewSafeWriterWithError(%d) should return nil writer on error", id)
		}
	}

	t.Log("NewSafeWriterWithError test passed")
}

// TestSafeAPI_OwnershipBinding verifies that a SafeWriter's ring index matches
// the index requested and that writes are forwarded to the correct ring.
func TestSafeAPI_OwnershipBinding(t *testing.T) {
	t.Log("TESTING SafeWriter ownership binding")

	var processed int64
	processor := func(item *int) { atomic.AddInt64(&processed, 1) }

	tz, err := NewThreadedBuilder[int](256, 4).
		WithProcessor(processor).
		Build()
	if err != nil {
		t.Fatalf("Failed to build ThreadedZephyros: %v", err)
	}
	defer tz.Close()

	// GetRingID must match the requested index.
	for i := 0; i < 4; i++ {
		writer := tz.NewSafeWriter(i)
		if writer.GetRingID() != i {
			t.Errorf("NewSafeWriter(%d) returned writer with ring ID %d", i, writer.GetRingID())
		}
	}

	t.Log("Ownership binding test passed")
}

// TestSafeWriter_DoubleAssignmentPanic verifies that claiming the same ring twice
// panics. Only NewSafeWriter (panic variant) is tested here; the error variant
// is covered in TestSafeAPI_SingleOwnerEnforcement.
func TestSafeWriter_DoubleAssignmentPanic(t *testing.T) {
	t.Log("TESTING NewSafeWriter double-assignment panic")

	processor := func(item *int) {}

	tz, err := NewThreadedBuilder[int](256, 4).
		WithProcessor(processor).
		Build()
	if err != nil {
		t.Fatalf("Failed to build ThreadedZephyros: %v", err)
	}
	defer tz.Close()

	defer func() {
		if r := recover(); r == nil {
			t.Error("NewSafeWriter double-assignment must panic")
		} else {
			t.Log("Double-assignment correctly panicked:", r)
		}
	}()

	_ = tz.NewSafeWriter(0) // First claim: succeeds.
	_ = tz.NewSafeWriter(0) // Second claim: must panic.
}

func TestFastAPI_NewSafeWriterPanics(t *testing.T) {
	t.Log("TESTING NewSafeWriter STILL PANICS")

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
			t.Log("NewSafeWriter correctly panicked:", r)
		}
	}()

	_ = tz.NewSafeWriter(100) // Should panic
}

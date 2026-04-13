// builder_unit_test.go: Unit tests for ZEPHYROS builder
//
// Copyright (c) 2025 AGILira
// Series: an AGLIra fragment
// SPDX-License-Identifier: MPL-2.0

package zephyros

import (
	"testing"
)

// TestBuilder_DefaultValues tests Builder initialization with default values
func TestBuilder_DefaultValues(t *testing.T) {
	builder := NewBuilder[int](64)

	if builder.capacity != 64 {
		t.Errorf("Expected capacity 64, got %d", builder.capacity)
	}

	if builder.batchSize != 16 { // For capacity 64, default batch size is 16
		t.Errorf("Expected default batch size 16, got %d", builder.batchSize)
	}

	if builder.processor != nil {
		t.Error("Expected default processor to be nil")
	}

	t.Logf("Builder default values test passed")
}

// TestBuilder_WithProcessor tests WithProcessor method
func TestBuilder_WithProcessor(t *testing.T) {
	processor := func(item *int) {}
	builder := NewBuilder[int](32).WithProcessor(processor)

	if builder.processor == nil {
		t.Error("Expected processor to be set")
	}

	t.Logf("Builder WithProcessor test passed")
}

// TestBuilder_WithBatchSize tests WithBatchSize method
func TestBuilder_WithBatchSize(t *testing.T) {
	builder := NewBuilder[int](32).WithBatchSize(16)

	if builder.batchSize != 16 {
		t.Errorf("Expected batch size 16, got %d", builder.batchSize)
	}

	t.Logf("Builder WithBatchSize test passed")
}

// TestBuilder_Build_Success tests successful Build
func TestBuilder_Build_Success(t *testing.T) {
	processor := func(item *int) {}

	zephyros, err := NewBuilder[int](64).
		WithProcessor(processor).
		WithBatchSize(16).
		Build()

	if err != nil {
		t.Fatalf("Build should succeed: %v", err)
	}

	if zephyros == nil {
		t.Fatal("Built Zephyros should not be nil")
	}

	defer zephyros.Close()

	t.Logf("Builder Build success test passed")
}

// TestBuilder_Build_NoProcessor tests Build without processor
func TestBuilder_Build_NoProcessor(t *testing.T) {
	_, err := NewBuilder[int](64).
		WithBatchSize(16).
		Build()

	if err == nil {
		t.Error("Build should fail without processor")
	}

	expectedError := "missing processor function"
	if err.Error() != expectedError {
		t.Errorf("Expected error '%s', got '%s'", expectedError, err.Error())
	}

	t.Logf("Builder Build no processor test passed")
}

// TestBuilder_Build_InvalidCapacity tests Build with invalid capacity
func TestBuilder_Build_InvalidCapacity(t *testing.T) {
	processor := func(item *int) {}

	// Test with capacity that's not power of 2
	_, err := NewBuilder[int](63).
		WithProcessor(processor).
		Build()

	if err == nil {
		t.Error("Build should fail with invalid capacity")
	}

	expectedError := "capacity must be a power of two"
	if err.Error() != expectedError {
		t.Errorf("Expected error '%s', got '%s'", expectedError, err.Error())
	}

	t.Logf("Builder Build invalid capacity test passed")
}

// TestBuilder_Build_InvalidBatchSize tests Build with invalid batch size
func TestBuilder_Build_InvalidBatchSize(t *testing.T) {
	processor := func(item *int) {}

	_, err := NewBuilder[int](64).
		WithProcessor(processor).
		WithBatchSize(0).
		Build()

	if err == nil {
		t.Error("Build should fail with invalid batch size")
	}

	expectedError := "batch size must be positive, got 0"
	if err.Error() != expectedError {
		t.Errorf("Expected error '%s', got '%s'", expectedError, err.Error())
	}

	t.Logf("Builder Build invalid batch size test passed")
}

// TestBuilder_MethodChaining tests method chaining
func TestBuilder_MethodChaining(t *testing.T) {
	processor := func(item *int) {}

	zephyros, err := NewBuilder[int](128).
		WithProcessor(processor).
		WithBatchSize(32).
		Build()

	if err != nil {
		t.Fatalf("Method chaining should work: %v", err)
	}

	if zephyros == nil {
		t.Fatal("Built Zephyros should not be nil")
	}

	defer zephyros.Close()

	// Verify the configuration was applied
	stats := zephyros.Stats()
	if stats["buffer_size"] != 128 {
		t.Errorf("Expected buffer size 128, got %d", stats["buffer_size"])
	}

	t.Logf("Builder method chaining test passed")
}

// TestBuilder_ZeroCapacity tests Builder auto-sizes with zero capacity
func TestBuilder_ZeroCapacity(t *testing.T) {
	processor := func(item *int) {}

	z, err := NewBuilder[int](0).
		WithProcessor(processor).
		Build()

	if err != nil {
		t.Fatalf("Zero capacity should auto-size, got error: %v", err)
	}
	defer z.Close()

	if z.capacity != DefaultRingCapacity {
		t.Errorf("Expected auto-sized capacity %d, got %d", DefaultRingCapacity, z.capacity)
	}

	t.Logf("Builder zero capacity auto-sizing test passed")
}

// TestBuilder_LargeConfiguration tests Builder with large configuration
func TestBuilder_LargeConfiguration(t *testing.T) {
	processor := func(item *int) {}

	zephyros, err := NewBuilder[int](1024).
		WithProcessor(processor).
		WithBatchSize(128).
		Build()

	if err != nil {
		t.Fatalf("Large configuration should work: %v", err)
	}

	if zephyros == nil {
		t.Fatal("Built Zephyros should not be nil")
	}

	defer zephyros.Close()

	stats := zephyros.Stats()
	if stats["buffer_size"] != 1024 {
		t.Errorf("Expected buffer size 1024, got %d", stats["buffer_size"])
	}

	t.Logf("Builder large configuration test passed")
}

// TestBuilder_NegativeCapacity tests Builder auto-sizes with negative capacity
func TestBuilder_NegativeCapacity(t *testing.T) {
	processor := func(item *int) {}

	z, err := NewBuilder[int](-1).
		WithProcessor(processor).
		Build()

	if err != nil {
		t.Fatalf("Negative capacity should auto-size, got error: %v", err)
	}
	defer z.Close()

	if z.capacity != DefaultRingCapacity {
		t.Errorf("Expected auto-sized capacity %d, got %d", DefaultRingCapacity, z.capacity)
	}
}

// TestThreadedBuilder_AutoSizing tests ThreadedBuilder auto-sizes both ringSize and numRings
func TestThreadedBuilder_AutoSizing(t *testing.T) {
	processor := func(item *int) {}

	tz, err := NewThreadedBuilder[int](0, 0).
		WithProcessor(processor).
		Build()

	if err != nil {
		t.Fatalf("Auto-sizing should succeed, got error: %v", err)
	}
	defer tz.Close()

	stats := tz.Stats()

	// numRings should be runtime.NumCPU()
	if stats["num_rings"] <= 0 {
		t.Errorf("Expected positive num_rings from auto-sizing, got %d", stats["num_rings"])
	}

	// Each ring should have DefaultRingCapacity
	for i := 0; i < int(stats["num_rings"]); i++ {
		ringStats := tz.rings[i].Stats()
		if ringStats["buffer_size"] != DefaultRingCapacity {
			t.Errorf("Ring %d: expected buffer_size %d, got %d", i, DefaultRingCapacity, ringStats["buffer_size"])
		}
	}
}

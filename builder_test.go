// builder_unit_test.go: Unit tests for ZEPHYROS builder
//
// Copyright (c) 2025 AGILira
// Series: an AGLIra fragment
// SPDX-License-Identifier: MPL-2.0

package zephyros

import (
	"testing"
)

// Test NewBuilder with different capacities
func TestNewBuilder_CapacityHandling(t *testing.T) {
	tests := []struct {
		name              string
		capacity          int64
		expectedBatchSize int64
	}{
		{"Very small buffer", 4, 1},
		{"Small buffer", 64, 16},
		{"Medium buffer", 512, 16},
		{"Large buffer", 1024, 256},
		{"Very large buffer", 8192, 256},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := NewBuilder[int](tt.capacity)

			if builder.capacity != tt.capacity {
				t.Errorf("Expected capacity %d, got %d", tt.capacity, builder.capacity)
			}

			if builder.batchSize != tt.expectedBatchSize {
				t.Errorf("Expected batch size %d, got %d", tt.expectedBatchSize, builder.batchSize)
			}
		})
	}
}

// Test Builder fluent interface
func TestBuilder_FluentInterface(t *testing.T) {
	var processorCalled bool
	processor := func(item *int) {
		processorCalled = true
	}

	// Test fluent chaining
	zephyros, err := NewBuilder[int](8).
		WithProcessor(processor).
		WithBatchSize(4).
		Build()

	if err != nil {
		t.Fatalf("Failed to build zephyros: %v", err)
	}
	defer zephyros.Close()

	// Verify configuration
	if zephyros.capacity != 8 {
		t.Errorf("Expected capacity 8, got %d", zephyros.capacity)
	}

	if zephyros.batchSize != 4 {
		t.Errorf("Expected batch size 4, got %d", zephyros.batchSize)
	}

	// Test processor works
	zephyros.Write(func(slot *int) { *slot = 42 })
	zephyros.ProcessBatch()

	if !processorCalled {
		t.Error("Processor was not called")
	}
}

// Test Build validation errors
func TestBuilder_ValidationErrors(t *testing.T) {
	processor := func(item *int) {}

	tests := []struct {
		name          string
		capacity      int64
		processor     ProcessorFunc[int]
		batchSize     int64
		expectedError string
	}{
		{"Non-power-of-two capacity", 7, processor, 1, "capacity must be a power of two"},
		{"Missing processor", 8, nil, 1, "missing processor function"},
		{"Zero batch size", 8, processor, 0, "batch size must be positive, got 0"},
		{"Negative batch size", 8, processor, -1, "batch size must be positive, got -1"},
		{"Batch size exceeds capacity", 8, processor, 16, "batch size (16) cannot exceed capacity (8)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := NewBuilder[int](tt.capacity)
			if tt.processor != nil {
				builder = builder.WithProcessor(tt.processor)
			}
			builder = builder.WithBatchSize(tt.batchSize)

			zephyros, err := builder.Build()

			if err == nil {
				if zephyros != nil {
					zephyros.Close()
				}
				t.Fatalf("Expected error, but got none")
			}

			if err.Error() != tt.expectedError {
				t.Errorf("Expected error '%s', got '%s'", tt.expectedError, err.Error())
			}
		})
	}
}

// Test valid power-of-two capacities
func TestBuilder_PowerOfTwoValidation(t *testing.T) {
	processor := func(item *int) {}

	validCapacities := []int64{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192}

	for _, capacity := range validCapacities {
		t.Run("capacity "+string(rune('0'+capacity)), func(t *testing.T) {
			zephyros, err := NewBuilder[int](capacity).
				WithProcessor(processor).
				Build()

			if err != nil {
				t.Errorf("Expected capacity %d to be valid, got error: %v", capacity, err)
			}

			if zephyros != nil {
				defer zephyros.Close()

				// Verify mask calculation
				expectedMask := capacity - 1
				if zephyros.mask != expectedMask {
					t.Errorf("Expected mask %d, got %d", expectedMask, zephyros.mask)
				}
			}
		})
	}
}

// Test invalid power-of-two capacities
func TestBuilder_InvalidCapacities(t *testing.T) {
	processor := func(item *int) {}

	invalidCapacities := []int64{3, 5, 6, 7, 9, 10, 11, 12, 13, 14, 15, 17, 100, 255, 1000}

	for _, capacity := range invalidCapacities {
		t.Run("invalid capacity "+string(rune('0'+capacity)), func(t *testing.T) {
			_, err := NewBuilder[int](capacity).
				WithProcessor(processor).
				Build()

			if err == nil {
				t.Errorf("Expected capacity %d to be invalid", capacity)
			}

			if err != ErrCapacity {
				t.Errorf("Expected ErrCapacity, got %v", err)
			}
		})
	}
}

// Test initial state of built Zephyros
func TestBuilder_InitialState(t *testing.T) {
	processor := func(item *int) {}

	zephyros, err := NewBuilder[int](8).
		WithProcessor(processor).
		WithBatchSize(2).
		Build()

	if err != nil {
		t.Fatalf("Failed to build zephyros: %v", err)
	}
	defer zephyros.Close()

	// Test initial cursor values
	if zephyros.writerCursor.Load() != 0 {
		t.Errorf("Expected initial writer cursor 0, got %d", zephyros.writerCursor.Load())
	}

	if zephyros.readerCursor.Load() != 0 {
		t.Errorf("Expected initial reader cursor 0, got %d", zephyros.readerCursor.Load())
	}

	if zephyros.closed.Load() != 0 {
		t.Errorf("Expected initial closed state 0, got %d", zephyros.closed.Load())
	}

	// Test availability buffer initialization
	for i := range zephyros.availableBuffer {
		if zephyros.availableBuffer[i].Load() != -2 {
			t.Errorf("Expected availability[%d] to be -2, got %d", i, zephyros.availableBuffer[i].Load())
		}
	}

	// Test buffer allocation
	if len(zephyros.buffer) != int(zephyros.capacity) {
		t.Errorf("Expected buffer length %d, got %d", zephyros.capacity, len(zephyros.buffer))
	}
}

// Test Builder with different types
func TestBuilder_GenericTypes(t *testing.T) {
	t.Run("String type", func(t *testing.T) {
		var result string
		processor := func(item *string) {
			result = *item
		}

		zephyros, err := NewBuilder[string](4).
			WithProcessor(processor).
			Build()

		if err != nil {
			t.Fatalf("Failed to build string zephyros: %v", err)
		}
		defer zephyros.Close()

		zephyros.Write(func(slot *string) { *slot = "hello" })
		zephyros.ProcessBatch()

		if result != "hello" {
			t.Errorf("Expected 'hello', got '%s'", result)
		}
	})

	t.Run("Struct type", func(t *testing.T) {
		type TestStruct struct {
			ID   int
			Name string
		}

		var result TestStruct
		processor := func(item *TestStruct) {
			result = *item
		}

		zephyros, err := NewBuilder[TestStruct](4).
			WithProcessor(processor).
			Build()

		if err != nil {
			t.Fatalf("Failed to build struct zephyros: %v", err)
		}
		defer zephyros.Close()

		expected := TestStruct{ID: 42, Name: "test"}
		zephyros.Write(func(slot *TestStruct) { *slot = expected })
		zephyros.ProcessBatch()

		if result != expected {
			t.Errorf("Expected %+v, got %+v", expected, result)
		}
	})
}

// Test Builder configuration edge cases
func TestBuilder_EdgeCases(t *testing.T) {
	processor := func(item *int) {}

	t.Run("Batch size equals capacity", func(t *testing.T) {
		zephyros, err := NewBuilder[int](8).
			WithProcessor(processor).
			WithBatchSize(8).
			Build()

		if err != nil {
			t.Fatalf("Expected batch size equal to capacity to be valid, got: %v", err)
		}
		defer zephyros.Close()
	})

	t.Run("Batch size 1", func(t *testing.T) {
		zephyros, err := NewBuilder[int](8).
			WithProcessor(processor).
			WithBatchSize(1).
			Build()

		if err != nil {
			t.Fatalf("Expected batch size 1 to be valid, got: %v", err)
		}
		defer zephyros.Close()
	})
}

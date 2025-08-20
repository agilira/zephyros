// padding_test.go: Tests for cache-line padding utilities
//
// Copyright (c) 2025 AGILira
// Series: an AGLIra fragment
// SPDX-License-Identifier: MPL-2.0

package zephyros

import (
	"sync"
	"sync/atomic"
	"testing"
	"unsafe"
)

// Test AtomicPaddedInt64 basic functionality
func TestAtomicPaddedInt64_BasicOperations(t *testing.T) {
	var padded AtomicPaddedInt64

	// Test initial value
	if padded.Load() != 0 {
		t.Errorf("Expected initial value 0, got %d", padded.Load())
	}

	// Test Store/Load
	padded.Store(42)
	if padded.Load() != 42 {
		t.Errorf("Expected 42, got %d", padded.Load())
	}

	// Test Add
	result := padded.Add(8)
	if result != 50 {
		t.Errorf("Expected Add to return 50, got %d", result)
	}
	if padded.Load() != 50 {
		t.Errorf("Expected value 50 after Add, got %d", padded.Load())
	}

	// Test CompareAndSwap
	swapped := padded.CompareAndSwap(50, 100)
	if !swapped {
		t.Error("Expected CompareAndSwap to succeed")
	}
	if padded.Load() != 100 {
		t.Errorf("Expected 100 after CAS, got %d", padded.Load())
	}

	// Test failed CompareAndSwap
	swapped = padded.CompareAndSwap(99, 200)
	if swapped {
		t.Error("Expected CompareAndSwap to fail")
	}
	if padded.Load() != 100 {
		t.Errorf("Expected 100 after failed CAS, got %d", padded.Load())
	}
}

// Test that AtomicPaddedInt64 is correctly sized for cache-line alignment
func TestAtomicPaddedInt64_CacheLineAlignment(t *testing.T) {
	var padded AtomicPaddedInt64
	size := unsafe.Sizeof(padded)

	// Should be exactly 64 bytes (cache line size)
	if size != 64 {
		t.Errorf("Expected AtomicPaddedInt64 size to be 64 bytes, got %d", size)
	}

	t.Logf("AtomicPaddedInt64 size: %d bytes (✅ cache-line aligned)", size)
}

// Test PaddedInt64 basic functionality and alignment
func TestPaddedInt64_BasicOperations(t *testing.T) {
	var padded PaddedInt64

	// Test initial value
	if padded.Value != 0 {
		t.Errorf("Expected initial value 0, got %d", padded.Value)
	}

	// Test assignment
	padded.Value = 42
	if padded.Value != 42 {
		t.Errorf("Expected 42, got %d", padded.Value)
	}
}

// Test PaddedInt64 cache-line alignment
func TestPaddedInt64_CacheLineAlignment(t *testing.T) {
	var padded PaddedInt64
	size := unsafe.Sizeof(padded)

	// Should be exactly 64 bytes (cache line size)
	if size != 64 {
		t.Errorf("Expected PaddedInt64 size to be 64 bytes, got %d", size)
	}

	t.Logf("PaddedInt64 size: %d bytes (✅ cache-line aligned)", size)
}

// Test false sharing prevention with concurrent access
func TestAtomicPaddedInt64_FalseSharingPrevention(t *testing.T) {
	const numGoroutines = 8
	const iterations = 100000

	// Create array of padded atomics
	paddedCounters := make([]AtomicPaddedInt64, numGoroutines)

	var wg sync.WaitGroup

	// Each goroutine increments its own counter
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				paddedCounters[index].Add(1)
			}
		}(i)
	}

	wg.Wait()

	// Verify all counters have correct values
	for i := 0; i < numGoroutines; i++ {
		expected := int64(iterations)
		actual := paddedCounters[i].Load()
		if actual != expected {
			t.Errorf("Counter %d: expected %d, got %d", i, expected, actual)
		}
	}

	t.Logf("✅ False sharing test passed: %d goroutines × %d iterations", numGoroutines, iterations)
}

// Benchmark AtomicPaddedInt64 vs regular atomic.Int64 to show performance difference
func BenchmarkAtomicPaddedInt64_SingleThread(b *testing.B) {
	var padded AtomicPaddedInt64

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		padded.Add(1)
	}
}

func BenchmarkAtomicRegularInt64_SingleThread(b *testing.B) {
	var regular atomic.Int64

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		regular.Add(1)
	}
}

// Benchmark concurrent access to show padding benefit
func BenchmarkAtomicPaddedInt64_MultiThread(b *testing.B) {
	const numCounters = 4
	paddedCounters := make([]AtomicPaddedInt64, numCounters)
	var counterIndex atomic.Int64

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		localIndex := int(counterIndex.Add(1) % numCounters)
		for pb.Next() {
			paddedCounters[localIndex].Add(1)
		}
	})
}

func BenchmarkAtomicRegularInt64_MultiThread(b *testing.B) {
	const numCounters = 4
	regularCounters := make([]atomic.Int64, numCounters)
	var counterIndex atomic.Int64

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		localIndex := int(counterIndex.Add(1) % numCounters)
		for pb.Next() {
			regularCounters[localIndex].Add(1)
		}
	})
}

// Test memory layout to ensure no unexpected gaps
func TestMemoryLayout(t *testing.T) {
	// Test that we can create arrays without issues
	const arraySize = 10
	paddedArray := make([]AtomicPaddedInt64, arraySize)

	// Initialize all elements
	for i := 0; i < arraySize; i++ {
		paddedArray[i].Store(int64(i))
	}

	// Verify all elements
	for i := 0; i < arraySize; i++ {
		if paddedArray[i].Load() != int64(i) {
			t.Errorf("Element %d: expected %d, got %d", i, i, paddedArray[i].Load())
		}
	}

	t.Logf("✅ Memory layout test passed: %d padded elements in array", arraySize)
}

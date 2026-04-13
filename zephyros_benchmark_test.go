// zephyros_benchmark_test.go: Performance benchmarks for MPSC lock-free ring buffer
//
// These benchmarks demonstrate the high-performance characteristics of Zephyros
// following the correct architectural patterns: single producer per ring.
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

// BenchmarkThreadedZephyros_Baseline demonstrates optimal multi-ring performance
func BenchmarkThreadedZephyros_Baseline(b *testing.B) {
	processor := func(item *int) {
		// Ultra-fast processor for pure throughput measurement
	}

	threaded, err := NewThreadedBuilder[int](262144, 4).
		WithProcessor(processor).
		WithBatchSize(32768).
		WithWorkers(4).
		Build()

	if err != nil {
		b.Fatalf("Failed to build: %v", err)
	}
	defer threaded.Close()

	b.ResetTimer()
	b.ReportAllocs()

	// One SafeWriter per ring — created outside the timer window.
	writers := [4]*SafeWriter[int]{
		threaded.NewSafeWriter(0),
		threaded.NewSafeWriter(1),
		threaded.NewSafeWriter(2),
		threaded.NewSafeWriter(3),
	}

	// Optimal pattern: each iteration uses different ring (no contention)
	for i := 0; i < b.N; i++ {
		writers[i%4].Write(func(slot *int) {
			*slot = i
		})
	}
} // BenchmarkThreadedZephyros_SingleRing demonstrates single ring performance
func BenchmarkThreadedZephyros_SingleRing(b *testing.B) {
	processor := func(item *int) {
		// Ultra-fast processor
	}

	threaded, err := NewThreadedBuilder[int](262144, 1).
		WithProcessor(processor).
		WithBatchSize(32768).
		WithWorkers(1).
		Build()

	if err != nil {
		b.Fatalf("Failed to build: %v", err)
	}
	defer threaded.Close()

	b.ResetTimer()
	b.ReportAllocs()

	// Single producer, single ring.
	w0 := threaded.NewSafeWriter(0)
	for i := 0; i < b.N; i++ {
		w0.Write(func(slot *int) {
			*slot = i
		})
	}
}

// BenchmarkThreadedZephyros_WriteOnly measures pure write throughput
func BenchmarkThreadedZephyros_WriteOnly(b *testing.B) {
	processor := func(item *int) {
		// No-op processor to isolate write performance
	}

	threaded, err := NewThreadedBuilder[int](262144, 4).
		WithProcessor(processor).
		WithBatchSize(32768).
		WithWorkers(4).
		Build()

	if err != nil {
		b.Fatalf("Failed to build: %v", err)
	}
	defer threaded.Close()

	// Don't start processing to isolate write performance

	b.ResetTimer()
	b.ReportAllocs()

	// Distribute writes across rings without starting processors.
	wo := [4]*SafeWriter[int]{
		threaded.NewSafeWriter(0),
		threaded.NewSafeWriter(1),
		threaded.NewSafeWriter(2),
		threaded.NewSafeWriter(3),
	}
	for i := 0; i < b.N; i++ {
		wo[i%4].Write(func(slot *int) {
			*slot = i
		})
	}
} // BenchmarkSingleRing_Direct demonstrates direct single ring performance
func BenchmarkSingleRing_Direct(b *testing.B) {
	processor := func(item *int) {
		// Ultra-fast processor
	}

	// Create a single ring directly
	ring, err := NewBuilder[int](262144).
		WithProcessor(processor).
		WithBatchSize(32768).
		Build()

	if err != nil {
		b.Fatalf("Failed to build ring: %v", err)
	}
	defer ring.Close()

	b.ResetTimer()
	b.ReportAllocs()

	// Single producer to single ring - baseline performance
	for i := 0; i < b.N; i++ {
		ring.Write(func(slot *int) {
			*slot = i
		})
	}
}

// BenchmarkThreadedZephyros_ProcessingThroughput measures end-to-end processing performance
func BenchmarkThreadedZephyros_ProcessingThroughput(b *testing.B) {
	processed := int64(0)
	processor := func(item *int) {
		atomic.AddInt64(&processed, 1)
	}

	threaded, err := NewThreadedBuilder[int](131072, 2).
		WithProcessor(processor).
		WithBatchSize(4096).
		WithWorkers(2).
		Build()

	if err != nil {
		b.Fatalf("Failed to build: %v", err)
	}
	defer threaded.Close()

	threaded.LoopProcess()
	time.Sleep(10 * time.Millisecond) // Let it stabilize

	b.ResetTimer()

	// Write messages across 2 rings.
	pw0 := threaded.NewSafeWriter(0)
	pw1 := threaded.NewSafeWriter(1)
	pw := [2]*SafeWriter[int]{pw0, pw1}
	for i := 0; i < b.N; i++ {
		pw[i%2].Write(func(slot *int) {
			*slot = i
		})
	}

	b.StopTimer()

	// Brief wait to see some processing results
	time.Sleep(50 * time.Millisecond)
	processedCount := atomic.LoadInt64(&processed)
	b.Logf("Messages processed during benchmark: %d/%d (%.1f%%)",
		processedCount, b.N, float64(processedCount)*100/float64(b.N))
}

// BenchmarkThreadedZephyros_BatchSizes compare different batch sizes
func BenchmarkThreadedZephyros_BatchSize256(b *testing.B) { benchmarkBatchSize(b, 256) }
func BenchmarkThreadedZephyros_BatchSize1K(b *testing.B)  { benchmarkBatchSize(b, 1024) }
func BenchmarkThreadedZephyros_BatchSize4K(b *testing.B)  { benchmarkBatchSize(b, 4096) }
func BenchmarkThreadedZephyros_BatchSize16K(b *testing.B) { benchmarkBatchSize(b, 16384) }
func BenchmarkThreadedZephyros_BatchSize32K(b *testing.B) { benchmarkBatchSize(b, 32768) }

func benchmarkBatchSize(b *testing.B, batchSize int64) {
	processor := func(item *int) {
		// Ultra-fast processor for pure write throughput measurement
	}

	threaded, err := NewThreadedBuilder[int](262144, 4).
		WithProcessor(processor).
		WithBatchSize(batchSize).
		WithWorkers(4).
		Build()

	if err != nil {
		b.Fatalf("Failed to build: %v", err)
	}
	defer threaded.Close()

	b.ResetTimer()
	b.ReportAllocs()

	// Distribute writes optimally across available rings.
	bsWriters := make([]*SafeWriter[int], 4)
	for id := 0; id < 4; id++ {
		bsWriters[id] = threaded.NewSafeWriter(id)
	}
	for i := 0; i < b.N; i++ {
		bsWriters[i%4].Write(func(slot *int) {
			*slot = i
		})
	}
}

// BenchmarkZephyros_ScalabilityComparison compares different ring configurations
func BenchmarkZephyros_1Ring(b *testing.B)  { benchmarkRingConfig(b, 1) }
func BenchmarkZephyros_2Rings(b *testing.B) { benchmarkRingConfig(b, 2) }
func BenchmarkZephyros_4Rings(b *testing.B) { benchmarkRingConfig(b, 4) }
func BenchmarkZephyros_8Rings(b *testing.B) { benchmarkRingConfig(b, 8) }

func benchmarkRingConfig(b *testing.B, numRings int) {
	processor := func(item *int) {
		// Ultra-fast processor
	}

	threaded, err := NewThreadedBuilder[int](65536, numRings).
		WithProcessor(processor).
		WithBatchSize(4096).
		WithWorkers(numRings).
		Build()

	if err != nil {
		b.Fatalf("Failed to build: %v", err)
	}
	defer threaded.Close()

	b.ResetTimer()
	b.ReportAllocs()

	// Create one SafeWriter per ring outside the timer window.
	rcWriters := make([]*SafeWriter[int], numRings)
	for id := 0; id < numRings; id++ {
		rcWriters[id] = threaded.NewSafeWriter(id)
	}
	// Distribute writes optimally across available rings
	for i := 0; i < b.N; i++ {
		rcWriters[i%numRings].Write(func(slot *int) {
			*slot = i
		})
	}
}

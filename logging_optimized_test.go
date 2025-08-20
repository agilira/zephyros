// logging_optimized_test.go: Tests for logging optimizations
//
// Copyright (c) 2025 AGILira
// Series: an AGLIra fragment
// SPDX-License-Identifier: MPL-2.0

package zephyros

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestHighPerformanceOptimized - High performance test with optimal configuration
// This test is designed for local performance validation and is skipped in CI
func TestHighPerformanceOptimized(t *testing.T) {
	// Skip in CI environments where performance is unpredictable
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}
	// OPTIMAL CONFIGURATION for maximum throughput
	numRings := 4                 // Perfect for 4-core contention distribution
	bufferSize := int64(262144)   // 256K buffer (larger for stability)
	batchSize := int64(16384)     // 16K batch (larger for efficiency)
	numProducers := 4             // 1 producer per ring = zero contention
	messagesPerProducer := 100000 // 400K total messages (reasonable for CI)
	totalMessages := numProducers * messagesPerProducer

	t.Logf("🚀 HIGH PERFORMANCE OPTIMIZED TEST")
	t.Logf("  Target: Demonstrate Zephyros ultra-high performance capabilities")
	t.Logf("  Config: %d rings × %d producers × %dK messages = %dK total",
		numRings, numProducers, messagesPerProducer/1000, totalMessages/1000)

	// ULTRA-FAST PROCESSOR: Direct atomic counting (no overhead)
	processed := int64(0)
	processor := func(item *int) {
		atomic.AddInt64(&processed, 1)
	}

	// Build with optimal settings
	threaded, err := NewThreadedBuilder[int](bufferSize, numRings).
		WithProcessor(processor).
		WithBatchSize(batchSize).
		WithWorkers(numRings). // Dedicated worker per ring
		Build()

	if err != nil {
		t.Fatalf("Failed to build ThreadedZephyros: %v", err)
	}
	defer threaded.Close()

	// Start processing immediately
	threaded.LoopProcess()
	time.Sleep(10 * time.Millisecond) // Let consumers stabilize

	t.Logf("📊 BENCHMARK START")

	startTime := time.Now()

	// 🚀 ZERO-CONTENTION PRODUCERS: Each producer has dedicated ring
	var wg sync.WaitGroup
	successfulWrites := int64(0)

	for producerID := 0; producerID < numProducers; producerID++ {
		wg.Add(1)
		go func(pID int) {
			defer wg.Done()

			// Dedicated ring = zero atomic contention between producers
			ringID := pID
			localWrites := int64(0)

			// Tight write loop with minimal overhead
			for i := 0; i < messagesPerProducer; i++ {
				if threaded.Write(ringID, func(slot *int) {
					*slot = i // Simple assignment
				}) {
					localWrites++
				} else {
					// Handle backpressure with brief yield
					time.Sleep(time.Microsecond)
					i-- // Retry this message
				}
			}

			atomic.AddInt64(&successfulWrites, localWrites)
		}(producerID)
	}

	// Wait for all writes to complete
	wg.Wait()
	writeTime := time.Since(startTime)

	t.Logf("📝 WRITING PHASE COMPLETED:")
	t.Logf("  Successful writes: %d/%d (%.1f%%)",
		successfulWrites, totalMessages, float64(successfulWrites)*100/float64(totalMessages))
	t.Logf("  Write time: %v", writeTime)
	writeThroughput := float64(successfulWrites) / writeTime.Seconds()
	t.Logf("  Write throughput: %.1fM ops/sec", writeThroughput/1000000)

	// 🏃‍♂️ PROCESSING PHASE: Wait for all processing to complete
	t.Logf("⏳ PROCESSING PHASE:")

	for atomic.LoadInt64(&processed) < successfulWrites {
		current := atomic.LoadInt64(&processed)
		percentage := float64(current) * 100 / float64(successfulWrites)

		if percentage >= 90 {
			// Near completion - more frequent checks
			time.Sleep(time.Millisecond * 10)
		} else {
			// Early phase - less frequent checks
			time.Sleep(time.Millisecond * 50)
		}
	}

	totalTime := time.Since(startTime)
	finalProcessed := atomic.LoadInt64(&processed)

	// 📊 CALCULATE FINAL METRICS
	overallThroughput := float64(finalProcessed) / totalTime.Seconds()
	processingThroughput := float64(finalProcessed) / (totalTime.Seconds() - writeTime.Seconds())

	// RESULTS ANALYSIS
	t.Logf("")
	t.Logf("HIGH PERFORMANCE OPTIMIZED RESULTS:")
	t.Logf("  Messages processed: %d", finalProcessed)
	t.Logf("  Total time: %v", totalTime)
	t.Logf("  Overall throughput: %.1fM ops/sec", overallThroughput/1000000)
	t.Logf("  Processing throughput: %.1fM ops/sec", processingThroughput/1000000)

	perRingThroughput := overallThroughput / float64(numRings) / 1000000
	t.Logf("  Per-ring efficiency: %.1fM ops/sec", perRingThroughput)

	// PERFORMANCE ANALYSIS
	expectedMinThroughput := 500000.0 // 0.5M ops/sec minimum (realistic for race detector)

	t.Logf("")
	t.Logf("📊 PERFORMANCE ANALYSIS:")
	t.Logf("  Achieved: %.1fM ops/sec", overallThroughput/1000000)
	t.Logf("  Minimum expected: %.1fM ops/sec", expectedMinThroughput/1000000)

	// PERFORMANCE CLASSIFICATION (CI-friendly thresholds)
	if overallThroughput >= 5000000 {
		t.Logf("")
		t.Logf("EXCEPTIONAL PERFORMANCE!")
		t.Logf("Zephyros throughput: %.1fM ops/sec", overallThroughput/1000000)
		t.Logf("Ultra-high performance achieved!")
		t.Logf("ZEPHYROS EXCELLENCE DEMONSTRATED!")
	} else if overallThroughput >= 2000000 {
		t.Logf("")
		t.Logf("EXCELLENT PERFORMANCE!")
		t.Logf("High-performance target achieved")
		t.Logf("Close to optimal throughput")
	} else if overallThroughput >= 1000000 {
		t.Logf("")
		t.Logf("GOOD PERFORMANCE")
		t.Logf("Acceptable performance level")
	} else {
		t.Logf("")
		t.Logf("PERFORMANCE ISSUE DETECTED")
		t.Logf("Below expected performance levels")
	}

	// Ensure we meet minimum performance standards
	if overallThroughput < expectedMinThroughput {
		t.Errorf("FAILED: Expected >%.1fM ops/sec, got %.1fM ops/sec",
			expectedMinThroughput/1000000, overallThroughput/1000000)
	}
}

// diagnostic_test.go: Simple diagnostics
//
// Copyright (c) 2025 AGILira
// Series: an AGLIra fragment
// SPDX-License-Identifier: MPL-2.0

package zephyros

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestThreadedDiagnostic performs detailed diagnostic of ThreadedZephyros performance
func TestThreadedDiagnostic(t *testing.T) {
	// Test different configurations to find the bottleneck
	configurations := []struct {
		name         string
		numRings     int
		numProducers int
		bufferSize   int64
		batchSize    int64
	}{
		{"1Ring-1Producer", 1, 1, 262144, 16384},
		{"2Ring-4Producers", 2, 4, 262144, 16384}, // Valid MPSC: 2 producers per ring
		{"4Ring-1Producer", 4, 1, 262144, 16384},
		{"4Ring-4Producers", 4, 4, 262144, 16384},
		{"4Ring-4Producers-SmallBatch", 4, 4, 262144, 256},
		{"4Ring-4Producers-LargeBatch", 4, 4, 262144, 32768},
	}

	for _, config := range configurations {
		t.Run(config.name, func(t *testing.T) {
			runDiagnosticTest(t, config.numRings, config.numProducers, config.bufferSize, config.batchSize)
		})
	}
}

func runDiagnosticTest(t *testing.T, numRings, numProducers int, bufferSize, batchSize int64) {
	messagesPerProducer := 250000 // Smaller test for speed
	totalMessages := numProducers * messagesPerProducer

	// Ultra-fast processor
	processed := int64(0)
	processor := func(item *int) {
		atomic.AddInt64(&processed, 1)
	}

	t.Logf("Config: %d rings, %d producers, buffer=%d, batch=%d",
		numRings, numProducers, bufferSize, batchSize)

	// Build ThreadedZephyros
	threaded, err := NewThreadedBuilder[int](bufferSize, numRings).
		WithProcessor(processor).
		WithBatchSize(batchSize).
		WithWorkers(numRings).
		Build()

	if err != nil {
		t.Fatalf("Failed to build: %v", err)
	}
	defer threaded.Close()

	// Start processing
	threaded.LoopProcess()

	// DETAILED TIMING
	startTime := time.Now()

	var wg sync.WaitGroup
	successfulWrites := int64(0)

	// WRITING PHASE with detailed timing
	writeStart := time.Now()

	for i := 0; i < numProducers; i++ {
		wg.Add(1)
		go func(producerID int) {
			defer wg.Done()

			localWrites := int64(0)

			// MPSC COMPLIANCE: Use dedicated writers when producers > rings
			if numProducers > numRings {
				// Create dedicated writer for load balancing across rings
				ringID := producerID % numRings
				writer := threaded.NewSafeWriter(ringID)

				for j := 0; j < messagesPerProducer; j++ {
					if writer.Write(func(slot *int) {
						*slot = j
					}) {
						localWrites++
					}
				}
			} else {
				// Direct ring access when producers <= rings (optimal case)
				ringID := producerID % numRings
				for j := 0; j < messagesPerProducer; j++ {
					if threaded.Write(ringID, func(slot *int) {
						*slot = j
					}) {
						localWrites++
					}
				}
			}

			atomic.AddInt64(&successfulWrites, localWrites)
		}(i)
	}

	wg.Wait()
	writeTime := time.Since(writeStart)

	// PROCESSING PHASE with detailed timing
	processStart := time.Now()

	for atomic.LoadInt64(&processed) < successfulWrites && time.Since(processStart) < 2*time.Second {
		time.Sleep(100 * time.Microsecond)
	}

	totalTime := time.Since(startTime)
	finalProcessed := atomic.LoadInt64(&processed)

	// DETAILED METRICS
	writeThroughput := float64(successfulWrites) / writeTime.Seconds()
	totalThroughput := float64(finalProcessed) / totalTime.Seconds()

	// Get ring distribution stats
	stats := threaded.Stats()

	t.Logf("Results:")
	t.Logf("  Writes: %d/%d (%.1f%%) in %v",
		successfulWrites, totalMessages, float64(successfulWrites)*100/float64(totalMessages), writeTime)
	t.Logf("  Processed: %d in %v", finalProcessed, totalTime)
	t.Logf("  Write throughput: %.1fM ops/sec", writeThroughput/1000000)
	t.Logf("  Total throughput: %.1fM ops/sec", totalThroughput/1000000)
	t.Logf("  Efficiency: %.1f%% (%.1fM expected for %d rings)",
		(totalThroughput/26000000)*100/float64(numRings), 26.0*float64(numRings), numRings)

	// Ring distribution analysis
	for i := 0; i < numRings; i++ {
		ringItems := stats[fmt.Sprintf("ring_%d_items", i)]
		t.Logf("  Ring %d: %d items", i, ringItems)
	}

	t.Logf("  Buffer utilization: %.1f%%",
		float64(stats["items_buffered"])*100/float64(bufferSize))
}

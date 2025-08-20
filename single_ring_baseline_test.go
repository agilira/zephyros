// single_ring_baseline_test.go: Tests single ring performance as baseline
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

// TestSingleRing_Performance tests single ring performance as baseline
func TestSingleRing_Performance(t *testing.T) {
	// Configuration for baseline test
	bufferSize := int64(262144) // 256K buffer
	batchSize := int64(16384)   // 16K batch
	numProducers := 4
	messagesPerProducer := 500000
	totalMessages := numProducers * messagesPerProducer

	// Ultra-fast processor
	processed := int64(0)
	processor := func(item *int) {
		atomic.AddInt64(&processed, 1)
	}

	t.Logf("SINGLE RING BASELINE TEST:")
	t.Logf("  Config: 1 ring, %d producers × %d messages = %d total",
		numProducers, messagesPerProducer, totalMessages)

	// Build ThreadedZephyros for multiple producers
	threaded, err := NewThreadedBuilder[int](bufferSize, numProducers).
		WithProcessor(processor).
		WithBatchSize(batchSize).
		Build()

	if err != nil {
		t.Fatalf("Failed to build ThreadedZephyros: %v", err)
	}
	defer threaded.Close()

	// Start background processing
	go threaded.LoopProcess()

	// WRITING PHASE
	startTime := time.Now()

	var wg sync.WaitGroup
	successfulWrites := int64(0)

	for i := 0; i < numProducers; i++ {
		wg.Add(1)
		go func(producerID int) {
			defer wg.Done()

			localWrites := int64(0)

			for j := 0; j < messagesPerProducer; j++ {
				if threaded.Write(producerID, func(slot *int) {
					*slot = j
				}) {
					localWrites++
				}
			}

			atomic.AddInt64(&successfulWrites, localWrites)
		}(i)
	}

	wg.Wait()
	writeTime := time.Since(startTime)

	t.Logf("WRITING PHASE:")
	t.Logf("  Successful writes: %d/%d (%.1f%%)",
		successfulWrites, totalMessages, float64(successfulWrites)*100/float64(totalMessages))
	t.Logf("  Write time: %v", writeTime)
	writeThroughput := float64(successfulWrites) / writeTime.Seconds()
	t.Logf("  Write throughput: %.0f ops/sec", writeThroughput)

	// PROCESSING PHASE - wait for all messages to be processed
	t.Logf("PROCESSING PHASE:")

	totalTime := time.Since(startTime)
	for atomic.LoadInt64(&processed) < successfulWrites && totalTime < 5*time.Second {
		time.Sleep(time.Millisecond)
		totalTime = time.Since(startTime)
	}

	finalProcessed := atomic.LoadInt64(&processed)
	t.Logf("PROCESSED: %d messages", finalProcessed)

	// Calculate final throughput
	throughput := float64(finalProcessed) / totalTime.Seconds()

	t.Logf("")
	t.Logf("SINGLE RING BASELINE RESULTS:")
	t.Logf("  Messages processed: %d", finalProcessed)
	t.Logf("  Total time: %v", totalTime)
	t.Logf("  Throughput: %.0f ops/sec", throughput)

	// Get final stats
	stats := threaded.Stats()
	t.Logf("  Final stats: %+v", stats)

	t.Logf("BASELINE: %.0f ops/sec", throughput)
}

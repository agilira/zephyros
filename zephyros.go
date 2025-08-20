// zephyros.go: Ultra-high performance MPSC lock-free ring buffer
//
// Copyright (c) 2025 AGILira
// Series: an AGLIra fragment
// SPDX-License-Identifier: MPL-2.0

package zephyros

import (
	"runtime"
	"time"
)

// ProcessorFunc is the ultra-fast processing function signature
type ProcessorFunc[T any] func(*T)

// Zephyros is the ultra-high performance MPSC lock-free ring buffer
type Zephyros[T any] struct {
	// Ring buffer core
	buffer   []T
	capacity int64
	mask     int64 // capacity - 1 for bit masking

	// MPSC atomic cursors (cache-line padded)
	writerCursor AtomicPaddedInt64 // Claim sequence (multiple producers)
	readerCursor AtomicPaddedInt64 // Reader sequence (single consumer)

	// PARALLEL PUBLICATION: Replace commitCursor with available buffer
	// Each producer publishes independently to eliminate serialization
	availableBuffer []AtomicPaddedInt64 // Per-slot availability markers

	// Processor function
	processor ProcessorFunc[T]

	// Batching configuration
	batchSize int64

	// Control
	closed AtomicPaddedInt64 // 0 = open, 1 = closed

	// Cache line padding to prevent false sharing
	_ [64]byte
}

// Write adds an item to the ring buffer using ultra-fast atomic operations
// MPSC CONTRACT: Single producer per ring for maximum performance
// For multiple producers on same ring, use ThreadedZephyros with separate rings
func (z *Zephyros[T]) Write(writerFunc func(*T)) bool {
	// Check if buffer is closed (essential for reliability)
	if z.closed.Load() != 0 {
		return false
	}

	// SINGLE PRODUCER OPTIMIZATION: Direct sequence claim for maximum speed
	sequence := z.writerCursor.Add(1) - 1

	// Check if we're about to lap the reader (buffer full check)
	if sequence >= z.readerCursor.Load()+z.capacity {
		// Buffer full - rollback sequence and return false
		z.writerCursor.Add(-1)
		return false
	}

	// ULTRA-FAST: Direct write with guaranteed single producer
	slot := &z.buffer[sequence&z.mask]
	writerFunc(slot)

	// Mark slot as available for reading
	z.availableBuffer[sequence&z.mask].Store(sequence)

	return true
}

// Flush ensures all pending writes are visible to reader (MPSC compatible)
func (z *Zephyros[T]) Flush() {
	// In MPSC, flush is automatic via commitCursor ordering
	// This method exists for API compatibility
	// All writes are already properly committed via Write() method
}

// ProcessBatch processes items in a single batch (ZERO-ALLOCATION ULTRA PERFORMANCE)
func (z *Zephyros[T]) ProcessBatch() int {
	current := z.readerCursor.Load()

	// SMART DYNAMIC BATCH: Only adapt in extreme conditions to minimize overhead
	writerPos := z.writerCursor.Load()
	bufferOccupancy := writerPos - current

	// Use dynamic batching only when buffer is critically full or empty
	var adaptiveBatchSize int64 = z.batchSize
	if bufferOccupancy > z.capacity*3/4 {
		// CRITICAL: Buffer >75% full - emergency drain with max batch
		adaptiveBatchSize = min(z.batchSize*4, z.capacity/2)
	} else if bufferOccupancy < 128 {
		// CRITICAL: Buffer nearly empty - ultra-low latency mode
		adaptiveBatchSize = 128
	}

	// ZERO-ALLOCATION: Direct linear scan without slice allocation
	available := current - 1 // Start before current to detect if we find any sequences
	maxScan := current + adaptiveBatchSize

	// Boundary check
	if maxScan > writerPos {
		maxScan = writerPos // Don't scan beyond what's been written
	}

	// Ultra-fast linear scan for contiguous sequences with optimal prefetch
	for seq := current; seq < maxScan; seq++ {
		// OPTIMAL PREFETCH: Single prefetch distance for best L1 cache hit rate
		if seq+4 < maxScan {
			_ = z.availableBuffer[(seq+4)&z.mask].Load() // Prefetch 4 slots ahead
		}

		if z.availableBuffer[seq&z.mask].Load() == seq {
			available = seq // Track the last continuous sequence
		} else {
			// Stop at first gap to maintain order
			break
		}
	}

	if available < current {
		return 0 // Nothing available
	}

	// ULTRA-FAST: Unrolled processing loop for better CPU utilization
	endSequence := available
	processed := int(endSequence - current + 1) // +1 because we include current

	// OPTIMIZATION: Process and reset in immediate groups to avoid slice allocation
	// This reduces cache invalidations while keeping zero allocations
	seq := current
	remainder := processed & 3 // processed % 4
	chunks := processed >> 2   // processed / 4

	// Process 4-item chunks with immediate reset and optimal prefetch
	for i := 0; i < chunks; i++ {
		// OPTIMAL PREFETCH: Single cache line ahead for best hit rate
		if seq+8 <= endSequence {
			_ = z.buffer[(seq+8)&z.mask] // Prefetch data 8 slots ahead
		}

		// Process 4 items
		idx1 := seq & z.mask
		z.processor(&z.buffer[idx1])
		seq++

		idx2 := seq & z.mask
		z.processor(&z.buffer[idx2])
		seq++

		idx3 := seq & z.mask
		z.processor(&z.buffer[idx3])
		seq++

		idx4 := seq & z.mask
		z.processor(&z.buffer[idx4])
		seq++

		// Batch reset for this group of 4 (better cache locality)
		z.availableBuffer[idx1].Store(-2)
		z.availableBuffer[idx2].Store(-2)
		z.availableBuffer[idx3].Store(-2)
		z.availableBuffer[idx4].Store(-2)
	}

	// Process remaining items with immediate reset
	for i := 0; i < remainder; i++ {
		idx := seq & z.mask
		z.processor(&z.buffer[idx])
		z.availableBuffer[idx].Store(-2)
		seq++
	}

	z.readerCursor.Store(endSequence + 1) // Next sequence to read
	return processed
}

// TryProcessBatch processes items with thread-safe CAS for work stealing (THREAD-SAFE VERSION)
func (z *Zephyros[T]) TryProcessBatch() int {
	for {
		current := z.readerCursor.Load()

		// Quick check: is there anything to process?
		writerPos := z.writerCursor.Load()
		if current >= writerPos {
			return 0 // Nothing to process
		}

		// Scan for available continuous sequences
		available := current - 1
		maxScan := current + z.batchSize
		if maxScan > writerPos {
			maxScan = writerPos // Don't scan beyond writer
		}

		for seq := current; seq < maxScan; seq++ {
			if z.availableBuffer[seq&z.mask].Load() == seq {
				available = seq
			} else {
				break // Stop at first gap
			}
		}

		if available < current {
			return 0 // No continuous sequence found
		}

		endSequence := available
		processed := int(endSequence - current + 1)

		// THREAD-SAFE: Claim this batch with CAS
		if !z.readerCursor.CompareAndSwap(current, endSequence+1) {
			// Another worker claimed this batch, try again
			continue
		}

		// Successfully claimed batch - now process it
		seq := current
		remainder := processed & 3
		chunks := processed >> 2

		// Process 4-item chunks
		for i := 0; i < chunks; i++ {
			z.processor(&z.buffer[seq&z.mask])
			z.availableBuffer[seq&z.mask].Store(-2)
			seq++
			z.processor(&z.buffer[seq&z.mask])
			z.availableBuffer[seq&z.mask].Store(-2)
			seq++
			z.processor(&z.buffer[seq&z.mask])
			z.availableBuffer[seq&z.mask].Store(-2)
			seq++
			z.processor(&z.buffer[seq&z.mask])
			z.availableBuffer[seq&z.mask].Store(-2)
			seq++
		}

		// Process remaining items
		for i := 0; i < remainder; i++ {
			z.processor(&z.buffer[seq&z.mask])
			z.availableBuffer[seq&z.mask].Store(-2)
			seq++
		}

		return processed
	}
} // LoopProcess continuously processes items (ULTRA HIGH PERFORMANCE)
func (z *Zephyros[T]) LoopProcess() {
	spins := 0

	for z.closed.Load() == 0 {
		processed := z.ProcessBatch()

		if processed > 0 {
			// ULTRA PERFORMANCE: Reset spins immediately when work found
			spins = 0

			// If we processed a full batch, immediately try again
			// This creates a "burst mode" for high throughput scenarios
			if processed >= int(z.batchSize/2) {
				continue // Hot loop for maximum throughput
			}
		} else {
			// ULTRA PERFORMANCE: Optimized idle strategy
			spins++

			if spins < 10000 {
				// Hot spinning for ultra-low latency (first 10k iterations)
				continue
			} else if spins < 50000 {
				// Cooperative yielding for medium latency
				if spins&3 == 0 { // Yield every 4 iterations
					runtime.Gosched()
				}
			} else {
				// Microsleep only after extensive spinning
				time.Sleep(time.Microsecond)
				spins = 0 // Reset counter
			}
		}
	}

	// Process remaining items after close - ensure ALL items are processed
	// Keep trying until we're absolutely sure there's nothing left
	consecutiveEmpty := 0
	for consecutiveEmpty < 3 { // Require 3 consecutive empty attempts
		z.Flush() // Always ensure writes are visible first
		processed := z.ProcessBatch()
		if processed > 0 {
			// Found work, reset empty counter and continue
			consecutiveEmpty = 0
			continue
		}

		// No work found, increment empty counter
		consecutiveEmpty++

		// Wait a bit for any in-flight writes to complete
		time.Sleep(time.Microsecond)

		// Try one more time after wait
		if z.ProcessBatch() > 0 {
			consecutiveEmpty = 0 // Reset if we found work after wait
		}
	}
}

// Close gracefully shuts down the processor
func (z *Zephyros[T]) Close() {
	z.closed.Store(1)
	z.Flush() // Ensure all writes are published
}

// Stats returns performance statistics (MPSC)
func (z *Zephyros[T]) Stats() map[string]int64 {
	writerPos := z.writerCursor.Load() // Last claimed sequence
	readerPos := z.readerCursor.Load() // Reader position

	return map[string]int64{
		"writer_position": writerPos, // Last claimed sequence
		"reader_position": readerPos,
		"buffer_size":     z.capacity,
		"items_buffered":  writerPos - readerPos, // Claimed but not yet consumed
		"closed":          z.closed.Load(),
	}
}

// Helper functions for adaptive batching
func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

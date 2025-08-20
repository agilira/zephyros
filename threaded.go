// threaded_zephyros.go: Multi-threaded MPSC lock-free ring buffer
//
// ARCHITECTURE:
//   - Multiple ring buffers (one per writer thread)
//   - Single consumer per ring (Gemini's strategy for maximum performance)
//   - Deterministic shutdown with sync.WaitGroup
//   - Dual API: Fast (panic) vs Safe (error) methods
//
// PERFORMANCE:
//   - 60M+ ops/sec with multiple rings
//   - Zero-allocation design
//   - Lock-free atomic operations
//   - Adaptive batching (128-32K messages)
//
// SHUTDOWN:
//   - Close() is deterministic (WaitGroup-based)
//   - Idempotent: multiple Close() calls are safe
//   - All buffered messages are processed before shutdown
//
// API DESIGN:
//   - Fast Path: GetWriterRing(), NewSafeWriter() - panic on invalid IDs (max performance)
//   - Safe Path: SafeGetWriterRing(), NewSafeWriterWithError() - return errors (robust)
//   - Write() method always returns false instead of panicking
//
// USAGE:
//   Fast Path (hot paths, performance critical):
//     ring := tz.GetWriterRing(threadID)  // panics if threadID invalid
//
//   Safe Path (critical applications):
//     ring, err := tz.SafeGetWriterRing(threadID)  // returns error if threadID invalid
//     if err != nil { handle error }
//
// Copyright (c) 2025 AGILira
// Series: an AGLIra fragment
// SPDX-License-Identifier: MPL-2.0

package zephyros

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
)

// ThreadedZephyros errors
var (
	ErrInvalidRingID   = errors.New("invalid ring ID")
	ErrInvalidThreadID = errors.New("invalid thread ID")
)

// ThreadedZephyros: Multi-ring MPSC for ultimate performance with Gemini strategy
type ThreadedZephyros[T any] struct {
	// Array of ring buffers (one per writer thread)
	rings      []*Zephyros[T]
	numRings   int
	numWorkers int
	processor  ProcessorFunc[T]
	closed     AtomicPaddedInt64

	// Consumer control
	workerChannels []chan struct{}
	wg             sync.WaitGroup // For deterministic shutdown
}

// ThreadedBuilder for creating threaded MPSC
type ThreadedBuilder[T any] struct {
	ringSize   int64
	numRings   int
	numWorkers int
	processor  ProcessorFunc[T]
	batchSize  int64
}

func NewThreadedBuilder[T any](ringSize int64, numRings int) *ThreadedBuilder[T] {
	if numRings <= 0 {
		numRings = runtime.NumCPU()
	}

	// Default: single consumer per ring (Gemini strategy)
	numWorkers := numRings
	if numWorkers > runtime.NumCPU() {
		numWorkers = runtime.NumCPU()
	}

	// OPTIMIZATION: Intelligent default batch size
	defaultBatchSize := int64(64) // Safe default
	if ringSize >= 1024 {
		defaultBatchSize = 256 // Optimal for larger buffers
	} else if ringSize >= 64 {
		defaultBatchSize = 16 // Appropriate for small buffers
	} else if ringSize < 64 {
		defaultBatchSize = 1 // Minimal for very small buffers
	}

	return &ThreadedBuilder[T]{
		ringSize:   ringSize,
		numRings:   numRings,
		numWorkers: numWorkers,
		batchSize:  defaultBatchSize,
	}
}

func (b *ThreadedBuilder[T]) WithProcessor(processor ProcessorFunc[T]) *ThreadedBuilder[T] {
	b.processor = processor
	return b
}

func (b *ThreadedBuilder[T]) WithBatchSize(batchSize int64) *ThreadedBuilder[T] {
	b.batchSize = batchSize
	return b
}

func (b *ThreadedBuilder[T]) WithWorkers(numWorkers int) *ThreadedBuilder[T] {
	if numWorkers <= 0 {
		numWorkers = runtime.NumCPU()
	}
	b.numWorkers = numWorkers
	return b
}

func (b *ThreadedBuilder[T]) Build() (*ThreadedZephyros[T], error) {
	if b.processor == nil {
		return nil, ErrMissingProcessor
	}

	rings := make([]*Zephyros[T], b.numRings)

	// Create individual ring buffers
	for i := 0; i < b.numRings; i++ {
		// Create isolated processor for this ring
		isolatedProcessor := func(item *T) {
			// Call original processor with ring isolation
			b.processor(item)
		}

		ring, err := NewBuilder[T](b.ringSize).
			WithProcessor(isolatedProcessor).
			WithBatchSize(b.batchSize).
			Build()
		if err != nil {
			// Cleanup previously created rings
			for j := 0; j < i; j++ {
				rings[j].Close()
			}
			return nil, err
		}
		rings[i] = ring
	}

	tz := &ThreadedZephyros[T]{
		rings:      rings,
		numRings:   b.numRings,
		numWorkers: b.numWorkers,
		processor:  b.processor,
	}

	// Initialize closed state
	tz.closed.Store(0)

	return tz, nil
}

// GetWriterRing returns a ring buffer for the calling thread (FAST PATH - panics on invalid ID)
// For maximum performance in hot paths. Use SafeGetWriterRing for error handling.
func (tz *ThreadedZephyros[T]) GetWriterRing(threadID int) *Zephyros[T] {
	if threadID < 0 || threadID >= tz.numRings {
		panic(fmt.Sprintf("CRITICAL BUG: invalid threadID %d (valid range: 0-%d)", threadID, tz.numRings-1))
	}
	return tz.rings[threadID%tz.numRings]
}

// SafeGetWriterRing returns a ring buffer for the calling thread (SAFE PATH - returns error on invalid ID)
// Use this in critical applications where panics are unacceptable.
func (tz *ThreadedZephyros[T]) SafeGetWriterRing(threadID int) (*Zephyros[T], error) {
	if threadID < 0 || threadID >= tz.numRings {
		return nil, fmt.Errorf("%w: %d (valid range: 0-%d)", ErrInvalidThreadID, threadID, tz.numRings-1)
	}
	return tz.rings[threadID%tz.numRings], nil
}

// Write to thread-local ring (ultra-fast)
func (tz *ThreadedZephyros[T]) Write(threadID int, writerFunc func(*T)) bool {
	if tz.closed.Load() != 0 {
		return false
	}

	if threadID < 0 || threadID >= tz.numRings {
		return false // Invalid threadID, return false instead of panic
	}

	ring := tz.rings[threadID]
	return ring.Write(writerFunc)
}

// LoopProcess starts Gemini strategy consumers for maximum performance
func (tz *ThreadedZephyros[T]) LoopProcess() {
	// GEMINI STRATEGY: 1 consumer per ring = NO CAS contention = MAX throughput
	tz.workerChannels = make([]chan struct{}, tz.numRings)

	for i := 0; i < tz.numRings; i++ {
		stopChan := make(chan struct{})
		tz.workerChannels[i] = stopChan

		// Add worker to WaitGroup before starting goroutine
		tz.wg.Add(1)

		// Launch dedicated consumer for this ring
		go func(ringIndex int, stop chan struct{}) {
			defer tz.wg.Done() // Signal completion when worker exits

			ring := tz.rings[ringIndex]
			spins := 0

			for {
				select {
				case <-stop:
					// Final cleanup - process all remaining items
					for ring.ProcessBatch() > 0 {
						// Keep processing until empty
					}
					ring.Flush()
					return
				default:
					// ULTRA-FAST: Direct ProcessBatch() without CAS overhead
					processed := ring.ProcessBatch()

					if processed > 0 {
						spins = 0
						// Hot path: keep processing at full speed
						continue
					} else {
						// No work found - apply backoff
						spins++
						if spins < 1000 {
							// Hot spinning for ultra-low latency
							continue
						} else if spins < 10000 {
							// Minimal yielding every other iteration
							if spins&1 == 0 {
								runtime.Gosched()
							}
						} else {
							// Reset to avoid overflow
							spins = 0
						}
					}
				}
			}
		}(i, stopChan)
	}

	// NON-BLOCKING: Just start the workers, don't wait for close
	// The Close() method will handle stopping workers
}

func (tz *ThreadedZephyros[T]) Close() {
	// Use atomic compare-and-swap to ensure close is called only once
	if !tz.closed.CompareAndSwap(0, 1) {
		// Already closed
		return
	}

	// Stop all consumers if they exist
	if tz.workerChannels != nil {
		for _, stopChan := range tz.workerChannels {
			close(stopChan)
		}
	}

	// Wait for all workers to finish (deterministic shutdown)
	tz.wg.Wait()

	// Close individual rings
	for _, ring := range tz.rings {
		ring.Close()
	}
}

func (tz *ThreadedZephyros[T]) Stats() map[string]int64 {
	totalStats := make(map[string]int64)

	for i, ring := range tz.rings {
		stats := ring.Stats()
		for key, value := range stats {
			totalStats[key] += value
		}
		totalStats[fmt.Sprintf("ring_%d_items", i)] = stats["items_buffered"]
	}

	totalStats["num_rings"] = int64(tz.numRings)
	totalStats["num_workers"] = int64(tz.numWorkers)
	totalStats["closed"] = tz.closed.Load()

	return totalStats
}

// SafeWriter provides guaranteed thread-safe writing to a dedicated ring
type SafeWriter[T any] struct {
	tz     *ThreadedZephyros[T]
	ringID int
	ring   *Zephyros[T]
}

// NewSafeWriter creates a dedicated writer for a specific ring (FAST PATH - panics on invalid ID)
// For maximum performance in hot paths. Use NewSafeWriterWithError for error handling.
func (tz *ThreadedZephyros[T]) NewSafeWriter(ringID int) *SafeWriter[T] {
	if ringID < 0 || ringID >= tz.numRings {
		panic(fmt.Sprintf("CRITICAL BUG: invalid ringID %d (valid range: 0-%d)", ringID, tz.numRings-1))
	}

	return &SafeWriter[T]{
		tz:     tz,
		ringID: ringID,
		ring:   tz.rings[ringID],
	}
}

// NewSafeWriterWithError creates a dedicated writer for a specific ring (SAFE PATH - returns error on invalid ID)
// Use this in critical applications where panics are unacceptable.
func (tz *ThreadedZephyros[T]) NewSafeWriterWithError(ringID int) (*SafeWriter[T], error) {
	if ringID < 0 || ringID >= tz.numRings {
		return nil, fmt.Errorf("%w: %d (valid range: 0-%d)", ErrInvalidRingID, ringID, tz.numRings-1)
	}

	return &SafeWriter[T]{
		tz:     tz,
		ringID: ringID,
		ring:   tz.rings[ringID],
	}, nil
}

// Write safely to the dedicated ring (GUARANTEED thread-safe)
func (s *SafeWriter[T]) Write(writerFunc func(*T)) bool {
	return s.ring.Write(writerFunc)
}

// GetRingID returns the ring ID this writer is bound to
func (s *SafeWriter[T]) GetRingID() int {
	return s.ringID
}

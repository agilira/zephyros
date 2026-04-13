// threaded_zephyros.go: Multi-ring SPSC lock-free ring buffer (Anemoi strategy)
//
// ARCHITECTURE:
//   - N ring buffers, N SafeWriters: one ring owned by exactly one producer.
//   - One consumer goroutine per ring (Gemini strategy for maximum throughput).
//   - Ownership is enforced at Build time via atomic CAS: calling
//     NewSafeWriter(ringID) twice for the same ring is a programming error
//     and panics. NewSafeWriterWithError returns ErrRingAlreadyAssigned.
//   - Deterministic shutdown with sync.WaitGroup.
//
// WHY ownership enforcement:
//   The ring's rollback-on-full is only safe under a single-producer-per-ring
//   invariant (see Write() in zephyros.go). Without enforcement, a caller
//   mistake (two goroutines sharing a raw ring) causes silent slot corruption.
//   The SafeWriter token is the structural guarantee that makes the invariant
//   impossible to violate accidentally.
//
// USAGE:
//   writer, err := tz.NewSafeWriterWithError(ringID) // once per ring at init
//   if err != nil { handle error }                   // ErrRingAlreadyAssigned
//   writer.Write(func(slot *T) { *slot = value })    // from one goroutine only
//
// Copyright (c) 2025 AGILira
// Series: an AGLIra fragment
// SPDX-License-Identifier: MPL-2.0

package zephyros

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"time"
)

// ThreadedZephyros errors
var (
	ErrInvalidRingID       = errors.New("invalid ring ID")
	ErrRingAlreadyAssigned = errors.New("ring already assigned to a writer")
)

// ThreadedZephyros: N-ring SPSC buffer cluster with enforced single-owner-per-ring.
type ThreadedZephyros[T any] struct {
	// Array of ring buffers (one per writer goroutine)
	rings      []*Zephyros[T]
	numRings   int
	numWorkers int
	processor  ProcessorFunc[T]
	closed     AtomicPaddedInt64

	// assigned tracks ownership: assigned[i] == 1 means ring i has been claimed
	// by a SafeWriter. CAS 0→1 in NewSafeWriter* is the enforcement mechanism.
	assigned []AtomicPaddedInt64

	// Consumer control
	workerChannels []chan struct{}
	wg             sync.WaitGroup // For deterministic shutdown
}

// ThreadedBuilder for creating threaded MPSC
type ThreadedBuilder[T any] struct {
	ringSize       int64
	numRings       int
	numWorkers     int
	processor      ProcessorFunc[T]
	batchProcessor BatchProcessorFunc[T]
	batchSize      int64

	// Consumer-side observability (forwarded to each ring at Build time)
	onPressure        PressureCallback
	pressureThreshold float64
	onStall           StallCallback
	stallThreshold    time.Duration
	onBatchError      BatchErrorCallback[T]
	onPoisonSkip      PoisonSkipCallback[T]
}

func NewThreadedBuilder[T any](ringSize int64, numRings int) *ThreadedBuilder[T] {
	// WHY auto-size: Zephyros is a built-in driver, not a user-tunable knob.
	// Zero means "choose a sensible default" so the caller does not need to
	// understand ring-buffer internals.
	if ringSize <= 0 {
		ringSize = DefaultRingCapacity
	}

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

// WithBatchProcessor sets a batch processing function on the ThreadedBuilder.
// See Builder.WithBatchProcessor for the full contract.
func (b *ThreadedBuilder[T]) WithBatchProcessor(fn BatchProcessorFunc[T]) *ThreadedBuilder[T] {
	b.batchProcessor = fn
	return b
}

// WithOnBatchError registers a batch error callback on the ThreadedBuilder.
// See Builder.WithOnBatchError for details.
func (b *ThreadedBuilder[T]) WithOnBatchError(cb BatchErrorCallback[T]) *ThreadedBuilder[T] {
	b.onBatchError = cb
	return b
}

// WithOnPoisonSkip registers a quarantine callback on the ThreadedBuilder.
// See Builder.WithOnPoisonSkip for the full contract.
func (b *ThreadedBuilder[T]) WithOnPoisonSkip(cb PoisonSkipCallback[T]) *ThreadedBuilder[T] {
	b.onPoisonSkip = cb
	return b
}

func (b *ThreadedBuilder[T]) WithWorkers(numWorkers int) *ThreadedBuilder[T] {
	if numWorkers <= 0 {
		numWorkers = runtime.NumCPU()
	}
	b.numWorkers = numWorkers
	return b
}

// WithOnPressure registers a callback invoked per-ring when occupancy exceeds
// the given threshold. See Builder.WithOnPressure for full semantics.
func (b *ThreadedBuilder[T]) WithOnPressure(threshold float64, cb PressureCallback) *ThreadedBuilder[T] {
	if threshold <= 0 || threshold > 1 {
		threshold = 0.75
	}
	b.onPressure = cb
	b.pressureThreshold = threshold
	return b
}

// WithOnStall registers a callback invoked per-ring when no items are
// processed for longer than threshold. See Builder.WithOnStall for details.
func (b *ThreadedBuilder[T]) WithOnStall(threshold time.Duration, cb StallCallback) *ThreadedBuilder[T] {
	b.onStall = cb
	b.stallThreshold = threshold
	return b
}

// createRings builds N individual ring buffers with the correct processor
// and callback wiring. Extracted from Build() to keep cyclomatic complexity
// under 10.
func (b *ThreadedBuilder[T]) createRings(hasProcessor bool) ([]*Zephyros[T], error) {
	rings := make([]*Zephyros[T], b.numRings)

	for i := 0; i < b.numRings; i++ {
		builder := NewBuilder[T](b.ringSize).
			WithBatchSize(b.batchSize)

		// Wire the correct processor type.
		if hasProcessor {
			isolatedProcessor := func(item *T) {
				b.processor(item)
			}
			builder = builder.WithProcessor(isolatedProcessor)
		} else {
			builder = builder.WithBatchProcessor(b.batchProcessor)
			if b.onBatchError != nil {
				builder = builder.WithOnBatchError(b.onBatchError)
			}
			if b.onPoisonSkip != nil {
				builder = builder.WithOnPoisonSkip(b.onPoisonSkip)
			}
		}

		// Forward observability callbacks to each ring.
		if b.onPressure != nil {
			builder = builder.WithOnPressure(b.pressureThreshold, b.onPressure)
		}
		if b.onStall != nil {
			builder = builder.WithOnStall(b.stallThreshold, b.onStall)
		}

		ring, err := builder.Build()
		if err != nil {
			// Cleanup previously created rings.
			for j := 0; j < i; j++ {
				rings[j].Close()
			}
			return nil, err
		}
		// WHY set per-ring: the StallCallback receives the ring index so
		// the operator knows which producer is stuck.
		ring.stallRingIndex = i
		rings[i] = ring
	}
	return rings, nil
}

func (b *ThreadedBuilder[T]) Build() (*ThreadedZephyros[T], error) {
	// Exactly one processor must be set.
	hasProcessor := b.processor != nil
	hasBatch := b.batchProcessor != nil
	if !hasProcessor && !hasBatch {
		return nil, ErrMissingProcessor
	}
	if hasProcessor && hasBatch {
		return nil, fmt.Errorf("cannot set both ProcessorFunc and BatchProcessorFunc")
	}

	rings, err := b.createRings(hasProcessor)
	if err != nil {
		return nil, err
	}

	tz := &ThreadedZephyros[T]{
		rings:      rings,
		numRings:   b.numRings,
		numWorkers: b.numWorkers,
		processor:  b.processor,
		assigned:   make([]AtomicPaddedInt64, b.numRings),
	}

	// Initialize closed state
	tz.closed.Store(0)

	return tz, nil
}

// NewSafeWriter claims exclusive ownership of ring ringID and returns a
// SafeWriter bound to it. Panics if ringID is invalid or already assigned.
//
// WHY panic on double-assign: two goroutines sharing a raw ring violates the
// SPSC invariant and causes silent slot corruption. The panic surfaces the
// programming error immediately rather than letting it produce hard-to-debug
// data races. Use NewSafeWriterWithError in contexts where panics are not
// acceptable (e.g. operator-supplied ring IDs from config).
func (tz *ThreadedZephyros[T]) NewSafeWriter(ringID int) *SafeWriter[T] {
	if ringID < 0 || ringID >= tz.numRings {
		panic(fmt.Sprintf("CRITICAL BUG: invalid ringID %d (valid range: 0-%d)", ringID, tz.numRings-1))
	}
	if !tz.assigned[ringID].CompareAndSwap(0, 1) {
		panic(fmt.Sprintf("CRITICAL BUG: ringID %d is already assigned to a SafeWriter", ringID))
	}
	return &SafeWriter[T]{
		tz:     tz,
		ringID: ringID,
		ring:   tz.rings[ringID],
	}
}

// NewSafeWriterWithError claims exclusive ownership of ring ringID and returns
// a SafeWriter or an error. Returns ErrInvalidRingID for out-of-range IDs and
// ErrRingAlreadyAssigned if another SafeWriter already owns that ring.
func (tz *ThreadedZephyros[T]) NewSafeWriterWithError(ringID int) (*SafeWriter[T], error) {
	if ringID < 0 || ringID >= tz.numRings {
		return nil, fmt.Errorf("%w: %d (valid range: 0-%d)", ErrInvalidRingID, ringID, tz.numRings-1)
	}
	if !tz.assigned[ringID].CompareAndSwap(0, 1) {
		return nil, fmt.Errorf("%w: ring %d", ErrRingAlreadyAssigned, ringID)
	}
	return &SafeWriter[T]{
		tz:     tz,
		ringID: ringID,
		ring:   tz.rings[ringID],
	}, nil
}

// LoopProcess starts the Anemoi consumer goroutines (one per ring) and
// returns a channel that is closed once all consumers are ready to receive.
//
// WHY ready channel: the caller must not write to rings before the consumers
// are live. Without this barrier, items written in the window between
// LoopProcess() returning and the goroutines actually starting are processed
// correctly (the ring persists them), but the ready channel lets the caller
// know the system is fully up. The old pattern of time.Sleep(10ms) in tests
// is fragile; this is deterministic.
//
// The returned channel is closed, never sent to. Callers wait with:
//
//	<-tz.LoopProcess()
//
// or discard it if ordering guarantees are not needed.
func (tz *ThreadedZephyros[T]) LoopProcess() <-chan struct{} {
	ready := make(chan struct{})

	// startWg counts goroutines that have reached the hot loop.
	// It is separate from tz.wg (which tracks shutdown completion).
	var startWg sync.WaitGroup
	startWg.Add(tz.numRings)

	tz.workerChannels = make([]chan struct{}, tz.numRings)

	for i := 0; i < tz.numRings; i++ {
		stopChan := make(chan struct{})
		tz.workerChannels[i] = stopChan
		tz.wg.Add(1)

		go tz.runConsumer(i, stopChan, &startWg)
	}

	// Close ready once every goroutine has signalled.
	go func() {
		startWg.Wait()
		close(ready)
	}()

	return ready
}

// runConsumer is the per-ring consumer goroutine launched by LoopProcess.
// Separated to keep LoopProcess under the complexity limit.
//
// WHY exponential backoff: mirrors LoopProcess on the single-ring path.
// Audit events do not require sub-millisecond delivery -- the bottleneck
// is always the store (SQLite fsync). Burning a core on idle spinning
// raises CPU temperature, increases power draw, and starves other goroutines.
func (tz *ThreadedZephyros[T]) runConsumer(ringIndex int, stop <-chan struct{}, startWg *sync.WaitGroup) {
	defer tz.wg.Done()

	const (
		hotSpinLimit = 100
		goschedLimit = 1000
		idleSleep    = time.Millisecond
	)

	ring := tz.rings[ringIndex]

	// Dispatch to the correct consumer loop based on processor type.
	if ring.batchProcessor != nil {
		tz.runBatchConsumer(ring, stop, startWg, hotSpinLimit, goschedLimit, idleSleep)
	} else {
		tz.runItemConsumer(ring, stop, startWg, hotSpinLimit, goschedLimit, idleSleep)
	}
}

// runItemConsumer is the per-item consumer loop for ThreadedZephyros.
func (tz *ThreadedZephyros[T]) runItemConsumer(ring *Zephyros[T], stop <-chan struct{}, startWg *sync.WaitGroup, hotSpinLimit, goschedLimit int, idleSleep time.Duration) {
	spins := 0
	lastProgress := time.Now()
	startWg.Done()

	for {
		select {
		case <-stop:
			for ring.ProcessBatch() > 0 {
			}
			ring.Flush()
			return
		default:
			start := time.Now()
			processed := ring.ProcessBatch()
			if processed > 0 {
				ring.updateProcessorAvg(time.Since(start).Nanoseconds())
				spins = 0
				ring.checkPressure()
				lastProgress = time.Now()
			} else {
				lastProgress = ring.checkStall(0, lastProgress)
				spins = ring.idleBackoff(spins, hotSpinLimit, goschedLimit, idleSleep)
			}
		}
	}
}

// runBatchConsumer is the batch-mode consumer loop for ThreadedZephyros.
func (tz *ThreadedZephyros[T]) runBatchConsumer(ring *Zephyros[T], stop <-chan struct{}, startWg *sync.WaitGroup, hotSpinLimit, goschedLimit int, idleSleep time.Duration) {
	spins := 0
	lastProgress := time.Now()
	retryDelay := time.Duration(0)
	startWg.Done()

	for {
		select {
		case <-stop:
			for ring.ProcessBatchFunc() > 0 {
			}
			ring.Flush()
			return
		default:
			if retryDelay > 0 {
				time.Sleep(retryDelay)
			}

			start := time.Now()
			processed := ring.ProcessBatchFunc()
			if processed > 0 {
				ring.updateProcessorAvg(time.Since(start).Nanoseconds())
				spins = 0
				retryDelay = 0
				ring.checkPressure()
				lastProgress = time.Now()
			} else if ring.writerCursor.Load() > ring.readerCursor.Load() {
				retryDelay = ring.nextRetryDelay(retryDelay)
			} else {
				retryDelay = 0
				lastProgress = ring.checkStall(0, lastProgress)
				spins = ring.idleBackoff(spins, hotSpinLimit, goschedLimit, idleSleep)
			}
		}
	}
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
		totalStats[fmt.Sprintf("ring_%d_dropped", i)] = stats["dropped"]
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

// Write delivers one item to the owned ring. Returns false when the ring is
// full (back-pressure). Must be called from the goroutine that owns this
// SafeWriter — the SPSC invariant is structurally enforced, not checked at
// runtime (that would add overhead to every write).
func (s *SafeWriter[T]) Write(writerFunc func(*T)) bool {
	return s.ring.Write(writerFunc)
}

// WriteWait blocks until the item is successfully written, the context is
// cancelled, or the ring is closed. Returns nil on success.
// WHY: audit events must never be silently dropped. This is the primary
// write method for Metis integration -- fire-and-forget with guaranteed
// delivery (unless the process is shutting down).
func (s *SafeWriter[T]) WriteWait(ctx context.Context, writerFunc func(*T)) error {
	return s.ring.WriteWait(ctx, writerFunc)
}

// GetRingID returns the ring index this writer is bound to.
func (s *SafeWriter[T]) GetRingID() int {
	return s.ringID
}

// Dropped returns the number of items that were silently dropped on this ring
// because the buffer was full at write time. A non-zero value means the
// producer is outpacing the consumer; audit logs may have gaps.
func (s *SafeWriter[T]) Dropped() int64 {
	return s.ring.dropped.Load()
}

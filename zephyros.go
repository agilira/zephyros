// zephyros.go: Ultra-high performance MPSC lock-free ring buffer
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

// ErrClosed is returned by WriteWait when the ring is closed.
var ErrClosed = errors.New("ring buffer closed")

// ProcessorFunc is the ultra-fast processing function signature.
// Processes one item at a time. Mutually exclusive with BatchProcessorFunc.
type ProcessorFunc[T any] func(*T)

// BatchProcessorFunc receives a slice of contiguous items from the ring.
// The consumer calls it once per batch cycle with all available items.
//
// CONTRACT:
//   - Return nil on success. The consumer advances the ring cursor.
//   - Return non-nil error on failure. The consumer does NOT advance
//     the cursor. The SAME batch will be retried with exponential
//     backoff (1ms, 2ms, 4ms, ... capped at 1s). Items remain in
//     the ring, safe from overwrite, until the batch succeeds.
//   - The batch slice is borrowed from a sync.Pool. Do NOT retain it
//     after the function returns.
//   - The items in the slice are copies (not pointers into the ring).
//   - Panics are recovered. After 3 consecutive panics on the SAME
//     batch, the batch is skipped (poison batch protection).
type BatchProcessorFunc[T any] func(batch []T) error

// BatchErrorCallback is invoked when a batch fails (error or panic).
// WHY separate from PressureCallback: batch errors are persistence-layer
// failures (DB down, disk full). The operator needs the batch contents
// and the error for diagnosis. This callback is observability, not control.
type BatchErrorCallback[T any] func(batch []T, err error)

// PoisonSkipCallback is invoked ONCE when a poison batch is permanently
// skipped after 3 consecutive panics. This is the application's LAST CHANCE
// to preserve the batch for forensic analysis ("quarantine").
//
// WHY separate from BatchErrorCallback: OnBatchError fires on every retry
// attempt (transient). OnPoisonSkip fires once, irreversibly, when data is
// about to be lost. Mixing them in one callback forces an if-permanent
// branch that can be silently forgotten -- separate hooks make the intent
// impossible to confuse.
type PoisonSkipCallback[T any] func(batch []T, err error)

// PressureCallback is invoked by the consumer when ring occupancy exceeds
// the configured threshold. The consumer calls it at most once per batch
// cycle to avoid flooding. Parameters: current occupancy ratio (0.0-1.0)
// and the raw item count.
type PressureCallback func(ratio float64, items int64)

// StallCallback is invoked by the consumer when the ring makes no progress
// for longer than the configured threshold. Parameters: ring index (-1 for
// single-ring), current reader position, current writer position.
type StallCallback func(ringIndex int, readerPos int64, writerPos int64)

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

	// Processor function (mutually exclusive: exactly one must be set)
	processor      ProcessorFunc[T]
	batchProcessor BatchProcessorFunc[T]

	// Batch error monitoring callback (optional, only used with batchProcessor)
	onBatchError BatchErrorCallback[T]

	// Quarantine callback: fires once when a poison batch is permanently
	// skipped. The application should persist the batch for forensic analysis.
	onPoisonSkip PoisonSkipCallback[T]

	// batchPool recycles []T slices for batch copies. WHY sync.Pool: batch
	// copies happen on every ProcessBatchFunc cycle. Without pooling, each
	// cycle allocates and discards a slice, creating GC pressure proportional
	// to event throughput. The pool amortizes this to near-zero.
	batchPool sync.Pool

	// batchPanicCount tracks consecutive panics on the current batch. Reset
	// to 0 on success. Reaches 3 -> poison batch skipped.
	// Only accessed by the consumer goroutine -- no synchronization needed.
	batchPanicCount int

	// Batching configuration
	batchSize int64

	// Control
	closed AtomicPaddedInt64 // 0 = open, 1 = closed

	// Audit integrity: counts events discarded because the ring was full.
	// WHY monotonic counter instead of silent drop: a flood attacker can fill
	// the ring to hide subsequent events. The consumer reads this counter and
	// emits an AuditGap event, making the gap itself auditable.
	dropped AtomicPaddedInt64

	// --- Consumer-side observability callbacks (set at build time, immutable) ---

	// onPressure is called when occupancy exceeds pressureThreshold.
	// WHY consumer-side: only the consumer knows the true reader position.
	onPressure        PressureCallback
	pressureThreshold float64 // 0.0-1.0, default 0.75

	// onStall is called when no items are processed for stallThreshold duration.
	// WHY consumer-side: a stall means the consumer is alive but the producer
	// has stopped writing, or the ring is stuck. Either case is an operational
	// anomaly that Metis must know about.
	onStall        StallCallback
	stallThreshold time.Duration
	stallRingIndex int // -1 for single-ring Zephyros, 0..N for threaded

	// processorAvgNs tracks the EWMA of batch processing time in nanoseconds.
	// WHY EWMA: smooths out spikes (GC pauses, fsync outliers) while still
	// adapting to sustained changes in processor speed.
	// Only accessed by the consumer goroutine -- no synchronization needed.
	processorAvgNs int64

	// Cache line padding to prevent false sharing
	_ [64]byte
}

// Write adds an item to the ring buffer using lock-free atomic operations.
// Returns true on success, false when the ring is closed or buffer is full.
//
// ANEMOI INVARIANT: exactly one goroutine must call Write on a given ring.
// ThreadedZephyros enforces this by construction (one ring per producer).
// Violating the invariant produces silent data corruption.
//
// WHY Add-then-rollback (not peek-then-claim):
// The atomic Add gives each producer a globally unique sequence -- this is
// the correct MPSC algorithm. Peeking with Load + conditional Add would let
// two concurrent goroutines claim the SAME sequence, creating a slot race.
//
// The rollback (Add(-1) when full) is safe under the Anemoi invariant because
// there is exactly one producer per ring: only one goroutine can ever do the
// rollback, so the ABA window (two concurrent rollbacks corrupting the counter)
// is structurally impossible. Callers using raw Zephyros with multiple
// concurrent producers MUST use ThreadedZephyros instead.
func (z *Zephyros[T]) Write(writerFunc func(*T)) bool {
	if z.closed.Load() != 0 {
		return false
	}

	// Atomically claim a unique sequence. Under the Anemoi invariant this
	// is a simple increment; with multiple producers each gets a distinct
	// slot via the atomic (MPSC-safe).
	sequence := z.writerCursor.Add(1) - 1

	// Lap check: if we are capacity slots ahead of the consumer, the ring
	// is full. Roll back so we do not ghost-claim a slot the consumer
	// would never see published (availableBuffer stays at sentinel -2).
	if sequence >= z.readerCursor.Load()+z.capacity {
		z.writerCursor.Add(-1)
		z.dropped.Add(1) // Record the gap for audit integrity.
		return false
	}

	// Publish: write the user value then mark the slot visible.
	// The consumer scans availableBuffer for contiguous sequences and
	// will not advance past this slot until Store() below completes.
	writerFunc(&z.buffer[sequence&z.mask])
	z.availableBuffer[sequence&z.mask].Store(sequence)

	return true
}

// WriteWait blocks until the item is successfully written, the context is
// cancelled, or the ring is closed. Returns nil on success.
//
// WHY this exists: audit systems must never silently drop events. Write()
// returns false on back-pressure, forcing the caller to build retry logic.
// WriteWait encapsulates that retry with adaptive backoff so the caller can
// simply write-and-forget, trusting the ring to absorb bursts.
//
// BACKOFF PROFILE (same as consumer idle loop for symmetry):
//
//	0-99  attempts : Gosched (cooperative yield, near-zero latency)
//	100-999        : 100us sleep (light back-off, lets consumer catch up)
//	1000+          : 1ms sleep (steady state, near-zero CPU)
//
// WHY no dropped counter increment: WriteWait retries until success, so no
// event is lost. The dropped counter in Write() counts fire-and-forget drops
// only. If the caller wants non-blocking semantics, they use Write().
func (z *Zephyros[T]) WriteWait(ctx context.Context, writerFunc func(*T)) error {
	const (
		goschedLimit = 100
		lightSleep   = 100 * time.Microsecond
		lightLimit   = 1000
		heavySleep   = time.Millisecond
	)

	attempts := 0
	for {
		if z.closed.Load() != 0 {
			return ErrClosed
		}

		if z.Write(writerFunc) {
			return nil
		}

		// Check context before sleeping -- fail fast on cancellation.
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		attempts++
		switch {
		case attempts < goschedLimit:
			runtime.Gosched()
		case attempts < lightLimit:
			time.Sleep(lightSleep)
		default:
			time.Sleep(heavySleep)
		}
	}
}

// Flush ensures all pending writes are visible to reader (MPSC compatible)
func (z *Zephyros[T]) Flush() {
	// In MPSC, flush is automatic via commitCursor ordering
	// This method exists for API compatibility
	// All writes are already properly committed via Write() method
}

// ProcessBatch drains a batch of published items from the ring and invokes
// the processor on each one. Returns the number of items processed.
// Called exclusively by the single consumer goroutine -- not thread-safe.
//
// WHY split: the original function had cyclomatic complexity 11. Each
// sub-function below has complexity <= 5, keeping audit-readability high.
func (z *Zephyros[T]) ProcessBatch() int {
	current := z.readerCursor.Load()
	writerPos := z.writerCursor.Load()

	batchSize := z.adaptBatchSize(current, writerPos)
	maxScan := writerPos
	if current+batchSize < writerPos {
		maxScan = current + batchSize
	}

	end := z.scanAvailable(current, maxScan)
	if end < current {
		return 0
	}

	processed := z.executeAndReset(current, end)
	z.readerCursor.Store(end + 1)
	return processed
}

// ProcessBatchFunc drains available items from the ring, copies them into a
// pooled buffer, and passes the batch to the BatchProcessorFunc. Returns the
// number of items in the batch (0 if nothing was available).
//
// ERROR SEMANTICS: if the batch processor returns an error, the cursor is NOT
// advanced. The same batch will be presented again on the next call. The
// caller (LoopProcess/runConsumer) handles retry backoff.
//
// PANIC SEMANTICS: panics inside the batch processor are recovered and treated
// as errors. After 3 consecutive panics on the SAME batch (same reader cursor
// position), the batch is skipped to prevent infinite poison-batch loops.
// The skip is reported via OnBatchError for audit visibility.
//
// Called exclusively by the single consumer goroutine -- not thread-safe.
func (z *Zephyros[T]) ProcessBatchFunc() int {
	current := z.readerCursor.Load()
	writerPos := z.writerCursor.Load()

	batchSize := z.adaptBatchSize(current, writerPos)
	maxScan := writerPos
	if current+batchSize < writerPos {
		maxScan = current + batchSize
	}

	end := z.scanAvailable(current, maxScan)
	if end < current {
		return 0
	}

	count := int(end-current) + 1
	batch := z.copyBatchFromRing(current, count)

	err, wasPanic := z.invokeBatchProcessor(batch)
	if err != nil {
		z.handleBatchError(batch, err, wasPanic, current)
		returnBatch := batch[:0]
		z.batchPool.Put(&returnBatch) // Return buffer even on error.
		return 0                      // Cursor NOT advanced.
	}

	// Success: reset availability markers and advance cursor.
	z.resetAvailability(current, end)
	z.readerCursor.Store(end + 1)
	z.batchPanicCount = 0 // Reset poison counter on success.
	returnBatch := batch[:0]
	z.batchPool.Put(&returnBatch)
	return count
}

// copyBatchFromRing copies count items starting at current out of the ring
// into a pooled []T slice. Handles wrap-around transparently.
func (z *Zephyros[T]) copyBatchFromRing(current int64, count int) []T {
	raw := z.batchPool.Get()
	var batch []T
	if raw != nil {
		batch = *raw.(*[]T)
	}
	if cap(batch) < count {
		batch = make([]T, 0, count)
	}
	batch = batch[:count]

	for i := 0; i < count; i++ {
		batch[i] = z.buffer[(current+int64(i))&z.mask]
	}
	return batch
}

// invokeBatchProcessor calls the BatchProcessorFunc with panic recovery.
// Returns the error and a boolean indicating whether the error came from a
// panic (true) vs a normal return (false).
func (z *Zephyros[T]) invokeBatchProcessor(batch []T) (retErr error, wasPanic bool) {
	defer func() {
		if r := recover(); r != nil {
			retErr = fmt.Errorf("batch processor panic: %v", r)
			wasPanic = true
		}
	}()
	return z.batchProcessor(batch), false
}

// handleBatchError reports the error and manages the 3-strike poison counter.
// Only PANICS count toward the 3-strike rule. Normal errors (DB down, disk
// full) are retried indefinitely via exponential backoff -- the persistence
// layer will recover eventually.
//
// WHY distinguish panic from error: a normal error (SQLITE_BUSY, network
// timeout) is transient by nature. The ring holds the events safely while
// the consumer retries. A panic indicates corrupt data or a logic bug that
// will NEVER self-heal. After 3 panics, skipping the batch is the only way
// to prevent the consumer from freezing forever on a poison batch.
func (z *Zephyros[T]) handleBatchError(batch []T, err error, wasPanic bool, cursorPos int64) {
	if z.onBatchError != nil {
		z.onBatchError(batch, err)
	}

	if !wasPanic {
		return // Normal error: retry via backoff, no strike count.
	}

	z.batchPanicCount++

	// WHY 3 strikes: a transient panic (OOM, nil ptr from corrupted DB state)
	// deserves a retry. But a batch that ALWAYS panics (corrupt data, logic
	// bug) must be skipped to prevent the consumer from freezing. 3 is
	// conservative: gives the system 2 extra chances.
	if z.batchPanicCount >= 3 {
		// WHY quarantine BEFORE advancing cursor: once the cursor moves,
		// the batch data is gone forever. This is the last chance.
		// WHY recover: if the quarantine callback itself panics, the
		// cursor must still advance. A crashing quarantine handler must
		// not become a secondary denial-of-service freezing the consumer.
		if z.onPoisonSkip != nil {
			func() {
				defer func() { _ = recover() }()
				z.onPoisonSkip(batch, err)
			}()
		}
		z.resetAvailability(cursorPos, cursorPos+int64(len(batch))-1)
		z.readerCursor.Store(cursorPos + int64(len(batch)))
		z.batchPanicCount = 0
	}
}

// resetAvailability resets availability markers for processed slots.
func (z *Zephyros[T]) resetAvailability(start, end int64) {
	for seq := start; seq <= end; seq++ {
		z.availableBuffer[seq&z.mask].Store(-2)
	}
}

// adaptBatchSize returns a batch size tuned to the current buffer occupancy.
// WHY: draining aggressively when the buffer is under pressure prevents
// the producer from stalling while keeping latency low when it is quiet.
func (z *Zephyros[T]) adaptBatchSize(current, writerPos int64) int64 {
	occupancy := writerPos - current
	switch {
	case occupancy > z.capacity*3/4:
		// Buffer >75% full: emergency drain -- use the largest safe batch.
		half := z.capacity / 2
		if z.batchSize*4 < half {
			return z.batchSize * 4
		}
		return half
	case occupancy < 128:
		// Nearly empty: ensure we still collect whatever is there.
		return 128
	default:
		return z.batchSize
	}
}

// scanAvailable walks the availableBuffer for the contiguous published range
// [current, maxScan) and returns the last sequence that is ready to consume.
// Returns current-1 if nothing is available (sentinel: < current).
//
// WHY stop at first gap: the ring is SPSC ordered. A gap means the producer
// has not yet finished writing that slot. We must not skip ahead.
func (z *Zephyros[T]) scanAvailable(current, maxScan int64) int64 {
	available := current - 1
	for seq := current; seq < maxScan; seq++ {
		// Prefetch the availability marker 4 slots ahead to warm L1 cache.
		if seq+4 < maxScan {
			_ = z.availableBuffer[(seq+4)&z.mask].Load()
		}
		if z.availableBuffer[seq&z.mask].Load() != seq {
			break
		}
		available = seq
	}
	return available
}

// executeAndReset processes items [current, endSequence] with 4x loop unrolling
// and resets each availability marker after processing.
// Returns the number of items processed.
//
// WHY unroll: 4x unrolling lets the CPU overlap the independent Store() calls
// with the next iteration's Load(), improving instruction-level parallelism.
func (z *Zephyros[T]) executeAndReset(current, endSequence int64) int {
	processed := int(endSequence-current) + 1
	seq := current
	chunks := processed >> 2
	remainder := processed & 3

	for i := 0; i < chunks; i++ {
		if seq+8 <= endSequence {
			_ = z.buffer[(seq+8)&z.mask] // data prefetch 8 slots ahead
		}
		idx1, idx2 := seq&z.mask, (seq+1)&z.mask
		idx3, idx4 := (seq+2)&z.mask, (seq+3)&z.mask
		z.processor(&z.buffer[idx1])
		z.processor(&z.buffer[idx2])
		z.processor(&z.buffer[idx3])
		z.processor(&z.buffer[idx4])
		z.availableBuffer[idx1].Store(-2)
		z.availableBuffer[idx2].Store(-2)
		z.availableBuffer[idx3].Store(-2)
		z.availableBuffer[idx4].Store(-2)
		seq += 4
	}
	for i := 0; i < remainder; i++ {
		idx := seq & z.mask
		z.processor(&z.buffer[idx])
		z.availableBuffer[idx].Store(-2)
		seq++
	}
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
} // LoopProcess runs the consumer loop until Close() is called.
// Returns only after all buffered items have been drained.
//
// WHY exponential backoff: the audit use case does not require sub-millisecond
// latency. An event written to the audit ring eventually reaches SQLite -- the
// disk is the bottleneck, not the ring. Burning a CPU core to shave a few
// microseconds off idle polling is a bad trade for a system that may run for
// months. The backoff profile is:
//
//	0-99 empty batches : hot spin (keeps latency low on bursts)
//	100-999            : cooperative yield (hand CPU to scheduler)
//	1000+              : 1 ms sleep (near-zero CPU when idle)
func (z *Zephyros[T]) LoopProcess() {
	const (
		hotSpinLimit = 100
		goschedLimit = 1000
		idleSleep    = time.Millisecond
	)

	// WHY dispatch once at start: checking batchProcessor != nil on every
	// iteration adds a branch to the hot path for the common case (ProcessorFunc).
	// Selecting the consumer function once avoids that branch entirely.
	consumeFn := z.loopProcessorFunc(hotSpinLimit, goschedLimit, idleSleep)
	consumeFn()
}

// loopProcessorFunc returns the appropriate hot-loop function based on which
// processor is configured. Keeps LoopProcess itself under the complexity limit.
func (z *Zephyros[T]) loopProcessorFunc(hotSpinLimit, goschedLimit int, idleSleep time.Duration) func() {
	if z.batchProcessor != nil {
		return func() { z.loopBatchProcessor(hotSpinLimit, goschedLimit, idleSleep) }
	}
	return func() { z.loopItemProcessor(hotSpinLimit, goschedLimit, idleSleep) }
}

// loopItemProcessor is the original per-item consumer loop.
func (z *Zephyros[T]) loopItemProcessor(hotSpinLimit, goschedLimit int, idleSleep time.Duration) {
	spins := 0
	lastProgress := time.Now()
	for z.closed.Load() == 0 {
		start := time.Now()
		processed := z.ProcessBatch()
		if processed > 0 {
			z.updateProcessorAvg(time.Since(start).Nanoseconds())
			spins = 0
			z.checkPressure()
			lastProgress = time.Now()
			continue
		}
		lastProgress = z.checkStall(0, lastProgress)
		spins = z.idleBackoff(spins, hotSpinLimit, goschedLimit, idleSleep)
	}

	// Drain: two consecutive empty scans mean the ring is truly empty.
	empty := 0
	for empty < 2 {
		if z.ProcessBatch() > 0 {
			empty = 0
		} else {
			empty++
			runtime.Gosched()
		}
	}
}

// loopBatchProcessor is the batch-mode consumer loop with retry + backoff.
func (z *Zephyros[T]) loopBatchProcessor(hotSpinLimit, goschedLimit int, idleSleep time.Duration) {
	spins := 0
	lastProgress := time.Now()
	retryDelay := time.Duration(0) // 0 = no retry pending

	for z.closed.Load() == 0 {
		// WHY retry delay here: if the last ProcessBatchFunc returned 0
		// because of an error (cursor not advanced), we sleep before retrying.
		// This implements the exponential backoff (1ms -> 1s cap).
		if retryDelay > 0 {
			time.Sleep(retryDelay)
		}

		start := time.Now()
		processed := z.ProcessBatchFunc()
		if processed > 0 {
			z.updateProcessorAvg(time.Since(start).Nanoseconds())
			spins = 0
			retryDelay = 0
			z.checkPressure()
			lastProgress = time.Now()
			continue
		}

		// processed == 0: either nothing available, or batch error (cursor stuck).
		// Distinguish by checking if there ARE items in the ring.
		if z.writerCursor.Load() > z.readerCursor.Load() {
			// Items exist but ProcessBatchFunc returned 0 -> error/retry.
			retryDelay = z.nextRetryDelay(retryDelay)
		} else {
			// Genuinely empty ring.
			retryDelay = 0
			lastProgress = z.checkStall(0, lastProgress)
			spins = z.idleBackoff(spins, hotSpinLimit, goschedLimit, idleSleep)
		}
	}

	// Drain: process everything before returning.
	empty := 0
	for empty < 2 {
		if z.ProcessBatchFunc() > 0 {
			empty = 0
		} else {
			empty++
			runtime.Gosched()
		}
	}
}

// nextRetryDelay computes exponential backoff for batch processor retries.
// Starts at 1ms, doubles each time, capped at 1s.
// WHY cap at 1s: longer delays increase latency without helping recovery.
// The ring holds events safely (cursor not advanced), so there is no data
// loss risk -- we are just waiting for the persistence layer to recover.
func (z *Zephyros[T]) nextRetryDelay(current time.Duration) time.Duration {
	const (
		minDelay = time.Millisecond
		maxDelay = time.Second
	)
	if current == 0 {
		return minDelay
	}
	next := current * 2
	if next > maxDelay {
		return maxDelay
	}
	return next
}

// idleBackoff advances the backoff state when no items were available and
// returns the updated spin counter. Extracted to keep LoopProcess simple.
//
// WHY adaptive sleep: when the processor is slow (e.g. SQLite fsync ~5ms),
// polling every 1ms wastes CPU. The EWMA-tracked processorAvgNs lets the
// idle sleep match the natural rhythm of the consumer-processor pair.
func (z *Zephyros[T]) idleBackoff(spins, hotLimit, goschedLimit int, sleepDur time.Duration) int {
	spins++
	switch {
	case spins < hotLimit:
		// Hot spin: keeps latency low during bursts.
	case spins < goschedLimit:
		runtime.Gosched()
	default:
		// Adaptive: sleep proportional to processor speed, clamped to [1ms, 10ms].
		sleep := sleepDur
		if avg := z.processorAvgNs; avg > int64(sleepDur) {
			sleep = time.Duration(avg)
			if sleep > 10*time.Millisecond {
				sleep = 10 * time.Millisecond
			}
		}
		time.Sleep(sleep)
		spins = 0
	}
	return spins
}

// updateProcessorAvg updates the EWMA of batch processing time.
// Uses integer arithmetic (7/8 old + 1/8 new) for zero-alloc operation.
// Only called from the consumer goroutine -- no synchronization needed.
func (z *Zephyros[T]) updateProcessorAvg(elapsedNs int64) {
	z.processorAvgNs = z.processorAvgNs*7/8 + elapsedNs/8
}

// checkPressure invokes onPressure if buffer occupancy exceeds the threshold.
// Called once per batch cycle by the consumer to avoid flooding the callback.
func (z *Zephyros[T]) checkPressure() {
	if z.onPressure == nil {
		return
	}
	items := z.writerCursor.Load() - z.readerCursor.Load()
	ratio := float64(items) / float64(z.capacity)
	if ratio >= z.pressureThreshold {
		z.onPressure(ratio, items)
	}
}

// checkStall invokes onStall if no items have been processed for longer than
// stallThreshold. Returns the updated lastProgress time.
// WHY separate method: keeps LoopProcess and runConsumer under complexity 10.
func (z *Zephyros[T]) checkStall(processed int, lastProgress time.Time) time.Time {
	if processed > 0 {
		return time.Now()
	}
	if z.onStall == nil || z.stallThreshold <= 0 {
		return lastProgress
	}
	if time.Since(lastProgress) >= z.stallThreshold {
		z.onStall(z.stallRingIndex, z.readerCursor.Load(), z.writerCursor.Load())
		// Reset so we do not fire on every spin cycle.
		return time.Now()
	}
	return lastProgress
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
		"writer_position": writerPos,
		"reader_position": readerPos,
		"buffer_size":     z.capacity,
		"items_buffered":  writerPos - readerPos,
		"dropped":         z.dropped.Load(), // Events lost due to full ring.
		"closed":          z.closed.Load(),
	}
}

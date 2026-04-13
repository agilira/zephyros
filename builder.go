// builder.go: Builder pattern for ZEPHYROS configuration
//
// Copyright (c) 2025 AGILira
// Series: an AGLIra fragment
// SPDX-License-Identifier: MPL-2.0

package zephyros

import (
	"fmt"
	"time"
)

// DefaultRingCapacity is the ring size used when the caller passes 0 (or
// negative) to a builder. 16384 slots absorbs audit-event bursts from
// hundreds of concurrent producers without losing events, yet each ring
// consumes only a few MB for typical event structs (~200 bytes each).
// WHY 16384: power of two for bit-masking, fits comfortably in L2 cache
// per ring, and provides ~3 seconds of headroom at 5000 events/sec.
const DefaultRingCapacity int64 = 16384

var (
	// ErrCapacity is returned when capacity is not a power of two
	ErrCapacity = fmt.Errorf("capacity must be a power of two")

	// ErrMissingProcessor is returned when no processor function is provided
	ErrMissingProcessor = fmt.Errorf("missing processor function")
)

// Builder builds a NOTUS MPSC ring buffer with fluent configuration
type Builder[T any] struct {
	capacity       int64
	processor      ProcessorFunc[T]
	batchProcessor BatchProcessorFunc[T]
	batchSize      int64 // Batch size for processing

	// Consumer-side observability (immutable after Build)
	onPressure        PressureCallback
	pressureThreshold float64
	onStall           StallCallback
	stallThreshold    time.Duration
	onBatchError      BatchErrorCallback[T]
	onPoisonSkip      PoisonSkipCallback[T]
} // NewBuilder creates a new builder with the specified capacity.
// Pass capacity <= 0 for automatic sizing (DefaultRingCapacity).
func NewBuilder[T any](capacity int64) *Builder[T] {
	// WHY auto-size here (not in Build): the batch-size heuristic below
	// depends on the resolved capacity, so it must be known at construction.
	if capacity <= 0 {
		capacity = DefaultRingCapacity
	}

	// OPTIMIZATION: Intelligent default batch size based on capacity
	defaultBatchSize := int64(64) // Safe default
	if capacity >= 1024 {
		defaultBatchSize = 256 // Optimal for larger buffers
	} else if capacity >= 64 {
		defaultBatchSize = 16 // Appropriate for small buffers
	} else if capacity < 64 {
		defaultBatchSize = 1 // Minimal for very small buffers
	}

	return &Builder[T]{
		capacity:  capacity,
		batchSize: defaultBatchSize,
	}
}

// WithProcessor sets the processing function for items
func (b *Builder[T]) WithProcessor(processor ProcessorFunc[T]) *Builder[T] {
	b.processor = processor
	return b
}

// WithBatchSize sets the batch size for processing
func (b *Builder[T]) WithBatchSize(batchSize int64) *Builder[T] {
	b.batchSize = batchSize
	return b
}

// WithBatchProcessor sets a batch processing function. Mutually exclusive
// with WithProcessor -- Build() rejects configurations with both or neither.
// WHY: enables single-transaction persistence for entire batches (1 fsync
// per N events instead of N fsyncs). This is the Metis audit pipeline path.
func (b *Builder[T]) WithBatchProcessor(fn BatchProcessorFunc[T]) *Builder[T] {
	b.batchProcessor = fn
	return b
}

// WithOnBatchError registers a callback invoked when BatchProcessorFunc
// returns an error or panics. The callback receives the failed batch and
// the error. Used for monitoring and alerting, not for control flow.
func (b *Builder[T]) WithOnBatchError(cb BatchErrorCallback[T]) *Builder[T] {
	b.onBatchError = cb
	return b
}

// WithOnPoisonSkip registers a quarantine callback invoked ONCE when a poison
// batch is permanently skipped after 3 consecutive panics. The callback
// receives the doomed batch and the last panic error. The application MUST
// persist the batch for forensic analysis -- this is the LAST CHANCE before
// data loss.
func (b *Builder[T]) WithOnPoisonSkip(cb PoisonSkipCallback[T]) *Builder[T] {
	b.onPoisonSkip = cb
	return b
}

// WithOnPressure registers a callback invoked by the consumer when ring
// occupancy exceeds the given threshold (0.0-1.0). The consumer checks
// once per batch cycle to avoid flooding. Pass threshold <= 0 to use the
// default (0.75). The callback must be safe to call from a single goroutine.
func (b *Builder[T]) WithOnPressure(threshold float64, cb PressureCallback) *Builder[T] {
	if threshold <= 0 || threshold > 1 {
		threshold = 0.75
	}
	b.onPressure = cb
	b.pressureThreshold = threshold
	return b
}

// WithOnStall registers a callback invoked by the consumer when the ring
// makes no progress (zero items processed) for longer than threshold.
// WHY: detects dead producers, blocked writerFuncs, or consumer starvation.
// The callback must be safe to call from a single goroutine.
func (b *Builder[T]) WithOnStall(threshold time.Duration, cb StallCallback) *Builder[T] {
	b.onStall = cb
	b.stallThreshold = threshold
	return b
}

// validateBuilder checks capacity, processor exclusivity, and batch size.
// Extracted from Build() to keep cyclomatic complexity under 10.
func (b *Builder[T]) validateBuilder() error {
	if b.capacity <= 0 || (b.capacity&(b.capacity-1)) != 0 {
		return ErrCapacity
	}

	hasProcessor := b.processor != nil
	hasBatch := b.batchProcessor != nil
	if !hasProcessor && !hasBatch {
		return ErrMissingProcessor
	}
	if hasProcessor && hasBatch {
		return fmt.Errorf("cannot set both ProcessorFunc and BatchProcessorFunc")
	}

	if b.batchSize <= 0 {
		return fmt.Errorf("batch size must be positive, got %d", b.batchSize)
	}
	if b.batchSize > b.capacity {
		return fmt.Errorf("batch size (%d) cannot exceed capacity (%d)", b.batchSize, b.capacity)
	}
	return nil
}

// Build creates and initializes a new ZEPHYROS disruptor
func (b *Builder[T]) Build() (*Zephyros[T], error) {
	if err := b.validateBuilder(); err != nil {
		return nil, err
	}

	// OPTIMIZATION: Pre-compute mask during construction
	mask := b.capacity - 1

	z := &Zephyros[T]{
		buffer:            make([]T, b.capacity),
		capacity:          b.capacity,
		mask:              mask,
		processor:         b.processor,
		batchProcessor:    b.batchProcessor,
		onBatchError:      b.onBatchError,
		onPoisonSkip:      b.onPoisonSkip,
		batchSize:         b.batchSize,
		availableBuffer:   make([]AtomicPaddedInt64, b.capacity),
		onPressure:        b.onPressure,
		pressureThreshold: b.pressureThreshold,
		onStall:           b.onStall,
		stallThreshold:    b.stallThreshold,
		stallRingIndex:    -1, // single-ring sentinel
	}

	// OPTIMIZATION: Batch initialize atomic fields for better cache locality
	z.writerCursor.Store(0) // Writer cursor starts at 0 (next Add(1) gives 1, sequence 0)
	z.readerCursor.Store(0) // Reader cursor starts at 0

	// CRITICAL: Initialize availability buffer correctly
	// Slots start as unavailable with sequence numbers that will never match
	// Use sequence -2 for all slots, which is less than any valid sequence (0, 1, 2, ...)
	for i := range z.availableBuffer {
		z.availableBuffer[i].Store(-2)
	}
	z.closed.Store(0)

	return z, nil
}

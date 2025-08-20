// builder.go: Builder pattern for ZEPHYROS configuration
//
// Copyright (c) 2025 AGILira
// Series: an AGLIra fragment
// SPDX-License-Identifier: MPL-2.0

package zephyros

import (
	"fmt"
)

var (
	// ErrCapacity is returned when capacity is not a power of two
	ErrCapacity = fmt.Errorf("capacity must be a power of two")

	// ErrMissingProcessor is returned when no processor function is provided
	ErrMissingProcessor = fmt.Errorf("missing processor function")
)

// Builder builds a NOTUS MPSC ring buffer with fluent configuration
type Builder[T any] struct {
	capacity  int64
	processor ProcessorFunc[T]
	batchSize int64 // Batch size for processing
} // NewBuilder creates a new NOTUS builder with the specified capacity
func NewBuilder[T any](capacity int64) *Builder[T] {
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

// Build creates and initializes a new ZEPHYROS disruptor
func (b *Builder[T]) Build() (*Zephyros[T], error) {
	// OPTIMIZATION: Single capacity validation with bit operations
	if b.capacity <= 0 || (b.capacity&(b.capacity-1)) != 0 {
		return nil, ErrCapacity
	}

	// OPTIMIZATION: Early validation to avoid allocation if invalid
	if b.processor == nil {
		return nil, ErrMissingProcessor
	}

	// OPTIMIZATION: Combined validation checks
	if b.batchSize <= 0 || b.batchSize > b.capacity {
		if b.batchSize <= 0 {
			return nil, fmt.Errorf("batch size must be positive, got %d", b.batchSize)
		}
		return nil, fmt.Errorf("batch size (%d) cannot exceed capacity (%d)", b.batchSize, b.capacity)
	}

	// OPTIMIZATION: Pre-compute mask during construction
	mask := b.capacity - 1

	z := &Zephyros[T]{
		buffer:          make([]T, b.capacity),
		capacity:        b.capacity,
		mask:            mask,
		processor:       b.processor,
		batchSize:       b.batchSize,
		availableBuffer: make([]AtomicPaddedInt64, b.capacity),
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

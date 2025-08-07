// batch.go: Batch processing for zephyros operation pool library
//
// Copyright (c) 2025 AGILira
// Licensed under the Business Source License (BSL). Change Date: NEVER

package zephyros

import (
	"context"
	"fmt"
	"sync"
	"time"

	goerrors "github.com/agilira/go-errors"
)

// BatchProcessor handles batch processing of operations
type BatchProcessor struct {
	config     BatchConfig
	operations chan Operation
	results    chan OperationResult
	mu         sync.RWMutex
	closed     bool
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	pool       *OperationPool
}

// NewBatchProcessor creates a new batch processor with the given configuration
func NewBatchProcessor(config BatchConfig) *BatchProcessor {
	if config.BatchSize <= 0 {
		config.BatchSize = 10
	}
	if config.BatchTimeout <= 0 {
		config.BatchTimeout = 100 * time.Millisecond
	}
	if config.FlushInterval <= 0 {
		config.FlushInterval = 50 * time.Millisecond
	}
	if config.MaxBatchSize <= 0 {
		config.MaxBatchSize = 100
	}
	ctx, cancel := context.WithCancel(context.Background())
	bp := &BatchProcessor{
		config:     config,
		operations: make(chan Operation, config.MaxBatchSize*10),
		results:    make(chan OperationResult, config.MaxBatchSize*10),
		ctx:        ctx,
		cancel:     cancel,
	}
	if config.EnableBatchProcessing {
		bp.wg.Add(1)
		go bp.processBatches()
	}
	return bp
}

func (bp *BatchProcessor) processBatches() {
	defer bp.wg.Done()
	ticker := time.NewTicker(bp.config.FlushInterval)
	defer ticker.Stop()
	var batch []Operation
	batchTimer := time.NewTimer(bp.config.BatchTimeout)
	defer batchTimer.Stop()
	for {
		select {
		case op, ok := <-bp.operations:
			if !ok {
				if len(batch) > 0 {
					bp.processBatch(batch)
				}
				return
			}
			batch = append(batch, op)
			if len(batch) >= bp.config.BatchSize {
				bp.processBatch(batch)
				batch = batch[:0]
				batchTimer.Reset(bp.config.BatchTimeout)
			}
		case <-batchTimer.C:
			if len(batch) > 0 {
				bp.processBatch(batch)
				batch = batch[:0]
			}
			batchTimer.Reset(bp.config.BatchTimeout)
		case <-ticker.C:
			if len(batch) > 0 {
				bp.processBatch(batch)
				batch = batch[:0]
			}
		case <-bp.ctx.Done():
			if len(batch) > 0 {
				bp.processBatch(batch)
			}
			return
		}
	}
}

func (bp *BatchProcessor) processBatch(operations []Operation) {
	if len(operations) == 0 {
		return
	}
	results := make([]OperationResult, len(operations))
	for i, op := range operations {
		start := time.Now()
		var result OperationResult
		var err error
		if bp.pool != nil && bp.pool.handler != nil {
			func() {
				defer func() {
					if r := recover(); r != nil {
						result = OperationResult{
							OperationID: op.ID,
							Success:     false,
							Error:       fmt.Errorf("handler panic: %v", r),
							Duration:    time.Since(start),
						}
						err = fmt.Errorf("handler panic: %v", r)
					}
				}()
				result, err = bp.pool.handler.Process(bp.ctx, op)
			}()
			if err != nil {
				result = OperationResult{
					OperationID: op.ID,
					Success:     false,
					Error:       err,
					Duration:    time.Since(start),
				}
			}
		} else {
			result = OperationResult{
				OperationID: op.ID,
				Success:     true,
				Data:        op.Value,
				Duration:    time.Since(start),
			}
		}
		results[i] = result
		// Update metrics for the processed operation
		if bp.pool != nil {
			if err != nil {
				bp.pool.metrics.IncrementFailedOps()
			}
			bp.pool.metrics.IncrementProcessedOps()
		}
	}
	for _, result := range results {
		select {
		case bp.results <- result:
		case <-bp.ctx.Done():
			return
		}
	}
}

// Submit submits an operation to the batch processor
func (bp *BatchProcessor) Submit(ctx context.Context, op Operation) error {
	if ctx == nil {
		richErr := goerrors.New(ErrCodeContextNil, "context cannot be nil")
		return fmt.Errorf("%w: %w", ErrContextNil, richErr)
	}
	bp.mu.RLock()
	if bp.closed {
		bp.mu.RUnlock()
		richErr := goerrors.New(ErrCodePoolClosed, "batch processor is closed")
		return fmt.Errorf("%w: %w", ErrPoolClosed, richErr)
	}
	bp.mu.RUnlock()
	select {
	case bp.operations <- op:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-bp.ctx.Done():
		richErr := goerrors.New(ErrCodePoolClosed, "batch processor is closed")
		return fmt.Errorf("%w: %w", ErrPoolClosed, richErr)
	}
}

// GetResult retrieves a result from the batch processor
func (bp *BatchProcessor) GetResult(ctx context.Context) (OperationResult, error) {
	if ctx == nil {
		richErr := goerrors.New(ErrCodeContextNil, "context cannot be nil")
		return OperationResult{}, fmt.Errorf("%w: %w", ErrContextNil, richErr)
	}
	timeout := bp.config.BatchTimeout * 5
	if timeout < 500*time.Millisecond {
		timeout = 500 * time.Millisecond
	}
	select {
	case result, ok := <-bp.results:
		if !ok {
			richErr := goerrors.New(ErrCodePoolClosed, "results channel closed")
			return OperationResult{}, fmt.Errorf("%w: %w", ErrPoolClosed, richErr)
		}
		return result, nil
	case <-ctx.Done():
		return OperationResult{}, ctx.Err()
	case <-bp.ctx.Done():
		richErr := goerrors.New(ErrCodePoolClosed, "batch processor is closed")
		return OperationResult{}, fmt.Errorf("%w: %w", ErrPoolClosed, richErr)
	case <-time.After(timeout):
		richErr := goerrors.New(ErrCodeBatchTimeout, "result retrieval timeout")
		return OperationResult{}, fmt.Errorf("%w: %w", ErrBatchTimeout, richErr)
	}
}

// Close closes the batch processor and waits for all operations to complete
func (bp *BatchProcessor) Close() error {
	bp.mu.Lock()
	if bp.closed {
		bp.mu.Unlock()
		return nil
	}
	bp.closed = true
	bp.mu.Unlock()
	bp.cancel()
	close(bp.operations)
	done := make(chan struct{})
	go func() {
		bp.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
	}
	close(bp.results)
	return nil
}

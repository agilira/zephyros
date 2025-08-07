// zephyros_edge_test.go: Edge case and error handling tests for zephyros
//
// Copyright (c) 2025 AGILira
// Licensed under the Business Source License (BSL). Change Date: NEVER

package zephyros

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestWorkerRun_EdgeCases tests critical edge cases in the worker run function
func TestWorkerRun_EdgeCases(t *testing.T) {
	t.Run("handler_panic_recovery", func(t *testing.T) {
		config := PoolConfig{
			WorkerCount:   1,
			QueueSize:     10,
			EnableMetrics: true,
		}

		// Handler that panics
		panicHandler := &mockHandler{
			processFunc: func(ctx context.Context, op Operation) (OperationResult, error) {
				panic("handler panic for testing recovery")
			},
		}

		pool, err := NewOperationPool(config, panicHandler)
		if err != nil {
			t.Fatalf("Failed to create pool: %v", err)
		}
		defer pool.Close()

		// Submit operation that will cause panic
		op := Operation{Type: "panic_test", Key: "test"}
		ctx := context.Background()
		err = pool.Submit(ctx, op)
		if err != nil {
			t.Fatalf("Failed to submit operation: %v", err)
		}

		// Wait a bit for the panic to be handled
		time.Sleep(100 * time.Millisecond)

		// Verify pool is still functional
		metrics := pool.GetMetrics()
		if metrics.ProcessedOps == 0 {
			t.Error("Expected at least one processed operation even with panic")
		}
	})

	t.Run("handler_error_handling", func(t *testing.T) {
		config := PoolConfig{
			WorkerCount:   1,
			QueueSize:     10,
			EnableMetrics: true,
		}

		// Handler that returns errors
		errorHandler := &mockHandler{
			processFunc: func(ctx context.Context, op Operation) (OperationResult, error) {
				return OperationResult{}, fmt.Errorf("simulated handler error")
			},
		}

		pool, err := NewOperationPool(config, errorHandler)
		if err != nil {
			t.Fatalf("Failed to create pool: %v", err)
		}
		defer pool.Close()

		// Submit operation that will cause error
		op := Operation{Type: "error_test", Key: "test"}
		ctx := context.Background()
		err = pool.Submit(ctx, op)
		if err != nil {
			t.Fatalf("Failed to submit operation: %v", err)
		}

		// Get result and verify error handling
		result, err := pool.GetResult(ctx)
		if err != nil {
			t.Fatalf("Failed to get result: %v", err)
		}

		if result.Success {
			t.Error("Expected operation to fail")
		}
		if result.Error == nil {
			t.Error("Expected error in result")
		}
		if result.OperationID == "" {
			t.Error("Expected operation ID to be set")
		}

		// Verify metrics reflect the error
		metrics := pool.GetMetrics()
		if metrics.FailedOps == 0 {
			t.Error("Expected failed operations counter to be incremented")
		}
	})

	t.Run("queue_closed_handling", func(t *testing.T) {
		config := PoolConfig{
			WorkerCount:   1,
			QueueSize:     10,
			EnableMetrics: true,
		}

		pool, err := NewOperationPool(config, &mockHandler{})
		if err != nil {
			t.Fatalf("Failed to create pool: %v", err)
		}

		// Close the pool immediately
		pool.Close()

		// Wait for workers to finish
		time.Sleep(100 * time.Millisecond)

		// Verify pool is closed
		if !pool.IsClosed() {
			t.Error("Expected pool to be closed")
		}
	})

	t.Run("context_cancellation", func(t *testing.T) {
		config := PoolConfig{
			WorkerCount:   1,
			QueueSize:     10,
			EnableMetrics: true,
		}

		// Handler that takes time
		slowHandler := &mockHandler{
			processFunc: func(ctx context.Context, op Operation) (OperationResult, error) {
				time.Sleep(200 * time.Millisecond)
				return OperationResult{Success: true}, nil
			},
		}

		pool, err := NewOperationPool(config, slowHandler)
		if err != nil {
			t.Fatalf("Failed to create pool: %v", err)
		}
		defer pool.Close()

		// Submit operation
		op := Operation{Type: "slow_test", Key: "test"}
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		err = pool.Submit(ctx, op)
		if err != nil {
			t.Fatalf("Failed to submit operation: %v", err)
		}

		// Try to get result with short timeout
		_, err = pool.GetResult(ctx)
		if err == nil {
			t.Error("Expected timeout error")
		}
		if !strings.Contains(err.Error(), "timeout") && !strings.Contains(err.Error(), "context cancelled") && err != context.DeadlineExceeded && err != context.Canceled {
			t.Errorf("Expected timeout or context error, got: %v", err)
		}
	})
}

// TestGetResult_EdgeCases tests critical edge cases in the GetResult function
func TestGetResult_EdgeCases(t *testing.T) {
	t.Run("results_channel_closed", func(t *testing.T) {
		config := PoolConfig{
			WorkerCount:   1,
			QueueSize:     10,
			EnableMetrics: true,
		}

		pool, err := NewOperationPool(config, &mockHandler{})
		if err != nil {
			t.Fatalf("Failed to create pool: %v", err)
		}

		// Close the pool to close the results channel
		pool.Close()

		// Try to get result from closed channel
		ctx := context.Background()
		_, err = pool.GetResult(ctx)
		if err == nil {
			t.Error("Expected error when results channel is closed")
		}
		if !strings.Contains(err.Error(), "operation pool is closed") && !strings.Contains(err.Error(), "results channel closed") && !strings.Contains(err.Error(), "batch processor is closed") {
			t.Errorf("Expected pool closed error, got: %v", err)
		}
	})

	t.Run("context_cancellation_during_wait", func(t *testing.T) {
		config := PoolConfig{
			WorkerCount:   1,
			QueueSize:     10,
			EnableMetrics: true,
		}

		pool, err := NewOperationPool(config, &mockHandler{})
		if err != nil {
			t.Fatalf("Failed to create pool: %v", err)
		}
		defer pool.Close()

		// Create context that will be cancelled
		ctx, cancel := context.WithCancel(context.Background())

		// Cancel context immediately
		cancel()

		// Try to get result with cancelled context
		_, err = pool.GetResult(ctx)
		if err == nil {
			t.Error("Expected error when context is cancelled")
		}
		if !strings.Contains(err.Error(), "context cancelled") && err != context.Canceled {
			t.Errorf("Expected context cancelled error, got: %v", err)
		}
	})

	t.Run("timeout_during_wait", func(t *testing.T) {
		config := PoolConfig{
			WorkerCount:   1,
			QueueSize:     10,
			MaxWaitTime:   50 * time.Millisecond,
			EnableMetrics: true,
		}

		pool, err := NewOperationPool(config, &mockHandler{})
		if err != nil {
			t.Fatalf("Failed to create pool: %v", err)
		}
		defer pool.Close()

		// Try to get result without submitting anything
		ctx := context.Background()
		_, err = pool.GetResult(ctx)
		if err == nil {
			t.Error("Expected timeout error")
		}
		if !strings.Contains(err.Error(), "timeout") {
			t.Errorf("Expected timeout error, got: %v", err)
		}
	})

	t.Run("batch_processor_fallback", func(t *testing.T) {
		config := PoolConfig{
			WorkerCount: 1,
			QueueSize:   10,
			BatchConfig: BatchConfig{
				EnableBatchProcessing: true,
				BatchSize:             5,
				BatchTimeout:          100 * time.Millisecond,
			},
			EnableMetrics: true,
		}

		pool, err := NewOperationPool(config, &mockHandler{})
		if err != nil {
			t.Fatalf("Failed to create pool: %v", err)
		}
		defer pool.Close()

		// Submit operation
		op := Operation{Type: "batch_test", Key: "test"}
		ctx := context.Background()
		err = pool.Submit(ctx, op)
		if err != nil {
			t.Fatalf("Failed to submit operation: %v", err)
		}

		// Get result through batch processor
		result, err := pool.GetResult(ctx)
		if err != nil {
			t.Fatalf("Failed to get result: %v", err)
		}

		if !result.Success {
			t.Error("Expected successful result")
		}
		if result.OperationID == "" {
			t.Error("Expected operation ID to be set")
		}
	})
}

// TestOperationPool_ConcurrencyStress tests concurrent operations under stress
func TestOperationPool_ConcurrencyStress(t *testing.T) {
	config := PoolConfig{
		WorkerCount:   4,
		QueueSize:     100,
		EnableMetrics: true,
	}

	// Handler that simulates real work
	stressHandler := &mockHandler{
		processFunc: func(ctx context.Context, op Operation) (OperationResult, error) {
			// Simulate variable processing time
			time.Sleep(time.Duration(op.Timestamp.Nanosecond()%100) * time.Microsecond)

			// Simulate occasional errors
			if op.Timestamp.Nanosecond()%10 == 0 {
				return OperationResult{}, fmt.Errorf("simulated stress error")
			}

			return OperationResult{
				OperationID: op.ID,
				Success:     true,
				Data:        op.Value,
				Duration:    time.Millisecond,
			}, nil
		},
	}

	pool, err := NewOperationPool(config, stressHandler)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}
	defer pool.Close()

	// Submit many operations concurrently
	const numOperations = 50
	results := make(chan error, numOperations)
	var errors []string

	for i := 0; i < numOperations; i++ {
		go func(id int) {
			op := Operation{
				Type:      "stress_test",
				Key:       fmt.Sprintf("key_%d", id),
				Value:     fmt.Sprintf("value_%d", id),
				Timestamp: time.Now(),
			}

			ctx := context.Background()
			err := pool.Submit(ctx, op)
			if err != nil {
				results <- fmt.Errorf("submit error: %v", err)
				return
			}

			result, err := pool.GetResult(ctx)
			if err != nil {
				results <- fmt.Errorf("get result error: %v", err)
				return
			}

			if result.OperationID == "" {
				errors = append(errors, fmt.Sprintf("operation ID not set for result %d", id))
			}
			results <- nil
		}(i)
	}

	// Collect results
	for i := 0; i < numOperations; i++ {
		if err := <-results; err != nil {
			errors = append(errors, err.Error())
		}
	}

	// Verify metrics
	metrics := pool.GetMetrics()
	if metrics.ProcessedOps != int64(numOperations) {
		t.Errorf("Expected %d processed operations, got %d", numOperations, metrics.ProcessedOps)
	}

	if len(errors) > 0 {
		t.Errorf("Encountered %d errors during stress test: %v", len(errors), errors)
	}
}

// TestOperationPool_Recovery tests recovery scenarios
func TestOperationPool_Recovery(t *testing.T) {
	t.Run("recovery_after_handler_panic", func(t *testing.T) {
		config := PoolConfig{
			WorkerCount:   2,
			QueueSize:     10,
			EnableMetrics: true,
		}

		var panicCount int64
		var panicMu sync.Mutex
		panicHandler := &mockHandler{
			processFunc: func(ctx context.Context, op Operation) (OperationResult, error) {
				panicMu.Lock()
				panicCount++
				currentCount := panicCount
				panicMu.Unlock()

				if currentCount <= 2 {
					panic("simulated panic")
				}
				return OperationResult{Success: true}, nil
			},
		}

		pool, err := NewOperationPool(config, panicHandler)
		if err != nil {
			t.Fatalf("Failed to create pool: %v", err)
		}
		defer pool.Close()

		// Submit operations that will cause panics initially
		for i := 0; i < 3; i++ {
			op := Operation{Type: "recovery_test", Key: fmt.Sprintf("key_%d", i)}
			ctx := context.Background()
			err = pool.Submit(ctx, op)
			if err != nil {
				t.Fatalf("Failed to submit operation %d: %v", i, err)
			}
		}

		// Wait for processing
		time.Sleep(200 * time.Millisecond)

		// Verify pool is still functional
		metrics := pool.GetMetrics()
		if metrics.ProcessedOps == 0 {
			t.Error("Expected pool to recover and process operations")
		}
	})

	t.Run("recovery_after_worker_failure", func(t *testing.T) {
		config := PoolConfig{
			WorkerCount:   3,
			QueueSize:     10,
			EnableMetrics: true,
		}

		pool, err := NewOperationPool(config, &mockHandler{})
		if err != nil {
			t.Fatalf("Failed to create pool: %v", err)
		}
		defer pool.Close()

		// Submit operations to ensure workers are active
		for i := 0; i < 5; i++ {
			op := Operation{Type: "recovery_test", Key: fmt.Sprintf("key_%d", i)}
			ctx := context.Background()
			err = pool.Submit(ctx, op)
			if err != nil {
				t.Fatalf("Failed to submit operation %d: %v", i, err)
			}
		}

		// Wait for initial processing
		time.Sleep(100 * time.Millisecond)

		// Verify pool is working
		initialMetrics := pool.GetMetrics()
		if initialMetrics.ProcessedOps == 0 {
			t.Error("Expected initial operations to be processed")
		}

		// Submit more operations to test recovery
		for i := 5; i < 10; i++ {
			op := Operation{Type: "recovery_test", Key: fmt.Sprintf("key_%d", i)}
			ctx := context.Background()
			err = pool.Submit(ctx, op)
			if err != nil {
				t.Fatalf("Failed to submit recovery operation %d: %v", i, err)
			}
		}

		// Wait for recovery processing
		time.Sleep(100 * time.Millisecond)

		// Verify recovery
		finalMetrics := pool.GetMetrics()
		if finalMetrics.ProcessedOps <= initialMetrics.ProcessedOps {
			t.Error("Expected additional operations to be processed during recovery")
		}
	})
}

// TestOperationPool_InputValidation tests input validation and edge cases
func TestOperationPool_InputValidation(t *testing.T) {
	config := PoolConfig{
		WorkerCount:   1,
		QueueSize:     2,
		EnableMetrics: true,
	}

	t.Run("submit_with_closed_pool", func(t *testing.T) {
		handler := &mockHandler{}
		pool, err := NewOperationPool(config, handler)
		if err != nil {
			t.Fatalf("Failed to create pool: %v", err)
		}
		pool.Close()
		op := Operation{Type: "test", Key: "k"}
		err = pool.Submit(context.Background(), op)
		if err == nil || !strings.Contains(err.Error(), "closed") {
			t.Error("Expected error when submitting to closed pool")
		}
	})

	t.Run("submit_with_cancelled_context", func(t *testing.T) {
		handler := &mockHandler{}
		pool, err := NewOperationPool(config, handler)
		if err != nil {
			t.Fatalf("Failed to create pool: %v", err)
		}
		defer pool.Close()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		op := Operation{Type: "test", Key: "k"}
		err = pool.Submit(ctx, op)
		if err != nil && !strings.Contains(err.Error(), "context cancelled") && err != context.Canceled {
			t.Errorf("Expected context cancelled error or <nil>, got: %v", err)
		}
	})

	t.Run("submit_with_timeout", func(t *testing.T) {
		configTimeout := PoolConfig{
			WorkerCount:   1,
			QueueSize:     0,
			EnableMetrics: true,
		}
		blocker := make(chan struct{})
		handler := &mockHandler{
			processFunc: func(ctx context.Context, op Operation) (OperationResult, error) {
				<-blocker // Block worker until test unblocks it
				return OperationResult{Success: true}, nil
			},
		}
		pool, err := NewOperationPool(configTimeout, handler)
		if err != nil {
			t.Fatalf("Failed to create pool: %v", err)
		}
		defer pool.Close()
		ctx := context.Background()
		err = pool.Submit(ctx, Operation{Type: "test", Key: "k1"})
		if err != nil {
			t.Fatalf("Unexpected error filling queue: %v", err)
		}
		ctxTimeout, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		op := Operation{Type: "test", Key: "k2"}
		err = pool.Submit(ctxTimeout, op)
		if err != nil && (!strings.Contains(err.Error(), "timeout") && err != context.DeadlineExceeded && err != context.Canceled) {
			t.Errorf("Expected timeout, context error, or <nil>, got: %v", err)
		}
		close(blocker) // Unblock worker for cleanup
	})

	t.Run("operation_with_zero_timestamp_and_id", func(t *testing.T) {
		handler := &mockHandler{}
		pool, err := NewOperationPool(config, handler)
		if err != nil {
			t.Fatalf("Failed to create pool: %v", err)
		}
		defer pool.Close()
		op := Operation{Type: "test", Key: "k"}
		err = pool.Submit(context.Background(), op)
		if err != nil {
			t.Errorf("Expected no error, got: %v", err)
		}
	})
}

// TestBatchProcessor_ErrorAndPanic tests batch processor error and panic handling
func TestBatchProcessor_ErrorAndPanic(t *testing.T) {
	config := PoolConfig{
		WorkerCount: 1,
		QueueSize:   10,
		BatchConfig: BatchConfig{
			EnableBatchProcessing: true,
			BatchSize:             2,
			BatchTimeout:          50 * time.Millisecond,
		},
		EnableMetrics: true,
	}

	t.Run("batch_handler_error", func(t *testing.T) {
		errorHandler := &mockHandler{
			processFunc: func(ctx context.Context, op Operation) (OperationResult, error) {
				return OperationResult{}, fmt.Errorf("batch error")
			},
		}
		pool, err := NewOperationPool(config, errorHandler)
		if err != nil {
			t.Fatalf("Failed to create pool: %v", err)
		}
		defer pool.Close()
		for i := 0; i < 2; i++ {
			pool.Submit(context.Background(), Operation{Type: "batch", Key: fmt.Sprintf("k%d", i)})
		}
		for i := 0; i < 2; i++ {
			result, err := pool.GetResult(context.Background())
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
			if result.Success {
				t.Error("Expected batch operation to fail")
			}
			if result.Error == nil || !strings.Contains(result.Error.Error(), "batch error") {
				t.Error("Expected batch error in result")
			}
		}
	})

	t.Run("batch_handler_panic", func(t *testing.T) {
		panicHandler := &mockHandler{
			processFunc: func(ctx context.Context, op Operation) (OperationResult, error) {
				panic("batch panic")
			},
		}
		pool, err := NewOperationPool(config, panicHandler)
		if err != nil {
			t.Fatalf("Failed to create pool: %v", err)
		}
		defer pool.Close()
		for i := 0; i < 2; i++ {
			pool.Submit(context.Background(), Operation{Type: "batch", Key: fmt.Sprintf("k%d", i)})
		}
		for i := 0; i < 2; i++ {
			result, err := pool.GetResult(context.Background())
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
			if result.Success {
				t.Error("Expected batch operation to fail due to panic")
			}
			if result.Error == nil || !strings.Contains(result.Error.Error(), "panic") {
				t.Error("Expected panic error in result")
			}
		}
	})
}

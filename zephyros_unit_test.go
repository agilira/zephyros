// zephyros_unit_test.go: Unit tests for zephyros core functionality
//
// Copyright (c) 2025 AGILira
// Licensed under the Business Source License (BSL). Change Date: NEVER

package zephyros

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// mockHandler implements OperationHandler for testing
type mockHandler struct {
	processFunc func(ctx context.Context, op Operation) (OperationResult, error)
}

func (m *mockHandler) Process(ctx context.Context, op Operation) (OperationResult, error) {
	if m.processFunc != nil {
		return m.processFunc(ctx, op)
	}
	return OperationResult{
		OperationID: op.ID,
		Success:     true,
		Data:        op.Value,
		Duration:    time.Millisecond,
	}, nil
}

func TestNewOperationPool_Unit(t *testing.T) {
	t.Run("valid_configuration", func(t *testing.T) {
		config := PoolConfig{
			WorkerCount:   4,
			QueueSize:     100,
			EnableMetrics: true,
		}
		pool, err := NewOperationPool(config, nil)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}
		if pool == nil {
			t.Fatal("Expected pool to be created")
		}
	})

	t.Run("zero_worker_count", func(t *testing.T) {
		config := PoolConfig{
			WorkerCount:   0,
			QueueSize:     100,
			EnableMetrics: true,
		}
		pool, err := NewOperationPool(config, nil)
		if err != nil {
			t.Fatalf("Expected no error for zero worker count, got %v", err)
		}
		if pool == nil {
			t.Fatal("Expected pool to be created with default worker count")
		}
		if pool.config.WorkerCount <= 0 {
			t.Errorf("Expected default worker count > 0, got %d", pool.config.WorkerCount)
		}
	})

	t.Run("negative_worker_count", func(t *testing.T) {
		config := PoolConfig{
			WorkerCount:   -5,
			QueueSize:     100,
			EnableMetrics: true,
		}
		pool, err := NewOperationPool(config, nil)
		if err != nil {
			t.Fatalf("Expected no error for negative worker count, got %v", err)
		}
		if pool == nil {
			t.Fatal("Expected pool to be created with default worker count")
		}
		if pool.config.WorkerCount <= 0 {
			t.Errorf("Expected default worker count > 0, got %d", pool.config.WorkerCount)
		}
	})

	t.Run("zero_queue_size", func(t *testing.T) {
		config := PoolConfig{
			WorkerCount:   4,
			QueueSize:     0,
			EnableMetrics: true,
		}
		pool, err := NewOperationPool(config, nil)
		if err != nil {
			t.Fatalf("Expected no error for zero queue size, got %v", err)
		}
		if pool == nil {
			t.Fatal("Expected pool to be created with default queue size")
		}
		if pool.config.QueueSize <= 0 {
			t.Errorf("Expected default queue size > 0, got %d", pool.config.QueueSize)
		}
	})

	t.Run("negative_queue_size", func(t *testing.T) {
		config := PoolConfig{
			WorkerCount:   4,
			QueueSize:     -10,
			EnableMetrics: true,
		}
		pool, err := NewOperationPool(config, nil)
		if err != nil {
			t.Fatalf("Expected no error for negative queue size, got %v", err)
		}
		if pool == nil {
			t.Fatal("Expected pool to be created with default queue size")
		}
		if pool.config.QueueSize <= 0 {
			t.Errorf("Expected default queue size > 0, got %d", pool.config.QueueSize)
		}
	})

	t.Run("default_timeouts", func(t *testing.T) {
		config := PoolConfig{
			WorkerCount:     4,
			QueueSize:       100,
			EnableMetrics:   true,
			MaxWaitTime:     0,
			ShutdownTimeout: 0,
		}
		pool, err := NewOperationPool(config, nil)
		if err != nil {
			t.Fatalf("Expected no error for zero timeouts, got %v", err)
		}
		if pool == nil {
			t.Fatal("Expected pool to be created with default timeouts")
		}
		if pool.config.MaxWaitTime <= 0 {
			t.Errorf("Expected default MaxWaitTime > 0, got %v", pool.config.MaxWaitTime)
		}
		if pool.config.ShutdownTimeout <= 0 {
			t.Errorf("Expected default ShutdownTimeout > 0, got %v", pool.config.ShutdownTimeout)
		}
	})
}

func TestOperationPool_Submit_Unit(t *testing.T) {
	config := PoolConfig{
		WorkerCount:   2,
		QueueSize:     10,
		EnableMetrics: true,
	}
	handler := &mockHandler{}
	pool, err := NewOperationPool(config, handler)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}
	defer pool.Close()

	// Test valid operation submission
	op := Operation{
		Type:  "test",
		Key:   "test_key",
		Value: "test_value",
	}

	ctx := context.Background()
	err = pool.Submit(ctx, op)
	if err != nil {
		t.Errorf("Submit() error = %v", err)
	}

	// Test operation with empty ID (should be auto-generated)
	op2 := Operation{
		Type:  "test2",
		Key:   "test_key2",
		Value: "test_value2",
	}

	err = pool.Submit(ctx, op2)
	if err != nil {
		t.Errorf("Submit() with empty ID error = %v", err)
	}

	// Test operation with zero timestamp (should be auto-set)
	op3 := Operation{
		Type:  "test3",
		Key:   "test_key3",
		Value: "test_value3",
	}

	err = pool.Submit(ctx, op3)
	if err != nil {
		t.Errorf("Submit() with zero timestamp error = %v", err)
	}
}

func TestOperationPool_GetResult_Unit(t *testing.T) {
	config := PoolConfig{
		WorkerCount:   2,
		QueueSize:     10,
		EnableMetrics: true,
	}

	handler := &mockHandler{
		processFunc: func(ctx context.Context, op Operation) (OperationResult, error) {
			return OperationResult{
				OperationID: op.ID,
				Success:     true,
				Data:        op.Value,
				Duration:    time.Millisecond,
			}, nil
		},
	}

	pool, err := NewOperationPool(config, handler)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}
	defer pool.Close()

	// Submit an operation
	op := Operation{
		Type:  "test",
		Key:   "test_key",
		Value: "test_value",
	}

	ctx := context.Background()
	err = pool.Submit(ctx, op)
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}

	// Get result
	result, err := pool.GetResult(ctx)
	if err != nil {
		t.Errorf("GetResult() error = %v", err)
	}

	if !result.Success {
		t.Error("Expected successful result")
	}

	if result.OperationID == "" {
		t.Error("Expected operation ID to be set")
	}

	if result.Data != op.Value {
		t.Errorf("Expected data %s, got %v", op.Value, result.Data)
	}
}

func TestOperationPool_GetMetrics_Unit(t *testing.T) {
	config := PoolConfig{
		WorkerCount:   2,
		QueueSize:     10,
		EnableMetrics: true,
		BatchConfig: BatchConfig{
			EnableBatchProcessing: true,
			BatchSize:             5,
			BatchTimeout:          50 * time.Millisecond,
			FlushInterval:         25 * time.Millisecond,
		},
	}

	handler := &mockHandler{
		processFunc: func(ctx context.Context, op Operation) (OperationResult, error) {
			// Simulate processing time
			time.Sleep(5 * time.Millisecond)
			return OperationResult{
				OperationID: op.ID,
				Success:     true,
				Data:        op.Value,
				Duration:    time.Millisecond,
			}, nil
		},
	}

	pool, err := NewOperationPool(config, handler)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	defer pool.Close()

	// Get initial metrics
	metrics := pool.GetMetrics()
	if metrics.ActiveWorkers != 0 {
		t.Errorf("Expected 0 active workers initially, got %d", metrics.ActiveWorkers)
	}

	if metrics.ProcessedOps != 0 {
		t.Errorf("Expected 0 processed operations initially, got %d", metrics.ProcessedOps)
	}

	// Submit and process an operation
	op := Operation{
		Type:  "test",
		Key:   "test_key",
		Value: "test_value",
	}

	ctx := context.Background()
	err = pool.Submit(ctx, op)
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}

	// Wait for processing to complete with smart polling
	maxWait := 2 * time.Second
	interval := 20 * time.Millisecond
	waited := time.Duration(0)

	for waited < maxWait {
		metrics = pool.GetMetrics()
		if metrics.ProcessedOps >= 1 {
			break
		}
		time.Sleep(interval)
		waited += interval
	}

	if metrics.ProcessedOps < 1 {
		t.Errorf("Expected at least 1 processed operation after %v, got %d", waited, metrics.ProcessedOps)
	}

	if metrics.FailedOps != 0 {
		t.Errorf("Expected 0 failed operations, got %d", metrics.FailedOps)
	}
}

func TestOperationPool_ResetMetrics_Unit(t *testing.T) {
	config := PoolConfig{
		WorkerCount:   2,
		QueueSize:     10,
		EnableMetrics: true,
	}
	pool, err := NewOperationPool(config, nil)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	// Simulate some metrics
	pool.metrics.ProcessedOps = 5
	pool.metrics.FailedOps = 2
	pool.metrics.AverageDuration = 123
	pool.metrics.PoolHits = 3
	pool.metrics.PoolMisses = 1

	pool.ResetMetrics()
	metrics := pool.GetMetrics()
	if metrics.ProcessedOps != 0 || metrics.FailedOps != 0 || metrics.AverageDuration != 0 || metrics.PoolHits != 0 || metrics.PoolMisses != 0 {
		t.Errorf("Expected all metrics to be reset to zero, got ProcessedOps=%d, FailedOps=%d, AverageDuration=%d, PoolHits=%d, PoolMisses=%d",
			metrics.ProcessedOps, metrics.FailedOps, metrics.AverageDuration, metrics.PoolHits, metrics.PoolMisses)
	}
}

func TestOperationPool_Close_Unit(t *testing.T) {
	config := PoolConfig{
		WorkerCount:   2,
		QueueSize:     10,
		EnableMetrics: true,
	}

	handler := &mockHandler{}
	pool, err := NewOperationPool(config, handler)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}

	// Verify pool is not closed initially
	if pool.IsClosed() {
		t.Error("Pool should not be closed initially")
	}

	// Close the pool
	err = pool.Close()
	if err != nil {
		t.Errorf("Close() error = %v", err)
	}

	// Verify pool is closed
	if !pool.IsClosed() {
		t.Error("Pool should be closed after Close()")
	}

	// Test double close (should not error)
	err = pool.Close()
	if err != nil {
		t.Errorf("Double Close() should not error, got %v", err)
	}
}

func TestOperationPool_IsClosed_Unit(t *testing.T) {
	config := PoolConfig{
		WorkerCount:   2,
		QueueSize:     10,
		EnableMetrics: true,
	}

	handler := &mockHandler{}
	pool, err := NewOperationPool(config, handler)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}

	// Test initial state
	if pool.IsClosed() {
		t.Error("New pool should not be closed")
	}

	// Close and test
	pool.Close()
	if !pool.IsClosed() {
		t.Error("Pool should be closed after Close()")
	}
}

// TestOperationPool_DirectProcessing tests direct operation processing without batch processing
func TestOperationPool_DirectProcessing(t *testing.T) {
	config := PoolConfig{
		WorkerCount:   1,
		QueueSize:     10,
		EnableMetrics: true,
		BatchConfig: BatchConfig{
			EnableBatchProcessing: false, // Disable batch processing to test direct processing
		},
	}

	handler := &mockHandler{
		processFunc: func(ctx context.Context, op Operation) (OperationResult, error) {
			return OperationResult{
				OperationID: op.ID,
				Success:     true,
				Data:        op.Value,
				Duration:    time.Millisecond,
			}, nil
		},
	}

	pool, err := NewOperationPool(config, handler)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}
	defer pool.Close()

	// Submit operation
	op := Operation{
		Type:  "test",
		Key:   "test_key",
		Value: "test_value",
	}

	ctx := context.Background()
	err = pool.Submit(ctx, op)
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}

	// Get result
	result, err := pool.GetResult(ctx)
	if err != nil {
		t.Fatalf("GetResult() error = %v", err)
	}

	if !result.Success {
		t.Error("Expected successful operation")
	}
	if result.OperationID == "" {
		t.Error("Expected operation ID to be set")
	}

	// Verify metrics
	metrics := pool.GetMetrics()
	if metrics.ProcessedOps < 1 {
		t.Error("Expected at least 1 processed operation")
	}
}

// TestOperationPool_ErrorHandling tests error handling in direct processing
func TestOperationPool_ErrorHandling(t *testing.T) {
	config := PoolConfig{
		WorkerCount:   1,
		QueueSize:     10,
		EnableMetrics: true,
		BatchConfig: BatchConfig{
			EnableBatchProcessing: false,
		},
	}

	handler := &mockHandler{
		processFunc: func(ctx context.Context, op Operation) (OperationResult, error) {
			return OperationResult{}, fmt.Errorf("simulated error")
		},
	}

	pool, err := NewOperationPool(config, handler)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}
	defer pool.Close()

	// Submit operation
	op := Operation{
		Type:  "test",
		Key:   "test_key",
		Value: "test_value",
	}

	ctx := context.Background()
	err = pool.Submit(ctx, op)
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}

	// Get result
	result, err := pool.GetResult(ctx)
	if err != nil {
		t.Fatalf("GetResult() error = %v", err)
	}

	if result.Success {
		t.Error("Expected failed operation")
	}
	if result.Error == nil {
		t.Error("Expected error in result")
	}

	// Verify metrics
	metrics := pool.GetMetrics()
	if metrics.ProcessedOps < 1 {
		t.Error("Expected at least 1 processed operation")
	}
	if metrics.FailedOps < 1 {
		t.Error("Expected at least 1 failed operation")
	}
}

// TestOperationPool_CacheIntegration tests cache integration in direct processing
func TestOperationPool_CacheIntegration(t *testing.T) {
	config := PoolConfig{
		WorkerCount:   1,
		QueueSize:     10,
		EnableMetrics: true,
		BatchConfig: BatchConfig{
			EnableBatchProcessing: false,
		},
		CacheConfig: CacheConfig{
			EnableCaching: true,
			CacheSize:     100,
			TTL:           1 * time.Second,
		},
	}

	handler := &mockHandler{
		processFunc: func(ctx context.Context, op Operation) (OperationResult, error) {
			return OperationResult{
				OperationID: op.ID,
				Success:     true,
				Data:        op.Value,
				Duration:    time.Millisecond,
			}, nil
		},
	}

	pool, err := NewOperationPool(config, handler)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}
	defer pool.Close()

	// Submit operation
	op := Operation{
		Type:  "test",
		Key:   "test_key",
		Value: "test_value",
	}

	ctx := context.Background()
	err = pool.Submit(ctx, op)
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}

	// Get result
	result, err := pool.GetResult(ctx)
	if err != nil {
		t.Fatalf("GetResult() error = %v", err)
	}

	if !result.Success {
		t.Error("Expected successful operation")
	}

	// Submit same operation again to test cache
	err = pool.Submit(ctx, op)
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}

	// Get result from cache
	result2, err := pool.GetResult(ctx)
	if err != nil {
		t.Fatalf("GetResult() error = %v", err)
	}

	if !result2.Success {
		t.Error("Expected successful operation from cache")
	}
}

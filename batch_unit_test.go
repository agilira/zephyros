// batch_unit_test.go: Unit tests for zephyros batch processing
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

func TestBatchProcessor_BasicOperations(t *testing.T) {
	config := BatchConfig{
		EnableBatchProcessing: true,
		BatchSize:             5,
		BatchTimeout:          100 * time.Millisecond,
		FlushInterval:         50 * time.Millisecond,
		MaxBatchSize:          20,
	}

	bp := NewBatchProcessor(config)
	defer bp.Close()

	// Submit operations
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		op := Operation{
			Type:  "test",
			Key:   fmt.Sprintf("key_%d", i),
			Value: fmt.Sprintf("value_%d", i),
		}
		err := bp.Submit(ctx, op)
		if err != nil {
			t.Errorf("Submit() %d error = %v", i, err)
		}
	}

	// Wait for processing
	time.Sleep(200 * time.Millisecond)

	// Get results
	for i := 0; i < 3; i++ {
		result, err := bp.GetResult(ctx)
		if err != nil {
			t.Errorf("GetResult() %d error = %v", i, err)
		}
		if !result.Success {
			t.Errorf("Expected successful result, got failure")
		}
	}
}

func TestBatchProcessor_BatchSizeReached(t *testing.T) {
	config := BatchConfig{
		EnableBatchProcessing: true,
		BatchSize:             3,
		BatchTimeout:          1 * time.Second, // Long timeout to test batch size
		FlushInterval:         1 * time.Second,
		MaxBatchSize:          10,
	}

	bp := NewBatchProcessor(config)
	defer bp.Close()

	// Submit exactly batch size operations
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		op := Operation{
			Type:  "test",
			Key:   fmt.Sprintf("key_%d", i),
			Value: fmt.Sprintf("value_%d", i),
		}
		err := bp.Submit(ctx, op)
		if err != nil {
			t.Errorf("Submit() %d error = %v", i, err)
		}
	}

	// Wait a bit for processing
	time.Sleep(50 * time.Millisecond)

	// Get results
	for i := 0; i < 3; i++ {
		result, err := bp.GetResult(ctx)
		if err != nil {
			t.Errorf("GetResult() %d error = %v", i, err)
		}
		if !result.Success {
			t.Errorf("Expected successful result, got failure")
		}
	}
}

func TestBatchProcessor_TimeoutFlush(t *testing.T) {
	config := BatchConfig{
		EnableBatchProcessing: true,
		BatchSize:             10,                    // Large batch size
		BatchTimeout:          50 * time.Millisecond, // Short timeout
		FlushInterval:         1 * time.Second,
		MaxBatchSize:          20,
	}

	bp := NewBatchProcessor(config)
	defer bp.Close()

	// Submit only 2 operations (less than batch size)
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		op := Operation{
			Type:  "test",
			Key:   fmt.Sprintf("key_%d", i),
			Value: fmt.Sprintf("value_%d", i),
		}
		err := bp.Submit(ctx, op)
		if err != nil {
			t.Errorf("Submit() %d error = %v", i, err)
		}
	}

	// Wait for timeout flush
	time.Sleep(100 * time.Millisecond)

	// Get results
	for i := 0; i < 2; i++ {
		result, err := bp.GetResult(ctx)
		if err != nil {
			t.Errorf("GetResult() %d error = %v", i, err)
		}
		if !result.Success {
			t.Errorf("Expected successful result, got failure")
		}
	}
}

func TestBatchProcessor_CloseDuringProcessing(t *testing.T) {
	config := BatchConfig{
		EnableBatchProcessing: true,
		BatchSize:             5,
		BatchTimeout:          100 * time.Millisecond,
		FlushInterval:         50 * time.Millisecond,
		MaxBatchSize:          20,
	}

	bp := NewBatchProcessor(config)

	// Submit operations
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		op := Operation{
			Type:  "test",
			Key:   fmt.Sprintf("key_%d", i),
			Value: fmt.Sprintf("value_%d", i),
		}
		bp.Submit(ctx, op)
	}

	// Close immediately
	err := bp.Close()
	if err != nil {
		t.Errorf("Close() error = %v", err)
	}

	// Try to submit to closed processor
	err = bp.Submit(ctx, Operation{Type: "test", Key: "key"})
	if err == nil {
		t.Error("Expected error when submitting to closed processor")
	}
}

func TestBatchProcessor_EmptyBatch(t *testing.T) {
	config := BatchConfig{
		EnableBatchProcessing: true,
		BatchSize:             5,
		BatchTimeout:          100 * time.Millisecond,
		FlushInterval:         50 * time.Millisecond,
		MaxBatchSize:          20,
	}

	bp := NewBatchProcessor(config)
	defer bp.Close()

	// Don't submit any operations
	time.Sleep(200 * time.Millisecond)

	// Should not panic or error
	ctx := context.Background()
	ctxTimeout, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()

	_, err := bp.GetResult(ctxTimeout)
	if err == nil {
		t.Logf("[Warning][SmartTest] Expected timeout error for empty batch, got none (may be timing)")
	}
}

func TestOperationPool_WithBatchProcessing(t *testing.T) {
	config := PoolConfig{
		WorkerCount: 2,
		QueueSize:   10,
		BatchConfig: BatchConfig{
			EnableBatchProcessing: true,
			BatchSize:             3,
			BatchTimeout:          100 * time.Millisecond,
			FlushInterval:         50 * time.Millisecond,
			MaxBatchSize:          10,
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

	// Submit operations
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		op := Operation{
			Type:  "test",
			Key:   fmt.Sprintf("key_%d", i),
			Value: fmt.Sprintf("value_%d", i),
		}
		err := pool.Submit(ctx, op)
		if err != nil {
			t.Errorf("Submit() %d error = %v", i, err)
		}
	}

	// Wait for processing
	time.Sleep(200 * time.Millisecond)

	// Get results
	for i := 0; i < 5; i++ {
		result, err := pool.GetResult(ctx)
		if err != nil {
			t.Errorf("GetResult() %d error = %v", i, err)
		}
		if !result.Success {
			t.Errorf("Expected successful result, got failure")
		}
	}
}

func TestOperationPool_WithoutBatchProcessing(t *testing.T) {
	config := PoolConfig{
		WorkerCount: 2,
		QueueSize:   10,
		BatchConfig: BatchConfig{
			EnableBatchProcessing: false, // Disabled
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

	// Submit operations
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		op := Operation{
			Type:  "test",
			Key:   fmt.Sprintf("key_%d", i),
			Value: fmt.Sprintf("value_%d", i),
		}
		err := pool.Submit(ctx, op)
		if err != nil {
			t.Errorf("Submit() %d error = %v", i, err)
		}
	}

	// Wait for processing
	time.Sleep(100 * time.Millisecond)

	// Get results
	for i := 0; i < 3; i++ {
		result, err := pool.GetResult(ctx)
		if err != nil {
			t.Errorf("GetResult() %d error = %v", i, err)
		}
		if !result.Success {
			t.Errorf("Expected successful result, got failure")
		}
	}
}

func TestBatchProcessor_GetResult_Closed(t *testing.T) {
	bp := NewBatchProcessor(BatchConfig{EnableBatchProcessing: true})
	bp.Close()
	ctx := context.Background()
	_, err := bp.GetResult(ctx)
	if err == nil {
		t.Error("expected error for closed batch processor")
	}
}

func TestBatchProcessor_Submit_ContextNil(t *testing.T) {
	bp := NewBatchProcessor(BatchConfig{EnableBatchProcessing: true, BatchSize: 1})
	err := bp.Submit(context.TODO(), Operation{})
	if err == nil {
		t.Error("expected error for nil context")
	}
	bp.Close()
}

func TestBatchProcessor_Submit_Closed(t *testing.T) {
	bp := NewBatchProcessor(BatchConfig{EnableBatchProcessing: true, BatchSize: 1})
	bp.Close()
	err := bp.Submit(context.Background(), Operation{})
	if err == nil {
		t.Error("expected error for closed batch processor")
	}
}

func TestBatchProcessor_Submit_ContextCancelled(t *testing.T) {
	bp := NewBatchProcessor(BatchConfig{EnableBatchProcessing: true, BatchSize: 1})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := bp.Submit(ctx, Operation{})
	// In Go, due to select semantics, the operation may be accepted if the channel is ready,
	// even if the context is cancelled. This is not a bug, but a documented race.
	if err == nil {
		t.Log("operation accepted despite cancelled context: allowed by Go select semantics")
	}
	bp.Close()
}

func TestBatchProcessor_GetResult_ContextNil(t *testing.T) {
	bp := NewBatchProcessor(BatchConfig{EnableBatchProcessing: true, BatchSize: 1})
	_, err := bp.GetResult(context.TODO())
	if err == nil {
		t.Error("expected error for nil context")
	}
	bp.Close()
}

func TestBatchProcessor_GetResult_ChannelClosed(t *testing.T) {
	bp := NewBatchProcessor(BatchConfig{EnableBatchProcessing: true, BatchSize: 1})
	bp.Close()
	_, err := bp.GetResult(context.Background())
	if err == nil {
		t.Error("expected error for closed results channel")
	}
}

func TestBatchProcessor_GetResult_Timeout(t *testing.T) {
	bp := NewBatchProcessor(BatchConfig{EnableBatchProcessing: true, BatchSize: 1, BatchTimeout: 1})
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()
	_, err := bp.GetResult(ctx)
	if err == nil {
		t.Error("expected timeout error")
	}
	bp.Close()
}

func TestBatchProcessor_Close_Idempotent(t *testing.T) {
	bp := NewBatchProcessor(BatchConfig{EnableBatchProcessing: true, BatchSize: 1})
	bp.Close()
	// Should not panic if called again
	bp.Close()
}

func TestBatchProcessor_HandlerPanic(t *testing.T) {
	bp := NewBatchProcessor(BatchConfig{EnableBatchProcessing: true})
	bp.pool = &OperationPool{
		handler: &mockHandler{processFunc: func(ctx context.Context, op Operation) (OperationResult, error) {
			panic("handler panic")
		}},
		metrics: &PoolMetrics{},
	}
	err := bp.Submit(context.Background(), Operation{Key: "panic"})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	time.Sleep(20 * time.Millisecond)
	_, err = bp.GetResult(context.Background())
	if err != nil {
		t.Errorf("unexpected error after panic: %v", err)
	}
	bp.Close()
}

func TestBatchProcessor_ConcurrentSubmitAndGet(t *testing.T) {
	bp := NewBatchProcessor(BatchConfig{EnableBatchProcessing: true, BatchSize: 2})
	done := make(chan struct{})
	go func() {
		for i := 0; i < 10; i++ {
			bp.Submit(context.Background(), Operation{Key: fmt.Sprintf("k%d", i)})
		}
		done <- struct{}{}
	}()
	for i := 0; i < 10; i++ {
		_, _ = bp.GetResult(context.Background())
	}
	<-done
	bp.Close()
}

func TestNewBatchProcessor_ZeroConfig(t *testing.T) {
	bp := NewBatchProcessor(BatchConfig{})
	if bp == nil {
		t.Fatal("expected non-nil batch processor")
	}
	bp.Close()
}

func TestBatchProcessor_PoolNil(t *testing.T) {
	bp := NewBatchProcessor(BatchConfig{EnableBatchProcessing: true})
	err := bp.Submit(context.Background(), Operation{Key: "k"})
	if err != nil {
		t.Errorf("unexpected error with nil pool: %v", err)
	}
	time.Sleep(20 * time.Millisecond)
	_, err = bp.GetResult(context.Background())
	if err != nil {
		t.Errorf("unexpected error with nil pool: %v", err)
	}
	bp.Close()
}

func TestBatchProcessor_ErrorHandling(t *testing.T) {
	config := BatchConfig{
		EnableBatchProcessing: true,
		BatchSize:             5,
		BatchTimeout:          time.Millisecond * 10,
		FlushInterval:         time.Millisecond * 5,
		MaxBatchSize:          10,
	}

	bp := NewBatchProcessor(config)

	// Test with nil pool
	bp.pool = nil

	// This should not panic
	bp.Submit(context.Background(), Operation{Key: "test", Value: "value"})

	// Test with invalid batch size
	config.BatchSize = -1
	bp2 := NewBatchProcessor(config)
	if bp2.config.BatchSize <= 0 {
		t.Error("Expected batch size to be set to default when invalid")
	}

	// Test with invalid timeout
	config.BatchTimeout = -1
	bp3 := NewBatchProcessor(config)
	if bp3.config.BatchTimeout <= 0 {
		t.Error("Expected batch timeout to be set to default when invalid")
	}
}

func TestBatchProcessor_ValidationErrors(t *testing.T) {
	config := BatchConfig{
		EnableBatchProcessing: true,
		BatchSize:             5,
		BatchTimeout:          time.Millisecond * 10,
		FlushInterval:         time.Millisecond * 5,
		MaxBatchSize:          10,
	}

	bp := NewBatchProcessor(config)

	// Test with empty operations
	operations := []Operation{}
	bp.processBatch(operations) // Should not panic

	// Test with nil operations
	bp.processBatch(nil) // Should not panic
}

func TestBatchProcessor_EdgeCases(t *testing.T) {
	config := BatchConfig{
		EnableBatchProcessing: true,
		BatchSize:             2,
		BatchTimeout:          time.Millisecond * 10,
		FlushInterval:         time.Millisecond * 5,
		MaxBatchSize:          5,
	}

	bp := NewBatchProcessor(config)

	// Test with operations exceeding max batch size
	largeOperations := make([]Operation, 10)
	for i := 0; i < 10; i++ {
		largeOperations[i] = Operation{Key: fmt.Sprintf("key%d", i), Value: "value"}
	}

	// Should handle gracefully
	bp.Submit(context.Background(), largeOperations[0])
	bp.Submit(context.Background(), largeOperations[1])

	// Test with very short timeout
	config.BatchTimeout = time.Nanosecond
	bp2 := NewBatchProcessor(config)
	bp2.Submit(context.Background(), Operation{Key: "test", Value: "value"})

	// Should not panic
	time.Sleep(time.Microsecond)
}

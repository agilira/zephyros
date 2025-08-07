// objectpool_unit_test.go: Unit tests for zephyros object pool
//
// Copyright (c) 2025 AGILira
// Licensed under the Business Source License (BSL). Change Date: NEVER

package zephyros

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestObjectPool_BasicReuse(t *testing.T) {
	pool := NewObjectPool(2)

	// Get new operation (pool empty)
	op1 := pool.GetOperation()
	if op1 == nil {
		t.Fatal("Expected non-nil Operation from empty pool")
	}
	op1.Type = "set"
	pool.PutOperation(op1)

	// Get operation (should reuse)
	op2 := pool.GetOperation()
	if op2 == nil {
		t.Fatal("Expected non-nil Operation from pool")
	}
	if op2.Type != "" {
		t.Errorf("Expected reused Operation to be reset, got type=%s", op2.Type)
	}

	// Fill pool, then test overflow
	pool.PutOperation(op2)
	pool.PutOperation(&Operation{}) // Should be accepted
	pool.PutOperation(&Operation{}) // Should be discarded (overflow)
}

func TestObjectPool_ResultReuse(t *testing.T) {
	pool := NewObjectPool(1)
	res1 := pool.GetResult()
	if res1 == nil {
		t.Fatal("Expected non-nil OperationResult from empty pool")
	}
	res1.Success = true
	pool.PutResult(res1)

	res2 := pool.GetResult()
	if res2 == nil {
		t.Fatal("Expected non-nil OperationResult from pool")
	}
	if res2.Success {
		t.Errorf("Expected reused OperationResult to be reset, got Success=true")
	}
}

func TestObjectPool_NilAndClose(t *testing.T) {
	pool := NewObjectPool(1)
	pool.PutOperation(nil) // Should not panic
	pool.PutResult(nil)    // Should not panic
	pool.Close()
	// After close, further puts should not panic
	pool.PutOperation(&Operation{})
	pool.PutResult(&OperationResult{})
}

func TestObjectPool_DropInCompatibility(t *testing.T) {
	pool := NewObjectPool(2)

	// Test Get(id) compatibility method
	op, err := pool.Get("test_id")
	if err != nil {
		t.Errorf("Get() compatibility method error: %v", err)
	}
	if op == nil {
		t.Fatal("Expected non-nil Operation from Get()")
	}

	// Test Put(op) compatibility method
	op.Type = "test"
	pool.Put(op) // Should not panic

	// Verify pool still works normally
	op2 := pool.GetOperation()
	if op2 == nil {
		t.Fatal("Expected pool to still work after compatibility methods")
	}
}

func TestOperationPool_WithObjectPool(t *testing.T) {
	config := PoolConfig{
		WorkerCount:      2,
		QueueSize:        4,
		EnableMetrics:    true,
		EnableObjectPool: true,
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
		t.Fatalf("Failed to create pool with object pool: %v", err)
	}
	defer pool.Close()

	op := Operation{Type: "test", Key: "k", Value: "v"}
	err = pool.Submit(context.Background(), op)
	if err != nil {
		t.Errorf("Submit() error with object pool: %v", err)
	}
	_, err = pool.GetResult(context.Background())
	if err != nil {
		t.Errorf("GetResult() error with object pool: %v", err)
	}
}

func TestOperationPool_WithoutObjectPool(t *testing.T) {
	config := PoolConfig{
		WorkerCount:      2,
		QueueSize:        4,
		EnableMetrics:    true,
		EnableObjectPool: false,
	}
	handler := &mockHandler{}
	pool, err := NewOperationPool(config, handler)
	if err != nil {
		t.Fatalf("Failed to create pool without object pool: %v", err)
	}
	defer pool.Close()

	op := Operation{Type: "test", Key: "k", Value: "v"}
	err = pool.Submit(context.Background(), op)
	if err != nil {
		t.Errorf("Submit() error without object pool: %v", err)
	}
}

func TestObjectPool_OverflowAndClosed(t *testing.T) {
	pool := NewObjectPool(1)
	pool.PutOperation(&Operation{Key: "A"})
	pool.PutOperation(&Operation{Key: "B"}) // overflow, should not panic
	pool.Close()
	pool.PutOperation(&Operation{Key: "C"}) // after close, should not panic
	pool.PutResult(&OperationResult{OperationID: "X"})
	pool.PutResult(&OperationResult{OperationID: "Y"}) // overflow, should not panic
}

func TestObjectPool_GetOperation_ResetFields(t *testing.T) {
	pool := NewObjectPool(1)
	op := &Operation{Type: "set", Key: "k", Value: "v", Tags: []string{"t"}, Metadata: map[string]interface{}{}}
	pool.PutOperation(op)
	got := pool.GetOperation()
	if got.Type != "" || got.Key != "" || got.Value != "" || got.ID != "" {
		t.Error("Expected fields to be reset on reuse")
	}
}

func TestObjectPool_GetResult_ResetFields(t *testing.T) {
	pool := NewObjectPool(1)
	res := &OperationResult{OperationID: "id", Success: true, Data: 123, Error: nil, Duration: 1, Metadata: map[string]interface{}{}}
	pool.PutResult(res)
	got := pool.GetResult()
	if got.OperationID != "" || got.Success || got.Data != nil || got.Error != nil || got.Duration != 0 {
		t.Error("Expected fields to be reset on reuse")
	}
}

func TestObjectPool_Close_Idempotent(t *testing.T) {
	pool := NewObjectPool(1)
	pool.Close()
	pool.Close() // should not panic
}

func TestObjectPool_Concurrency(t *testing.T) {
	pool := NewObjectPool(10)
	done := make(chan struct{})
	go func() {
		for i := 0; i < 100; i++ {
			pool.PutOperation(&Operation{Key: "K"})
			pool.PutResult(&OperationResult{OperationID: "ID"})
		}
		done <- struct{}{}
	}()
	for i := 0; i < 100; i++ {
		_ = pool.GetOperation()
		_ = pool.GetResult()
	}
	<-done
}

func TestObjectPool_AutoGrow(t *testing.T) {
	pool := NewObjectPool(4)
	// Force many misses to trigger growth
	for i := 0; i < 100; i++ {
		_ = pool.GetOperation()
	}
	pool.mu.RLock()
	grown := pool.maxSize > 4
	pool.mu.RUnlock()
	if !grown {
		t.Error("expected pool to grow after many misses")
	}
}

func TestObjectPool_AutoShrink(t *testing.T) {
	pool := NewObjectPool(4)
	// First force growth
	for i := 0; i < 100; i++ {
		_ = pool.GetOperation()
	}
	// Fill the pool to max
	for i := 0; i < pool.maxSize; i++ {
		pool.PutOperation(&Operation{})
	}
	// For test: repeatedly force the resize threshold to 1 for immediate shrink
	for cycle := 0; cycle < 10; cycle++ {
		pool.SetResizeThreshold(1)
		for i := 0; i < pool.maxSize; i++ {
			_ = pool.GetOperation()
			pool.PutOperation(&Operation{})
		}
	}
	pool.mu.RLock()
	shrunk := pool.maxSize == 4
	pool.mu.RUnlock()
	if !shrunk {
		t.Errorf("expected pool to shrink back to min size after many hits, got %d", pool.maxSize)
	}
}

func TestObjectPool_Stress_NoLeak(t *testing.T) {
	pool := NewObjectPool(8)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				op := pool.GetOperation()
				pool.PutOperation(op)
				res := pool.GetResult()
				pool.PutResult(res)
			}
		}()
	}
	wg.Wait()
	// After stress, the pool should not be closed and should not lose objects
	pool.mu.RLock()
	if pool.closed {
		pool.mu.RUnlock()
		t.Fatal("pool should not be closed")
	}
	if len(pool.operations) > pool.maxSize || len(pool.results) > pool.maxSize {
		pool.mu.RUnlock()
		t.Fatal("pool exceeded max size")
	}
	pool.mu.RUnlock()
}

func TestObjectPool_ErrorHandling(t *testing.T) {
	// Test with invalid capacity
	pool := NewObjectPool(-1)
	if pool == nil {
		t.Error("Expected pool to be created even with invalid capacity")
	}

	// Test with zero capacity
	pool2 := NewObjectPool(0)
	if pool2 == nil {
		t.Error("Expected pool to be created with zero capacity")
	}

	// Test GetOperation with nil pool - this will panic, so we test it differently
	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic when calling GetOperation on nil pool")
		}
	}()
	var nilPool *ObjectPool
	nilPool.GetOperation()
}

func TestObjectPool_CapacityErrors(t *testing.T) {
	pool := NewObjectPool(2)

	// Fill the pool
	obj1 := pool.GetOperation()
	obj2 := pool.GetOperation()

	// Try to get more than capacity
	obj3 := pool.GetOperation()
	if obj3 == nil {
		t.Error("Expected object even when pool is empty")
	}

	// Return objects
	pool.PutOperation(obj1)
	pool.PutOperation(obj2)
	pool.PutOperation(obj3)

	// Pool should handle overflow gracefully
	pool.PutOperation(&Operation{Key: "extra"})
}

func TestObjectPool_CloseErrors(t *testing.T) {
	pool := NewObjectPool(5)

	// Test close with empty pool
	pool.Close()

	// Test close with some objects
	pool2 := NewObjectPool(5)
	obj1 := pool2.GetOperation()
	obj2 := pool2.GetOperation()
	pool2.PutOperation(obj1)
	pool2.PutOperation(obj2)
	pool2.Close()

	// Test close with nil pool - this will panic, so we test it differently
	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic when calling Close on nil pool")
		}
	}()
	var nilPool *ObjectPool
	nilPool.Close()
}

func TestObjectPool_EdgeCases(t *testing.T) {
	pool := NewObjectPool(1)

	// Test with nil object
	pool.PutOperation(nil) // Should not panic

	// Test GetOperation after putting nil
	obj := pool.GetOperation()
	if obj == nil {
		t.Error("Expected new operation after putting nil")
	}

	// Test multiple puts of same object
	testObj := &Operation{Key: "test"}
	pool.PutOperation(testObj)
	pool.PutOperation(testObj) // Should handle gracefully

	// Test GetOperation after multiple puts
	obj1 := pool.GetOperation()
	obj2 := pool.GetOperation()
	if obj1 == nil || obj2 == nil {
		t.Error("Expected objects after multiple puts")
	}
}

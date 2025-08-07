// zephyros_concurrent_test.go: Concurrent tests for zephyros
//
// Copyright (c) 2025 AGILira
// Licensed under the Business Source License (BSL). Change Date: NEVER

package zephyros

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestOperationPool_Concurrent_Submit(t *testing.T) {
	config := PoolConfig{
		WorkerCount:   0, // auto
		QueueSize:     0, // auto
		EnableMetrics: true,
		BatchConfig: BatchConfig{
			EnableBatchProcessing: true,
			BatchSize:             10,
			BatchTimeout:          20 * time.Millisecond,
			FlushInterval:         10 * time.Millisecond,
			MaxBatchSize:          100,
		},
	}

	handler := &mockHandler{
		processFunc: func(ctx context.Context, op Operation) (OperationResult, error) {
			// Simulate some processing time
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
		t.Fatalf("Failed to create pool: %v", err)
	}
	defer pool.Close()

	const numGoroutines = 10
	const operationsPerGoroutine = 20
	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines*operationsPerGoroutine)

	// Submit operations concurrently
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < operationsPerGoroutine; j++ {
				op := Operation{
					Type:  "concurrent_test",
					Key:   fmt.Sprintf("key_%d_%d", id, j),
					Value: fmt.Sprintf("value_%d_%d", id, j),
				}

				ctx := context.Background()
				err := pool.Submit(ctx, op)
				if err != nil {
					errors <- fmt.Errorf("goroutine %d operation %d: %v", id, j, err)
				}
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		t.Errorf("Concurrent submit error: %v", err)
	}

	// Wait for processing to complete in a smart way
	expectedOps := int64(numGoroutines * operationsPerGoroutine)
	maxWait := 5 * time.Second
	interval := 20 * time.Millisecond
	waited := time.Duration(0)
	var metrics PoolMetricsSnapshot
	for waited < maxWait {
		metrics = pool.GetMetrics()
		if metrics.ProcessedOps >= expectedOps {
			break
		}
		time.Sleep(interval)
		waited += interval
	}

	if metrics.ProcessedOps < expectedOps {
		t.Fatalf("[SmartTest] Expected at least %d processed operations, got %d after %v (real bug or system too slow)", expectedOps, metrics.ProcessedOps, waited)
	}
}

func TestOperationPool_Concurrent_GetResult(t *testing.T) {
	config := PoolConfig{
		WorkerCount:   0, // auto
		QueueSize:     0, // auto
		EnableMetrics: true,
		BatchConfig: BatchConfig{
			EnableBatchProcessing: true,
			BatchSize:             10,
			BatchTimeout:          20 * time.Millisecond,
			FlushInterval:         10 * time.Millisecond,
			MaxBatchSize:          100,
		},
	}

	handler := &mockHandler{
		processFunc: func(ctx context.Context, op Operation) (OperationResult, error) {
			// Simulate processing time
			time.Sleep(10 * time.Millisecond)
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

	const numGoroutines = 8
	const operationsPerGoroutine = 10
	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines*operationsPerGoroutine)
	results := make(chan OperationResult, numGoroutines*operationsPerGoroutine)

	// Submit operations
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < operationsPerGoroutine; j++ {
				op := Operation{
					Type:  "concurrent_result_test",
					Key:   fmt.Sprintf("key_%d_%d", id, j),
					Value: fmt.Sprintf("value_%d_%d", id, j),
				}

				ctx := context.Background()
				err := pool.Submit(ctx, op)
				if err != nil {
					errors <- fmt.Errorf("submit goroutine %d operation %d: %v", id, j, err)
					continue
				}
			}
		}(i)
	}

	// Get results concurrently
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < operationsPerGoroutine; j++ {
				ctx := context.Background()
				result, err := pool.GetResult(ctx)
				if err != nil {
					errors <- fmt.Errorf("get result goroutine %d operation %d: %v", id, j, err)
					continue
				}

				if !result.Success {
					errors <- fmt.Errorf("unsuccessful result in goroutine %d operation %d", id, j)
					continue
				}

				results <- result
			}
		}(i)
	}

	wg.Wait()
	close(errors)
	close(results)

	// Check for errors
	for err := range errors {
		t.Errorf("Concurrent get result error: %v", err)
	}

	// Check results
	resultCount := 0
	for result := range results {
		resultCount++
		if result.OperationID == "" {
			t.Error("Result should have operation ID")
		}
	}

	expectedResults := numGoroutines * operationsPerGoroutine
	if resultCount != expectedResults {
		t.Errorf("Expected %d results, got %d", expectedResults, resultCount)
	}
}

func TestOperationPool_Concurrent_MixedOperations(t *testing.T) {
	config := PoolConfig{
		WorkerCount:   0, // auto
		QueueSize:     0, // auto
		EnableMetrics: true,
		BatchConfig: BatchConfig{
			EnableBatchProcessing: true,
			BatchSize:             10,
			BatchTimeout:          20 * time.Millisecond,
			FlushInterval:         10 * time.Millisecond,
			MaxBatchSize:          100,
		},
	}

	handler := &mockHandler{
		processFunc: func(ctx context.Context, op Operation) (OperationResult, error) {
			// Simulate variable processing time
			time.Sleep(time.Duration(5+int(op.Key[len(op.Key)-1]-'0')) * time.Millisecond)
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

	const numGoroutines = 12
	const operationsPerGoroutine = 15
	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines*operationsPerGoroutine)

	// Mixed operations: submit and get results in same goroutine
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < operationsPerGoroutine; j++ {
				op := Operation{
					Type:  "mixed_test",
					Key:   fmt.Sprintf("key_%d_%d", id, j),
					Value: fmt.Sprintf("value_%d_%d", id, j),
				}

				ctx := context.Background()
				err := pool.Submit(ctx, op)
				if err != nil {
					errors <- fmt.Errorf("submit goroutine %d operation %d: %v", id, j, err)
					continue
				}

				// Get result immediately
				result, err := pool.GetResult(ctx)
				if err != nil {
					errors <- fmt.Errorf("get result goroutine %d operation %d: %v", id, j, err)
					continue
				}

				if !result.Success {
					errors <- fmt.Errorf("unsuccessful result in goroutine %d operation %d", id, j)
				}
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// Check for errors
	errorCount := 0
	for err := range errors {
		t.Errorf("Mixed operation error: %v", err)
		errorCount++
	}

	// Wait for final processing with smart polling
	expectedOps := int64(numGoroutines * operationsPerGoroutine)
	maxWait := 5 * time.Second
	interval := 20 * time.Millisecond
	waited := time.Duration(0)
	var metrics PoolMetricsSnapshot
	for waited < maxWait {
		metrics = pool.GetMetrics()
		if metrics.ProcessedOps >= expectedOps {
			break
		}
		time.Sleep(interval)
		waited += interval
	}

	if metrics.ProcessedOps < expectedOps {
		t.Fatalf("[SmartTest] Expected at least %d processed operations, got %d after %v", expectedOps, metrics.ProcessedOps, waited)
	}
}

func TestOperationPool_Concurrent_StressTest(t *testing.T) {
	config := PoolConfig{
		WorkerCount:   0, // auto
		QueueSize:     0, // auto
		EnableMetrics: true,
		BatchConfig: BatchConfig{
			EnableBatchProcessing: true,
			BatchSize:             10,
			BatchTimeout:          20 * time.Millisecond,
			FlushInterval:         10 * time.Millisecond,
			MaxBatchSize:          100,
		},
	}

	handler := &mockHandler{
		processFunc: func(ctx context.Context, op Operation) (OperationResult, error) {
			// Simulate variable processing time
			time.Sleep(time.Duration(1+int(op.Key[len(op.Key)-1]-'0')) * time.Millisecond)
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

	const numGoroutines = 10
	const operationsPerGoroutine = 20
	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines*operationsPerGoroutine)

	// Submit operations under stress
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < operationsPerGoroutine; j++ {
				op := Operation{
					Type:  "stress_test",
					Key:   fmt.Sprintf("key_%d_%d", id, j),
					Value: fmt.Sprintf("value_%d_%d", id, j),
				}

				ctx := context.Background()
				err := pool.Submit(ctx, op)
				if err != nil {
					errors <- fmt.Errorf("stress goroutine %d operation %d: %v", id, j, err)
				}
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// Check for errors
	errorCount := 0
	for err := range errors {
		t.Errorf("Stress test error: %v", err)
		errorCount++
	}

	// Wait for final processing with smart polling
	expectedOps := int64(numGoroutines * operationsPerGoroutine)
	maxWait := 10 * time.Second
	interval := 50 * time.Millisecond
	waited := time.Duration(0)
	var metrics PoolMetricsSnapshot
	for waited < maxWait {
		metrics = pool.GetMetrics()
		if metrics.ProcessedOps >= expectedOps {
			break
		}
		time.Sleep(interval)
		waited += interval
	}

	if metrics.ProcessedOps < expectedOps {
		t.Fatalf("[SmartTest] Expected at least %d processed operations, got %d after %v", expectedOps, metrics.ProcessedOps, waited)
	}

	t.Logf("Stress test completed: %d errors in %v", errorCount, waited)
}

func TestOperationPool_Concurrent_MultiplePools(t *testing.T) {
	// Test multiple pools running concurrently
	const numPools = 3
	const numGoroutines = 5
	const operationsPerGoroutine = 10

	pools := make([]*OperationPool, numPools)
	var wg sync.WaitGroup
	errors := make(chan error, numPools*numGoroutines*operationsPerGoroutine)

	// Create multiple pools
	for i := 0; i < numPools; i++ {
		config := PoolConfig{
			WorkerCount:   0, // auto
			QueueSize:     0, // auto
			EnableMetrics: true,
			BatchConfig: BatchConfig{
				EnableBatchProcessing: true,
				BatchSize:             10,
				BatchTimeout:          20 * time.Millisecond,
				FlushInterval:         10 * time.Millisecond,
				MaxBatchSize:          100,
			},
		}

		handler := &mockHandler{
			processFunc: func(ctx context.Context, op Operation) (OperationResult, error) {
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
			t.Fatalf("Failed to create pool %d: %v", i, err)
		}
		defer pool.Close()
		pools[i] = pool
	}

	// Submit operations to all pools concurrently
	for poolID := 0; poolID < numPools; poolID++ {
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(poolID, goroutineID int) {
				defer wg.Done()
				for j := 0; j < operationsPerGoroutine; j++ {
					op := Operation{
						Type:  fmt.Sprintf("multi_pool_%d", poolID),
						Key:   fmt.Sprintf("key_%d_%d_%d", poolID, goroutineID, j),
						Value: fmt.Sprintf("value_%d_%d_%d", poolID, goroutineID, j),
					}

					ctx := context.Background()
					err := pools[poolID].Submit(ctx, op)
					if err != nil {
						errors <- fmt.Errorf("pool %d goroutine %d operation %d: %v", poolID, goroutineID, j, err)
					}
				}
			}(poolID, i)
		}
	}

	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		t.Errorf("Multiple pools error: %v", err)
	}

	// Wait for all pools to complete processing
	expectedOps := int64(numPools * numGoroutines * operationsPerGoroutine)
	maxWait := 10 * time.Second
	interval := 50 * time.Millisecond
	waited := time.Duration(0)

	for waited < maxWait {
		totalProcessed := int64(0)
		for _, pool := range pools {
			metrics := pool.GetMetrics()
			totalProcessed += metrics.ProcessedOps
		}

		if totalProcessed >= expectedOps {
			break
		}
		time.Sleep(interval)
		waited += interval
	}

	totalProcessed := int64(0)
	for _, pool := range pools {
		metrics := pool.GetMetrics()
		totalProcessed += metrics.ProcessedOps
	}

	if totalProcessed < expectedOps {
		t.Fatalf("[SmartTest] Expected at least %d total processed operations across all pools, got %d after %v", expectedOps, totalProcessed, waited)
	}
}

func TestOperationPool_Concurrent_ExtremeStress(t *testing.T) {
	config := PoolConfig{
		WorkerCount:   0, // auto
		QueueSize:     0, // auto
		EnableMetrics: true,
		BatchConfig: BatchConfig{
			EnableBatchProcessing: true,
			BatchSize:             10,
			BatchTimeout:          20 * time.Millisecond,
			FlushInterval:         10 * time.Millisecond,
			MaxBatchSize:          100,
		},
		CacheConfig: CacheConfig{
			EnableCaching: true,
			CacheSize:     1000,
			TTL:           1 * time.Second,
		},
		RetryConfig: RetryConfig{
			EnableRetry:       true,
			MaxRetries:        3,
			RetryDelay:        10 * time.Millisecond,
			BackoffMultiplier: 2.0,
		},
		RateLimitConfig: RateLimitConfig{
			EnableRateLimit:   true,
			RequestsPerSecond: 1000,
			BurstSize:         100,
		},
		HealthConfig: HealthConfig{
			EnableHealthCheck:   true,
			HealthCheckInterval: 100 * time.Millisecond,
		},
		ValidationConfig: ValidationConfig{
			EnableValidation: true,
			MaxKeyLength:     1024,
			MaxValueLength:   1024 * 1024,
		},
	}

	handler := &mockHandler{
		processFunc: func(ctx context.Context, op Operation) (OperationResult, error) {
			// Simulate variable processing time with occasional errors
			time.Sleep(time.Duration(1+int(op.Timestamp.Nanosecond()%50)) * time.Microsecond)

			// Simulate occasional errors for retry testing
			if op.Timestamp.Nanosecond()%20 == 0 {
				return OperationResult{}, fmt.Errorf("simulated error for retry testing")
			}

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

	const numGoroutines = 20
	const operationsPerGoroutine = 50
	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines*operationsPerGoroutine)
	results := make(chan OperationResult, numGoroutines*operationsPerGoroutine)

	// Submit operations concurrently with extreme stress
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < operationsPerGoroutine; j++ {
				op := Operation{
					Type:      "extreme_stress_test",
					Key:       fmt.Sprintf("key_%d_%d", id, j),
					Value:     fmt.Sprintf("value_%d_%d", id, j),
					Timestamp: time.Now(),
				}

				ctx := context.Background()
				err := pool.Submit(ctx, op)
				if err != nil {
					errors <- fmt.Errorf("submit goroutine %d operation %d: %v", id, j, err)
					continue
				}

				// Get result immediately to test concurrent submit/get
				result, err := pool.GetResult(ctx)
				if err != nil {
					errors <- fmt.Errorf("get result goroutine %d operation %d: %v", id, j, err)
					continue
				}

				results <- result
			}
		}(i)
	}

	// Start additional goroutines for metrics and health monitoring
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				// Get metrics
				metrics := pool.GetMetrics()
				if metrics.ProcessedOps < 0 {
					errors <- fmt.Errorf("invalid processed ops count: %d", metrics.ProcessedOps)
				}

				// Get health status
				health := pool.GetHealthStatus()
				if health.LastCheck.IsZero() {
					errors <- fmt.Errorf("invalid health check time")
				}

				// Get cache stats
				cacheStats := pool.GetCacheStats()
				if cacheStats.Size < 0 {
					errors <- fmt.Errorf("invalid cache size: %d", cacheStats.Size)
				}

				time.Sleep(10 * time.Millisecond)
			}
		}(i)
	}

	wg.Wait()
	close(errors)
	close(results)

	// Check for errors
	errorCount := 0
	for err := range errors {
		t.Errorf("Extreme stress test error: %v", err)
		errorCount++
	}

	// Check results
	resultCount := 0
	for result := range results {
		resultCount++
		if result.OperationID == "" {
			t.Error("Result should have operation ID")
		}
	}

	expectedResults := numGoroutines * operationsPerGoroutine
	if resultCount != expectedResults {
		t.Errorf("Expected %d results, got %d", expectedResults, resultCount)
	}

	// Verify final metrics
	metrics := pool.GetMetrics()
	if metrics.ProcessedOps < int64(expectedResults) {
		t.Errorf("Expected at least %d processed operations, got %d", expectedResults, metrics.ProcessedOps)
	}

	if errorCount > 0 {
		t.Errorf("Encountered %d errors during extreme stress test", errorCount)
	}

	t.Logf("Extreme stress test completed: %d results, %d errors", resultCount, errorCount)
}

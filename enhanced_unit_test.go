package zephyros

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestNewRateLimiter_Allow_BurstAndRefill(t *testing.T) {
	rl := NewRateLimiter(RateLimitConfig{RequestsPerSecond: 10, BurstSize: 5})
	allowed := 0
	for i := 0; i < 5; i++ {
		if rl.Allow() {
			allowed++
		}
	}
	if allowed != 5 {
		t.Errorf("expected 5 allowed, got %d", allowed)
	}
	time.Sleep(200 * time.Millisecond)
	if !rl.Allow() {
		t.Error("expected token refill after sleep")
	}
}

func TestRateLimiter_ThreadSafety(t *testing.T) {
	rl := NewRateLimiter(RateLimitConfig{RequestsPerSecond: 100, BurstSize: 10})
	wg := sync.WaitGroup{}
	allowed := 0
	mu := sync.Mutex{}
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			if rl.Allow() {
				mu.Lock()
				allowed++
				mu.Unlock()
			}
			wg.Done()
		}()
	}
	wg.Wait()
	if allowed > 10 {
		t.Errorf("expected at most 10 allowed, got %d", allowed)
	}
}

func TestValidator_Validate_Errors(t *testing.T) {
	v := NewValidator(ValidationConfig{
		MaxKeyLength:    3,
		MaxValueLength:  3,
		MaxMetadataSize: 3,
		AllowedTypes:    []string{"A"},
		ForbiddenKeys:   []string{"forbidden"},
	})
	err := v.Validate(Operation{Key: "abcd"})
	if err == nil {
		t.Error("expected key length error")
	}
	err = v.Validate(Operation{Key: "a", Value: "abcd"})
	if err == nil {
		t.Error("expected value length error")
	}
	err = v.Validate(Operation{Key: "a", Value: "a", Metadata: map[string]interface{}{"k": "abcd"}})
	if err == nil {
		t.Error("expected metadata size error")
	}
	err = v.Validate(Operation{Key: "a", Value: "a", Type: "B"})
	if err == nil {
		t.Error("expected type not allowed error")
	}
	err = v.Validate(Operation{Key: "forbidden", Value: "a", Type: "A"})
	if err == nil {
		t.Error("expected forbidden key error")
	}
}

func TestValidator_Validate_Success(t *testing.T) {
	v := NewValidator(ValidationConfig{
		MaxKeyLength:    10,
		MaxValueLength:  10,
		MaxMetadataSize: 100,
		AllowedTypes:    []string{"A", "B"},
		ForbiddenKeys:   []string{"forbidden"},
	})
	err := v.Validate(Operation{Key: "ok", Value: "ok", Type: "A", Metadata: map[string]interface{}{}})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestHealthChecker_Basic(t *testing.T) {
	pool := &OperationPool{
		metrics: &PoolMetrics{},
	}
	hc := NewHealthChecker(HealthConfig{
		HealthCheckInterval:  10 * time.Millisecond,
		MaxQueueLatency:      1 * time.Second,
		MaxProcessingLatency: 1 * time.Second,
	}, pool)
	time.Sleep(20 * time.Millisecond)
	status := hc.GetStatus()
	if !status.Healthy {
		t.Error("expected healthy status")
	}
	hc.Stop()
}

func TestLatencyTracker_RecordAndPercentile(t *testing.T) {
	lt := NewLatencyTracker()
	for i := 1; i <= 100; i++ {
		lt.Record(time.Duration(i) * time.Millisecond)
	}
	p90 := lt.GetPercentile(90)
	if p90 < 90*time.Millisecond || p90 > 100*time.Millisecond {
		t.Errorf("unexpected 90th percentile: %v", p90)
	}
}

func TestLatencyTracker_ThroughputAndReset(t *testing.T) {
	lt := NewLatencyTracker()
	lt.Record(10 * time.Millisecond)
	if lt.GetThroughput() < 0 {
		t.Error("throughput should not be negative")
	}
	lt.Reset()
	if lt.GetPercentile(50) != 0 {
		t.Error("expected zero percentile after reset")
	}
}

func TestMin(t *testing.T) {
	if min(1, 2) != 1 || min(2, 1) != 1 {
		t.Error("min function failed")
	}
}

func TestNewRateLimiter_ZeroConfig(t *testing.T) {
	rl := NewRateLimiter(RateLimitConfig{RequestsPerSecond: 0, BurstSize: 0})
	if rl.Allow() {
		t.Error("expected Allow to return false with zero config")
	}
}

func TestLatencyTracker_Empty(t *testing.T) {
	lt := NewLatencyTracker()
	if lt.GetPercentile(50) != 0 {
		t.Error("expected zero percentile for empty tracker")
	}
	if lt.GetThroughput() != 0 {
		t.Error("expected zero throughput for empty tracker")
	}
}

func TestHealthChecker_GetStatus_ThreadSafety(t *testing.T) {
	pool := &OperationPool{metrics: &PoolMetrics{}}
	hc := NewHealthChecker(HealthConfig{HealthCheckInterval: 1 * time.Millisecond}, pool)
	t.Cleanup(hc.Stop)
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = hc.GetStatus()
		}()
	}
	wg.Wait()
}

func TestHealthChecker_Stop_Idempotent(t *testing.T) {
	pool := &OperationPool{metrics: &PoolMetrics{}}
	hc := NewHealthChecker(HealthConfig{HealthCheckInterval: 1 * time.Millisecond}, pool)
	hc.Stop()
	// Should not panic if called again
	hc.Stop()
}

func TestLatencyTracker_GetPercentile_EdgeCases(t *testing.T) {
	lt := NewLatencyTracker()
	if got := lt.GetPercentile(50); got != 0 {
		t.Errorf("expected 0 for empty latencies, got %v", got)
	}
	lt.Record(10 * time.Millisecond)
	lt.Record(20 * time.Millisecond)
	lt.Record(30 * time.Millisecond)
	if got := lt.GetPercentile(0); got != 10*time.Millisecond {
		t.Errorf("expected min latency, got %v", got)
	}
	if got := lt.GetPercentile(100); got != 30*time.Millisecond {
		t.Errorf("expected max latency, got %v", got)
	}
	if got := lt.GetPercentile(200); got != 30*time.Millisecond {
		t.Errorf("expected max latency for >100 percentile, got %v", got)
	}
}

func TestLatencyTracker_GetThroughput(t *testing.T) {
	lt := NewLatencyTracker()
	if got := lt.GetThroughput(); got != 0 {
		t.Errorf("expected 0 throughput for no ops, got %v", got)
	}
	lt.Record(10 * time.Millisecond)
	time.Sleep(10 * time.Millisecond)
	if got := lt.GetThroughput(); got <= 0 {
		t.Errorf("expected positive throughput, got %v", got)
	}
}

func TestLatencyTracker_Reset(t *testing.T) {
	lt := NewLatencyTracker()
	for i := 0; i < lt.maxSize; i++ {
		lt.Record(time.Duration(i) * time.Millisecond)
	}
	lt.Reset()
	if lt.count != 0 {
		t.Errorf("expected count to be 0 after reset, got %d", lt.count)
	}
	if lt.GetPercentile(50) != 0 {
		t.Errorf("expected percentile to be 0 after reset, got %v", lt.GetPercentile(50))
	}
}

func TestMinFunction(t *testing.T) {
	if min(1, 2) != 1 {
		t.Error("expected min(1,2) == 1")
	}
	if min(2, 1) != 1 {
		t.Error("expected min(2,1) == 1")
	}
	if min(2, 2) != 2 {
		t.Error("expected min(2,2) == 2")
	}
}

func TestLatencyTracker_RingBuffer_WrapAround(t *testing.T) {
	lt := NewLatencyTracker()
	for i := 0; i < lt.maxSize+10; i++ {
		lt.Record(time.Duration(i) * time.Millisecond)
	}
	if lt.count != int64(lt.maxSize) {
		t.Errorf("expected count to be %d, got %d", lt.maxSize, lt.count)
	}
	// The oldest value should be 10ms
	if got := lt.GetPercentile(0); got != 10*time.Millisecond {
		t.Errorf("expected min latency to be 10ms, got %v", got)
	}
	// The newest value should be (maxSize+9)ms
	if got := lt.GetPercentile(100); got != time.Duration(lt.maxSize+9)*time.Millisecond {
		t.Errorf("expected max latency to be %v, got %v", time.Duration(lt.maxSize+9)*time.Millisecond, got)
	}
}

func TestLatencyTracker_RingBuffer_Reset(t *testing.T) {
	lt := NewLatencyTracker()
	for i := 0; i < lt.maxSize; i++ {
		lt.Record(time.Duration(i) * time.Millisecond)
	}
	lt.Reset()
	if lt.count != 0 {
		t.Errorf("expected count to be 0 after reset, got %d", lt.count)
	}
	if got := lt.GetPercentile(50); got != 0 {
		t.Errorf("expected percentile to be 0 after reset, got %v", got)
	}
}

func TestValidator_ErrorHandling(t *testing.T) {
	config := ValidationConfig{
		MaxKeyLength:   10,
		MaxValueLength: 100,
		ForbiddenKeys:  []string{"forbidden"},
	}

	validator := NewValidator(config)

	// Test with forbidden key
	err := validator.Validate(Operation{Key: "forbidden", Value: "test"})
	if err == nil {
		t.Error("Expected error for forbidden key")
	}
	if err.Error() != "key forbidden is forbidden" {
		t.Errorf("Expected specific error message, got: %v", err)
	}

	// Test with valid key
	err = validator.Validate(Operation{Key: "valid", Value: "test"})
	if err != nil {
		t.Errorf("Expected no error for valid key, got: %v", err)
	}
}

func TestHealthChecker_ErrorHandling(t *testing.T) {
	pool, err := NewOperationPool(PoolConfig{
		WorkerCount: 2,
		QueueSize:   100,
	}, nil)
	if err != nil {
		t.Fatalf("Failed to create operation pool: %v", err)
	}
	defer pool.Close()

	config := HealthConfig{
		HealthCheckInterval:  time.Millisecond * 10,
		MaxQueueLatency:      time.Second,
		MaxProcessingLatency: time.Second,
	}

	hc := NewHealthChecker(config, pool)
	defer hc.Stop()

	// Wait for health check to run
	time.Sleep(time.Millisecond * 20)

	status := hc.GetStatus()
	if !status.Healthy {
		t.Error("Expected healthy status for new pool")
	}

	// Test health checker stop
	hc.Stop()
	time.Sleep(time.Millisecond * 10)

	// Should still be able to get status after stop
	status = hc.GetStatus()
	if status.Status == "" {
		t.Error("Expected status to be available after stop")
	}
}

func TestHealthChecker_UnhealthyConditions(t *testing.T) {
	// Create a handler that takes time to process
	handler := &slowHandler{}

	pool, err := NewOperationPool(PoolConfig{
		WorkerCount: 1,
		QueueSize:   2, // Small pool to trigger issues
	}, handler)
	if err != nil {
		t.Fatalf("Failed to create operation pool: %v", err)
	}
	defer pool.Close()

	config := HealthConfig{
		HealthCheckInterval:  time.Millisecond * 10,
		MaxQueueLatency:      time.Millisecond * 1, // Very low threshold
		MaxProcessingLatency: time.Millisecond * 1, // Very low threshold
	}

	hc := NewHealthChecker(config, pool)
	defer hc.Stop()

	// Add operations to trigger queue latency
	for i := 0; i < 10; i++ {
		pool.Submit(context.Background(), Operation{Key: fmt.Sprintf("key%d", i), Value: "value"})
	}

	// Wait for health check to run
	time.Sleep(time.Millisecond * 100)

	status := hc.GetStatus()
	if status.Healthy {
		t.Error("Expected unhealthy status due to queue latency")
	}

	if status.ErrorCount == 0 {
		t.Error("Expected error count to be greater than 0")
	}
}

// slowHandler implements OperationHandler for testing slow operations
type slowHandler struct{}

func (h *slowHandler) Process(ctx context.Context, op Operation) (OperationResult, error) {
	time.Sleep(time.Millisecond * 10) // Simulate slow processing
	return OperationResult{
		OperationID: op.ID,
		Success:     true,
		Duration:    time.Millisecond * 10,
	}, nil
}

func TestLatencyTracker_EdgeCases(t *testing.T) {
	lt := NewLatencyTracker()

	// Test with zero duration
	lt.Record(0)
	percentile := lt.GetPercentile(50)
	if percentile != 0 {
		t.Errorf("Expected 0 percentile for zero duration, got %v", percentile)
	}

	// Test with negative percentile
	lt.Record(time.Millisecond)
	percentile = lt.GetPercentile(-1)
	if percentile != 0 {
		t.Errorf("Expected 0 percentile for negative value, got %v", percentile)
	}

	// Test with percentile > 100
	percentile = lt.GetPercentile(101)
	if percentile != time.Millisecond {
		t.Errorf("Expected max duration for percentile > 100, got %v", percentile)
	}

	// Test throughput with no operations
	lt.Reset()
	throughput := lt.GetThroughput()
	if throughput != 0 {
		t.Errorf("Expected 0 throughput for empty tracker, got %f", throughput)
	}
}

func TestRateLimiter_EdgeCases(t *testing.T) {
	config := RateLimitConfig{
		RequestsPerSecond: 10,
		BurstSize:         5,
	}

	rl := NewRateLimiter(config)

	// Test with negative tokens
	rl.tokens = -1
	if rl.Allow() {
		t.Error("Expected no allowance with negative tokens")
	}

	// Test with more tokens than max
	rl.tokens = 100
	if !rl.Allow() {
		t.Error("Expected allowance with sufficient tokens")
	}

	// Test with zero requests per second
	config.RequestsPerSecond = 0
	rl2 := NewRateLimiter(config)
	rl2.tokens = 0
	if rl2.Allow() {
		t.Error("Expected no allowance with zero requests per second")
	}
}

func TestOperationPool_ConcurrentStress(t *testing.T) {
	config := PoolConfig{
		WorkerCount: 4,
		QueueSize:   100,
	}

	// Create a simple handler
	handler := &testHandler{}

	pool, err := NewOperationPool(config, handler)
	if err != nil {
		t.Fatalf("Failed to create operation pool: %v", err)
	}
	defer pool.Close()

	// Add many operations concurrently
	var wg sync.WaitGroup
	numOps := 1000
	numGoroutines := 10

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOps/numGoroutines; j++ {
				pool.Submit(context.Background(), Operation{
					Key:   fmt.Sprintf("key_%d_%d", id, j),
					Value: fmt.Sprintf("value_%d_%d", id, j),
				})
			}
		}(i)
	}

	wg.Wait()

	// Wait for processing to complete
	time.Sleep(time.Millisecond * 100)

	metrics := pool.GetMetrics()
	if metrics.ProcessedOps == 0 {
		t.Error("Expected some processed operations")
	}

	if metrics.FailedOps > 0 {
		t.Errorf("Expected no failed operations, got %d", metrics.FailedOps)
	}
}

// testHandler implements OperationHandler for testing
type testHandler struct{}

func (h *testHandler) Process(ctx context.Context, op Operation) (OperationResult, error) {
	time.Sleep(time.Microsecond) // Simulate work
	return OperationResult{
		OperationID: op.ID,
		Success:     true,
		Duration:    time.Microsecond,
	}, nil
}

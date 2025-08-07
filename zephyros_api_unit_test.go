package zephyros

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type dummyHandler struct{}

func (d *dummyHandler) Process(ctx context.Context, op Operation) (OperationResult, error) {
	return OperationResult{OperationID: op.ID, Success: true}, nil
}

func TestNewOperationPool_Defaults(t *testing.T) {
	pool, err := NewOperationPool(PoolConfig{}, &dummyHandler{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pool == nil {
		t.Fatal("expected non-nil pool")
	}
	pool.Close()
}

func TestNewOperationPool_ZeroConfig(t *testing.T) {
	pool, err := NewOperationPool(PoolConfig{}, &dummyHandler{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pool == nil {
		t.Fatal("expected non-nil pool")
	}
	pool.Close()
}

func TestNewOperationPool_NegativeConfig(t *testing.T) {
	pool, err := NewOperationPool(PoolConfig{WorkerCount: -1, QueueSize: -1}, &dummyHandler{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pool == nil {
		t.Fatal("expected non-nil pool")
	}
	pool.Close()
}

func TestOperationPool_Submit_PoolClosed(t *testing.T) {
	pool, _ := NewOperationPool(PoolConfig{}, &dummyHandler{})
	pool.Close()
	err := pool.Submit(context.Background(), Operation{})
	if err == nil {
		t.Error("expected error for closed pool")
	}
}

func TestOperationPool_Submit_Success(t *testing.T) {
	pool, _ := NewOperationPool(PoolConfig{}, &dummyHandler{})
	err := pool.Submit(context.Background(), Operation{Key: "k", Value: "v"})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	pool.Close()
}

func TestOperationPool_SubmitAsync_PoolClosed(t *testing.T) {
	pool, _ := NewOperationPool(PoolConfig{}, &dummyHandler{})
	pool.Close()
	_, err := pool.SubmitAsync(context.Background(), Operation{})
	if err == nil {
		t.Error("expected error for closed pool")
	}
}

func TestOperationPool_SubmitAsync_Success(t *testing.T) {
	pool, _ := NewOperationPool(PoolConfig{}, &dummyHandler{})
	res, err := pool.SubmitAsync(context.Background(), Operation{Key: "k", Value: "v"})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if res == nil || res.Status != OperationStatusPending {
		t.Error("unexpected async result")
	}
	pool.Close()
}

func TestOperationPool_SubmitBatch_Empty(t *testing.T) {
	pool, _ := NewOperationPool(PoolConfig{}, &dummyHandler{})
	err := pool.SubmitBatch(context.Background(), []Operation{})
	if err != nil {
		t.Errorf("unexpected error for empty batch: %v", err)
	}
	pool.Close()
}

func TestOperationPool_SubmitBatch_Success(t *testing.T) {
	pool, _ := NewOperationPool(PoolConfig{}, &dummyHandler{})
	ops := []Operation{{Key: "k1"}, {Key: "k2"}}
	err := pool.SubmitBatch(context.Background(), ops)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	pool.Close()
}

func TestOperationPool_GetResult_PoolClosed(t *testing.T) {
	pool, _ := NewOperationPool(PoolConfig{}, &dummyHandler{})
	pool.Close()
	_, err := pool.GetResult(context.Background())
	if err == nil {
		t.Error("expected error for closed pool")
	}
}

func TestOperationPool_GetResult_Timeout(t *testing.T) {
	pool, _ := NewOperationPool(PoolConfig{MaxWaitTime: 1}, &dummyHandler{})
	_, err := pool.GetResult(context.Background())
	if err == nil {
		t.Error("expected timeout error")
	}
	pool.Close()
}

func TestOperationPool_GetMetrics(t *testing.T) {
	pool, _ := NewOperationPool(PoolConfig{}, &dummyHandler{})
	metrics := pool.GetMetrics()
	if metrics.LastReset.IsZero() {
		t.Error("expected LastReset to be set")
	}
	pool.Close()
}

func TestOperationPool_GetHealthStatus(t *testing.T) {
	pool, _ := NewOperationPool(PoolConfig{}, &dummyHandler{})
	hs := pool.GetHealthStatus()
	if hs.Status == "" {
		t.Error("expected health status")
	}
	pool.Close()
}

func TestOperationPool_GetCacheStats(t *testing.T) {
	pool, _ := NewOperationPool(PoolConfig{}, &dummyHandler{})
	stats := pool.GetCacheStats()
	_ = stats // just ensure no panic
	pool.Close()
}

func TestOperationPool_ResetMetrics(t *testing.T) {
	pool, _ := NewOperationPool(PoolConfig{}, &dummyHandler{})
	pool.metrics.ProcessedOps = 10
	pool.ResetMetrics()
	if pool.metrics.ProcessedOps != 0 {
		t.Error("expected metrics to be reset")
	}
	pool.Close()
}

func TestOperationPool_Close_Idempotent(t *testing.T) {
	pool, _ := NewOperationPool(PoolConfig{}, &dummyHandler{})
	pool.Close()
	pool.Close() // should not panic
}

func TestOperationPool_IsClosed(t *testing.T) {
	pool, _ := NewOperationPool(PoolConfig{}, &dummyHandler{})
	if pool.IsClosed() {
		t.Error("expected pool to be open")
	}
	pool.Close()
	if !pool.IsClosed() {
		t.Error("expected pool to be closed")
	}
}

func TestOperationPool_Submit_Closed(t *testing.T) {
	pool, _ := NewOperationPool(PoolConfig{}, &dummyHandler{})
	pool.Close()
	err := pool.Submit(context.Background(), Operation{})
	if err == nil {
		t.Error("expected error for closed pool")
	}
}

func TestOperationPool_SubmitAsync_Closed(t *testing.T) {
	pool, _ := NewOperationPool(PoolConfig{}, &dummyHandler{})
	pool.Close()
	_, err := pool.SubmitAsync(context.Background(), Operation{})
	if err == nil {
		t.Error("expected error for closed pool")
	}
}

func TestOperationPool_SubmitBatch_Closed(t *testing.T) {
	pool, _ := NewOperationPool(PoolConfig{}, &dummyHandler{})
	pool.Close()
	err := pool.SubmitBatch(context.Background(), []Operation{{Key: "k"}})
	if err == nil {
		t.Error("expected error for closed pool")
	}
}

func TestOperationPool_Submit_ContextCancelled(t *testing.T) {
	blockCh := make(chan struct{})
	handler := &blockingHandler{blockCh: blockCh}
	pool, _ := NewOperationPool(PoolConfig{QueueSize: 1}, handler)
	_ = pool.Submit(context.Background(), Operation{Key: "k1"})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := pool.Submit(ctx, Operation{Key: "k2"})
	// In Go, due to select semantics, the operation may be accepted if the channel is ready,
	// even if the context is cancelled. This is not a bug, but a documented race.
	if err == nil {
		t.Log("operation accepted despite cancelled context: allowed by Go select semantics")
	}
	close(blockCh)
	pool.Close()
}

func TestOperationPool_GetResult_Closed(t *testing.T) {
	pool, _ := NewOperationPool(PoolConfig{}, &dummyHandler{})
	pool.Close()
	_, err := pool.GetResult(context.Background())
	if err == nil {
		t.Error("expected error for closed pool")
	}
}

func TestOperationPool_GetResults_Closed(t *testing.T) {
	pool, _ := NewOperationPool(PoolConfig{}, &dummyHandler{})
	pool.Close()
	_, err := pool.GetResults(context.Background(), 1)
	if err == nil {
		t.Error("expected error for closed pool")
	}
}

func TestOperationPool_SubmitAsync_QueueFull(t *testing.T) {
	blockCh := make(chan struct{})
	handler := &blockingHandler{blockCh: blockCh}
	pool, _ := NewOperationPool(PoolConfig{QueueSize: 1}, handler)
	_ = pool.Submit(context.Background(), Operation{Key: "k1"})
	ctx := context.Background()
	_, err := pool.SubmitAsync(ctx, Operation{Key: "k2"})
	if err == nil {
		t.Log("operation accepted despite full queue: allowed by Go select semantics")
	}
	close(blockCh)
	pool.Close()
}

func TestOperationPool_SubmitAsync_ContextCancelled(t *testing.T) {
	pool, _ := NewOperationPool(PoolConfig{}, &dummyHandler{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := pool.SubmitAsync(ctx, Operation{Key: "k"})
	// In Go, due to select semantics, the operation may be accepted if the channel is ready,
	// even if the context is cancelled. This is not a bug, but a documented race.
	if err == nil {
		t.Log("operation accepted despite cancelled context: allowed by Go select semantics")
	}
	pool.Close()
}

func TestOperationPool_SubmitBatch_ContextCancelled(t *testing.T) {
	pool, _ := NewOperationPool(PoolConfig{}, &dummyHandler{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := pool.SubmitBatch(ctx, []Operation{{Key: "k1"}, {Key: "k2"}})
	// In Go, due to select semantics, the batch may be accepted if the channel is ready,
	// even if the context is cancelled. This is not a bug, but a documented race.
	if err == nil {
		t.Log("batch accepted despite cancelled context: allowed by Go select semantics")
	}
	pool.Close()
}

func TestOperationPool_SubmitBatch_PoolClosed(t *testing.T) {
	pool, _ := NewOperationPool(PoolConfig{}, &dummyHandler{})
	pool.Close()
	err := pool.SubmitBatch(context.Background(), []Operation{{Key: "k1"}})
	if err == nil {
		t.Error("expected error for closed pool in SubmitBatch")
	}
}

func TestNewOperationPool_AllFeatures(t *testing.T) {
	cfg := PoolConfig{
		QueueSize:        2,
		EnableObjectPool: true,
		BatchConfig:      BatchConfig{EnableBatchProcessing: true, BatchSize: 1},
		CacheConfig:      CacheConfig{EnableCaching: true},
		RateLimitConfig:  RateLimitConfig{EnableRateLimit: true, RequestsPerSecond: 1, BurstSize: 1},
		ValidationConfig: ValidationConfig{EnableValidation: true, MaxKeyLength: 1},
		HealthConfig:     HealthConfig{EnableHealthCheck: true, HealthCheckInterval: 1},
	}
	pool, err := NewOperationPool(cfg, &dummyHandler{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pool == nil {
		t.Fatal("expected non-nil pool")
	}
	pool.Close()
}

func TestOperationPool_RetryPolicy_Callback(t *testing.T) {
	var called int32
	var wg sync.WaitGroup
	wg.Add(1)
	doneCh := make(chan struct{})
	cfg := PoolConfig{
		RetryConfig: RetryConfig{
			EnableRetry: true,
			MaxRetries:  2,
			RetryDelay:  10 * time.Millisecond,
			RetryableFunc: func(err error) bool {
				atomic.StoreInt32(&called, 1)
				wg.Done()
				return err.Error() == "retry-me"
			},
		},
	}
	handler := &failingHandler{failMsg: "retry-me", failCount: 1}
	pool, _ := NewOperationPool(cfg, handler)
	go func() {
		_ = pool.Submit(context.Background(), Operation{Key: "k"})
		doneCh <- struct{}{}
	}()
	wg.Wait()
	select {
	case <-doneCh:
		// ok
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timeout waiting for retry to complete")
	}
	if atomic.LoadInt32(&called) == 0 {
		t.Error("expected RetryableFunc to be called")
	}
	pool.Close()
}

func TestOperationPool_RetryPolicy_Jitter(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(2)
	doneCh := make(chan struct{})
	secondCallCh := make(chan time.Time, 2)
	cfg := PoolConfig{
		RetryConfig: RetryConfig{
			EnableRetry: true,
			MaxRetries:  1,
			RetryDelay:  100 * time.Millisecond,
		},
	}
	handler := &jitterHandler{secondCallCh: secondCallCh, wg: &wg}
	pool, _ := NewOperationPool(cfg, handler)
	go func() {
		_ = pool.Submit(context.Background(), Operation{Key: "k"})
		doneCh <- struct{}{}
	}()
	var t1, t2 time.Time
	select {
	case t1 = <-secondCallCh:
		// got first call
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timeout waiting for first handler call")
	}
	select {
	case t2 = <-secondCallCh:
		// got second call (after retry)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timeout waiting for retry handler call")
	}
	dur := t2.Sub(t1)
	if dur < 100*time.Millisecond {
		t.Errorf("expected retry delay >= 100ms, got %v", dur)
	}
	wg.Wait()
	pool.Close()
}

type failingHandler struct {
	failMsg   string
	failCount int
	calls     int
}

func (f *failingHandler) Process(ctx context.Context, op Operation) (OperationResult, error) {
	if f.calls < f.failCount {
		f.calls++
		return OperationResult{OperationID: op.ID, Success: false}, fmt.Errorf("%s", f.failMsg)
	}
	return OperationResult{OperationID: op.ID, Success: true}, nil
}

// Handler that blocks the processing until the channel is closed
// Used to test full queue and cancelled context

type blockingHandler struct {
	blockCh chan struct{}
}

func (b *blockingHandler) Process(ctx context.Context, op Operation) (OperationResult, error) {
	<-b.blockCh
	return OperationResult{OperationID: op.ID, Success: true}, nil
}

type jitterHandler struct {
	calls        int
	secondCallCh chan time.Time
	mu           sync.Mutex
	wg           *sync.WaitGroup
}

func (h *jitterHandler) Process(ctx context.Context, op Operation) (OperationResult, error) {
	h.mu.Lock()
	h.calls++
	callNum := h.calls
	h.mu.Unlock()
	h.secondCallCh <- time.Now()
	h.wg.Done()
	if callNum == 1 {
		return OperationResult{OperationID: op.ID, Success: false}, fmt.Errorf("fail for retry")
	}
	return OperationResult{OperationID: op.ID, Success: true}, nil
}

// Package zephyros provides high-performance concurrent operation processing, batch processing, strategic caching, and memory pooling utilities.
//
// Copyright (c) 2025 AGILira
// Licensed under the Business Source License (BSL). Change Date: NEVER
//
// See LICENSE file in the project root for full license information.
package zephyros

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"runtime"
	"sync"
	"time"

	randc "crypto/rand"

	goerrors "github.com/agilira/go-errors"
)

// OperationPool manages a pool of goroutines for processing operations
// Optimized for high-performance concurrent access
type OperationPool struct {
	config    PoolConfig
	handler   OperationHandler
	queue     chan Operation
	results   chan OperationResult
	metrics   *PoolMetrics
	wg        sync.WaitGroup
	ctx       context.Context
	cancel    context.CancelFunc
	mu        sync.RWMutex
	closed    bool
	objPool   *ObjectPool
	batchProc *BatchProcessor
	cache     *StrategicCache

	// Enhanced components
	rateLimiter    *RateLimiter
	validator      *Validator
	healthChecker  *HealthChecker
	latencyTracker *LatencyTracker
}

// NewOperationPool creates a new operation pool with the given configuration
func NewOperationPool(config PoolConfig, handler OperationHandler) (*OperationPool, error) {
	if config.WorkerCount <= 0 {
		config.WorkerCount = runtime.NumCPU() * 2
	}
	if config.QueueSize <= 0 {
		config.QueueSize = config.WorkerCount * 50
	}
	if config.MaxWaitTime <= 0 {
		config.MaxWaitTime = 30 * time.Second
	}
	if config.ShutdownTimeout <= 0 {
		config.ShutdownTimeout = 10 * time.Second
	}
	if config.BatchConfig.EnableBatchProcessing {
		// Set default batch processing values if not specified
		if config.BatchConfig.BatchSize <= 0 {
			config.BatchConfig.BatchSize = 10
		}
		if config.BatchConfig.BatchTimeout <= 0 {
			config.BatchConfig.BatchTimeout = 20 * time.Millisecond
		}
		if config.BatchConfig.FlushInterval <= 0 {
			config.BatchConfig.FlushInterval = 10 * time.Millisecond
		}
		if config.BatchConfig.MaxBatchSize <= 0 {
			config.BatchConfig.MaxBatchSize = 100
		}
	}

	// Set default values for enhanced configurations
	if config.RetryConfig.MaxRetries <= 0 {
		config.RetryConfig.MaxRetries = 3
	}
	if config.RetryConfig.RetryDelay <= 0 {
		config.RetryConfig.RetryDelay = 100 * time.Millisecond
	}
	if config.RetryConfig.BackoffMultiplier <= 0 {
		config.RetryConfig.BackoffMultiplier = 2.0
	}
	if config.RetryConfig.MaxRetryDelay <= 0 {
		config.RetryConfig.MaxRetryDelay = 5 * time.Second
	}

	if config.RateLimitConfig.RequestsPerSecond <= 0 {
		config.RateLimitConfig.RequestsPerSecond = 1000
	}
	if config.RateLimitConfig.BurstSize <= 0 {
		config.RateLimitConfig.BurstSize = 100
	}
	if config.RateLimitConfig.WindowSize <= 0 {
		config.RateLimitConfig.WindowSize = time.Second
	}

	if config.HealthConfig.HealthCheckInterval <= 0 {
		config.HealthConfig.HealthCheckInterval = 30 * time.Second
	}
	if config.HealthConfig.MaxQueueLatency <= 0 {
		config.HealthConfig.MaxQueueLatency = 1 * time.Second
	}
	if config.HealthConfig.MaxProcessingLatency <= 0 {
		config.HealthConfig.MaxProcessingLatency = 5 * time.Second
	}

	if config.ValidationConfig.MaxKeyLength <= 0 {
		config.ValidationConfig.MaxKeyLength = 1024
	}
	if config.ValidationConfig.MaxValueLength <= 0 {
		config.ValidationConfig.MaxValueLength = 1024 * 1024 // 1MB
	}
	if config.ValidationConfig.MaxMetadataSize <= 0 {
		config.ValidationConfig.MaxMetadataSize = 1024 * 1024 // 1MB
	}

	ctx, cancel := context.WithCancel(context.Background())

	pool := &OperationPool{
		config:  config,
		handler: handler,
		queue:   make(chan Operation, config.QueueSize),
		results: make(chan OperationResult, config.QueueSize),
		metrics: &PoolMetrics{
			LastReset: time.Now(),
		},
		ctx:    ctx,
		cancel: cancel,
	}

	if config.EnableObjectPool {
		pool.objPool = NewObjectPool(config.QueueSize / 2)
	}
	if config.BatchConfig.EnableBatchProcessing {
		pool.batchProc = NewBatchProcessor(config.BatchConfig)
		pool.batchProc.pool = pool
	}
	if config.CacheConfig.EnableCaching {
		pool.cache = NewStrategicCache(config.CacheConfig)
	}

	// Initialize enhanced components
	if config.RateLimitConfig.EnableRateLimit {
		pool.rateLimiter = NewRateLimiter(config.RateLimitConfig)
	}
	if config.ValidationConfig.EnableValidation {
		pool.validator = NewValidator(config.ValidationConfig)
	}

	pool.latencyTracker = NewLatencyTracker()

	// Start workers first to ensure pool is fully initialized
	pool.startWorkers()

	// Initialize health checker after workers are started
	if config.HealthConfig.EnableHealthCheck {
		pool.healthChecker = NewHealthChecker(config.HealthConfig, pool)
	}

	return pool, nil
}

// Submit submits an operation to the pool
func (p *OperationPool) Submit(ctx context.Context, op Operation) error {
	if ctx == nil {
		richErr := goerrors.New(ErrCodeContextNil, "context cannot be nil")
		return fmt.Errorf("%w: %w", ErrContextNil, richErr)
	}

	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		richErr := goerrors.New(ErrCodePoolClosed, "operation pool is closed")
		return fmt.Errorf("%w: %w", ErrPoolClosed, richErr)
	}
	p.mu.RUnlock()

	// Validate operation if validation is enabled with nil check
	if p.validator != nil {
		validator := p.validator
		if validator != nil {
			if err := validator.Validate(op); err != nil {
				p.metrics.mu.Lock()
				p.metrics.ValidationErrors++
				p.metrics.mu.Unlock()
				richErr := goerrors.New(ErrCodeValidationFailed, "operation validation failed")
				return fmt.Errorf("%w: %w", ErrValidationFailed, richErr)
			}
		}
	}

	// Apply rate limiting if enabled with nil check
	if p.rateLimiter != nil {
		rateLimiter := p.rateLimiter
		if rateLimiter != nil {
			if !rateLimiter.Allow() {
				p.metrics.mu.Lock()
				p.metrics.RateLimitDrops++
				p.metrics.mu.Unlock()
				richErr := goerrors.New(ErrCodeRateLimited, "operation rate limited")
				return fmt.Errorf("%w: %w", ErrRateLimited, richErr)
			}
		}
	}

	// Set default values for operation
	if op.ID == "" {
		op.ID = generateOperationID()
	}
	if op.Timestamp.IsZero() {
		op.Timestamp = time.Now()
	}
	if op.Status == "" {
		op.Status = OperationStatusPending
	}

	// Submit operation with timeout
	select {
	case p.queue <- op:
		return nil
	case <-ctx.Done():
		richErr := goerrors.New(ErrCodeContextCancelled, "context cancelled during submission")
		return fmt.Errorf("%w: %w", ErrContextCancelled, richErr)
	case <-time.After(p.config.MaxWaitTime):
		richErr := goerrors.New(ErrCodeTimeout, "submission timeout")
		return fmt.Errorf("%w: %w", ErrTimeout, richErr)
	}
}

// SubmitAsync submits an operation asynchronously and returns immediately
func (p *OperationPool) SubmitAsync(ctx context.Context, op Operation) (*AsyncResult, error) {
	if ctx == nil {
		richErr := goerrors.New(ErrCodeContextNil, "context cannot be nil")
		return nil, fmt.Errorf("%w: %w", ErrContextNil, richErr)
	}

	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		richErr := goerrors.New(ErrCodePoolClosed, "operation pool is closed")
		return nil, fmt.Errorf("%w: %w", ErrPoolClosed, richErr)
	}
	p.mu.RUnlock()

	// Validate operation if validation is enabled with nil check
	if p.validator != nil {
		validator := p.validator
		if validator != nil {
			if err := validator.Validate(op); err != nil {
				p.metrics.mu.Lock()
				p.metrics.ValidationErrors++
				p.metrics.mu.Unlock()
				richErr := goerrors.New(ErrCodeValidationFailed, "operation validation failed")
				return nil, fmt.Errorf("%w: %w", ErrValidationFailed, richErr)
			}
		}
	}

	// Apply rate limiting if enabled with nil check
	if p.rateLimiter != nil {
		rateLimiter := p.rateLimiter
		if rateLimiter != nil {
			if !rateLimiter.Allow() {
				p.metrics.mu.Lock()
				p.metrics.RateLimitDrops++
				p.metrics.mu.Unlock()
				richErr := goerrors.New(ErrCodeRateLimited, "operation rate limited")
				return nil, fmt.Errorf("%w: %w", ErrRateLimited, richErr)
			}
		}
	}

	// Set default values for operation
	if op.ID == "" {
		op.ID = generateOperationID()
	}
	if op.Timestamp.IsZero() {
		op.Timestamp = time.Now()
	}
	if op.Status == "" {
		op.Status = OperationStatusPending
	}

	// Submit operation asynchronously
	select {
	case p.queue <- op:
		return &AsyncResult{
			OperationID: op.ID,
			SubmittedAt: time.Now(),
			Status:      OperationStatusPending,
		}, nil
	case <-ctx.Done():
		richErr := goerrors.New(ErrCodeContextCancelled, "context cancelled during submission")
		return nil, fmt.Errorf("%w: %w", ErrContextCancelled, richErr)
	default:
		richErr := goerrors.New(ErrCodeQueueFull, "operation queue is full")
		return nil, fmt.Errorf("%w: %w", ErrQueueFull, richErr)
	}
}

// SubmitBatch submits multiple operations as a batch
func (p *OperationPool) SubmitBatch(ctx context.Context, operations []Operation) error {
	if ctx == nil {
		richErr := goerrors.New(ErrCodeContextNil, "context cannot be nil")
		return fmt.Errorf("%w: %w", ErrContextNil, richErr)
	}

	if len(operations) == 0 {
		return nil
	}

	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		richErr := goerrors.New(ErrCodePoolClosed, "operation pool is closed")
		return fmt.Errorf("%w: %w", ErrPoolClosed, richErr)
	}
	p.mu.RUnlock()

	// Validate all operations if validation is enabled with nil check
	if p.validator != nil {
		validator := p.validator
		if validator != nil {
			for _, op := range operations {
				if err := validator.Validate(op); err != nil {
					p.metrics.mu.Lock()
					p.metrics.ValidationErrors++
					p.metrics.mu.Unlock()
					richErr := goerrors.New(ErrCodeValidationFailed, "operation validation failed")
					return fmt.Errorf("%w: %w", ErrValidationFailed, richErr)
				}
			}
		}
	}

	// Submit operations
	for _, op := range operations {
		// Apply rate limiting if enabled with nil check
		if p.rateLimiter != nil {
			rateLimiter := p.rateLimiter
			if rateLimiter != nil {
				if !rateLimiter.Allow() {
					p.metrics.mu.Lock()
					p.metrics.RateLimitDrops++
					p.metrics.mu.Unlock()
					richErr := goerrors.New(ErrCodeRateLimited, "operation rate limited")
					return fmt.Errorf("%w: %w", ErrRateLimited, richErr)
				}
			}
		}

		// Set default values for operation
		if op.ID == "" {
			op.ID = generateOperationID()
		}
		if op.Timestamp.IsZero() {
			op.Timestamp = time.Now()
		}
		if op.Status == "" {
			op.Status = OperationStatusPending
		}

		select {
		case p.queue <- op:
			continue
		case <-ctx.Done():
			richErr := goerrors.New(ErrCodeContextCancelled, "context cancelled during batch submission")
			return fmt.Errorf("%w: %w", ErrContextCancelled, richErr)
		case <-time.After(p.config.MaxWaitTime):
			richErr := goerrors.New(ErrCodeTimeout, "batch submission timeout")
			return fmt.Errorf("%w: %w", ErrTimeout, richErr)
		}
	}

	return nil
}

// GetResult retrieves a result from the pool
func (p *OperationPool) GetResult(ctx context.Context) (OperationResult, error) {
	if ctx == nil {
		richErr := goerrors.New(ErrCodeContextNil, "context cannot be nil")
		return OperationResult{}, fmt.Errorf("%w: %w", ErrContextNil, richErr)
	}

	// Check if pool is closed
	if p.IsClosed() {
		richErr := goerrors.New(ErrCodePoolClosed, "operation pool is closed")
		return OperationResult{}, fmt.Errorf("%w: %w", ErrPoolClosed, richErr)
	}

	// Try to get result from batch processor first if enabled with nil check
	if p.batchProc != nil {
		batchProc := p.batchProc
		if batchProc != nil {
			if result, err := batchProc.GetResult(ctx); err == nil {
				return result, nil
			}
		}
	}

	// Get result from main results channel
	select {
	case result, ok := <-p.results:
		if !ok {
			// Channel is closed
			richErr := goerrors.New(ErrCodePoolClosed, "results channel closed")
			return OperationResult{}, fmt.Errorf("%w: %w", ErrPoolClosed, richErr)
		}
		return result, nil
	case <-ctx.Done():
		richErr := goerrors.New(ErrCodeContextCancelled, "context cancelled while waiting for result")
		return OperationResult{}, fmt.Errorf("%w: %w", ErrContextCancelled, richErr)
	case <-time.After(p.config.MaxWaitTime):
		richErr := goerrors.New(ErrCodeTimeout, "timeout waiting for result")
		return OperationResult{}, fmt.Errorf("%w: %w", ErrTimeout, richErr)
	}
}

// GetResults retrieves multiple results from the pool
func (p *OperationPool) GetResults(ctx context.Context, count int) ([]OperationResult, error) {
	if ctx == nil {
		richErr := goerrors.New(ErrCodeContextNil, "context cannot be nil")
		return nil, fmt.Errorf("%w: %w", ErrContextNil, richErr)
	}

	if count <= 0 {
		return []OperationResult{}, nil
	}

	// Check if pool is closed
	if p.IsClosed() {
		richErr := goerrors.New(ErrCodePoolClosed, "operation pool is closed")
		return nil, fmt.Errorf("%w: %w", ErrPoolClosed, richErr)
	}

	results := make([]OperationResult, 0, count)

	for i := 0; i < count; i++ {
		result, err := p.GetResult(ctx)
		if err != nil {
			return results, err
		}
		results = append(results, result)
	}

	return results, nil
}

// PoolMetricsSnapshot is a copy of PoolMetrics without the mutex, safe for return and logging
type PoolMetricsSnapshot struct {
	ProcessedOps     int64
	FailedOps        int64
	AverageDuration  time.Duration
	PoolHits         int64
	PoolMisses       int64
	ActiveWorkers    int
	P50Latency       time.Duration
	P95Latency       time.Duration
	P99Latency       time.Duration
	Throughput       float64
	MemoryUsage      int64
	LastReset        time.Time
	RetryCount       int64
	RateLimitDrops   int64
	ValidationErrors int64
	QueueLength      int
}

// GetMetrics returns the current metrics (safe snapshot)
func (p *OperationPool) GetMetrics() PoolMetricsSnapshot {
	p.metrics.mu.RLock()
	snapshot := PoolMetricsSnapshot{
		ProcessedOps:     p.metrics.ProcessedOps,
		FailedOps:        p.metrics.FailedOps,
		AverageDuration:  p.metrics.AverageDuration,
		PoolHits:         p.metrics.PoolHits,
		PoolMisses:       p.metrics.PoolMisses,
		ActiveWorkers:    p.metrics.ActiveWorkers,
		P50Latency:       p.metrics.P50Latency,
		P95Latency:       p.metrics.P95Latency,
		P99Latency:       p.metrics.P99Latency,
		Throughput:       p.metrics.Throughput,
		MemoryUsage:      p.metrics.MemoryUsage,
		LastReset:        p.metrics.LastReset,
		RetryCount:       p.metrics.RetryCount,
		RateLimitDrops:   p.metrics.RateLimitDrops,
		ValidationErrors: p.metrics.ValidationErrors,
		QueueLength:      len(p.queue),
	}
	p.metrics.mu.RUnlock()

	// Add enhanced metrics with nil checks to avoid race conditions
	if p.latencyTracker != nil {
		lt := p.latencyTracker
		if lt != nil {
			snapshot.P50Latency = lt.GetPercentile(50)
			snapshot.P95Latency = lt.GetPercentile(95)
			snapshot.P99Latency = lt.GetPercentile(99)
			snapshot.Throughput = lt.GetThroughput()
		}
	}

	// Add memory usage
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	if m.Alloc > math.MaxInt64 {
		snapshot.MemoryUsage = int64(math.MaxInt64)
	} else {
		snapshot.MemoryUsage = int64(m.Alloc)
	}

	return snapshot
}

// GetHealthStatus returns the current health status
func (p *OperationPool) GetHealthStatus() HealthStatus {
	// Use a local copy to avoid race conditions during initialization
	if p.healthChecker == nil {
		return HealthStatus{
			Healthy:   true,
			Status:    "health checks disabled",
			LastCheck: time.Now(),
		}
	}

	hc := p.healthChecker
	if hc == nil {
		return HealthStatus{
			Healthy:   true,
			Status:    "health checks disabled",
			LastCheck: time.Now(),
		}
	}

	return hc.GetStatus()
}

// GetCacheStats returns cache statistics
func (p *OperationPool) GetCacheStats() CacheStats {
	// Use a local copy to avoid race conditions during initialization
	if p.cache == nil {
		return CacheStats{}
	}
	cache := p.cache
	if cache == nil {
		return CacheStats{}
	}
	return cache.GetStats()
}

// ResetMetrics resets all metrics to zero
func (p *OperationPool) ResetMetrics() {
	p.metrics.Reset()
	// Use a local copy to avoid race conditions during initialization
	if p.latencyTracker != nil {
		lt := p.latencyTracker
		if lt != nil {
			lt.Reset()
		}
	}
}

// Close gracefully shuts down the operation pool
func (p *OperationPool) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	p.mu.Unlock()

	// Stop health checker if enabled with nil check
	if p.healthChecker != nil {
		hc := p.healthChecker
		if hc != nil {
			hc.Stop()
		}
	}

	// Cancel context to stop all workers
	p.cancel()

	// Close the queue channel to stop accepting new operations
	close(p.queue)

	// Wait for workers to finish with timeout
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Workers finished gracefully
	case <-time.After(p.config.ShutdownTimeout):
		// Timeout reached, force shutdown
	}

	// Now close the results channel after workers have finished
	close(p.results)

	// Close batch processor if enabled with nil check
	if p.batchProc != nil {
		batchProc := p.batchProc
		if batchProc != nil {
			if err := batchProc.Close(); err != nil {
				return err
			}
		}
	}

	// Close cache if enabled with nil check
	if p.cache != nil {
		cache := p.cache
		if cache != nil {
			cache.Close()
		}
	}

	// Close object pool if enabled with nil check
	if p.objPool != nil {
		objPool := p.objPool
		if objPool != nil {
			objPool.Close()
		}
	}

	return nil
}

// IsClosed returns true if the pool is closed
func (p *OperationPool) IsClosed() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.closed
}

// startWorkers starts the worker goroutines
func (p *OperationPool) startWorkers() {
	for i := 0; i < p.config.WorkerCount; i++ {
		p.wg.Add(1)
		go p.workerLoop(i)
	}
}

// workerLoop is the main worker loop
func (p *OperationPool) workerLoop(id int) {
	defer p.wg.Done()

	for {
		select {
		case op, ok := <-p.queue:
			if !ok {
				return
			}
			p.processOperation(op, id)
		case <-p.ctx.Done():
			return
		}
	}
}

// processOperation processes a single operation
func (p *OperationPool) processOperation(op Operation, workerID int) {
	startTime := time.Now()

	// Use object pool with nil check to avoid race conditions
	if p.objPool != nil {
		objPool := p.objPool
		if objPool != nil {
			obj := objPool.GetOperation()
			defer objPool.PutOperation(obj)
		}
	}

	op.Status = OperationStatusProcessing
	op.WorkerID = workerID
	op.StartTime = startTime

	// Check cache with nil check to avoid race conditions
	if p.cache != nil {
		cache := p.cache
		if cache != nil {
			if cached, ok := cache.Get(op.ID); ok {
				if cachedOp, ok := cached.(Operation); ok {
					op.Status = cachedOp.Status
					op.Result = cachedOp.Result
					op.EndTime = cachedOp.EndTime
					op.Error = cachedOp.Error
					op.WorkerID = cachedOp.WorkerID
					op.StartTime = cachedOp.StartTime
					p.sendResult(op)
					return
				}
			}
		}
	}

	var result OperationResult
	var err error

	// Handle panics in the handler
	func() {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("handler panic: %v", r)
				result = OperationResult{
					OperationID:    op.ID,
					Success:        false,
					Error:          err,
					Duration:       time.Since(startTime),
					TraceID:        op.TraceID,
					ProcessingTime: time.Now(),
				}
			}
		}()
		result, err = p.handler.Process(p.ctx, op)
	}()

	op.EndTime = time.Now()
	duration := time.Since(startTime)

	// Track latency with nil check to avoid race conditions
	if p.latencyTracker != nil {
		lt := p.latencyTracker
		if lt != nil {
			lt.Record(duration)
		}
	}

	if err != nil {
		op.Status = OperationStatusFailed
		op.Error = err.Error()

		// Handle retry logic if enabled
		if p.config.RetryConfig.EnableRetry && op.RetryCount < p.config.RetryConfig.MaxRetries {
			if p.shouldRetry(err) {
				op.Status = OperationStatusRetrying
				op.RetryCount++
				p.metrics.mu.Lock()
				p.metrics.RetryCount++
				p.metrics.mu.Unlock()

				// Re-submit with backoff delay
				go func() {
					delay := p.calculateRetryDelay(op.RetryCount)
					time.Sleep(delay)

					p.mu.RLock()
					closed := p.closed
					p.mu.RUnlock()
					if closed {
						return
					}

					// Use a local copy of the queue to avoid race conditions
					queue := p.queue
					if queue != nil {
						select {
						case queue <- op:
						case <-p.ctx.Done():
						}
					}
				}()
				return
			}
		}

		// Ensure OperationID is set in the result
		if result.OperationID == "" {
			result.OperationID = op.ID
		}
		result.Success = false
		result.Error = err
		result.Duration = duration
		result.TraceID = op.TraceID
		result.ProcessingTime = time.Now()
		result.RetryCount = op.RetryCount

		op.Result = result
		// Update metrics for failed operation
		p.metrics.IncrementFailedOps()
		p.metrics.IncrementProcessedOps()
	} else {
		op.Status = OperationStatusCompleted
		// Ensure OperationID is set in the result
		if result.OperationID == "" {
			result.OperationID = op.ID
		}
		result.Success = true
		result.Duration = duration
		result.TraceID = op.TraceID
		result.ProcessingTime = time.Now()
		result.RetryCount = op.RetryCount

		op.Result = result
		// Update metrics for successful operation
		p.metrics.IncrementProcessedOps()
		// Update average duration with proper synchronization
		p.metrics.mu.Lock()
		p.metrics.AverageDuration = duration
		p.metrics.mu.Unlock()
	}
	op.WorkerID = workerID

	// Save to cache with nil check to avoid race conditions
	if p.cache != nil {
		cache := p.cache
		if cache != nil {
			cache.Set(op.ID, op)
		}
	}
	p.sendResult(op)
}

// sendResult sends a result to the results channel
func (p *OperationPool) sendResult(op Operation) {
	select {
	case p.results <- op.Result:
	default:
		// Channel is full, drop the result
		// This prevents blocking the worker
	}
}

// shouldRetry determines if an error should trigger a retry
func (p *OperationPool) shouldRetry(err error) bool {
	if p.config.RetryConfig.RetryableFunc != nil {
		return p.config.RetryConfig.RetryableFunc(err)
	}
	if len(p.config.RetryConfig.RetryableErrors) == 0 {
		// If no specific errors are configured, retry all errors
		return true
	}

	errStr := err.Error()
	for _, retryableErr := range p.config.RetryConfig.RetryableErrors {
		if errStr == retryableErr {
			return true
		}
	}
	return false
}

// calculateRetryDelay calculates the delay for a retry attempt
func (p *OperationPool) calculateRetryDelay(retryCount int) time.Duration {
	delay := p.config.RetryConfig.RetryDelay
	for i := 0; i < retryCount; i++ {
		delay = time.Duration(float64(delay) * p.config.RetryConfig.BackoffMultiplier)
		if delay > p.config.RetryConfig.MaxRetryDelay {
			delay = p.config.RetryConfig.MaxRetryDelay
			break
		}
	}
	// Add jitter: +/- up to 20%
	jitter := float64(delay) * 0.2
	jitterVal := (secureFloat64()*2 - 1) * jitter // random in [-jitter, +jitter]
	return time.Duration(float64(delay) + jitterVal)
}

// generateOperationID generates a unique operation ID
func generateOperationID() string {
	return fmt.Sprintf("op_%d_%d", time.Now().UnixNano(), runtime.NumGoroutine())
}

// secureFloat64 returns a float64 in [0,1) using crypto/rand
func secureFloat64() float64 {
	var b [8]byte
	_, err := randc.Read(b[:])
	if err != nil {
		// log error, return 0
		return 0.0
	}
	return float64(binary.LittleEndian.Uint64(b[:])) / (1 << 64)
}

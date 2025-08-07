// enhanced.go: Enhanced components for zephyros
//
// Copyright (c) 2025 AGILira
// Licensed under the Business Source License (BSL). Change Date: NEVER

package zephyros

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

// RateLimiter implements token bucket rate limiting
type RateLimiter struct {
	config     RateLimitConfig
	tokens     int64
	lastRefill time.Time
	mu         sync.Mutex
}

// NewRateLimiter creates a new rate limiter
func NewRateLimiter(config RateLimitConfig) *RateLimiter {
	return &RateLimiter{
		config:     config,
		tokens:     int64(config.BurstSize),
		lastRefill: time.Now(),
	}
}

// Allow checks if a request is allowed
func (r *RateLimiter) Allow() bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(r.lastRefill)

	// Calculate tokens to add
	tokensToAdd := int64(elapsed.Seconds() * float64(r.config.RequestsPerSecond))
	if tokensToAdd > 0 {
		r.tokens = min(int64(r.config.BurstSize), r.tokens+tokensToAdd)
		r.lastRefill = now
	}

	if r.tokens > 0 {
		r.tokens--
		return true
	}
	return false
}

// Validator validates operations
type Validator struct {
	config ValidationConfig
}

// NewValidator creates a new validator
func NewValidator(config ValidationConfig) *Validator {
	return &Validator{
		config: config,
	}
}

// Validate validates an operation
func (v *Validator) Validate(op Operation) error {
	// Check key length
	if len(op.Key) > v.config.MaxKeyLength {
		return fmt.Errorf("key length %d exceeds maximum %d", len(op.Key), v.config.MaxKeyLength)
	}

	// Check value length
	if len(op.Value) > v.config.MaxValueLength {
		return fmt.Errorf("value length %d exceeds maximum %d", len(op.Value), v.config.MaxValueLength)
	}

	// Check metadata size
	if op.Metadata != nil {
		metadataSize := 0
		for k, v := range op.Metadata {
			metadataSize += len(k)
			if str, ok := v.(string); ok {
				metadataSize += len(str)
			}
		}
		if metadataSize > v.config.MaxMetadataSize {
			return fmt.Errorf("metadata size %d exceeds maximum %d", metadataSize, v.config.MaxMetadataSize)
		}
	}

	// Check allowed types
	if len(v.config.AllowedTypes) > 0 {
		allowed := false
		for _, allowedType := range v.config.AllowedTypes {
			if op.Type == allowedType {
				allowed = true
				break
			}
		}
		if !allowed {
			return fmt.Errorf("operation type %s not in allowed types", op.Type)
		}
	}

	// Check forbidden keys
	for _, forbiddenKey := range v.config.ForbiddenKeys {
		if op.Key == forbiddenKey {
			return fmt.Errorf("key %s is forbidden", op.Key)
		}
	}

	return nil
}

// HealthChecker monitors the health of the operation pool
type HealthChecker struct {
	config   HealthConfig
	pool     *OperationPool
	status   HealthStatus
	stopChan chan struct{}
	mu       sync.RWMutex
	stopOnce sync.Once
	ready    bool // Flag to ensure pool is fully initialized before health checks
	readyMu  sync.RWMutex
}

// NewHealthChecker creates a new health checker
func NewHealthChecker(config HealthConfig, pool *OperationPool) *HealthChecker {
	hc := &HealthChecker{
		config:   config,
		pool:     pool,
		stopChan: make(chan struct{}),
		status: HealthStatus{
			Healthy:   true,
			Status:    "healthy",
			LastCheck: time.Now(),
		},
		ready: false, // Start as not ready
	}

	// Start health checker in a separate goroutine to avoid initialization race
	go func() {
		// Wait a bit for the pool to be fully initialized
		time.Sleep(10 * time.Millisecond)
		hc.readyMu.Lock()
		hc.ready = true
		hc.readyMu.Unlock()
		hc.run()
	}()

	return hc
}

// run runs the health check loop
func (hc *HealthChecker) run() {
	ticker := time.NewTicker(hc.config.HealthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			hc.checkHealth()
		case <-hc.stopChan:
			return
		}
	}
}

// checkHealth performs a health check
func (hc *HealthChecker) checkHealth() {
	// Check if ready before proceeding
	hc.readyMu.RLock()
	if !hc.ready {
		hc.readyMu.RUnlock()
		return
	}
	hc.readyMu.RUnlock()

	hc.mu.Lock()
	defer hc.mu.Unlock()

	// Get metrics with proper synchronization
	metrics := hc.pool.GetMetrics()
	now := time.Now()

	// Check queue latency
	queueLatency := time.Duration(0)
	if metrics.QueueLength > 0 {
		queueLatency = time.Duration(metrics.QueueLength) * time.Millisecond
	}

	// Check processing latency
	processingLatency := metrics.AverageDuration

	// Determine health status
	healthy := true
	status := "healthy"
	errorCount := int64(0)
	warningCount := int64(0)

	if queueLatency > hc.config.MaxQueueLatency {
		healthy = false
		status = "queue latency too high"
		errorCount++
	}

	if processingLatency > hc.config.MaxProcessingLatency {
		healthy = false
		status = "processing latency too high"
		errorCount++
	}

	if metrics.FailedOps > 0 {
		warningCount = metrics.FailedOps
	}

	hc.status = HealthStatus{
		Healthy:           healthy,
		Status:            status,
		LastCheck:         now,
		QueueLatency:      queueLatency,
		ProcessingLatency: processingLatency,
		ErrorCount:        errorCount,
		WarningCount:      warningCount,
	}
}

// GetStatus returns the current health status
func (hc *HealthChecker) GetStatus() HealthStatus {
	hc.mu.RLock()
	defer hc.mu.RUnlock()
	return hc.status
}

// Stop stops the health checker
func (hc *HealthChecker) Stop() {
	hc.stopOnce.Do(func() {
		close(hc.stopChan)
	})
}

// LatencyTracker tracks operation latencies for percentile calculations
type LatencyTracker struct {
	latencies   []time.Duration // ring buffer
	head        int
	tail        int
	count       int64 // number of valid elements
	mu          sync.RWMutex
	startTime   time.Time
	opCount     int64
	maxSize     int
	initMu      sync.Mutex // Separate mutex for initialization
	initialized bool
}

// NewLatencyTracker creates a new LatencyTracker instance.
func NewLatencyTracker() *LatencyTracker {
	const defaultLatencyBufferSize = 1000
	lt := &LatencyTracker{
		latencies:   make([]time.Duration, defaultLatencyBufferSize),
		startTime:   time.Now(),
		maxSize:     defaultLatencyBufferSize,
		initialized: false,
	}

	// Mark as initialized after all fields are set
	lt.initMu.Lock()
	lt.initialized = true
	lt.initMu.Unlock()

	return lt
}

// Record adds a new latency measurement to the tracker.
func (lt *LatencyTracker) Record(duration time.Duration) {
	lt.initMu.Lock()
	if !lt.initialized {
		lt.initMu.Unlock()
		return // Skip if not yet initialized
	}
	lt.initMu.Unlock()

	lt.mu.Lock()
	defer lt.mu.Unlock()

	lt.opCount++
	lt.latencies[lt.head] = duration
	lt.head = (lt.head + 1) % lt.maxSize
	if lt.count < int64(lt.maxSize) {
		lt.count++
	} else {
		lt.tail = (lt.tail + 1) % lt.maxSize
	}
}

// GetPercentile returns the nth percentile latency.
func (lt *LatencyTracker) GetPercentile(n int) time.Duration {
	lt.initMu.Lock()
	if !lt.initialized {
		lt.initMu.Unlock()
		return 0 // Return 0 if not yet initialized
	}
	lt.initMu.Unlock()

	lt.mu.RLock()
	defer lt.mu.RUnlock()

	if lt.count == 0 {
		return 0
	}

	activeLatencies := make([]time.Duration, lt.count)
	if lt.head > lt.tail {
		copy(activeLatencies, lt.latencies[lt.tail:lt.head])
	} else {
		copy(activeLatencies, lt.latencies[lt.tail:])
		copy(activeLatencies[len(lt.latencies)-lt.tail:], lt.latencies[:lt.head])
	}

	sort.Slice(activeLatencies, func(i, j int) bool {
		return activeLatencies[i] < activeLatencies[j]
	})

	index := (n * int(lt.count)) / 100
	if index >= int(lt.count) {
		index = int(lt.count) - 1
	}
	return activeLatencies[index]
}

// GetThroughput returns the number of operations per second.
func (lt *LatencyTracker) GetThroughput() float64 {
	lt.initMu.Lock()
	if !lt.initialized {
		lt.initMu.Unlock()
		return 0 // Return 0 if not yet initialized
	}
	lt.initMu.Unlock()

	lt.mu.RLock()
	defer lt.mu.RUnlock()

	opCount := lt.opCount
	elapsed := time.Since(lt.startTime).Seconds()
	if elapsed == 0 {
		return 0
	}
	return float64(opCount) / elapsed
}

// Reset clears all recorded latencies and resets the tracker.
func (lt *LatencyTracker) Reset() {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	lt.head = 0
	lt.tail = 0
	lt.opCount = 0
	lt.count = 0
	lt.startTime = time.Now()
}

// min returns the minimum of two int64 values
func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

# Zephyros Best Practices

This document provides comprehensive best practices for using the Zephyros library effectively in production environments.

## Table of Contents

- [Configuration](#configuration)
- [Performance Optimization](#performance-optimization)
- [Error Handling](#error-handling)
- [Monitoring and Observability](#monitoring-and-observability)
- [Security Considerations](#security-considerations)
- [Testing Strategies](#testing-strategies)
- [Deployment Guidelines](#deployment-guidelines)

## Configuration

### Worker Count Optimization

Choose worker count based on your workload characteristics:

```go
// CPU-bound operations (e.g., data processing, calculations)
config := zephyros.PoolConfig{
    WorkerCount: runtime.NumCPU(), // or runtime.NumCPU() * 2
}

// I/O-bound operations (e.g., database queries, HTTP requests)
config := zephyros.PoolConfig{
    WorkerCount: runtime.NumCPU() * 4, // Higher for I/O bound
}

// Mixed workloads
config := zephyros.PoolConfig{
    WorkerCount: runtime.NumCPU() * 2, // Balanced approach
}
```

### Queue Size Configuration

Set queue size based on expected throughput and memory constraints:

```go
// High throughput applications
config := zephyros.PoolConfig{
    QueueSize: 10000, // Large queue for high throughput
}

// Memory-constrained environments
config := zephyros.PoolConfig{
    QueueSize: 500, // Smaller queue to limit memory usage
}

// Balanced approach
config := zephyros.PoolConfig{
    QueueSize: WorkerCount * 50, // Default recommendation
}
```

### Timeout Configuration

Configure timeouts based on your operation characteristics:

```go
config := zephyros.PoolConfig{
    MaxWaitTime:     30 * time.Second,  // Operation submission timeout
    ShutdownTimeout: 10 * time.Second,  // Graceful shutdown timeout
}
```

## Performance Optimization

### Batch Processing

Use batch processing for high-frequency operations:

```go
config := zephyros.PoolConfig{
    BatchConfig: zephyros.BatchConfig{
        EnableBatchProcessing: true,
        BatchSize:            10,              // Optimal for most use cases
        BatchTimeout:         20 * time.Millisecond,
        FlushInterval:        10 * time.Millisecond,
        MaxBatchSize:         100,             // Prevent oversized batches
    },
}
```

**When to use batch processing:**
- High-frequency operations (>1000 ops/sec)
- Network operations (HTTP, database)
- Operations with similar processing time
- Bulk data processing

### Strategic Caching

Implement caching for expensive operations:

```go
config := zephyros.PoolConfig{
    CacheConfig: zephyros.CacheConfig{
        EnableCaching:   true,
        CacheSize:       1000,               // Based on unique operation count
        TTL:             5 * time.Minute,    // Match data freshness requirements
        CleanupInterval: 1 * time.Minute,    // Regular cleanup
        MaxKeySize:      256,                // Prevent memory issues
        MaxValueSize:    1024,               // Limit cache entry size
    },
}
```

**Caching strategies:**
- Cache expensive computations
- Cache external API responses
- Cache database query results
- Use appropriate TTL based on data volatility

### Object Pooling

Enable object pooling for memory efficiency:

```go
config := zephyros.PoolConfig{
    EnableObjectPool: true, // Automatically sized as QueueSize / 2
}
```

**Benefits:**
- Reduces garbage collection pressure
- Improves memory allocation performance
- Suitable for high-throughput applications

## Error Handling

### Robust Operation Handlers

Implement comprehensive error handling in your operation handlers:

```go
type RobustHandler struct {
    db *sql.DB
    logger *log.Logger
}

func (h *RobustHandler) Process(ctx context.Context, op zephyros.Operation) (zephyros.OperationResult, error) {
    // Check context cancellation first
    select {
    case <-ctx.Done():
        return zephyros.OperationResult{
            OperationID: op.ID,
            Success:     false,
            Error:       ctx.Err(),
        }, ctx.Err()
    default:
    }

    // Validate input
    if op.Key == "" {
        return zephyros.OperationResult{
            OperationID: op.ID,
            Success:     false,
            Error:       fmt.Errorf("empty key not allowed"),
        }, fmt.Errorf("empty key not allowed")
    }

    // Process with error handling
    result, err := h.processOperation(ctx, op)
    if err != nil {
        h.logger.Printf("Operation failed: %v", err)
        return zephyros.OperationResult{
            OperationID: op.ID,
            Success:     false,
            Error:       err,
        }, err
    }

    return zephyros.OperationResult{
        OperationID: op.ID,
        Success:     true,
        Data:        result,
    }, nil
}

func (h *RobustHandler) processOperation(ctx context.Context, op zephyros.Operation) (interface{}, error) {
    // Implement your processing logic here
    // Include proper error handling and logging
    return nil, nil
}
```

### Error Recovery Strategies

Implement error recovery in your application:

```go
func submitWithRetry(pool *zephyros.OperationPool, op zephyros.Operation, maxRetries int) error {
    ctx := context.Background()
    
    for attempt := 0; attempt < maxRetries; attempt++ {
        err := pool.Submit(ctx, op)
        if err == nil {
            return nil
        }
        
        // Log retry attempt
        log.Printf("Submit attempt %d failed: %v", attempt+1, err)
        
        // Wait before retry (exponential backoff)
        time.Sleep(time.Duration(attempt+1) * 100 * time.Millisecond)
    }
    
    return fmt.Errorf("failed after %d attempts", maxRetries)
}
```

### Context Management

Always use contexts for cancellation and timeouts:

```go
// Operation submission with timeout
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

err := pool.Submit(ctx, operation)

// Result retrieval with timeout
ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()

result, err := pool.GetResult(ctx)
```

## Monitoring and Observability

### Metrics Collection

Implement comprehensive metrics monitoring:

```go
func startMetricsMonitoring(pool *zephyros.OperationPool) {
    go func() {
        ticker := time.NewTicker(30 * time.Second)
        defer ticker.Stop()
        
        for range ticker.C {
            metrics := pool.GetMetrics()
            cacheStats := pool.GetCacheStats()
            healthStatus := pool.GetHealthStatus()
            
            // Log metrics
            log.Printf("Pool Metrics: workers=%d, queue=%d, processed=%d, failed=%d, avg_duration=%v",
                metrics.ActiveWorkers, metrics.QueueLength, metrics.ProcessedOps, 
                metrics.FailedOps, metrics.AverageDuration)
            
            log.Printf("Cache Stats: size=%d/%d, total_access=%d, avg_access=%.2f",
                cacheStats.Size, cacheStats.MaxSize, cacheStats.TotalAccessCount, 
                float64(cacheStats.AverageAccessCount))
            
            log.Printf("Health Status: healthy=%v, status=%s, queue_latency=%v, processing_latency=%v",
                healthStatus.Healthy, healthStatus.Status, healthStatus.QueueLatency, 
                healthStatus.ProcessingLatency)
            
            // Send to monitoring system (Prometheus, etc.)
            recordMetrics(metrics, cacheStats, healthStatus)
        }
    }()
}

func recordMetrics(metrics zephyros.PoolMetrics, cacheStats zephyros.CacheStats, healthStatus zephyros.HealthStatus) {
    // Implement metrics recording for your monitoring system
    // Example: Prometheus, DataDog, etc.
}
```

### Health Checks

Implement health checks for your operation pool:

```go
func healthCheck(pool *zephyros.OperationPool) bool {
    if pool.IsClosed() {
        return false
    }
    
    healthStatus := pool.GetHealthStatus()
    return healthStatus.Healthy
}
```

### Alerting

Set up alerts for critical metrics:

```go
func checkAlerts(metrics zephyros.PoolMetrics, healthStatus zephyros.HealthStatus) {
    // High failure rate
    if metrics.FailedOps > 0 && float64(metrics.FailedOps)/float64(metrics.ProcessedOps) > 0.1 {
        alert("High operation failure rate detected")
    }
    
    // Queue backup
    if metrics.QueueLength > 1000 {
        alert("Operation queue is backing up")
    }
    
    // Worker starvation
    if metrics.ActiveWorkers == 0 {
        alert("No active workers detected")
    }
    
    // Health status alerts
    if !healthStatus.Healthy {
        alert("Operation pool health check failed: " + healthStatus.Status)
    }
    
    // Latency alerts
    if healthStatus.QueueLatency > 5*time.Second {
        alert("High queue latency detected")
    }
    
    if healthStatus.ProcessingLatency > 10*time.Second {
        alert("High processing latency detected")
    }
}
```

## Advanced Features

### Rate Limiting

Configure rate limiting to prevent system overload:

```go
config := zephyros.PoolConfig{
    RateLimitConfig: zephyros.RateLimitConfig{
        EnableRateLimit:   true,
        RequestsPerSecond: 1000,             // Limit requests per second
        BurstSize:         100,              // Allow burst of requests
        WindowSize:        time.Second,      // Time window for rate limiting
    },
}
```

**When to use rate limiting:**
- Protect downstream services from overload
- Implement fair usage policies
- Prevent resource exhaustion
- Control API usage

### Input Validation

Implement comprehensive input validation:

```go
config := zephyros.PoolConfig{
    ValidationConfig: zephyros.ValidationConfig{
        EnableValidation: true,
        MaxKeyLength:     256,               // Maximum key length
        MaxValueLength:   1024,              // Maximum value length
        MaxMetadataSize:  1024,              // Maximum metadata size
        AllowedTypes:     []string{"read", "write", "delete"}, // Allowed operation types
        ForbiddenKeys:    []string{"admin", "system"}, // Forbidden keys
    },
}
```

**Validation strategies:**
- Validate operation types
- Check input sizes
- Sanitize user inputs
- Prevent injection attacks

### Retry Logic

Configure retry mechanisms for transient failures:

```go
config := zephyros.PoolConfig{
    RetryConfig: zephyros.RetryConfig{
        EnableRetry:       true,
        MaxRetries:        3,                // Maximum retry attempts
        RetryDelay:        100 * time.Millisecond, // Initial retry delay
        BackoffMultiplier: 2.0,              // Exponential backoff multiplier
        MaxRetryDelay:     5 * time.Second,  // Maximum retry delay
        RetryableErrors:   []string{"timeout", "connection_error"}, // Retryable error types
    },
}
```

**Retry strategies:**
- Use exponential backoff
- Limit maximum retries
- Identify retryable errors
- Avoid retry storms

### Health Monitoring

Enable continuous health monitoring:

```go
config := zephyros.PoolConfig{
    HealthConfig: zephyros.HealthConfig{
        EnableHealthCheck:    true,
        HealthCheckInterval:  30 * time.Second, // Health check frequency
        MaxQueueLatency:      1 * time.Second,  // Maximum acceptable queue latency
        MaxProcessingLatency: 5 * time.Second,  // Maximum acceptable processing latency
    },
}
```

**Health monitoring benefits:**
- Early detection of issues
- Proactive alerting
- Performance tracking
- System reliability

## Security Considerations

### Input Validation

Always validate operation inputs:

```go
func (h *SecureHandler) Process(ctx context.Context, op zephyros.Operation) (zephyros.OperationResult, error) {
    // Validate operation type
    if !isValidOperationType(op.Type) {
        return zephyros.OperationResult{
            OperationID: op.ID,
            Success:     false,
            Error:       fmt.Errorf("invalid operation type: %s", op.Type),
        }, fmt.Errorf("invalid operation type: %s", op.Type)
    }
    
    // Validate key format
    if !isValidKey(op.Key) {
        return zephyros.OperationResult{
            OperationID: op.ID,
            Success:     false,
            Error:       fmt.Errorf("invalid key format: %s", op.Key),
        }, fmt.Errorf("invalid key format: %s", op.Key)
    }
    
    // Sanitize input
    sanitizedValue := sanitizeInput(op.Value)
    
    // Process with sanitized input
    return h.processSecurely(ctx, op.Type, op.Key, sanitizedValue)
}
```

### Rate Limiting

Implement rate limiting for operation submission:

```go
type RateLimitedPool struct {
    pool       *zephyros.OperationPool
    limiter    *rate.Limiter
}

func (rlp *RateLimitedPool) Submit(ctx context.Context, op zephyros.Operation) error {
    if !rlp.limiter.Allow() {
        return fmt.Errorf("rate limit exceeded")
    }
    
    return rlp.pool.Submit(ctx, op)
}
```

### Access Control

Implement access control for sensitive operations:

```go
type SecureHandler struct {
    allowedUsers map[string]bool
}

func (h *SecureHandler) Process(ctx context.Context, op zephyros.Operation) (zephyros.OperationResult, error) {
    // Extract user from context
    user := getUserFromContext(ctx)
    
    // Check access permissions
    if !h.allowedUsers[user] {
        return zephyros.OperationResult{
            OperationID: op.ID,
            Success:     false,
            Error:       fmt.Errorf("access denied for user: %s", user),
        }, fmt.Errorf("access denied for user: %s", user)
    }
    
    // Process operation
    return h.processOperation(ctx, op)
}
```

## Testing Strategies

### Unit Testing

Test your operation handlers thoroughly:

```go
func TestOperationHandler(t *testing.T) {
    handler := &MyHandler{}
    ctx := context.Background()
    
    tests := []struct {
        name    string
        op      zephyros.Operation
        wantErr bool
    }{
        {
            name: "valid operation",
            op: zephyros.Operation{
                Type:  "test",
                Key:   "valid_key",
                Value: "valid_value",
            },
            wantErr: false,
        },
        {
            name: "invalid operation",
            op: zephyros.Operation{
                Type:  "test",
                Key:   "",
                Value: "invalid_value",
            },
            wantErr: true,
        },
    }
    
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            result, err := handler.Process(ctx, tt.op)
            
            if tt.wantErr {
                assert.Error(t, err)
                assert.False(t, result.Success)
            } else {
                assert.NoError(t, err)
                assert.True(t, result.Success)
            }
        })
    }
}
```

### Integration Testing

Test the complete operation pool:

```go
func TestOperationPoolIntegration(t *testing.T) {
    config := zephyros.PoolConfig{
        WorkerCount:   2,
        QueueSize:     100,
        EnableMetrics: true,
    }
    
    handler := &TestHandler{}
    pool, err := zephyros.NewOperationPool(config, handler)
    require.NoError(t, err)
    defer pool.Close()
    
    ctx := context.Background()
    
    // Submit operations
    for i := 0; i < 10; i++ {
        op := zephyros.Operation{
            Type:  "test",
            Key:   fmt.Sprintf("key_%d", i),
            Value: fmt.Sprintf("value_%d", i),
        }
        
        err := pool.Submit(ctx, op)
        require.NoError(t, err)
    }
    
    // Verify results
    for i := 0; i < 10; i++ {
        result, err := pool.GetResult(ctx)
        require.NoError(t, err)
        assert.True(t, result.Success)
    }
    
    // Verify metrics
    metrics := pool.GetMetrics()
    assert.Equal(t, int64(10), metrics.ProcessedOps)
    assert.Equal(t, int64(0), metrics.FailedOps)
}
```

### Performance Testing

Test performance under load:

```go
func BenchmarkOperationPool(b *testing.B) {
    config := zephyros.PoolConfig{
        WorkerCount:   4,
        QueueSize:     1000,
        EnableMetrics: true,
    }
    
    handler := &BenchmarkHandler{}
    pool, err := zephyros.NewOperationPool(config, handler)
    require.NoError(b, err)
    defer pool.Close()
    
    ctx := context.Background()
    b.ResetTimer()
    
    b.RunParallel(func(pb *testing.PB) {
        for pb.Next() {
            op := zephyros.Operation{
                Type:  "benchmark",
                Key:   "test_key",
                Value: "test_value",
            }
            
            err := pool.Submit(ctx, op)
            if err != nil {
                b.Errorf("Submit error: %v", err)
                continue
            }
            
            result, err := pool.GetResult(ctx)
            if err != nil {
                b.Errorf("Get result error: %v", err)
                continue
            }
            
            if !result.Success {
                b.Errorf("Operation failed: %v", result.Error)
            }
        }
    })
}
```

## Deployment Guidelines

### Resource Allocation

Allocate appropriate resources:

```go
// Production configuration
config := zephyros.PoolConfig{
    WorkerCount:      runtime.NumCPU() * 2,  // Scale with CPU cores
    QueueSize:        5000,                   // Large queue for high throughput
    MaxWaitTime:      30 * time.Second,       // Generous timeout
    ShutdownTimeout:  10 * time.Second,       // Graceful shutdown
    EnableMetrics:    true,                   // Always enable in production
    EnableObjectPool: true,                   // Enable for performance
    
    BatchConfig: zephyros.BatchConfig{
        EnableBatchProcessing: true,
        BatchSize:            20,             // Larger batches for production
        BatchTimeout:         50 * time.Millisecond,
        FlushInterval:        25 * time.Millisecond,
    },
    
    CacheConfig: zephyros.CacheConfig{
        EnableCaching:   true,
        CacheSize:       10000,               // Large cache for production
        TTL:             10 * time.Minute,    // Longer TTL
        CleanupInterval: 2 * time.Minute,
    },
    
    RetryConfig: zephyros.RetryConfig{
        EnableRetry:       true,
        MaxRetries:        3,
        RetryDelay:        100 * time.Millisecond,
        BackoffMultiplier: 2.0,
        MaxRetryDelay:     5 * time.Second,
    },
    
    RateLimitConfig: zephyros.RateLimitConfig{
        EnableRateLimit:   true,
        RequestsPerSecond: 5000,              // Higher limit for production
        BurstSize:         500,
        WindowSize:        time.Second,
    },
    
    ValidationConfig: zephyros.ValidationConfig{
        EnableValidation: true,
        MaxKeyLength:     512,                // Larger limits for production
        MaxValueLength:   2048,
        MaxMetadataSize:  2048,
    },
    
    HealthConfig: zephyros.HealthConfig{
        EnableHealthCheck:    true,
        HealthCheckInterval:  30 * time.Second,
        MaxQueueLatency:      2 * time.Second,  // More lenient for production
        MaxProcessingLatency: 10 * time.Second,
    },
}
```

### Graceful Shutdown

Implement graceful shutdown:

```go
func gracefulShutdown(pool *zephyros.OperationPool) {
    // Stop accepting new operations
    log.Println("Stopping operation pool...")
    
    // Wait for existing operations to complete
    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()
    
    // Close the pool
    if err := pool.Close(); err != nil {
        log.Printf("Error closing pool: %v", err)
    }
    
    log.Println("Operation pool stopped gracefully")
}
```

### Configuration Management

Use environment-based configuration:

```go
func loadConfig() zephyros.PoolConfig {
    workerCount := getEnvInt("ZEPHYROS_WORKER_COUNT", runtime.NumCPU()*2)
    queueSize := getEnvInt("ZEPHYROS_QUEUE_SIZE", workerCount*50)
    maxWaitTime := getEnvDuration("ZEPHYROS_MAX_WAIT_TIME", 30*time.Second)
    
    return zephyros.PoolConfig{
        WorkerCount:   workerCount,
        QueueSize:     queueSize,
        MaxWaitTime:   maxWaitTime,
        EnableMetrics: true,
        // ... other configuration
    }
}

func getEnvInt(key string, defaultValue int) int {
    if value := os.Getenv(key); value != "" {
        if intValue, err := strconv.Atoi(value); err == nil {
            return intValue
        }
    }
    return defaultValue
}

func getEnvDuration(key string, defaultValue time.Duration) time.Duration {
    if value := os.Getenv(key); value != "" {
        if duration, err := time.ParseDuration(value); err == nil {
            return duration
        }
    }
    return defaultValue
}
```

### Monitoring Setup

Set up comprehensive monitoring:

```go
func setupMonitoring(pool *zephyros.OperationPool) {
    // Start metrics collection
    startMetricsMonitoring(pool)
    
    // Set up health checks
    go func() {
        ticker := time.NewTicker(1 * time.Minute)
        defer ticker.Stop()
        
        for range ticker.C {
            if !healthCheck(pool) {
                alert("Operation pool health check failed")
            }
        }
    }()
    
    // Set up performance alerts
    go func() {
        ticker := time.NewTicker(5 * time.Minute)
        defer ticker.Stop()
        
        for range ticker.C {
            metrics := pool.GetMetrics()
            healthStatus := pool.GetHealthStatus()
            checkAlerts(metrics, healthStatus)
        }
    }()
}
```

## Conclusion

Following these best practices will help you build robust, performant, and maintainable applications using the Zephyros library. Remember to:

1. **Monitor continuously** - Always enable metrics and set up monitoring
2. **Test thoroughly** - Implement comprehensive unit, integration, and performance tests
3. **Configure appropriately** - Tune configuration based on your specific workload
4. **Handle errors gracefully** - Implement proper error handling and recovery
5. **Secure your application** - Validate inputs and implement access controls
6. **Deploy carefully** - Use proper resource allocation and graceful shutdown

These practices will ensure your Zephyros-based applications are production-ready and perform optimally under various conditions. 
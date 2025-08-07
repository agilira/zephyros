# Zephyros

A high-performance Go library for concurrent operation processing with advanced features including batch processing, strategic caching, memory pooling, rate limiting, validation, health monitoring, and comprehensive metrics.

## Features

### Core Functionality
- **Concurrent Operation Processing**: Worker pool pattern with configurable worker count and queue size
- **Batch Processing**: Efficient batch operation handling with configurable batch sizes and timeouts
- **Strategic Caching**: LRU cache with TTL support and size limits
- **Memory Pooling**: Object pooling for improved memory efficiency
- **Graceful Shutdown**: Proper resource cleanup and worker coordination

### Enhanced Features
- **Rate Limiting**: Token bucket rate limiting with configurable requests per second and burst size
- **Input Validation**: Comprehensive operation validation with configurable rules
- **Health Monitoring**: Continuous health checks with latency tracking
- **Advanced Metrics**: Detailed performance metrics including P50, P95, P99 latencies and throughput
- **Retry Logic**: Configurable retry mechanisms with exponential backoff
- **Error Handling**: Robust error handling with panic recovery

### Performance Optimizations
- **Non-blocking Operations**: Asynchronous operation submission
- **Efficient Memory Management**: Object reuse and strategic caching
- **Concurrent Safety**: Thread-safe operations with proper synchronization
- **Configurable Timeouts**: Flexible timeout configuration for different use cases

## Installation

```bash
go get github.com/agilira/zephyros
```

## Quick Start

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    "github.com/agilira/zephyros"
)

// Example operation handler
type ExampleHandler struct{}

func (h *ExampleHandler) Process(ctx context.Context, op zephyros.Operation) (zephyros.OperationResult, error) {
    // Process the operation
    result := fmt.Sprintf("Processed: %s", op.Value)
    
    return zephyros.OperationResult{
        OperationID: op.ID,
        Success:     true,
        Data:        result,
        Duration:    time.Since(op.Timestamp),
    }, nil
}

func main() {
    // Create configuration
    config := zephyros.PoolConfig{
        WorkerCount:   4,
        QueueSize:     1000,
        MaxWaitTime:   30 * time.Second,
        EnableMetrics: true,
        
        // Enable batch processing
        BatchConfig: zephyros.BatchConfig{
            EnableBatchProcessing: true,
            BatchSize:            10,
            BatchTimeout:         20 * time.Millisecond,
        },
        
        // Enable caching
        CacheConfig: zephyros.CacheConfig{
            EnableCaching: true,
            CacheSize:     1000,
            TTL:           5 * time.Minute,
        },
        
        // Enable enhanced features
        RetryConfig: zephyros.RetryConfig{
            EnableRetry:       true,
            MaxRetries:        3,
            RetryDelay:        100 * time.Millisecond,
            BackoffMultiplier: 2.0,
        },
        
        RateLimitConfig: zephyros.RateLimitConfig{
            EnableRateLimit:   true,
            RequestsPerSecond: 1000,
            BurstSize:         100,
        },
        
        ValidationConfig: zephyros.ValidationConfig{
            EnableValidation: true,
            MaxKeyLength:     256,
            MaxValueLength:   1024,
        },
        
        HealthConfig: zephyros.HealthConfig{
            EnableHealthCheck:    true,
            HealthCheckInterval:  30 * time.Second,
            MaxQueueLatency:      1 * time.Second,
            MaxProcessingLatency: 5 * time.Second,
        },
    }

    // Create operation pool
    handler := &ExampleHandler{}
    pool, err := zephyros.NewOperationPool(config, handler)
    if err != nil {
        log.Fatal(err)
    }
    defer pool.Close()

    ctx := context.Background()

    // Submit operations
    for i := 0; i < 100; i++ {
        op := zephyros.Operation{
            Type:  "example",
            Key:   fmt.Sprintf("key_%d", i),
            Value: fmt.Sprintf("value_%d", i),
        }
        
        if err := pool.Submit(ctx, op); err != nil {
            log.Printf("Submit error: %v", err)
        }
    }

    // Get results
    for i := 0; i < 100; i++ {
        result, err := pool.GetResult(ctx)
        if err != nil {
            log.Printf("Get result error: %v", err)
            continue
        }
        
        if result.Success {
            fmt.Printf("Result: %v\n", result.Data)
        }
    }

    // Get metrics
    metrics := pool.GetMetrics()
    fmt.Printf("Processed: %d, Failed: %d, Avg Duration: %v\n",
        metrics.ProcessedOps, metrics.FailedOps, metrics.AverageDuration)
}
```

## Configuration

### PoolConfig

The main configuration struct that controls all aspects of the operation pool:

```go
type PoolConfig struct {
    WorkerCount      int           // Number of worker goroutines
    QueueSize        int           // Size of operation queue
    MaxWaitTime      time.Duration // Maximum wait time for operations
    ShutdownTimeout  time.Duration // Graceful shutdown timeout
    EnableMetrics    bool          // Enable metrics collection
    EnableObjectPool bool          // Enable object pooling
    
    BatchConfig      BatchConfig   // Batch processing configuration
    CacheConfig      CacheConfig   // Caching configuration
    RetryConfig      RetryConfig   // Retry configuration
    RateLimitConfig  RateLimitConfig // Rate limiting configuration
    HealthConfig     HealthConfig // Health monitoring configuration
    ValidationConfig ValidationConfig // Input validation configuration
}
```

### Batch Processing

Configure batch processing for high-throughput scenarios:

```go
BatchConfig: zephyros.BatchConfig{
    EnableBatchProcessing: true,
    BatchSize:            10,              // Operations per batch
    BatchTimeout:         20 * time.Millisecond, // Max wait for batch
    FlushInterval:        10 * time.Millisecond, // Regular flush interval
    MaxBatchSize:         100,             // Maximum batch size
}
```

### Caching

Configure strategic caching for expensive operations:

```go
CacheConfig: zephyros.CacheConfig{
    EnableCaching:   true,
    CacheSize:       1000,               // Maximum cache entries
    TTL:             5 * time.Minute,    // Time-to-live for entries
    CleanupInterval: 1 * time.Minute,    // Cleanup interval
    MaxKeySize:      256,                // Maximum key size
    MaxValueSize:    1024,               // Maximum value size
    EnableCompression: false,            // Enable compression
}
```

### Rate Limiting

Configure rate limiting to prevent overload:

```go
RateLimitConfig: zephyros.RateLimitConfig{
    EnableRateLimit:   true,
    RequestsPerSecond: 1000,             // Requests per second limit
    BurstSize:         100,              // Burst capacity
    WindowSize:        time.Second,      // Time window
}
```

### Validation

Configure input validation rules:

```go
ValidationConfig: zephyros.ValidationConfig{
    EnableValidation: true,
    MaxKeyLength:     256,               // Maximum key length
    MaxValueLength:   1024,              // Maximum value length
    MaxMetadataSize:  1024,              // Maximum metadata size
    AllowedTypes:     []string{"read", "write"}, // Allowed operation types
    ForbiddenKeys:    []string{"admin"}, // Forbidden keys
}
```

### Health Monitoring

Configure health monitoring and alerts:

```go
HealthConfig: zephyros.HealthConfig{
    EnableHealthCheck:    true,
    HealthCheckInterval:  30 * time.Second, // Health check frequency
    MaxQueueLatency:      1 * time.Second,  // Maximum queue latency
    MaxProcessingLatency: 5 * time.Second,  // Maximum processing latency
}
```

## API Reference

### Core Methods

#### NewOperationPool
Creates a new operation pool with the specified configuration.

```go
func NewOperationPool(config PoolConfig, handler OperationHandler) (*OperationPool, error)
```

#### Submit
Submits a single operation for processing.

```go
func (p *OperationPool) Submit(ctx context.Context, op Operation) error
```

#### SubmitAsync
Submits an operation asynchronously and returns immediately.

```go
func (p *OperationPool) SubmitAsync(ctx context.Context, op Operation) (*AsyncResult, error)
```

#### SubmitBatch
Submits multiple operations as a batch.

```go
func (p *OperationPool) SubmitBatch(ctx context.Context, operations []Operation) error
```

#### GetResult
Retrieves the next available result.

```go
func (p *OperationPool) GetResult(ctx context.Context) (OperationResult, error)
```

#### GetResults
Retrieves multiple results at once.

```go
func (p *OperationPool) GetResults(ctx context.Context, count int) ([]OperationResult, error)
```

### Monitoring and Metrics

#### GetMetrics
Returns current pool metrics.

```go
func (p *OperationPool) GetMetrics() PoolMetrics
```

#### GetHealthStatus
Returns current health status.

```go
func (p *OperationPool) GetHealthStatus() HealthStatus
```

#### GetCacheStats
Returns cache statistics.

```go
func (p *OperationPool) GetCacheStats() CacheStats
```

#### ResetMetrics
Resets all metrics to zero.

```go
func (p *OperationPool) ResetMetrics()
```

### Lifecycle Management

#### Close
Gracefully shuts down the operation pool.

```go
func (p *OperationPool) Close() error
```

#### IsClosed
Checks if the pool is closed.

```go
func (p *OperationPool) IsClosed() bool
```

## Data Structures

### Operation
Represents a single operation to be processed:

```go
type Operation struct {
    Type      string                 // Operation type
    Key       string                 // Operation key
    Value     string                 // Operation value
    Tags      []string               // Operation tags
    Metadata  map[string]interface{} // Additional metadata
    Timestamp time.Time              // Operation timestamp
    ID        string                 // Unique operation ID
    Status    OperationStatus        // Current status
    Result    OperationResult        // Operation result
    Error     string                 // Error message
    WorkerID  int                    // Worker ID
    StartTime time.Time              // Processing start time
    EndTime   time.Time              // Processing end time
    RetryCount int                   // Retry count
    Priority   int                   // Operation priority
    Deadline   time.Time             // Operation deadline
    TraceID    string                // Trace ID for observability
}
```

### OperationResult
Contains the result of a processed operation:

```go
type OperationResult struct {
    OperationID    string                 // Original operation ID
    Success        bool                   // Success status
    Data           interface{}            // Result data
    Error          error                  // Error if failed
    Duration       time.Duration          // Processing duration
    Metadata       map[string]interface{} // Result metadata
    RetryCount     int                    // Number of retries
    TraceID        string                 // Trace ID
    ProcessingTime time.Time              // Processing timestamp
}
```

### PoolMetrics
Contains performance metrics for the operation pool:

```go
type PoolMetrics struct {
    ActiveWorkers   int           // Number of active workers
    QueueLength     int           // Current queue length
    ProcessedOps    int64         // Total processed operations
    FailedOps       int64         // Total failed operations
    AverageDuration time.Duration // Average processing duration
    LastReset       time.Time     // Last metrics reset time
    PoolHits        int64         // Object pool hits
    PoolMisses      int64         // Object pool misses
    RetryCount      int64         // Total retry count
    RateLimitDrops  int64         // Rate limited operations
    ValidationErrors int64        // Validation errors
    P50Latency      time.Duration // 50th percentile latency
    P95Latency      time.Duration // 95th percentile latency
    P99Latency      time.Duration // 99th percentile latency
    Throughput      float64       // Operations per second
    MemoryUsage     int64         // Memory usage in bytes
}
```

## Error Handling

The library provides comprehensive error handling with specific error types:

```go
var (
    ErrPoolClosed       = errors.New("zephyros: operation pool is closed")
    ErrBatchTimeout     = errors.New("zephyros: batch processing timeout")
    ErrCacheDisabled    = errors.New("zephyros: caching is not enabled")
    ErrContextNil       = errors.New("zephyros: context cannot be nil")
    ErrResultTimeout    = errors.New("zephyros: result retrieval timeout")
    ErrInvalidConfig    = errors.New("zephyros: invalid configuration")
    ErrTimeout          = errors.New("zephyros: operation timeout")
    ErrContextCancelled = errors.New("zephyros: context cancelled")
    ErrQueueFull        = errors.New("zephyros: operation queue is full")
    ErrValidationFailed = errors.New("zephyros: operation validation failed")
    ErrRateLimited      = errors.New("zephyros: operation rate limited")
)
```

## Performance Considerations

### Worker Count Optimization
- **CPU-bound operations**: Use `runtime.NumCPU()` or `runtime.NumCPU() * 2`
- **I/O-bound operations**: Use `runtime.NumCPU() * 4` or higher
- **Mixed workloads**: Use `runtime.NumCPU() * 2`

### Queue Size Configuration
- **High throughput**: Use larger queues (1000-10000)
- **Memory constrained**: Use smaller queues (100-500)
- **Balanced**: Use `WorkerCount * 50` as default

### Batch Processing
Use batch processing for:
- High-frequency operations (>1000 ops/sec)
- Network operations (HTTP, database)
- Operations with similar processing time
- Bulk data processing

### Caching Strategy
- Cache expensive computations
- Cache external API responses
- Cache database query results
- Use appropriate TTL based on data volatility

## Testing

The library includes comprehensive tests covering:
- Unit tests for all components
- Integration tests for complete workflows
- Concurrent stress tests
- Edge case handling
- Error scenarios
- Performance benchmarks

Run tests with:

```bash
go test -v
go test -cover
go test -bench=.
```

## Examples

See the `examples/` directory for complete working examples demonstrating:
- Basic usage patterns
- Advanced configuration
- Performance optimization
- Error handling
- Monitoring and metrics

## Benchmarks

See the `benchmarks/` directory for performance benchmarks covering:
- Single operation processing
- Batch processing performance
- Concurrent operation handling
- Memory usage patterns
- Cache performance

## License

Copyright (c) 2025 AGILira
Licensed under the Business Source License (BSL). Change Date: NEVER

See LICENSE file in the project root for full license information.
# Zephyros API Reference

This document provides a comprehensive API reference for the Zephyros library.

## Table of Contents

- [Core Types](#core-types)
- [Configuration](#configuration)
- [Operation Pool](#operation-pool)
- [StrategicCache (Sharded Cache)](#strategiccache-sharded-cache)
- [Enhanced Components](#enhanced-components)
- [Error Handling](#error-handling)
- [Examples](#examples)

## Core Types

### Operation

Represents a single operation to be processed by the operation pool.

```go
type Operation struct {
    Type      string                 // Operation type identifier
    Key       string                 // Operation key
    Value     string                 // Operation value
    Tags      []string               // Operation tags for categorization
    Metadata  map[string]interface{} // Additional metadata
    Timestamp time.Time              // Operation creation timestamp
    ID        string                 // Unique operation identifier
    
    // Status tracking
    Status    OperationStatus        // Current operation status
    Result    OperationResult        // Operation result
    Error     string                 // Error message if failed
    WorkerID  int                    // Worker ID that processed the operation
    StartTime time.Time              // Processing start time
    EndTime   time.Time              // Processing end time
    
    // Enhanced fields
    RetryCount int       // Number of retry attempts
    Priority   int       // Operation priority (higher = more important)
    Deadline   time.Time // Operation deadline
    TraceID    string    // Trace ID for observability
}
```

### OperationResult

Contains the result of a processed operation.

```go
type OperationResult struct {
    OperationID    string                 // Original operation ID
    Success        bool                   // Success status
    Data           interface{}            // Result data
    Error          error                  // Error if operation failed
    Duration       time.Duration          // Processing duration
    Metadata       map[string]interface{} // Result metadata
    
    // Enhanced fields
    RetryCount     int       // Number of retries performed
    TraceID        string    // Trace ID for observability
    ProcessingTime time.Time // Processing completion timestamp
}
```

### OperationStatus

Represents the current status of an operation.

```go
type OperationStatus string

const (
    OperationStatusPending          OperationStatus = "pending"
    OperationStatusProcessing       OperationStatus = "processing"
    OperationStatusCompleted        OperationStatus = "completed"
    OperationStatusFailed           OperationStatus = "failed"
    OperationStatusRetrying         OperationStatus = "retrying"
    OperationStatusRateLimited      OperationStatus = "rate_limited"
    OperationStatusValidationFailed OperationStatus = "validation_failed"
)
```

### OperationHandler

Interface that must be implemented to process operations.

```go
type OperationHandler interface {
    Process(ctx context.Context, op Operation) (OperationResult, error)
}
```

## Configuration

### PoolConfig

Main configuration struct for the operation pool.

```go
type PoolConfig struct {
    // Core configuration
    WorkerCount      int           // Number of worker goroutines
    QueueSize        int           // Size of operation queue
    MaxWaitTime      time.Duration // Maximum wait time for operations
    ShutdownTimeout  time.Duration // Graceful shutdown timeout
    EnableMetrics    bool          // Enable metrics collection
    EnableObjectPool bool          // Enable object pooling
    
    // Feature configurations
    BatchConfig      BatchConfig   // Batch processing configuration
    CacheConfig      CacheConfig   // Caching configuration
    RetryConfig      RetryConfig   // Retry configuration
    RateLimitConfig  RateLimitConfig // Rate limiting configuration
    HealthConfig     HealthConfig // Health monitoring configuration
    ValidationConfig ValidationConfig // Input validation configuration
}
```

### BatchConfig

Configuration for batch processing.

```go
type BatchConfig struct {
    EnableBatchProcessing bool          // Enable batch processing
    BatchSize             int           // Number of operations per batch
    BatchTimeout          time.Duration // Maximum wait time for batch completion
    FlushInterval         time.Duration // Regular flush interval
    MaxBatchSize          int           // Maximum batch size limit
}
```

### CacheConfig

Configuration for the sharded, high-performance cache.

```go
type CacheConfig struct {
    EnableCaching     bool          // Enable caching
    CacheSize         int           // Maximum number of cache entries (total across all shards)
    TTL               time.Duration // Time-to-live for cache entries
    CleanupInterval   time.Duration // Cache cleanup interval per shard
    MaxKeySize        int           // Maximum key size in bytes
    MaxValueSize      int           // Maximum value size in bytes
    EnableCompression bool          // Enable gzip compression for string values
    EvictionPolicy    string        // "lru" (O(1) per shard) or "lfu" (O(N) per shard)
    AdmissionProbability float64    // Probability (0.0-1.0) to admit new items (1.0=always, 0.0=never, 0.5=50%)
    ShardCount        int           // Number of shards (default: 16, set to 1 for deterministic eviction in tests)
}
```

- **EvictionPolicy**: "lru" (least recently used, O(1) per shard) or "lfu" (least frequently used, O(N) per shard).
- **AdmissionProbability**: Controls probabilistic admission of new items. 1.0 = always admit, 0.0 = never admit, values in between for probabilistic admission.
- **ShardCount**: Number of shards for striped locking and parallelism. For deterministic eviction in tests, set to 1.

## StrategicCache (Sharded Cache)

Zephyros provides a high-performance, sharded cache with the following features:

- **Sharding**: The cache is split into multiple shards (default: 16), each with its own lock and eviction list for reduced lock contention and high concurrency.
- **O(1) LRU eviction per shard**: Each shard maintains a doubly-linked list for fast LRU eviction.
- **Optional LFU eviction**: Each shard can use LFU eviction (O(N) per shard, optimized for typical use).
- **Configurable admission policy**: Always admit, never admit, or probabilistic (0.0-1.0).
- **Configurable cache size, TTL, key/value size, and number of shards**.
- **Optional value compression**: String values can be stored compressed (gzip).
- **Thread safety**: All operations are safe for concurrent use.
- **Deterministic eviction for tests**: Set `ShardCount: 1` for predictable eviction order in unit tests.

### Example Configuration

```go
cache := zephyros.NewStrategicCache(zephyros.CacheConfig{
    EnableCaching:        true,
    CacheSize:            10000, // total entries across all shards
    TTL:                  5 * time.Minute,
    CleanupInterval:      1 * time.Minute,
    MaxKeySize:           256,
    MaxValueSize:         4096,
    EnableCompression:    false,
    EvictionPolicy:       "lru", // or "lfu"
    AdmissionProbability: 1.0,   // 1.0 = always admit, 0.0 = never, 0.5 = 50% chance
    ShardCount:           16,    // number of shards (set to 1 for deterministic tests)
})
```

### Basic Operations

```go
cache.Set("key", "value")
v, ok := cache.Get("key")
cache.Delete("key")
cache.Clear()
stats := cache.GetStats()
```

### Admission Policy
- `AdmissionProbability: 1.0` — always admit new items
- `AdmissionProbability: 0.0` — never admit new items
- `AdmissionProbability: 0.5` — admit new items with 50% probability

### Eviction Policy
- `EvictionPolicy: "lru"` — least recently used eviction (O(1) per shard)
- `EvictionPolicy: "lfu"` — least frequently used eviction (O(N) per shard, optimized for typical use)

### Sharding
- The cache is split into `ShardCount` shards, each with its own lock and eviction list
- For deterministic eviction in tests, set `ShardCount: 1`
- For high concurrency, use the default (16) or a higher value

### Thread Safety
- All cache operations are safe for concurrent use
- Each shard is independently locked for maximum parallelism

### Compression
- If `EnableCompression` is true, string values are stored compressed (gzip)

### TTL and Cleanup
- Entries expire after `TTL` (time-to-live)
- Cleanup runs periodically per shard (`CleanupInterval`)
- Expired entries are also removed on access

## Operation Pool

### NewOperationPool

Creates a new operation pool with the specified configuration.

```go
func NewOperationPool(config PoolConfig, handler OperationHandler) (*OperationPool, error)
```

**Parameters:**
- `config`: Pool configuration
- `handler`: Operation handler implementation

**Returns:**
- `*OperationPool`: New operation pool instance
- `error`: Error if creation fails

**Example:**
```go
config := zephyros.PoolConfig{
    WorkerCount:   4,
    QueueSize:     1000,
    EnableMetrics: true,
}
handler := &MyHandler{}
pool, err := zephyros.NewOperationPool(config, handler)
if err != nil {
    log.Fatal(err)
}
```

### Submit

Submits a single operation for processing.

```go
func (p *OperationPool) Submit(ctx context.Context, op Operation) error
```

**Parameters:**
- `ctx`: Context for cancellation and timeout
- `op`: Operation to submit

**Returns:**
- `error`: Error if submission fails

**Example:**
```go
op := zephyros.Operation{
    Type:  "process",
    Key:   "user_123",
    Value: "data",
}
err := pool.Submit(ctx, op)
```

### SubmitAsync

Submits an operation asynchronously and returns immediately.

```go
func (p *OperationPool) SubmitAsync(ctx context.Context, op Operation) (*AsyncResult, error)
```

**Parameters:**
- `ctx`: Context for cancellation and timeout
- `op`: Operation to submit

**Returns:**
- `*AsyncResult`: Async result with operation ID and status
- `error`: Error if submission fails

**Example:**
```go
asyncResult, err := pool.SubmitAsync(ctx, op)
if err != nil {
    log.Printf("Async submit error: %v", err)
} else {
    log.Printf("Operation submitted with ID: %s", asyncResult.OperationID)
}
```

### SubmitBatch

Submits multiple operations as a batch.

```go
func (p *OperationPool) SubmitBatch(ctx context.Context, operations []Operation) error
```

**Parameters:**
- `ctx`: Context for cancellation and timeout
- `operations`: Slice of operations to submit

**Returns:**
- `error`: Error if batch submission fails

**Example:**
```go
operations := []zephyros.Operation{
    {Type: "read", Key: "key1", Value: "value1"},
    {Type: "write", Key: "key2", Value: "value2"},
}
err := pool.SubmitBatch(ctx, operations)
```

### GetResult

Retrieves the next available result.

```go
func (p *OperationPool) GetResult(ctx context.Context) (OperationResult, error)
```

**Parameters:**
- `ctx`: Context for cancellation and timeout

**Returns:**
- `OperationResult`: Next available result
- `error`: Error if retrieval fails

**Example:**
```go
result, err := pool.GetResult(ctx)
if err != nil {
    log.Printf("Get result error: %v", err)
} else if result.Success {
    log.Printf("Operation completed: %v", result.Data)
}
```

### GetResults

Retrieves multiple results at once.

```go
func (p *OperationPool) GetResults(ctx context.Context, count int) ([]OperationResult, error)
```

**Parameters:**
- `ctx`: Context for cancellation and timeout
- `count`: Number of results to retrieve

**Returns:**
- `[]OperationResult`: Slice of results
- `error`: Error if retrieval fails

**Example:**
```go
results, err := pool.GetResults(ctx, 10)
if err != nil {
    log.Printf("Get results error: %v", err)
} else {
    for _, result := range results {
        if result.Success {
            log.Printf("Result: %v", result.Data)
        }
    }
}
```

### GetMetrics

Returns current pool metrics.

```go
func (p *OperationPool) GetMetrics() PoolMetrics
```

**Returns:**
- `PoolMetrics`: Current pool metrics

**Example:**
```go
metrics := pool.GetMetrics()
log.Printf("Processed: %d, Failed: %d, Queue: %d",
    metrics.ProcessedOps, metrics.FailedOps, metrics.QueueLength)
```

### GetHealthStatus

Returns current health status.

```go
func (p *OperationPool) GetHealthStatus() HealthStatus
```

**Returns:**
- `HealthStatus`: Current health status

**Example:**
```go
health := pool.GetHealthStatus()
if !health.Healthy {
    log.Printf("Pool unhealthy: %s", health.Status)
}
```

### GetCacheStats

Returns cache statistics.

```go
func (p *OperationPool) GetCacheStats() CacheStats
```

**Returns:**
- `CacheStats`: Current cache statistics

**Example:**
```go
cacheStats := pool.GetCacheStats()
log.Printf("Cache size: %d/%d, total_access=%d",
    cacheStats.Size, cacheStats.MaxSize, cacheStats.TotalAccessCount)
```

### ResetMetrics

Resets all metrics to zero.

```go
func (p *OperationPool) ResetMetrics()
```

**Example:**
```go
pool.ResetMetrics()
log.Println("Metrics reset")
```

### Close

Gracefully shuts down the operation pool.

```go
func (p *OperationPool) Close() error
```

**Returns:**
- `error`: Error if shutdown fails

**Example:**
```go
if err := pool.Close(); err != nil {
    log.Printf("Error closing pool: %v", err)
}
```

### IsClosed

Checks if the pool is closed.

```go
func (p *OperationPool) IsClosed() bool
```

**Returns:**
- `bool`: True if pool is closed

**Example:**
```go
if pool.IsClosed() {
    log.Println("Pool is closed")
}
```

## Enhanced Components

### RateLimiter

Token bucket rate limiter implementation.

```go
type RateLimiter struct {
    config     RateLimitConfig
    tokens     int64
    lastRefill time.Time
    mu         sync.Mutex
}
```

**Methods:**
- `NewRateLimiter(config RateLimitConfig) *RateLimiter`: Creates new rate limiter
- `Allow() bool`: Checks if request is allowed

### Validator

Input validation component.

```go
type Validator struct {
    config ValidationConfig
}
```

**Methods:**
- `NewValidator(config ValidationConfig) *Validator`: Creates new validator
- `Validate(op Operation) error`: Validates operation

### HealthChecker

Health monitoring component.

```go
type HealthChecker struct {
    config   HealthConfig
    pool     *OperationPool
    status   HealthStatus
    stopChan chan struct{}
    mu       sync.RWMutex
}
```

**Methods:**
- `NewHealthChecker(config HealthConfig, pool *OperationPool) *HealthChecker`: Creates new health checker
- `GetStatus() HealthStatus`: Returns current health status
- `Stop()`: Stops health monitoring

### LatencyTracker

Latency tracking component.

```go
type LatencyTracker struct {
    latencies []time.Duration
    mu        sync.RWMutex
    startTime time.Time
    opCount   int64
}
```

**Methods:**
- `NewLatencyTracker() *LatencyTracker`: Creates new latency tracker
- `Record(duration time.Duration)`: Records latency measurement
- `GetPercentile(n int) time.Duration`: Returns nth percentile latency
- `GetThroughput() float64`: Returns operations per second
- `Reset()`: Resets latency tracker

## Data Structures

### PoolMetrics

Contains performance metrics for the operation pool.

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
    
    // Enhanced metrics
    RetryCount       int64         // Total retry count
    RateLimitDrops   int64         // Rate limited operations
    ValidationErrors int64         // Validation errors
    P50Latency       time.Duration // 50th percentile latency
    P95Latency       time.Duration // 95th percentile latency
    P99Latency       time.Duration // 99th percentile latency
    Throughput       float64       // Operations per second
    MemoryUsage      int64         // Memory usage in bytes
}
```

### HealthStatus

Represents the health status of the operation pool.

```go
type HealthStatus struct {
    Healthy           bool          // Overall health status
    Status            string        // Health status description
    LastCheck         time.Time     // Last health check time
    QueueLatency      time.Duration // Current queue latency
    ProcessingLatency time.Duration // Current processing latency
    ErrorCount        int64         // Error count
    WarningCount      int64         // Warning count
}
```

### CacheStats

Contains cache performance statistics.

```go
type CacheStats struct {
    Size               int   // Current cache size
    MaxSize            int   // Maximum cache size
    Enabled            bool  // Cache enabled status
    TotalSizeBytes     int   // Total size in bytes
    TotalAccessCount   int64 // Total access count
    AverageAccessCount int64 // Average access count
}
```

### AsyncResult

Represents the result of an asynchronous operation submission.

```go
type AsyncResult struct {
    OperationID string           // Operation ID
    SubmittedAt time.Time        // Submission timestamp
    Status      OperationStatus  // Operation status
    Error       error            // Error if submission failed
}
```

### BatchOperation

Represents a batch of operations.

```go
type BatchOperation struct {
    Operations []Operation // Operations in the batch
    BatchID    string      // Unique batch identifier
    Timestamp  time.Time   // Batch creation timestamp
}
```

### BatchResult

Contains the result of processing a batch of operations.

```go
type BatchResult struct {
    BatchID     string            // Batch identifier
    Results     []OperationResult // Individual operation results
    Success     bool              // Overall batch success
    Error       error             // Batch error if failed
    Duration    time.Duration     // Batch processing duration
    ProcessedAt time.Time         // Processing completion time
}
```

### CacheEntry

Represents a cached operation.

```go
type CacheEntry struct {
    Key         string      // Cache key
    Value       interface{} // Cached value
    Timestamp   time.Time   // Cache entry timestamp
    AccessCount int64       // Access count
    Size        int         // Entry size in bytes
}
```

## Error Handling

### Standard Errors

The library provides specific error types for different scenarios:

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

### Error Codes

Internal error codes for integration with go-errors:

```go
const (
    ErrCodePoolClosed       = "ZEPHYROS_POOL_CLOSED"
    ErrCodeBatchTimeout     = "ZEPHYROS_BATCH_TIMEOUT"
    ErrCodeCacheDisabled    = "ZEPHYROS_CACHE_DISABLED"
    ErrCodeContextNil       = "ZEPHYROS_CONTEXT_NIL"
    ErrCodeResultTimeout    = "ZEPHYROS_RESULT_TIMEOUT"
    ErrCodeInvalidConfig    = "ZEPHYROS_INVALID_CONFIG"
    ErrCodeTimeout          = "ZEPHYROS_TIMEOUT"
    ErrCodeContextCancelled = "ZEPHYROS_CONTEXT_CANCELLED"
    ErrCodeQueueFull        = "ZEPHYROS_QUEUE_FULL"
    ErrCodeValidationFailed = "ZEPHYROS_VALIDATION_FAILED"
    ErrCodeRateLimited      = "ZEPHYROS_RATE_LIMITED"
)
```

## Examples

### Basic Usage

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    "github.com/agilira/zephyros"
)

type SimpleHandler struct{}

func (h *SimpleHandler) Process(ctx context.Context, op zephyros.Operation) (zephyros.OperationResult, error) {
    // Simple processing logic
    result := fmt.Sprintf("Processed %s: %s", op.Key, op.Value)
    
    return zephyros.OperationResult{
        OperationID: op.ID,
        Success:     true,
        Data:        result,
        Duration:    time.Since(op.Timestamp),
    }, nil
}

func main() {
    config := zephyros.PoolConfig{
        WorkerCount:   2,
        QueueSize:     100,
        EnableMetrics: true,
    }

    handler := &SimpleHandler{}
    pool, err := zephyros.NewOperationPool(config, handler)
    if err != nil {
        log.Fatal(err)
    }
    defer pool.Close()

    ctx := context.Background()

    // Submit operations
    for i := 0; i < 10; i++ {
        op := zephyros.Operation{
            Type:  "process",
            Key:   fmt.Sprintf("key_%d", i),
            Value: fmt.Sprintf("value_%d", i),
        }
        
        if err := pool.Submit(ctx, op); err != nil {
            log.Printf("Submit error: %v", err)
        }
    }

    // Get results
    for i := 0; i < 10; i++ {
        result, err := pool.GetResult(ctx)
        if err != nil {
            log.Printf("Get result error: %v", err)
            continue
        }
        
        if result.Success {
            fmt.Printf("Result: %v\n", result.Data)
        }
    }

    // Print metrics
    metrics := pool.GetMetrics()
    fmt.Printf("Processed: %d, Failed: %d\n", metrics.ProcessedOps, metrics.FailedOps)
}
```

### Advanced Configuration

```go
config := zephyros.PoolConfig{
    WorkerCount:   4,
    QueueSize:     1000,
    MaxWaitTime:   30 * time.Second,
    EnableMetrics: true,
    
    BatchConfig: zephyros.BatchConfig{
        EnableBatchProcessing: true,
        BatchSize:            10,
        BatchTimeout:         20 * time.Millisecond,
    },
    
    CacheConfig: zephyros.CacheConfig{
        EnableCaching: true,
        CacheSize:     1000,
        TTL:           5 * time.Minute,
    },
    
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
```

### Error Handling

```go
func submitWithRetry(pool *zephyros.OperationPool, op zephyros.Operation, maxRetries int) error {
    ctx := context.Background()
    
    for attempt := 0; attempt < maxRetries; attempt++ {
        err := pool.Submit(ctx, op)
        if err == nil {
            return nil
        }
        
        // Check for specific error types
        switch {
        case errors.Is(err, zephyros.ErrPoolClosed):
            return err // Don't retry if pool is closed
        case errors.Is(err, zephyros.ErrRateLimited):
            time.Sleep(time.Duration(attempt+1) * 100 * time.Millisecond)
            continue
        case errors.Is(err, zephyros.ErrValidationFailed):
            return err // Don't retry validation errors
        default:
            time.Sleep(time.Duration(attempt+1) * 100 * time.Millisecond)
            continue
        }
    }
    
    return fmt.Errorf("failed after %d attempts", maxRetries)
}
```

### Monitoring

```go
func monitorPool(pool *zephyros.OperationPool) {
    go func() {
        ticker := time.NewTicker(30 * time.Second)
        defer ticker.Stop()
        
        for range ticker.C {
            metrics := pool.GetMetrics()
            health := pool.GetHealthStatus()
            cacheStats := pool.GetCacheStats()
            
            log.Printf("Pool Metrics: workers=%d, queue=%d, processed=%d, failed=%d",
                metrics.ActiveWorkers, metrics.QueueLength, metrics.ProcessedOps, metrics.FailedOps)
            
            log.Printf("Health: %v, status=%s, queue_latency=%v, processing_latency=%v",
                health.Healthy, health.Status, health.QueueLatency, health.ProcessingLatency)
            
            log.Printf("Cache: size=%d/%d, total_access=%d",
                cacheStats.Size, cacheStats.MaxSize, cacheStats.TotalAccessCount)
        }
    }()
}
```

This API reference covers all the functionality currently implemented in the Zephyros library. For additional examples and best practices, see the `examples/` directory and `BEST_PRACTICES.md` file. 
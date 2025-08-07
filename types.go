// types.go: Core types for zephyros
//
// Copyright (c) 2025 AGILira
// Licensed under the Business Source License (BSL). Change Date: NEVER

package zephyros

import (
	"container/list"
	"context"
	"sync"
	"time"
)

// PoolConfig defines the configuration for an OperationPool
type PoolConfig struct {
	WorkerCount      int           `json:"worker_count"`
	QueueSize        int           `json:"queue_size"`
	MaxWaitTime      time.Duration `json:"max_wait_time"`
	ShutdownTimeout  time.Duration `json:"shutdown_timeout"`
	EnableMetrics    bool          `json:"enable_metrics"`
	EnableObjectPool bool          `json:"enable_object_pool"`
	BatchConfig      BatchConfig   `json:"batch_config"`
	CacheConfig      CacheConfig   `json:"cache_config"`

	// Enhanced configuration options
	RetryConfig      RetryConfig      `json:"retry_config,omitempty"`
	RateLimitConfig  RateLimitConfig  `json:"rate_limit_config,omitempty"`
	HealthConfig     HealthConfig     `json:"health_config,omitempty"`
	ValidationConfig ValidationConfig `json:"validation_config,omitempty"`
}

// RetryConfig defines retry behavior for failed operations
type RetryConfig struct {
	EnableRetry       bool             `json:"enable_retry"`
	MaxRetries        int              `json:"max_retries"`
	RetryDelay        time.Duration    `json:"retry_delay"`
	BackoffMultiplier float64          `json:"backoff_multiplier"`
	MaxRetryDelay     time.Duration    `json:"max_retry_delay"`
	RetryableErrors   []string         `json:"retryable_errors,omitempty"`
	RetryableFunc     func(error) bool `json:"-"` // Not serializable, for advanced retry logic
}

// RateLimitConfig defines rate limiting behavior
type RateLimitConfig struct {
	EnableRateLimit   bool          `json:"enable_rate_limit"`
	RequestsPerSecond int           `json:"requests_per_second"`
	BurstSize         int           `json:"burst_size"`
	WindowSize        time.Duration `json:"window_size"`
}

// HealthConfig defines health check configuration
type HealthConfig struct {
	EnableHealthCheck    bool          `json:"enable_health_check"`
	HealthCheckInterval  time.Duration `json:"health_check_interval"`
	MaxQueueLatency      time.Duration `json:"max_queue_latency"`
	MaxProcessingLatency time.Duration `json:"max_processing_latency"`
}

// ValidationConfig defines input validation rules
type ValidationConfig struct {
	EnableValidation bool     `json:"enable_validation"`
	MaxKeyLength     int      `json:"max_key_length"`
	MaxValueLength   int      `json:"max_value_length"`
	MaxMetadataSize  int      `json:"max_metadata_size"`
	AllowedTypes     []string `json:"allowed_types,omitempty"`
	ForbiddenKeys    []string `json:"forbidden_keys,omitempty"`
}

// BatchConfig defines the configuration for batch processing
type BatchConfig struct {
	EnableBatchProcessing bool          `json:"enable_batch_processing"`
	BatchSize             int           `json:"batch_size"`
	BatchTimeout          time.Duration `json:"batch_timeout"`
	FlushInterval         time.Duration `json:"flush_interval"`
	MaxBatchSize          int           `json:"max_batch_size"`
}

// CacheConfig defines the configuration for strategic caching
type CacheConfig struct {
	EnableCaching     bool          `json:"enable_caching"`
	CacheSize         int           `json:"cache_size"`
	TTL               time.Duration `json:"ttl"`
	CleanupInterval   time.Duration `json:"cleanup_interval"`
	MaxKeySize        int           `json:"max_key_size"`
	MaxValueSize      int           `json:"max_value_size"`
	EnableCompression bool          `json:"enable_compression"`
	EvictionPolicy    string        `json:"eviction_policy"` // "lru", "lfu" (default: lfu)
	// AdmissionProbability controls the probability (0.0-1.0) that a new item is admitted to the cache (for probabilistic admission policies). Default: -1 (unset, always admit).
	AdmissionProbability float64 `json:"admission_probability,omitempty"`
	// ShardCount controls the number of shards for the cache (striped locking). Default: 32.
	ShardCount int `json:"shard_count,omitempty"`
}

// PoolMetrics contains performance metrics for the operation pool
type PoolMetrics struct {
	ActiveWorkers   int           `json:"active_workers"`
	QueueLength     int           `json:"queue_length"`
	ProcessedOps    int64         `json:"processed_ops"`
	FailedOps       int64         `json:"failed_ops"`
	AverageDuration time.Duration `json:"average_duration"`
	LastReset       time.Time     `json:"last_reset"`
	PoolHits        int64         `json:"pool_hits"`
	PoolMisses      int64         `json:"pool_misses"`

	// Enhanced metrics
	RetryCount       int64         `json:"retry_count,omitempty"`
	RateLimitDrops   int64         `json:"rate_limit_drops,omitempty"`
	ValidationErrors int64         `json:"validation_errors,omitempty"`
	P50Latency       time.Duration `json:"p50_latency,omitempty"`
	P95Latency       time.Duration `json:"p95_latency,omitempty"`
	P99Latency       time.Duration `json:"p99_latency,omitempty"`
	Throughput       float64       `json:"throughput,omitempty"`
	MemoryUsage      int64         `json:"memory_usage,omitempty"`

	// Internal mutex for thread safety
	mu sync.RWMutex `json:"-"`
}

// HealthStatus represents the health status of the operation pool
type HealthStatus struct {
	Healthy           bool          `json:"healthy"`
	Status            string        `json:"status"`
	LastCheck         time.Time     `json:"last_check"`
	QueueLatency      time.Duration `json:"queue_latency"`
	ProcessingLatency time.Duration `json:"processing_latency"`
	ErrorCount        int64         `json:"error_count"`
	WarningCount      int64         `json:"warning_count"`
}

// OperationStatus represents the current status of an operation
type OperationStatus string

// Operation status constants
const (
	OperationStatusPending          OperationStatus = "pending"
	OperationStatusProcessing       OperationStatus = "processing"
	OperationStatusCompleted        OperationStatus = "completed"
	OperationStatusFailed           OperationStatus = "failed"
	OperationStatusRetrying         OperationStatus = "retrying"
	OperationStatusRateLimited      OperationStatus = "rate_limited"
	OperationStatusValidationFailed OperationStatus = "validation_failed"
)

// Operation represents a single operation to be processed
type Operation struct {
	Type      string                 `json:"type"`
	Key       string                 `json:"key,omitempty"`
	Value     string                 `json:"value,omitempty"`
	Tags      []string               `json:"tags,omitempty"`
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
	Timestamp time.Time              `json:"timestamp"`
	ID        string                 `json:"id"`

	Status    OperationStatus `json:"status,omitempty"`
	Result    OperationResult `json:"result,omitempty"`
	Error     string          `json:"error,omitempty"`
	WorkerID  int             `json:"worker_id,omitempty"`
	StartTime time.Time       `json:"start_time,omitempty"`
	EndTime   time.Time       `json:"end_time,omitempty"`

	// Enhanced fields
	RetryCount int       `json:"retry_count,omitempty"`
	Priority   int       `json:"priority,omitempty"`
	Deadline   time.Time `json:"deadline,omitempty"`
	TraceID    string    `json:"trace_id,omitempty"`
}

// OperationResult contains the result of a processed operation
type OperationResult struct {
	OperationID string                 `json:"operation_id"`
	Success     bool                   `json:"success"`
	Data        interface{}            `json:"data,omitempty"`
	Error       error                  `json:"error,omitempty"`
	Duration    time.Duration          `json:"duration"`
	Metadata    map[string]interface{} `json:"metadata,omitempty"`

	// Enhanced fields
	RetryCount     int       `json:"retry_count,omitempty"`
	TraceID        string    `json:"trace_id,omitempty"`
	ProcessingTime time.Time `json:"processing_time,omitempty"`
}

// OperationHandler defines the interface for processing operations
type OperationHandler interface {
	Process(ctx context.Context, op Operation) (OperationResult, error)
}

// BatchOperation represents a batch of operations
type BatchOperation struct {
	Operations []Operation `json:"operations"`
	BatchID    string      `json:"batch_id"`
	Timestamp  time.Time   `json:"timestamp"`
}

// BatchResult contains the result of processing a batch of operations
type BatchResult struct {
	BatchID     string            `json:"batch_id"`
	Results     []OperationResult `json:"results"`
	Success     bool              `json:"success"`
	Error       error             `json:"error,omitempty"`
	Duration    time.Duration     `json:"duration"`
	ProcessedAt time.Time         `json:"processed_at"`
}

// CacheEntry represents a cached operation
type CacheEntry struct {
	Key         string        `json:"key"`
	Value       interface{}   `json:"value"`
	Timestamp   time.Time     `json:"timestamp"`
	AccessCount int64         `json:"access_count"`
	Size        int           `json:"size"`
	llElem      *list.Element // Pointer to node in the LRU/LFU list (internal use)
}

// AsyncResult represents the result of an asynchronous operation submission
type AsyncResult struct {
	OperationID string
	SubmittedAt time.Time
	Status      OperationStatus
	Error       error
}

// BatchSubmission represents a batch of operations to be submitted
type BatchSubmission struct {
	Operations []Operation
	BatchID    string
	Priority   int
	Deadline   time.Time
}

// errors.go: Error definitions for zephyros operation pool library
//
// Copyright (c) 2025 AGILira
// Licensed under the Business Source License (BSL). Change Date: NEVER

package zephyros

import (
	"errors"
)

// Public standard errors for drop-in compatibility
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

// Error codes for internal use (for go-errors integration)
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

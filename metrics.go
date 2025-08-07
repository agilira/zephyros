// metrics.go: Metrics for zephyros operation pool library
//
// Copyright (c) 2025 AGILira
// Licensed under the Business Source License (BSL). Change Date: NEVER

package zephyros

import (
	"time"
)

// Reset resets all metrics to their initial values
func (m *PoolMetrics) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.ProcessedOps = 0
	m.FailedOps = 0
	m.AverageDuration = 0
	m.LastReset = time.Now()
	m.PoolHits = 0
	m.PoolMisses = 0
}

// IncrementProcessedOps increments the processed operations counter
func (m *PoolMetrics) IncrementProcessedOps() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.ProcessedOps++
}

// IncrementFailedOps increments the failed operations counter
func (m *PoolMetrics) IncrementFailedOps() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.FailedOps++
}

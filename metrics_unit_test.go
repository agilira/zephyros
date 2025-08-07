package zephyros

import (
	"sync"
	"testing"
)

func TestPoolMetrics_Reset(t *testing.T) {
	m := &PoolMetrics{ProcessedOps: 10, FailedOps: 5, AverageDuration: 123, PoolHits: 2, PoolMisses: 3}
	m.Reset()
	if m.ProcessedOps != 0 || m.FailedOps != 0 || m.AverageDuration != 0 || m.PoolHits != 0 || m.PoolMisses != 0 {
		t.Error("Reset did not zero all fields")
	}
}

func TestPoolMetrics_IncrementProcessedOps(t *testing.T) {
	m := &PoolMetrics{}
	m.IncrementProcessedOps()
	if m.ProcessedOps != 1 {
		t.Errorf("Expected ProcessedOps=1, got %d", m.ProcessedOps)
	}
}

func TestPoolMetrics_IncrementFailedOps(t *testing.T) {
	m := &PoolMetrics{}
	m.IncrementFailedOps()
	if m.FailedOps != 1 {
		t.Errorf("Expected FailedOps=1, got %d", m.FailedOps)
	}
}

func TestPoolMetrics_Concurrency(t *testing.T) {
	m := &PoolMetrics{}
	wg := sync.WaitGroup{}
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			m.IncrementProcessedOps()
			m.IncrementFailedOps()
			wg.Done()
		}()
	}
	wg.Wait()
	if m.ProcessedOps != 100 || m.FailedOps != 100 {
		t.Errorf("Expected 100 ops, got %d processed, %d failed", m.ProcessedOps, m.FailedOps)
	}
}

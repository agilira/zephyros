package benchmarks

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/agilira/zephyros"
)

// BenchmarkHandler is a simple handler for benchmarking
type BenchmarkHandler struct{}

func (h *BenchmarkHandler) Process(ctx context.Context, op zephyros.Operation) (zephyros.OperationResult, error) {
	// Simulate some processing time
	time.Sleep(1 * time.Millisecond)

	return zephyros.OperationResult{
		OperationID: op.ID,
		Success:     true,
		Data:        fmt.Sprintf("Processed: %s", op.Value),
		Duration:    time.Millisecond,
	}, nil
}

// BenchmarkBasicPool benchmarks basic operation pool performance
func BenchmarkBasicPool(b *testing.B) {
	config := zephyros.PoolConfig{
		WorkerCount:   4,
		QueueSize:     1000,
		EnableMetrics: true,
	}

	pool, err := zephyros.NewOperationPool(config, &BenchmarkHandler{})
	if err != nil {
		b.Fatal(err)
	}
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

			if err := pool.Submit(ctx, op); err != nil {
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

// BenchmarkBatchProcessing benchmarks batch processing performance
func BenchmarkBatchProcessing(b *testing.B) {
	config := zephyros.PoolConfig{
		WorkerCount: 4,
		QueueSize:   1000,
		BatchConfig: zephyros.BatchConfig{
			EnableBatchProcessing: true,
			BatchSize:             10,
			BatchTimeout:          20 * time.Millisecond,
			FlushInterval:         10 * time.Millisecond,
		},
		EnableMetrics: true,
	}

	pool, err := zephyros.NewOperationPool(config, &BenchmarkHandler{})
	if err != nil {
		b.Fatal(err)
	}
	defer pool.Close()

	ctx := context.Background()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			op := zephyros.Operation{
				Type:  "batch_benchmark",
				Key:   "test_key",
				Value: "test_value",
			}

			if err := pool.Submit(ctx, op); err != nil {
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

// BenchmarkCaching benchmarks caching performance
func BenchmarkCaching(b *testing.B) {
	config := zephyros.PoolConfig{
		WorkerCount: 4,
		QueueSize:   1000,
		CacheConfig: zephyros.CacheConfig{
			EnableCaching:   true,
			CacheSize:       10000,
			TTL:             5 * time.Minute,
			CleanupInterval: 1 * time.Minute,
		},
		EnableMetrics: true,
	}

	pool, err := zephyros.NewOperationPool(config, &BenchmarkHandler{})
	if err != nil {
		b.Fatal(err)
	}
	defer pool.Close()

	ctx := context.Background()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		counter := 0
		for pb.Next() {
			// Use same key to test caching
			op := zephyros.Operation{
				Type:  "cache_benchmark",
				Key:   "cached_key",
				Value: fmt.Sprintf("value_%d", counter%10), // Cycle through 10 values
			}

			if err := pool.Submit(ctx, op); err != nil {
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

			counter++
		}
	})
}

// BenchmarkObjectPooling benchmarks object pooling performance
func BenchmarkObjectPooling(b *testing.B) {
	config := zephyros.PoolConfig{
		WorkerCount:      4,
		QueueSize:        1000,
		EnableObjectPool: true,
		EnableMetrics:    true,
	}

	pool, err := zephyros.NewOperationPool(config, &BenchmarkHandler{})
	if err != nil {
		b.Fatal(err)
	}
	defer pool.Close()

	ctx := context.Background()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			op := zephyros.Operation{
				Type:  "pool_benchmark",
				Key:   "test_key",
				Value: "test_value",
			}

			if err := pool.Submit(ctx, op); err != nil {
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

// BenchmarkConcurrentWorkers benchmarks different worker counts
func BenchmarkConcurrentWorkers(b *testing.B) {
	workerCounts := []int{1, 2, 4, 8, 16}

	for _, workers := range workerCounts {
		b.Run(fmt.Sprintf("Workers_%d", workers), func(b *testing.B) {
			config := zephyros.PoolConfig{
				WorkerCount:   workers,
				QueueSize:     1000,
				EnableMetrics: true,
			}

			pool, err := zephyros.NewOperationPool(config, &BenchmarkHandler{})
			if err != nil {
				b.Fatal(err)
			}
			defer pool.Close()

			ctx := context.Background()
			b.ResetTimer()

			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					op := zephyros.Operation{
						Type:  "worker_benchmark",
						Key:   "test_key",
						Value: "test_value",
					}

					if err := pool.Submit(ctx, op); err != nil {
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
		})
	}
}

// BenchmarkQueueSizes benchmarks different queue sizes
func BenchmarkQueueSizes(b *testing.B) {
	queueSizes := []int{100, 500, 1000, 5000, 10000}

	for _, queueSize := range queueSizes {
		b.Run(fmt.Sprintf("QueueSize_%d", queueSize), func(b *testing.B) {
			config := zephyros.PoolConfig{
				WorkerCount:   4,
				QueueSize:     queueSize,
				EnableMetrics: true,
			}

			pool, err := zephyros.NewOperationPool(config, &BenchmarkHandler{})
			if err != nil {
				b.Fatal(err)
			}
			defer pool.Close()

			ctx := context.Background()
			b.ResetTimer()

			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					op := zephyros.Operation{
						Type:  "queue_benchmark",
						Key:   "test_key",
						Value: "test_value",
					}

					if err := pool.Submit(ctx, op); err != nil {
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
		})
	}
}

// BenchmarkBatchSizes benchmarks different batch sizes
func BenchmarkBatchSizes(b *testing.B) {
	batchSizes := []int{1, 5, 10, 20, 50}

	for _, batchSize := range batchSizes {
		b.Run(fmt.Sprintf("BatchSize_%d", batchSize), func(b *testing.B) {
			config := zephyros.PoolConfig{
				WorkerCount: 4,
				QueueSize:   1000,
				BatchConfig: zephyros.BatchConfig{
					EnableBatchProcessing: true,
					BatchSize:             batchSize,
					BatchTimeout:          20 * time.Millisecond,
					FlushInterval:         10 * time.Millisecond,
				},
				EnableMetrics: true,
			}

			pool, err := zephyros.NewOperationPool(config, &BenchmarkHandler{})
			if err != nil {
				b.Fatal(err)
			}
			defer pool.Close()

			ctx := context.Background()
			b.ResetTimer()

			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					op := zephyros.Operation{
						Type:  "batch_size_benchmark",
						Key:   "test_key",
						Value: "test_value",
					}

					if err := pool.Submit(ctx, op); err != nil {
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
		})
	}
}

// BenchmarkCacheSizes benchmarks different cache sizes
func BenchmarkCacheSizes(b *testing.B) {
	cacheSizes := []int{100, 500, 1000, 5000, 10000}

	for _, cacheSize := range cacheSizes {
		b.Run(fmt.Sprintf("CacheSize_%d", cacheSize), func(b *testing.B) {
			config := zephyros.PoolConfig{
				WorkerCount: 4,
				QueueSize:   1000,
				CacheConfig: zephyros.CacheConfig{
					EnableCaching:   true,
					CacheSize:       cacheSize,
					TTL:             5 * time.Minute,
					CleanupInterval: 1 * time.Minute,
				},
				EnableMetrics: true,
			}

			pool, err := zephyros.NewOperationPool(config, &BenchmarkHandler{})
			if err != nil {
				b.Fatal(err)
			}
			defer pool.Close()

			ctx := context.Background()
			b.ResetTimer()

			b.RunParallel(func(pb *testing.PB) {
				counter := 0
				for pb.Next() {
					// Use limited set of keys to test caching
					op := zephyros.Operation{
						Type:  "cache_size_benchmark",
						Key:   fmt.Sprintf("key_%d", counter%100), // Cycle through 100 keys
						Value: "test_value",
					}

					if err := pool.Submit(ctx, op); err != nil {
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

					counter++
				}
			})
		})
	}
}

// BenchmarkStressTest benchmarks under high load
func BenchmarkStressTest(b *testing.B) {
	config := zephyros.PoolConfig{
		WorkerCount: 8,
		QueueSize:   10000,
		BatchConfig: zephyros.BatchConfig{
			EnableBatchProcessing: true,
			BatchSize:             20,
			BatchTimeout:          10 * time.Millisecond,
			FlushInterval:         5 * time.Millisecond,
		},
		CacheConfig: zephyros.CacheConfig{
			EnableCaching:   true,
			CacheSize:       5000,
			TTL:             1 * time.Minute,
			CleanupInterval: 30 * time.Second,
		},
		EnableObjectPool: true,
		EnableMetrics:    true,
	}

	pool, err := zephyros.NewOperationPool(config, &BenchmarkHandler{})
	if err != nil {
		b.Fatal(err)
	}
	defer pool.Close()

	ctx := context.Background()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		counter := 0
		for pb.Next() {
			op := zephyros.Operation{
				Type:  "stress_benchmark",
				Key:   fmt.Sprintf("stress_key_%d", counter%1000),
				Value: fmt.Sprintf("stress_value_%d", counter),
			}

			if err := pool.Submit(ctx, op); err != nil {
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

			counter++
		}
	})
}

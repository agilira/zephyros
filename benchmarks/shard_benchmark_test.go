package benchmarks

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/agilira/zephyros"
)

func BenchmarkCache_ShardComparison(b *testing.B) {
	shardConfigs := []struct {
		name       string
		shardCount int
	}{
		{"2_shards", 2},
		{"8_shards", 8},
		{"16_shards", 16},
		{"32_shards", 32},
		{"64_shards", 64},
	}

	for _, config := range shardConfigs {
		b.Run(config.name, func(b *testing.B) {
			cache := zephyros.NewStrategicCache(zephyros.CacheConfig{
				EnableCaching:        true,
				EnableCompression:    false,
				CacheSize:            10000,
				ShardCount:           config.shardCount,
				MaxValueSize:         4096,
				AdmissionProbability: -1,
			})
			defer cache.Close()

			// Benchmark concurrent writes
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				counter := 0
				for pb.Next() {
					key := fmt.Sprintf("key_%d", counter)
					value := fmt.Sprintf("value_%d", counter)
					cache.Set(key, value)
					counter++
				}
			})
		})
	}
}

func BenchmarkCache_ShardComparison_ReadWrite(b *testing.B) {
	shardConfigs := []struct {
		name       string
		shardCount int
	}{
		{"2_shards", 2},
		{"8_shards", 8},
		{"16_shards", 16},
		{"32_shards", 32},
		{"64_shards", 64},
	}

	for _, config := range shardConfigs {
		b.Run(config.name, func(b *testing.B) {
			cache := zephyros.NewStrategicCache(zephyros.CacheConfig{
				EnableCaching:        true,
				EnableCompression:    false,
				CacheSize:            10000,
				ShardCount:           config.shardCount,
				MaxValueSize:         4096,
				AdmissionProbability: -1,
			})
			defer cache.Close()

			// Pre-populate cache
			for i := 0; i < 1000; i++ {
				key := fmt.Sprintf("key_%d", i)
				value := fmt.Sprintf("value_%d", i)
				cache.Set(key, value)
			}

			// Benchmark mixed read/write operations
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				counter := 0
				for pb.Next() {
					if counter%3 == 0 {
						// Write operation
						key := fmt.Sprintf("key_%d", counter)
						value := fmt.Sprintf("value_%d", counter)
						cache.Set(key, value)
					} else {
						// Read operation
						key := fmt.Sprintf("key_%d", counter%1000)
						cache.Get(key)
					}
					counter++
				}
			})
		})
	}
}

func BenchmarkCache_ShardComparison_ConcurrentLoad(b *testing.B) {
	shardConfigs := []struct {
		name       string
		shardCount int
	}{
		{"2_shards", 2},
		{"8_shards", 8},
		{"16_shards", 16},
		{"32_shards", 32},
		{"64_shards", 64},
	}

	for _, config := range shardConfigs {
		b.Run(config.name, func(b *testing.B) {
			cache := zephyros.NewStrategicCache(zephyros.CacheConfig{
				EnableCaching:        true,
				EnableCompression:    false,
				CacheSize:            10000,
				ShardCount:           config.shardCount,
				MaxValueSize:         4096,
				AdmissionProbability: -1,
			})
			defer cache.Close()

			// Pre-populate cache
			for i := 0; i < 1000; i++ {
				key := fmt.Sprintf("key_%d", i)
				value := fmt.Sprintf("value_%d", i)
				cache.Set(key, value)
			}

			// Benchmark with multiple goroutines simulating real-world load
			b.ResetTimer()

			const numGoroutines = 16
			var wg sync.WaitGroup

			for i := 0; i < numGoroutines; i++ {
				wg.Add(1)
				go func(goroutineID int) {
					defer wg.Done()

					// Each goroutine performs b.N/numGoroutines operations
					operationsPerGoroutine := b.N / numGoroutines
					for j := 0; j < operationsPerGoroutine; j++ {
						operationID := goroutineID*operationsPerGoroutine + j

						switch operationID % 4 {
						case 0: // Write
							key := fmt.Sprintf("key_%d_%d", goroutineID, j)
							value := fmt.Sprintf("value_%d_%d", goroutineID, j)
							cache.Set(key, value)
						case 1: // Read existing
							key := fmt.Sprintf("key_%d", j%1000)
							cache.Get(key)
						case 2: // Read potentially non-existent
							key := fmt.Sprintf("key_nonexistent_%d", j)
							cache.Get(key)
						case 3: // Delete
							key := fmt.Sprintf("key_%d", j%1000)
							cache.Delete(key)
						}
					}
				}(i)
			}

			wg.Wait()
		})
	}
}

func BenchmarkCache_ShardComparison_Latency(b *testing.B) {
	shardConfigs := []struct {
		name       string
		shardCount int
	}{
		{"2_shards", 2},
		{"8_shards", 8},
		{"16_shards", 16},
		{"32_shards", 32},
		{"64_shards", 64},
	}

	for _, config := range shardConfigs {
		b.Run(config.name, func(b *testing.B) {
			cache := zephyros.NewStrategicCache(zephyros.CacheConfig{
				EnableCaching:        true,
				EnableCompression:    false,
				CacheSize:            10000,
				ShardCount:           config.shardCount,
				MaxValueSize:         4096,
				AdmissionProbability: -1,
			})
			defer cache.Close()

			// Pre-populate cache
			for i := 0; i < 1000; i++ {
				key := fmt.Sprintf("key_%d", i)
				value := fmt.Sprintf("value_%d", i)
				cache.Set(key, value)
			}

			// Benchmark latency under concurrent load
			b.ResetTimer()

			const numGoroutines = 8
			var wg sync.WaitGroup
			var totalLatency time.Duration
			var latencyMutex sync.Mutex

			for i := 0; i < numGoroutines; i++ {
				wg.Add(1)
				go func(goroutineID int) {
					defer wg.Done()

					operationsPerGoroutine := b.N / numGoroutines
					for j := 0; j < operationsPerGoroutine; j++ {
						key := fmt.Sprintf("key_%d", j%1000)

						start := time.Now()
						cache.Get(key)
						latency := time.Since(start)

						latencyMutex.Lock()
						totalLatency += latency
						latencyMutex.Unlock()
					}
				}(i)
			}

			wg.Wait()

			// Report average latency
			avgLatency := totalLatency / time.Duration(b.N)
			b.ReportMetric(float64(avgLatency.Nanoseconds()), "ns/op")
		})
	}
}

# Zephyros Best Practices

## Production Deployment Patterns

### 1. Architecture Selection

#### Single Ring Buffer (`Zephyros[T]`)
**Use When:**
- ✅ Single producer thread per ring
- ✅ Need maximum single-threaded performance (104M+ ops/sec)
- ✅ Simple processing pipeline
- ✅ Predictable load patterns

**Example:**
```go
// High-frequency trading order processing
buffer, err := zephyros.NewBuilder[Order](4096).
    WithProcessor(func(order *Order) {
        processOrder(order) // Single consumer, ultra-fast
    }).
    WithBatchSize(128). // Dynamic batching will adapt
    Build()
```

#### Multi-Ring Buffer (`ThreadedZephyros[T]`)
**Use When:**
- ✅ Multiple concurrent producers
- ✅ Need linear scalability across CPU cores  
- ✅ Complex multi-threaded applications
- ✅ Variable producer workloads

**Example:**
```go
// Multi-feed market data processing
threaded, err := zephyros.NewThreadedBuilder[MarketData](2048, 8).
    WithProcessor(func(data *MarketData) {
        processMarketData(data) // Parallel processing
    }).
    WithBatchSize(64). // Per-ring batching
    Build()
```

### 2. Capacity Planning

#### Buffer Sizing Guidelines

```go
// Formula: capacity = 2^n where n >= log2(peak_concurrent_items * 2 + batch_size)

// High-throughput, low-latency
capacity := 4096   // 4K items, ~32KB memory footprint

// Balanced throughput/memory
capacity := 2048   // 2K items, ~16KB memory footprint  

// Memory-constrained environments
capacity := 1024   // 1K items, ~8KB memory footprint

// Ultra-high throughput
capacity := 8192   // 8K items, ~64KB memory footprint
```

#### Ring Count Selection (ThreadedZephyros)

```go
// Match producer threads
numRings := numberOfProducerThreads

// CPU-bound workloads
numRings := runtime.NumCPU()

// I/O-bound workloads
numRings := runtime.NumCPU() * 2 // Allow for blocking operations

// Memory-constrained
numRings := min(numberOfProducerThreads, 4) // Limit memory usage
```

### 3. Batch Size Optimization

#### Dynamic Batching (Recommended)
```go
// Let Zephyros adapt automatically - optimal for most cases
WithBatchSize(64) // Starting point, will adapt to:
// - 4x expansion when buffer >75% full (emergency drain)
// - 128 items when buffer nearly empty (ultra-low latency)
// - Original size for normal operation
```

#### Static Batching (Specialized Use Cases)
```go
// Ultra-low latency (real-time systems)
WithBatchSize(1)     // Process immediately

// High-throughput batch jobs  
WithBatchSize(256)   // Amortize processing overhead

// Balanced (general purpose)
WithBatchSize(32)    // Good for most applications
```

### 4. API Strategy Selection

#### Fast Path vs Safe Path (ThreadedZephyros)

**Fast Path - Use When:**
```go
// Performance-critical hot paths
// Thread/Ring IDs are compile-time known or pre-validated
ring := tz.GetWriterRing(threadID) // Panics on invalid ID
writer := tz.NewSafeWriter(0)      // Panics on invalid ID

// Example: High-frequency trading
for i := 0; i < 1000000; i++ {
    ring.Write(func(slot *Order) {
        slot.ID = i
        slot.Price = getCurrentPrice()
    })
}
```

**Safe Path - Use When:**
```go
// Critical infrastructure, external input validation
// Graceful error handling required
ring, err := tz.SafeGetWriterRing(threadID)
if err != nil {
    if errors.Is(err, zephyros.ErrInvalidThreadID) {
        // Handle gracefully
        return fmt.Errorf("producer %d not configured: %w", threadID, err)
    }
}

writer, err := tz.NewSafeWriterWithError(ringID)
if err != nil {
    return fmt.Errorf("failed to create writer: %w", err)
}
```

### 5. Error Handling Patterns

#### Backpressure Management
```go
func robustWrite(ring *zephyros.Zephyros[T], data T) error {
    const maxRetries = 3
    
    for i := 0; i < maxRetries; i++ {
        success := ring.Write(func(slot *T) {
            *slot = data
        })
        
        if success {
            return nil
        }
        
        // Backpressure detected - adaptive wait
        if i == 0 {
            runtime.Gosched() // First try: yield
        } else {
            time.Sleep(time.Microsecond * time.Duration(i)) // Exponential backoff
        }
    }
    
    return ErrBufferFull
}
```

#### Graceful Shutdown
```go
func gracefulShutdown(buffer *zephyros.Zephyros[T]) {
    // 1. Stop accepting new writes
    buffer.Close()
    
    // 2. LoopProcess() will automatically drain remaining items
    // No additional action needed - it's deterministic
    
    // 3. Optional: Monitor completion
    ticker := time.NewTicker(100 * time.Millisecond)
    defer ticker.Stop()
    
    for {
        select {
        case <-ticker.C:
            stats := buffer.Stats()
            remaining := stats["items_buffered"]
            if remaining == 0 {
                log.Printf("Shutdown complete - all items processed")
                return
            }
            log.Printf("Draining: %d items remaining", remaining)
        case <-time.After(5 * time.Second):
            log.Printf("Shutdown timeout - forcing exit")
            return
        }
    }
}
```

### 6. Performance Optimization

#### Producer Optimization
```go
// DON'T: Create closures in hot paths
for i := 0; i < 1000000; i++ {
    ring.Write(func(slot *Order) {
        slot.ID = i      // Captures i - closure allocation
        slot.Data = data // Captures data - closure allocation  
    })
}

// DO: Minimize allocations
var order Order
for i := 0; i < 1000000; i++ {
    order.ID = i
    order.Data = data
    
    ring.Write(func(slot *Order) {
        *slot = order // Copy value, no closure
    })
}
```

#### Processor Optimization
```go
// DON'T: Blocking operations in processor
processor := func(item *LogEntry) {
    // NEVER do this - blocks all processing
    http.Post(url, "application/json", bytes.NewReader(item.JSON))
}

// DO: Async processing
processor := func(item *LogEntry) {
    // Fast, non-blocking operation
    asyncLogger.Enqueue(*item) // Separate async system
}

// DO: Batch external calls
processor := func(item *LogEntry) {
    batchBuffer = append(batchBuffer, *item)
    if len(batchBuffer) >= batchSize {
        go flushBatch(batchBuffer) // Async batch
        batchBuffer = batchBuffer[:0]
    }
}
```

### 7. Monitoring and Observability

#### Production Metrics
```go
func monitorPerformance(buffer *zephyros.Zephyros[T]) {
    ticker := time.NewTicker(1 * time.Second)
    
    go func() {
        for {
            select {
            case <-ticker.C:
                stats := buffer.Stats()
                
                // Key metrics
                writerPos := stats["writer_position"]
                readerPos := stats["reader_position"]
                buffered := stats["items_buffered"]
                capacity := stats["buffer_size"]
                
                // Calculate utilization
                utilization := float64(buffered) / float64(capacity) * 100
                
                // Calculate throughput (requires tracking previous values)
                throughput := writerPos - lastWriterPos
                lastWriterPos = writerPos
                
                // Alert thresholds
                if utilization > 80 {
                    log.Printf("WARNING: Buffer %0.1f%% full", utilization)
                }
                
                if throughput == 0 {
                    log.Printf("WARNING: No throughput detected")
                }
                
                // Metrics export
                prometheus.BufferUtilization.Set(utilization)
                prometheus.BufferThroughput.Set(float64(throughput))
            }
        }
    }()
}
```

#### ThreadedZephyros Monitoring
```go
func monitorThreadedPerformance(threaded *zephyros.ThreadedZephyros[T]) {
    stats := threaded.Stats()
    
    numRings := stats["num_rings"]
    totalBuffered := stats["items_buffered"]
    
    // Per-ring analysis
    for i := 0; i < int(numRings); i++ {
        ringKey := fmt.Sprintf("ring_%d_items", i)
        ringItems := stats[ringKey]
        
        if ringItems > capacity*3/4 {
            log.Printf("Ring %d backpressure: %d items", i, ringItems)
        }
    }
    
    // Load balancing check
    avgItems := totalBuffered / numRings
    for i := 0; i < int(numRings); i++ {
        ringKey := fmt.Sprintf("ring_%d_items", i)
        ringItems := stats[ringKey]
        
        if ringItems > avgItems*2 {
            log.Printf("Ring %d hot: %d items (avg: %d)", i, ringItems, avgItems)
        }
    }
}
```

### 8. Testing Strategies

#### Unit Testing
```go
func TestBackpressure(t *testing.T) {
    buffer, err := zephyros.NewBuilder[int](4). // Small buffer
        WithProcessor(func(item *int) {
            // Slow processor to trigger backpressure
            time.Sleep(time.Millisecond)
        }).
        Build()
    require.NoError(t, err)
    defer buffer.Close()
    
    go buffer.LoopProcess()
    
    // Fill buffer
    successCount := 0
    for i := 0; i < 100; i++ {
        success := buffer.Write(func(slot *int) { *slot = i })
        if success {
            successCount++
        }
    }
    
    // Should experience backpressure
    assert.Less(t, successCount, 100)
}
```

#### Load Testing
```go
func BenchmarkProduction(b *testing.B) {
    buffer, _ := zephyros.NewBuilder[int](8192).
        WithProcessor(func(item *int) {
            // Simulate production workload
        }).
        Build()
    defer buffer.Close()
    
    go buffer.LoopProcess()
    
    b.ResetTimer()
    b.RunParallel(func(pb *testing.PB) {
        for pb.Next() {
            buffer.Write(func(slot *int) {
                *slot = b.N
            })
        }
    })
}
```

### 9. Common Anti-Patterns

#### ❌ Don't Do This

```go
// Multiple producers on single ring
ring := zephyros.NewBuilder[T](1024).Build()
go producer1(ring) // Race condition!
go producer2(ring) // Race condition!

// Blocking in processor
processor := func(item *T) {
    time.Sleep(time.Second) // Kills performance
    database.Save(*item)    // Blocking I/O
}

// Creating rings in hot path
for range items {
    ring := zephyros.NewBuilder[T](1024).Build() // Expensive!
}

// Ignoring backpressure
for range items {
    ring.Write(func(slot *T) { /* ... */ }) // May drop data
}
```

#### ✅ Do This Instead

```go
// Use ThreadedZephyros for multiple producers
threaded := zephyros.NewThreadedBuilder[T](1024, 4).Build()
go producer1(threaded, 0) // Ring 0
go producer2(threaded, 1) // Ring 1

// Non-blocking processor with async I/O
processor := func(item *T) {
    asyncSaver.Queue(*item) // Fast enqueue
}

// Pre-create rings
ring := zephyros.NewBuilder[T](1024).Build()
for range items {
    // Use existing ring
}

// Handle backpressure
success := ring.Write(func(slot *T) { /* ... */ })
if !success {
    // Handle appropriately: retry, drop, queue elsewhere
}
```

### 10. Production Checklist

#### Before Deployment
- [ ] Capacity planning validated with load tests
- [ ] Batch size tuned for workload characteristics
- [ ] Error handling implemented for backpressure scenarios
- [ ] Monitoring and alerting configured
- [ ] Graceful shutdown procedures tested
- [ ] Memory usage profiled under peak load
- [ ] CPU utilization optimized

#### Runtime Monitoring
- [ ] Buffer utilization < 80% under normal load
- [ ] Consistent throughput measurements
- [ ] Zero memory leaks over 24h+ runs
- [ ] Graceful degradation under extreme load
- [ ] Alert thresholds tuned to operational needs

This guide provides production-tested patterns for deploying Zephyros in demanding environments. Follow these practices to achieve optimal performance while maintaining system reliability.

---

Zephyros • an AGILira fragment

# Zephyros: Ultra-High Performance MPSC Ring Buffer for Go
### an AGILira fragment

Zephyros is a lock-free, zero-allocation MPSC ring buffer for Go applications requiring extreme multi-producer throughput and minimal latency.

[![CI/CD Pipeline](https://github.com/agilira/zephyros/workflows/CI/CD%20Pipeline/badge.svg)](https://github.com/agilira/zephyros/actions?query=workflow%3A%22CI%2FCD+Pipeline%22)
[![Gosec Security](https://github.com/agilira/zephyros/workflows/Gosec%20Security/badge.svg)](https://github.com/agilira/zephyros/actions?query=workflow%3A%22Gosec+Security%22)
[![Go Report Card](https://goreportcard.com/badge/github.com/agilira/zephyros?v=1)](https://goreportcard.com/report/github.com/agilira/zephyros)
[![Coverage](https://img.shields.io/badge/coverage-94.3%25-brightgreen)](https://github.com/agilira/zephyros)

## Architecture

Zephyros provides optimized ring buffer architectures for different concurrency patterns:

### Zephyros Core
- **MPSC Lock-Free**: Single consumer, optimized single producer per ring
- **Zero Allocations**: Pre-allocated buffers eliminate GC pressure
- **Cache-Line Padding**: Prevents false sharing for multi-threaded performance
- **Dynamic Adaptive Batching**: Intelligent batch sizing based on buffer load
- **Performance**: 104M+ ops/sec sustained throughput with ~9.5ns latency

### ThreadedZephyros
- **Multi-Ring Architecture**: Dedicated rings for concurrent producers
- **Gemini Strategy**: Eliminates producer contention through ring separation  
- **Linear Scalability**: Consistent performance across 1-8 rings
- **Unified Consumer**: Single consumer processes all rings efficiently

```
Gemini Strategy Architecture:

[Producer 1] ──► [Ring 1] ──┐
[Producer 2] ──► [Ring 2] ──┤
[Producer 3] ──► [Ring 3] ──┼──► [Unified Consumer]
[Producer 4] ──► [Ring 4] ──┘

- Zero contention between producers
- Single consumer eliminates coordination overhead
- Linear scaling: N producers = N rings = constant performance
```

## Performance

Zephyros is engineered for multi-producer performance. The following benchmarks demonstrate sustained throughput of 100M+ ops/sec with zero memory allocations and dynamic adaptive batching.

### AMD Ryzen 5 7520U with Radeon Graphics (8 cores)
```
BenchmarkThreadedZephyros_Baseline-8           374885246        9.548 ns/op    104.7M ops/sec      0 B/op    0 allocs/op
BenchmarkThreadedZephyros_ProcessingThroughput-8 55866120       63.78 ns/op     15.7M complete/sec
BenchmarkAtomicPaddedInt64_MultiThread-8      1000000000        1.928 ns/op    518.3M ops/sec      0 B/op    0 allocs/op
```

**Key Features:**
- **104M+ ops/sec** write throughput 
- **15.7M complete ops/sec** end-to-end processing
- **Dynamic adaptive batching** automatically optimizes for load
- **Linear scalability** across multiple rings
- **518M atomic ops/sec** with cache-line padding

## Installation

```bash
go get github.com/agilira/zephyros
```

## Quick Start

### Single Producer Usage

```go
package main

import (
    "fmt"
    "github.com/agilira/zephyros"
)

func main() {
    // Create ring buffer for single producer
    buffer, err := zephyros.NewBuilder[int](1024).
        WithProcessor(func(item *int) {
            fmt.Printf("Processing: %d\n", *item)
        }).
        WithBatchSize(32). // Dynamic batching will adapt this
        Build()

    if err != nil {
        panic(err)
    }
    defer buffer.Close()

    // Start consumer
    go buffer.LoopProcess()

    // Producer: write data (single producer per ring)
    for i := 0; i < 1000; i++ {
        success := buffer.Write(func(slot *int) {
            *slot = i
        })
        if !success {
            fmt.Println("Buffer full - backpressure active")
        }
    }
}
```

### Multi-Producer Usage (ThreadedZephyros)

```go
package main

import (
    "sync"
    "github.com/agilira/zephyros"
)

func main() {
    var wg sync.WaitGroup

    // Create multi-ring system for concurrent producers
    threaded, err := zephyros.NewThreadedBuilder[int](1024, 4). // 4 rings
        WithProcessor(func(item *int) {
            // Process item with automatic batch optimization
        }).
        WithBatchSize(64).
        Build()

    if err != nil {
        panic(err)
    }
    defer threaded.Close()

    // Start unified consumer
    go threaded.LoopProcess()

    // Multiple producers, each with dedicated ring
    for producerID := 0; producerID < 4; producerID++ {
        wg.Add(1)
        go func(id int) {
            defer wg.Done()
            for i := 0; i < 100000; i++ {
                threaded.Write(id, func(slot *int) {
                    *slot = id*100000 + i
                })
            }
        }(producerID)
    }

    wg.Wait()
    fmt.Println("Multi-producer processing complete")
}
```

## Advanced Features

### Dynamic Adaptive Batching
- **Automatic optimization**: Batch size adapts to buffer occupancy
- **Emergency drain**: 4x expansion when buffer >75% full  
- **Ultra-low latency**: Reduces to 128 items when nearly empty
- **Zero overhead**: Triggers only in extreme conditions

### MPSC Contract
- **Single producer per ring**: Optimized for maximum performance
- **ThreadedZephyros for multiple producers**: Use separate rings
- **Lock-free synchronization**: Pure atomic operations

## Use Cases

- **High-Frequency Trading**: Multi-feed market data processing
- **Real-Time Analytics**: Concurrent stream processing
- **Message Queues**: Lock-free multi-producer communication
- **IoT Data Ingestion**: High-volume concurrent sensor processing
- **Game Engines**: Multi-threaded entity processing

## API Reference

### Core Operations
```go
// Write to buffer (single producer per ring)
func (z *Zephyros[T]) Write(writerFunc func(*T)) bool

// Process batch with dynamic sizing
func (z *Zephyros[T]) ProcessBatch() int

// Continuous processing loop
func (z *Zephyros[T]) LoopProcess()
```

### ThreadedZephyros
```go
// Multi-ring builder
func NewThreadedBuilder[T any](capacity int64, numRings int) *ThreadedBuilder[T]

// Write to specific ring
func (t *ThreadedZephyros[T]) Write(ringID int, writerFunc func(*T)) bool
```

**📚 Complete API Documentation**: [pkg.go.dev/github.com/agilira/zephyros](https://pkg.go.dev/github.com/agilira/zephyros)

## Performance Tuning

```go
// Latency optimized
WithBatchSize(1)     // Immediate processing

// Balanced (with adaptive batching)  
WithBatchSize(32)    // Auto-adapts to load

// Throughput optimized
WithBatchSize(256)   // Amortize overhead
```

## Best Practices

### Do's ✅
- Use ThreadedZephyros for multiple producers
- Leverage dynamic adaptive batching
- Monitor backpressure signals
- Use power-of-2 capacities

### Don'ts ❌
- Multiple producers on single ring
- Multiple consumers
- Blocking operations in processor

## The Philosophy Behind Zephyros

In Greek mythology, Zephyros was the god of the west wind, known for bringing gentle breezes and enabling swift travel. Unlike chaotic storms, Zephyros represented controlled power—the ability to provide exactly the right amount of force when needed.

This embodies Zephyros' design philosophy: controlled multi-producer performance through intelligent architecture. The MPSC design provides gentle, predictable throughput patterns, while dynamic adaptive batching ensures the system provides exactly the right processing intensity for current load conditions. ThreadedZephyros enables swift scaling across multiple producers without the chaos of lock contention.

Zephyros doesn't just move data fast—it moves it intelligently, adapting to conditions while maintaining the reliability that production systems demand.

## Documentation

**Quick Links:**
- **[Quick Start Guide](./docs/QUICK_START.md)** - Get running in 2 minutes 🚀
- **[API Reference](https://pkg.go.dev/github.com/agilira/zephyros)** - Complete API documentation on pkg.go.dev
- **[Test Coverage Report](./coverage.html)** - Detailed coverage analysis (94.3%)
- **[Architecture Guide](./docs/ARCHITECTURE.md)** - Deep dive into MPSC design and Gemini Strategy
- **[ThreadedZephyros API](./docs/THREADED_API.md)** - Multi-ring API with Fast/Safe path documentation
- **[Dynamic Batching](./docs/DYNAMIC_BATCHING.md)** - Intelligent batch size adaptation explained
- **[Best Practices](./docs/BEST_PRACTICES.md)** - Production deployment patterns and optimization guide

## License

Zephyros is licensed under the [Mozilla Public License 2.0](./LICENSE).

---

Zephyros • an AGILira fragment

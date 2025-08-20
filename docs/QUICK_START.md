# Quick Start Guide

Get up and running with Zephyros in under 2 minutes.

## Installation

```bash
go get github.com/agilira/zephyros
```

## 30-Second Example

```go
package main

import (
    "fmt"
    "github.com/agilira/zephyros"
)

func main() {
    // 1. Create buffer
    buffer, _ := zephyros.NewBuilder[string](1024).
        WithProcessor(func(msg *string) {
            fmt.Println("Processed:", *msg)
        }).
        Build()
    defer buffer.Close()

    // 2. Start processing
    go buffer.LoopProcess()

    // 3. Write data
    buffer.Write(func(slot *string) {
        *slot = "Hello Zephyros!"
    })
}
```

## When to Use What

### Single Producer → Use `Zephyros`
```go
buffer, _ := zephyros.NewBuilder[int](1024).
    WithProcessor(func(n *int) { fmt.Println(*n) }).
    Build()

go buffer.LoopProcess()

buffer.Write(func(slot *int) { *slot = 42 })
```

### Multiple Producers → Use `ThreadedZephyros`
```go
threaded, _ := zephyros.NewThreadedBuilder[int](1024, 4). // 4 rings
    WithProcessor(func(n *int) { fmt.Println(*n) }).
    Build()

go threaded.LoopProcess()

// Each producer gets own ring (thread-safe)
go producer(threaded, 0) // Ring 0
go producer(threaded, 1) // Ring 1

func producer(tz *zephyros.ThreadedZephyros[int], ringID int) {
    tz.Write(ringID, func(slot *int) { *slot = 42 })
}
```

## Common Patterns

### Error Handling
```go
success := buffer.Write(func(slot *string) {
    *slot = "data"
})
if !success {
    // Buffer full - handle backpressure
    log.Println("Buffer full, try again later")
}
```

### Safe Multi-Producer
```go
// Error-safe ring access
ring, err := threaded.SafeGetWriterRing(threadID)
if err != nil {
    log.Printf("Invalid thread ID: %v", err)
    return
}

ring.Write(func(slot *string) { *slot = "safe write" })
```

### Graceful Shutdown
```go
// Close stops accepting writes and drains remaining items
buffer.Close() // Automatic - no additional steps needed
```

## Performance Tips

```go
// Good defaults for most cases
zephyros.NewBuilder[T](2048).          // Power of 2 capacity
    WithProcessor(fastProcessor).       // Non-blocking processor
    WithBatchSize(64).                 // Let dynamic batching adapt
    Build()
```

## Next Steps

- **[Full Examples](../README.md#quick-start)** - Complete working examples
- **[API Reference](./API.md)** - All methods and options
- **[Best Practices](./BEST_PRACTICES.md)** - Production deployment guide
- **[Dynamic Batching](./DYNAMIC_BATCHING.md)** - Performance optimization details

## Troubleshooting

### Buffer Full Errors
```go
// Problem: Write returns false
buffer.Write(func(slot *T) { *slot = data }) // returns false

// Solution: Check processor speed or increase capacity
zephyros.NewBuilder[T](4096). // Larger buffer
    WithProcessor(fasterProcessor). // Non-blocking processor
    Build()
```

### Low Performance
```go
// Problem: Slow processing
WithProcessor(func(item *T) {
    time.Sleep(time.Second) // DON'T: Blocking operations
    database.Save(*item)    // DON'T: Synchronous I/O
})

// Solution: Non-blocking processor
WithProcessor(func(item *T) {
    asyncQueue.Enqueue(*item) // DO: Fast enqueue
})
```

### Multiple Producers on Single Ring
```go
// Problem: Race conditions
ring := zephyros.NewBuilder[T](1024).Build()
go producer1(ring) // WRONG: Multiple producers
go producer2(ring) // WRONG: on same ring

// Solution: Use ThreadedZephyros
threaded := zephyros.NewThreadedBuilder[T](1024, 2).Build()
go producer1(threaded, 0) // Correct: Separate rings
go producer2(threaded, 1) // Correct: per producer
```

---

**Ready to build high-performance applications?** Start with these examples and explore the full documentation for advanced features.

---

Zephyros • an AGILira fragment

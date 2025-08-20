# Zephyros API Reference

## Overview

Zephyros is a ultra-high performance lock-free Multi-Producer Single-Consumer (MPSC) ring buffer implementation designed for Go applications requiring maximum throughput (40M+ ops/sec) with zero allocations.

## Core Components

### 1. Single Ring Buffer (`Zephyros[T]`)

#### Types

```go
type ProcessorFunc[T any] func(*T)

type Zephyros[T any] struct {
    // Implementation details omitted
}
```

#### Constructor

```go
func NewBuilder[T any](capacity int64) *Builder[T]
```

**Parameters:**
- `capacity`: Must be a power of 2. Determines ring buffer size.

**Returns:** Builder instance for fluent configuration.

#### Builder Pattern

```go
type Builder[T any] struct {
    capacity  int64
    processor ProcessorFunc[T]
    batchSize int64
}
```

##### Builder Methods

```go
func (b *Builder[T]) WithProcessor(processor ProcessorFunc[T]) *Builder[T]
```
- **Purpose:** Sets the processing function called for each item.
- **Required:** Yes, will return `ErrMissingProcessor` if not provided.

```go
func (b *Builder[T]) WithBatchSize(batchSize int64) *Builder[T]
```
- **Purpose:** Sets batch processing size (default: intelligent based on capacity).
- **Range:** Must be positive and ≤ capacity.

```go
func (b *Builder[T]) Build() (*Zephyros[T], error)
```
- **Purpose:** Creates and initializes the Zephyros instance.
- **Errors:** `ErrCapacity`, `ErrMissingProcessor`, validation errors.

#### Core Methods

```go
func (z *Zephyros[T]) Write(writerFunc func(*T)) bool
```
- **Purpose:** Ultra-fast write operation for MPSC pattern.
- **Parameters:** Function that populates the allocated slot.
- **Returns:** `true` if successful, `false` if buffer full or closed.
- **Thread Safety:** Multiple producers safe, assumes MPSC contract.

```go
func (z *Zephyros[T]) ProcessBatch() int
```
- **Purpose:** Processes available items in a single batch (single consumer).
- **Returns:** Number of items processed.
- **Performance:** Zero-allocation, optimized for single consumer.

```go
func (z *Zephyros[T]) TryProcessBatch() int
```
- **Purpose:** Thread-safe batch processing with CAS operations.
- **Returns:** Number of items processed.
- **Use Case:** Work-stealing scenarios with multiple consumers.

```go
func (z *Zephyros[T]) LoopProcess()
```
- **Purpose:** Continuous processing loop with optimized idle strategy.
- **Behavior:** Blocks until `Close()` called, processes all remaining items.

```go
func (z *Zephyros[T]) Flush()
```
- **Purpose:** Ensures all pending writes are visible to reader.
- **Thread Safety:** Safe to call from any thread.

```go
func (z *Zephyros[T]) Close()
```
- **Purpose:** Signals shutdown to processing loops.
- **Behavior:** Non-blocking, sets closed flag.

```go
func (z *Zephyros[T]) Stats() map[string]int64
```
- **Purpose:** Returns performance and state statistics.
- **Returns:** Map with keys: `writer_position`, `reader_position`, `buffer_size`, `items_buffered`, `closed`.

### 2. Multi-Ring Buffer (`ThreadedZephyros[T]`)

ThreadedZephyros provides a multi-ring architecture for concurrent producers with dedicated consumers per ring.

#### Constructor

```go
func NewThreadedBuilder[T any](ringSize int64, numRings int) *ThreadedBuilder[T]
```

**Parameters:**
- `ringSize`: Size of each individual ring (power of 2).
- `numRings`: Number of parallel rings (default: `runtime.NumCPU()`).

#### Key Methods

```go
func (tz *ThreadedZephyros[T]) Write(threadID int, writerFunc func(*T)) bool
func (tz *ThreadedZephyros[T]) LoopProcess()
func (tz *ThreadedZephyros[T]) Close()
func (tz *ThreadedZephyros[T]) Stats() map[string]int64
```

**Complete ThreadedZephyros Documentation**: See [THREADED_API.md](./THREADED_API.md) for detailed API reference including Fast Path vs Safe Path methods.

**Dynamic Batching**: See [DYNAMIC_BATCHING.md](./DYNAMIC_BATCHING.md) for intelligent batch size adaptation details.

## Error Types

```go
var (
    ErrCapacity         = fmt.Errorf("capacity must be a power of two")
    ErrMissingProcessor = fmt.Errorf("missing processor function")
    ErrInvalidRingID    = errors.New("invalid ring ID")
    ErrInvalidThreadID  = errors.New("invalid thread ID")
)
```

## Performance Characteristics

### Single Ring Buffer
- **Throughput:** 44M+ ops/sec (single producer), 25M+ ops/sec (multiple producers)
- **Latency:** Sub-microsecond processing
- **Memory:** Zero allocations during operation
- **Scalability:** Optimal with single consumer

### Multi-Ring Buffer
- **Throughput:** 40M+ ops/sec (4 rings, 4 producers)
- **Latency:** Sub-microsecond per ring
- **Memory:** Zero allocations during operation
- **Scalability:** Linear with number of rings up to CPU cores

## Usage Patterns

### Basic Single Ring

```go
processor := func(item *MyStruct) {
    // Process item
}

z, err := NewBuilder[MyStruct](1024).
    WithProcessor(processor).
    WithBatchSize(128).
    Build()
if err != nil {
    return err
}

// Producer
success := z.Write(func(slot *MyStruct) {
    slot.Data = "example"
})

// Consumer
go z.LoopProcess()
defer z.Close()
```

### Multi-Ring with Fast Path

```go
tz, err := NewThreadedBuilder[MyStruct](1024, 4).
    WithProcessor(processor).
    Build()
if err != nil {
    return err
}

// Start consumers
tz.LoopProcess()
defer tz.Close()

// Producer (hot path)
ring := tz.GetWriterRing(threadID) // Panics if invalid
success := ring.Write(func(slot *MyStruct) {
    slot.Data = "high-performance"
})
```

### Multi-Ring with Safe Path

```go
// Producer (safe path)
ring, err := tz.SafeGetWriterRing(threadID)
if err != nil {
    if errors.Is(err, ErrInvalidThreadID) {
        // Handle invalid thread ID
    }
    return err
}

success := ring.Write(func(slot *MyStruct) {
    slot.Data = "error-handled"
})
```

### Dedicated Writer Pattern

```go
// Create dedicated writer
writer, err := tz.NewSafeWriterWithError(0)
if err != nil {
    return err
}

// Use dedicated writer (thread-safe)
success := writer.Write(func(slot *MyStruct) {
    slot.Data = "dedicated"
})
```

**See [THREADED_API.md](./THREADED_API.md) for complete ThreadedZephyros examples and API details.**

## Thread Safety

- **Single Ring:** MPSC (Multiple Producers, Single Consumer)
- **Multi-Ring:** Each ring is MPSC, overall system supports multiple consumers
- **SafeWriter:** Fully thread-safe for dedicated ring access
- **Close():** Safe to call from any thread, idempotent
- **Stats():** Safe to call from any thread

## Memory Management

- **Zero Allocations:** During normal operation (Write/Process cycles)
- **Pre-allocated:** All buffers allocated during Build()
- **Cache Optimized:** Padded atomic structures prevent false sharing
- **Ring Buffer:** Reuses memory slots, no garbage collection pressure

## Configuration Guidelines

### Capacity Selection
- Must be power of 2
- Recommended: 1024-65536 for most use cases
- Consider: 2x peak concurrent items + batch size

### Batch Size Selection
- Default: Intelligent based on capacity
- Small capacity (< 64): batch = 1-16
- Medium capacity (64-1024): batch = 16-64  
- Large capacity (> 1024): batch = 128-256

### Ring Count Selection
- Default: `runtime.NumCPU()`
- Optimal: Match producer thread count
- Maximum: CPU core count for best performance

## Benchmarking

Use provided benchmark suite:
- `go test -bench=. -benchmem`
- Compare against other solutions using performance tests
- Monitor with `Stats()` for production tuning

---

Zephyros • an AGILira fragment

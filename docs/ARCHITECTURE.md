
# Zephyros Architecture

## Overview

Zephyros is a high-performance, lock-free MPSC (Multiple Producer Single Consumer) ring buffer library for Go. It is designed for ultra-low latency, zero-allocation operation, and maximum throughput in concurrent environments. The architecture is modular, with a focus on cache-line padding, atomic operations, and deterministic shutdown.

## System Architecture

### Design Principles

1. **Lock-Free Concurrency**: All coordination is via atomic operations, never locks.
2. **Zero Allocation**: No allocations in the hot path; all buffers are pre-allocated.
3. **Cache-Line Padding**: All shared state is padded to 64 bytes to prevent false sharing.
4. **Deterministic Shutdown**: All buffered items are processed before shutdown, with WaitGroup-based coordination.
5. **Modular API**: Builder pattern for configuration, multi-ring support for multi-threaded producers.

## Module Structure

### Core Types (`zephyros.go`)

- `ProcessorFunc[T any] func(*T)`: User-defined processing function for each item.
- `Zephyros[T any]`: Main MPSC ring buffer structure.
- `AtomicPaddedInt64`, `PaddedInt64`: Cache-line padded atomic and non-atomic integers for concurrency and performance.

### Builder Pattern (`builder.go`)

The builder pattern (`Builder[T]`) allows fluent configuration of buffer capacity, batch size, and processor function. Validation ensures capacity is a power of two and batch size is within bounds.

### Multi-Ring MPSC (`threaded.go`)

`ThreadedZephyros[T]` manages multiple independent ring buffers, each with a dedicated consumer. This enables true multi-threaded MPSC with no CAS contention between consumers. The API provides both fast (panic on invalid ID) and safe (error return) access to rings and writers.

### Padding Utilities (`padding.go`)

Provides cache-line padded integer types to prevent false sharing and optimize CPU cache usage.

## Data Flow Architecture

```
Multiple Producer Threads
        │
        ▼
   Write() Operation (atomic, lock-free)
        │
        ▼
   Lock-free Ring Buffer(s)
        │
        ▼
Dedicated Consumer Thread(s)
        │
        ▼
   ProcessBatch() (zero-allocation, adaptive batching)
        │
        ▼
   User-defined ProcessorFunc
        │
        ▼
   Statistics Collection
```

## Concurrency Model

- **MPSC**: Multiple producers write concurrently to the buffer; a single consumer processes items in order.
- **Multi-Ring**: For true multi-threaded MPSC, each producer thread can have its own ring and consumer, managed by `ThreadedZephyros`.
- **Atomic Operations**: All buffer state is managed via atomic operations for maximum performance and safety.

## Performance Characteristics

- **Lock-Free**: No mutexes; all coordination is atomic.
- **Zero Allocation**: No allocations in the hot path; all buffers and state are pre-allocated.
- **Cache-Line Padding**: Prevents false sharing and maximizes CPU cache efficiency.
- **Adaptive Batching**: Batch size adapts to buffer occupancy for optimal throughput and latency.
- **Deterministic Shutdown**: All items are processed before shutdown; multiple `Close()` calls are safe and idempotent.

## Configuration Parameters

- **Capacity**: Must be a power of two; determines buffer size.
- **Batch Size**: Configurable; must be >0 and <= capacity.
- **Number of Rings/Workers**: For multi-threaded use, configure via `ThreadedBuilder`.

## Error Handling Strategy

- **Validation**: Builder enforces valid configuration before allocation.
- **SafeWriter**: Provides thread-safe writing to a dedicated ring; invalid IDs return errors or panic (configurable).
- **Closed Buffer**: Writes to closed buffers return false; consumers process all items before shutdown.

## Testing Architecture

Zephyros includes a comprehensive test suite:

- **Unit Tests** (`*_unit_test.go`): Validate individual methods, edge cases, and configuration logic.
- **Integration Tests** (`*_integration_test.go`): End-to-end workflow validation, including shutdown and multi-threaded operation.
- **Benchmark Tests** (`*_benchmark_test.go`): Measure throughput, latency, and memory usage under various configurations.
- **Threaded Tests** (`threaded_unit_test.go`, `threaded_close_test.go`, `threaded_safe_api_test.go`): Validate multi-ring, multi-threaded operation, deterministic shutdown, and SafeWriter API.
- **Dynamic Batching Tests** (`dynamic_batching_test.go`): Validate adaptive batching logic under variable load.
- **Padding Tests** (`padding_test.go`): Ensure cache-line alignment and false sharing prevention.
- **Diagnostic Tests** (`diagnostic_test.go`): Collect and validate runtime statistics and buffer utilization.

## Extension Points

- **Custom Processor**: Implement custom logic via `ProcessorFunc[T]`.
- **Multi-Ring**: Scale out with `ThreadedZephyros` for multi-threaded producers.
- **SafeWriter**: Use for thread-safe, dedicated writing in high-concurrency scenarios.
- **Statistics**: Integrate with monitoring systems via `Stats()`.

## Future Architecture Considerations

- **NUMA Awareness**: CPU affinity and NUMA-optimized allocation.
- **Advanced Batching**: Smarter adaptive batching strategies.
- **Distributed Processing**: Networked/distributed ring buffers.
- **Observability**: Integration with OpenTelemetry, Prometheus, etc.
- **Zero-Copy**: Further memory optimizations for ultra-low latency.

---

Zephyros is designed for maintainability, extensibility, and maximum performance in demanding concurrent workloads.

---

Zephyros • an AGILira fragment

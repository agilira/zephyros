# ThreadedZephyros API Documentation

## Overview

ThreadedZephyros provides **dual API design**:
- **Fast Path**: Maximum performance with panics on errors
- **Safe Path**: Robust error handling for critical applications

## API Methods

### Fast Path (Performance Critical)

#### `GetWriterRing(threadID int) *Zephyros[T]`
- **Use Case**: Hot paths, performance-critical code
- **Behavior**: Panics on invalid threadID
- **Performance**: Zero overhead
- **Example**:
```go
// Fast path - panics if threadID is invalid
ring := tz.GetWriterRing(threadID)
success := ring.Write(func(slot *MyStruct) {
    slot.Data = "high-performance write"
})
```

#### `NewSafeWriter(ringID int) *SafeWriter[T]`
- **Use Case**: Creating dedicated writers for performance
- **Behavior**: Panics on invalid ringID
- **Performance**: Zero overhead
- **Example**:
```go
// Fast path - panics if ringID is invalid
writer := tz.NewSafeWriter(0)
success := writer.Write(func(slot *MyStruct) {
    slot.Data = "dedicated high-performance write"
})
```

### Safe Path (Critical Applications)

#### `SafeGetWriterRing(threadID int) (*Zephyros[T], error)`
- **Use Case**: Critical applications where panics are unacceptable
- **Behavior**: Returns `ErrInvalidThreadID` on invalid threadID
- **Performance**: Minimal error handling overhead
- **Example**:
```go
// Safe path - returns error if threadID is invalid
ring, err := tz.SafeGetWriterRing(threadID)
if err != nil {
    if errors.Is(err, zephyros.ErrInvalidThreadID) {
        log.Printf("Invalid thread ID: %v", err)
        return
    }
}
success := ring.Write(func(slot *MyStruct) {
    slot.Data = "safe write with error handling"
})
```

#### `NewSafeWriterWithError(ringID int) (*SafeWriter[T], error)`
- **Use Case**: Creating dedicated writers with error handling
- **Behavior**: Returns `ErrInvalidRingID` on invalid ringID
- **Performance**: Minimal error handling overhead
- **Example**:
```go
// Safe path - returns error if ringID is invalid
writer, err := tz.NewSafeWriterWithError(0)
if err != nil {
    if errors.Is(err, zephyros.ErrInvalidRingID) {
        log.Printf("Invalid ring ID: %v", err)
        return
    }
}
success := writer.Write(func(slot *MyStruct) {
    slot.Data = "safe dedicated write with error handling"
})
```

## Error Types

```go
var (
    ErrInvalidRingID   = errors.New("invalid ring ID")
    ErrInvalidThreadID = errors.New("invalid thread ID")
)
```

## Shutdown (WaitGroup-based)

### `Close()`
- **Behavior**: Deterministic shutdown using `sync.WaitGroup`
- **Idempotent**: Safe to call multiple times
- **Guarantee**: All buffered messages are processed before shutdown
- **Performance**: Microsecond-level shutdown (vs milliseconds with time.Sleep)

```go
// Deterministic and safe shutdown
tz.Close()  // Blocks until all workers finish processing
tz.Close()  // Safe to call again (idempotent)
```

## Design Philosophy

1. **Performance First**: Fast path methods optimize for zero overhead
2. **Safety Available**: Safe path methods provide robust error handling
3. **User Choice**: Developers choose the appropriate level based on their needs
4. **Consistency**: Both paths return the same underlying objects for valid inputs

## When to Use Which API

### Use Fast Path When:
- ✅ Performance is critical (hot paths)
- ✅ Thread/Ring IDs are statically known and validated
- ✅ Application can tolerate panics for programming errors
- ✅ Zero overhead is required

### Use Safe Path When:
- ✅ Building critical infrastructure
- ✅ Thread/Ring IDs come from external input
- ✅ Panics are unacceptable
- ✅ Graceful error handling is required

## Performance Characteristics

- **Fast Path**: 0ns overhead for error checking
- **Safe Path**: ~1-2ns overhead for error return
- **Both**: Same underlying performance for valid operations
- **Shutdown**: Deterministic, microsecond-level latency

## Thread Safety

- ✅ All methods are thread-safe
- ✅ Multiple writers can use different rings simultaneously  
- ✅ Close() can be called from any thread
- ✅ Error paths are fully thread-safe

---

*Zephyros • an AGILira fragment*

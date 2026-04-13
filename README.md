# Zephyros
### Lock-free SPSC ring buffer driver for audit-grade event pipelines

An AGILira fragment.

[![CI/CD Pipeline](https://github.com/agilira/zephyros/workflows/CI/CD%20Pipeline/badge.svg)](https://github.com/agilira/zephyros/actions?query=workflow%3A%22CI%2FCD+Pipeline%22)
[![Gosec Security](https://github.com/agilira/zephyros/workflows/Gosec%20Security/badge.svg)](https://github.com/agilira/zephyros/actions?query=workflow%3A%22Gosec+Security%22)
[![Go Report Card](https://goreportcard.com/badge/github.com/agilira/zephyros?v=1)](https://goreportcard.com/report/github.com/agilira/zephyros)

## What Zephyros Is

Zephyros is a **driver**, not a library. It owns every tuning decision internally -- capacity, batch size, backoff strategy, idle sleep, EWMA smoothing. The application provides **what** to process (a function), never **how**.

This is deliberate. In audit-critical systems, exposing knobs creates misconfiguration risk. A forgotten batch size parameter or a wrong backoff cap can cause silent data loss under pressure. Zephyros eliminates that class of bugs by design.

## What Zephyros Does

- **Zero-allocation lock-free ring buffer** with cache-line padded atomics
- **Two processing modes**: per-item (`ProcessorFunc`) or batch (`BatchProcessorFunc`)
- **Automatic retry with exponential backoff** for batch failures (1ms to 1s cap)
- **3-strike poison batch protection** with quarantine callback (`OnPoisonSkip`)
- **Adaptive idle backoff** via EWMA tracking of processor speed
- **Dynamic batch sizing** based on ring occupancy
- **Graceful shutdown** with complete drain guarantee
- **Multi-producer support** via ThreadedZephyros (auto-scales to NumCPU rings)

## Installation

```bash
go get github.com/agilira/zephyros
```

## Usage

### Per-Item Processing

```go
z, err := zephyros.NewBuilder[Event](0).
    WithProcessor(func(e *Event) {
        store.Append(*e)
    }).
    Build()
if err != nil {
    log.Fatal(err)
}
defer z.Close()

go z.LoopProcess()

z.Write(func(slot *Event) { *slot = event })
```

### Batch Processing (Audit Pipeline)

```go
z, err := zephyros.NewBuilder[AuditEvent](0).
    WithBatchProcessor(func(batch []AuditEvent) error {
        return store.AppendBatch(batch)
    }).
    WithOnBatchError(func(batch []AuditEvent, err error) {
        metrics.BatchFailure(err, len(batch))
    }).
    WithOnPoisonSkip(func(batch []AuditEvent, err error) {
        quarantine.Save(batch, err) // Last chance before data loss
    }).
    Build()
```

### Multi-Producer

Pass `(0, 0)` -- Zephyros auto-sizes both ring capacity and ring count
(defaults to `runtime.NumCPU()` rings). Each producer gets a `SafeWriter`
bound to one ring. Zero contention between producers.

```go
tz, err := zephyros.NewThreadedBuilder[Event](0, 0).
    WithBatchProcessor(persistBatch).
    WithOnPoisonSkip(quarantine).
    Build()
if err != nil {
    log.Fatal(err)
}

done := tz.LoopProcess()

w := tz.NewSafeWriter(0) // Bind producer to ring 0
w.Write(func(slot *Event) { *slot = event })

tz.Close()
<-done
```

## Error Semantics (Batch Mode)

| Failure Type | Behavior | Counter |
|---|---|---|
| `return error` | Cursor NOT advanced. Same batch retried with exponential backoff. | No strike count. Retries indefinitely. |
| `panic(...)` | Recovered. Treated as error. | Strike count incremented. |
| 3rd consecutive panic | Batch permanently skipped. `OnPoisonSkip` fires. | Counter reset. |

Normal errors (SQLITE_BUSY, disk full, network timeout) are transient by nature. The ring holds events safely while the consumer retries. A panic indicates corrupt data or a logic bug that will never self-heal.

## Observability Hooks

| Hook | Fires | Purpose |
|---|---|---|
| `OnBatchError` | Every batch failure | Metrics, alerting |
| `OnPoisonSkip` | Once, at permanent skip | Quarantine for forensic analysis |
| `OnPressure` | Ring occupancy > threshold | Backpressure signaling |
| `OnStall` | No progress for duration | Dead-producer detection |

All callbacks run on the consumer goroutine. They must not block.

## Architecture

```
Single Ring:
[Producer] --> [Ring Buffer] --> [Consumer: LoopProcess]
                                      |
                                      +--> ProcessorFunc(item)
                                      +--> BatchProcessorFunc([]item)

Multi Ring (ThreadedZephyros):
[SafeWriter 0] --> [Ring 0] --\
[SafeWriter 1] --> [Ring 1] ---+--> [Unified Consumer]
[SafeWriter 2] --> [Ring 2] --/
```

## Thread Safety

- **Single ring**: one producer, one consumer. Violating the single-producer invariant causes silent data corruption.
- **ThreadedZephyros**: enforces single-producer-per-ring by construction via `SafeWriter`.

## The Name

In Greek mythology, Zephyros was the god of the west wind -- controlled power, not chaotic storms. Zephyros moves data with exactly the right force: fast under load, quiet when idle.

## License

[Mozilla Public License 2.0](./LICENSE.md)

---

Zephyros -- an AGILira fragment

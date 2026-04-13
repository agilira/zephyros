// Package zephyros is a lock-free SPSC ring buffer driver for audit-grade
// event pipelines.
//
// Zephyros is not a general-purpose data structure. It is an opinionated
// driver that owns every tuning decision internally: capacity, batch size,
// backoff strategy, and idle sleep. The application provides WHAT to process
// (a function), never HOW to process it. This design eliminates an entire
// class of misconfiguration bugs in security-critical systems.
//
// # Driver Philosophy
//
// Zephyros is a driver, not a library. The distinction matters:
//
//   - A library exposes knobs. The caller tunes them. Misconfiguration is
//     the caller's problem.
//   - A driver makes decisions. The caller provides a function and trusts
//     the driver to do the right thing. Misconfiguration is impossible.
//
// Every internal parameter (hot spin limit, gosched threshold, idle sleep
// duration, adaptive batch sizing, EWMA smoothing factor, backoff cap) is
// chosen by Zephyros and is not exposed. The only caller-visible choices
// are: what type T to buffer, and which processing function to use.
//
// # Two Processing Modes
//
// Per-item mode processes one element at a time. Use it when each event is
// independent:
//
//	z, _ := zephyros.NewBuilder[Event](0).
//	    WithProcessor(func(e *Event) {
//	        store.Append(*e)
//	    }).
//	    Build()
//	go z.LoopProcess()
//
// Batch mode processes a slice of contiguous events. Use it when the
// persistence layer benefits from batching (e.g. one SQLite transaction
// per N events instead of N transactions):
//
//	z, _ := zephyros.NewBuilder[Event](0).
//	    WithBatchProcessor(func(batch []Event) error {
//	        return store.AppendBatch(batch)
//	    }).
//	    WithOnBatchError(func(batch []Event, err error) {
//	        logger.Error("batch failed", "err", err, "size", len(batch))
//	    }).
//	    WithOnPoisonSkip(func(batch []Event, err error) {
//	        quarantine.Save(batch, err)
//	    }).
//	    Build()
//	go z.LoopProcess()
//
// Exactly one of WithProcessor or WithBatchProcessor must be set. Build
// rejects configurations with both or neither.
//
// # Error and Panic Semantics (Batch Mode)
//
// When BatchProcessorFunc returns a non-nil error, the ring cursor is NOT
// advanced. The same batch is retried with exponential backoff (1ms to 1s
// cap). Items remain safely in the ring. This handles transient failures
// (SQLITE_BUSY, network timeout, disk full).
//
// When BatchProcessorFunc panics, the panic is recovered and treated as an
// error. After 3 consecutive panics on the same batch (same cursor position),
// the batch is classified as poison and permanently skipped. Before skipping,
// the OnPoisonSkip callback fires -- this is the application's last chance to
// quarantine the batch for forensic analysis.
//
// Normal errors never increment the poison counter. A batch that fails 1000
// times with SQLITE_BUSY is retried on attempt 1001. Only panics count.
//
// # Observability Hooks
//
// Four optional callbacks provide operational visibility without polluting
// the hot path:
//
//   - OnBatchError: fires on every batch failure (error or panic). Use for
//     metrics and alerting.
//   - OnPoisonSkip: fires once when a poison batch is permanently dropped.
//     Use for quarantine / forensic preservation.
//   - OnPressure: fires when ring occupancy exceeds a threshold. Use for
//     backpressure signaling.
//   - OnStall: fires when the ring makes no progress for a duration. Use
//     for dead-producer detection.
//
// All callbacks run on the consumer goroutine. They must not block.
//
// # Multi-Producer (ThreadedZephyros)
//
// For multiple concurrent producers, ThreadedZephyros creates N independent
// rings with a unified consumer. Each producer gets a SafeWriter bound to
// one ring. Zero contention between producers.
//
//	tz, _ := zephyros.NewThreadedBuilder[Event](0, 4).
//	    WithBatchProcessor(persistBatch).
//	    Build()
//	done := tz.LoopProcess()
//	w := tz.NewSafeWriter(0)
//	w.Write(func(slot *Event) { *slot = event })
//	tz.Close()
//	<-done
//
// # Shutdown
//
// Close signals the consumer to drain all remaining items and stop. For
// single-ring Zephyros, Close returns immediately; wait for LoopProcess to
// return. For ThreadedZephyros, Close blocks until all consumers exit.
//
// # Thread Safety
//
// Single-ring Zephyros: one producer goroutine, one consumer goroutine.
// Violating the single-producer invariant causes silent data corruption.
// ThreadedZephyros enforces this by construction via SafeWriter.
//
// Copyright (c) 2025-2026 AGILira
// Series: an AGILira fragment
// SPDX-License-Identifier: MPL-2.0
package zephyros

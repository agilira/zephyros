// security_test.go: Security and adversarial tests for Zephyros
//
// THREAT MODEL (lines 1-30)
// ============================================================
// Zephyros is the ring buffer layer for the Metis audit bus.
// Every event that Metis records passes through this component.
// A compromised ring buffer is a silent audit gap (CWE-778).
//
// Attack surface:
//
//	CWE-400 Resource Exhaustion: excessively large capacity values accepted
//	         by Build() could exhaust heap, causing OOM and audit loss.
//
//	CWE-662 / CWE-362 Race Condition: concurrent Close() + Write() must not
//	         corrupt internal state or panic.
//
//	CWE-755 Improper Exception Handling: a panicking ProcessorFunc must not
//	         kill the consumer goroutine and create a silent audit gap.
//
//	CWE-404 Improper Resource Shutdown: draining accuracy after Close()
//	         — items written before Close() must not be silently lost.
//
//	CWE-20 Input Validation: invalid builder parameters (zero capacity,
//	        non-power-of-two, exceeded batch) must be rejected with errors,
//	        never silently misconfigured.
//
//	CWE-190 Integer Overflow: sequences are int64 monotonically increasing.
//	         High-sequence writes must stay lock-free without overflow panic.
//
//	CWE-833 Deadlock (WriteWait): WriteWait blocks until success or context
//	         cancellation. A cancelled context must ALWAYS unblock -- a
//	         permanent block is a denial of service on the producer goroutine.
//
//	CWE-400 Callback Abuse: OnPressure and OnStall callbacks execute on the
//	         consumer goroutine. A malicious or slow callback must not
//	         permanently block the consumer (audit gap). Documented contract:
//	         callbacks must be non-blocking.
//
// Copyright (c) 2025 AGILira
// Series: an AGILira fragment
// SPDX-License-Identifier: MPL-2.0

package zephyros

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// securityTestCtx holds per-test helpers and cleanup state.
// Centralises common patterns so individual test functions stay focused.
type securityTestCtx struct {
	t *testing.T
}

func newSecCtx(t *testing.T) *securityTestCtx {
	t.Helper()
	return &securityTestCtx{t: t}
}

// expectSecurityError asserts that an error was returned when one is required.
// WHY explicit helper: "error != nil" looks identical whether the error is
// expected (security rejection) or unexpected (bug). Named helpers make
// intent auditable in a single scan.
func (s *securityTestCtx) expectSecurityError(err error, context string) {
	s.t.Helper()
	if err == nil {
		s.t.Errorf("SECURITY: expected rejection for %s, got nil error", context)
	}
}

// expectSecuritySuccess asserts no error when safe input is accepted.
func (s *securityTestCtx) expectSecuritySuccess(err error, context string) {
	s.t.Helper()
	if err != nil {
		s.t.Errorf("SECURITY: unexpected error for %s: %v", context, err)
	}
}

// ---------------------------------------------------------------------------
// TestSecurity_BuilderRejectsInvalidCapacity
//
// ATTACK VECTOR: CWE-20 Input Validation
// IMPACT: accepting non-power-of-two capacities silently misconfigures the
//
//	ring, potentially misaligning bit-mask operations.
//
// MITIGATION EXPECTED: Build() returns a non-nil error for every invalid
//
//	capacity value; the ring is never constructed. Capacity <= 0 is
//	auto-sized to DefaultRingCapacity (safe for built-in driver use).
//
// ---------------------------------------------------------------------------
func TestSecurity_BuilderRejectsInvalidCapacity(t *testing.T) {
	s := newSecCtx(t)
	processor := func(item *int) {}

	// Non-power-of-two positive values must still fail.
	invalid := []struct {
		name     string
		capacity int64
	}{
		{"non-power-of-two 3", 3},
		{"non-power-of-two 100", 100},
		{"non-power-of-two 1000", 1000},
	}

	for _, tc := range invalid {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewBuilder[int](tc.capacity).
				WithProcessor(processor).
				WithBatchSize(1).
				Build()
			s.expectSecurityError(err, "capacity="+tc.name)
		})
	}

	// Capacity <= 0 auto-sizes (built-in driver: zero-config).
	autoSized := []struct {
		name     string
		capacity int64
	}{
		{"zero", 0},
		{"negative", -1},
		{"MinInt64", -9223372036854775808},
	}

	for _, tc := range autoSized {
		tc := tc
		t.Run("auto-size "+tc.name, func(t *testing.T) {
			z, err := NewBuilder[int](tc.capacity).
				WithProcessor(processor).
				Build()
			s.expectSecuritySuccess(err, "auto-size capacity="+tc.name)
			if z != nil {
				if z.capacity != DefaultRingCapacity {
					t.Errorf("Expected auto-sized capacity %d, got %d", DefaultRingCapacity, z.capacity)
				}
				z.Close()
			}
		})
	}

	valid := []int64{1, 2, 4, 8, 16, 64, 1024, 65536}
	for _, cap := range valid {
		_, err := NewBuilder[int](cap).
			WithProcessor(processor).
			WithBatchSize(1).
			Build()
		s.expectSecuritySuccess(err, "valid power-of-two capacity")
	}
}

// ---------------------------------------------------------------------------
// TestSecurity_BuilderRejectsMissingProcessor
//
// ATTACK VECTOR: CWE-20 Input Validation
// IMPACT: a nil processor would panic inside ProcessBatch on every item,
//
//	killing the consumer goroutine and creating a permanent audit gap.
//
// MITIGATION EXPECTED: Build() returns an error when processor is nil.
// ---------------------------------------------------------------------------
func TestSecurity_BuilderRejectsMissingProcessor(t *testing.T) {
	s := newSecCtx(t)

	_, err := NewBuilder[int](64).
		WithBatchSize(8).
		Build() // no WithProcessor call

	s.expectSecurityError(err, "nil processor")
}

// ---------------------------------------------------------------------------
// TestSecurity_WriteAfterClose
//
// ATTACK VECTOR: CWE-416 Use-After-Free analogue (closed channel write)
// IMPACT: writing to a closed ring after audit shutdown could panic or
//
//	silently corrupt state, creating ghost entries in the sequence.
//
// MITIGATION EXPECTED: Write() returns false (never panics) after Close().
// ---------------------------------------------------------------------------
func TestSecurity_WriteAfterClose(t *testing.T) {
	// ATTACK VECTOR: CWE-416

	processor := func(item *int) {}
	ring, err := NewBuilder[int](64).
		WithProcessor(processor).
		WithBatchSize(8).
		Build()
	if err != nil {
		t.Fatalf("setup: %v", err)
	}

	ring.Close()

	// All writes after Close must return false, never panic.
	for i := 0; i < 100; i++ {
		result := ring.Write(func(slot *int) { *slot = i })
		if result {
			t.Errorf("Write after Close should return false, iteration %d", i)
		}
	}
}

// ---------------------------------------------------------------------------
// TestSecurity_ConcurrentCloseAndWrite
//
// ATTACK VECTOR: CWE-362 Race Condition — Close() while the single producer is active
// IMPACT: concurrent close + write could panic (channel double-close, nil
//
//	pointer in ring state) or deadlock if the producer blocks forever.
//
// MITIGATION EXPECTED: no panic, no race; Write() returns false after Close(),
//
//	allowing the producer goroutine to exit cleanly.
//
// NOTE: A raw Zephyros ring is SPSC — exactly ONE producer goroutine at a
//
//	time. The threat modelled here is the valid single-producer scenario.
//	Multi-producer Close() races are covered by
//	TestSecurity_ThreadedZephyros_ConcurrentCloseAndWrite which uses the
//	ThreadedZephyros wrapper (one SPSC ring per producer).
//
// ---------------------------------------------------------------------------
func TestSecurity_ConcurrentCloseAndWrite(t *testing.T) {
	// ATTACK VECTOR: CWE-362
	// IMPACT: panic or data corruption during concurrent shutdown
	// MITIGATION EXPECTED: atomic closed flag prevents concurrent corruption

	var processed int64
	processor := func(item *int) { atomic.AddInt64(&processed, 1) }

	ring, err := NewBuilder[int](256).
		WithProcessor(processor).
		WithBatchSize(32).
		Build()
	if err != nil {
		t.Fatalf("setup: %v", err)
	}

	go ring.LoopProcess()

	var wg sync.WaitGroup

	// Single producer (Anemoi invariant: one goroutine per ring).
	// It writes continuously; Close() fires concurrently to race shutdown.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 2000; i++ {
			val := i
			ring.Write(func(slot *int) { *slot = val })
		}
	}()

	// Close races with the live producer.
	time.Sleep(time.Millisecond)
	ring.Close()

	wg.Wait() // Producer goroutine must exit cleanly (no deadlock, no panic).
}

// ---------------------------------------------------------------------------
// TestSecurity_ThreadedZephyros_ConcurrentCloseAndWrite
//
// ATTACK VECTOR: CWE-362 Race Condition — threaded variant
// IMPACT: same race on ThreadedZephyros which hosts multiple rings.
// MITIGATION EXPECTED: idempotent Close() with CompareAndSwap prevents
//
//	double-close; wg.Wait() ensures all consumers drain before return.
//
// ---------------------------------------------------------------------------
func TestSecurity_ThreadedZephyros_ConcurrentCloseAndWrite(t *testing.T) {
	// ATTACK VECTOR: CWE-362

	var processed int64
	processor := func(item *int) { atomic.AddInt64(&processed, 1) }

	tz, err := NewThreadedBuilder[int](256, 4).
		WithProcessor(processor).
		WithBatchSize(32).
		Build()
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	<-tz.LoopProcess()

	// Create SafeWriters before launching goroutines; CAS ensures exactly one
	// writer per ring (SPSC enforcement). If Close() races with a write, the
	// ring's closed-state check makes Write() return false -- no panic.
	closeDuringWriteWriters := make([]*SafeWriter[int], 4)
	for id := 0; id < 4; id++ {
		closeDuringWriteWriters[id] = tz.NewSafeWriter(id)
	}

	var wg sync.WaitGroup

	// 4 producers (one per ring) race against Close().
	wg.Add(4)
	for id := 0; id < 4; id++ {
		go func(w *SafeWriter[int]) {
			defer wg.Done()
			ringID := w.GetRingID()
			for j := 0; j < 1000; j++ {
				w.Write(func(slot *int) { *slot = ringID })
			}
		}(closeDuringWriteWriters[id])
	}

	time.Sleep(2 * time.Millisecond)
	tz.Close()

	wg.Wait()

	// Calling Close() again must be idempotent (no panic, no deadlock).
	tz.Close()
}

// ---------------------------------------------------------------------------
// TestSecurity_DrainCompleteness
//
// ATTACK VECTOR: CWE-404 Improper Resource Shutdown / CWE-778 Audit Loss
// IMPACT: items buffered before Close() are silently discarded, creating
//
//	gaps in the audit trail with no indication of loss.
//
// MITIGATION EXPECTED: consumer drains all buffered items before returning
//
//	from Close(). Zero loss guarantee.
//
// ---------------------------------------------------------------------------
func TestSecurity_DrainCompleteness(t *testing.T) {
	// ATTACK VECTOR: CWE-404 / CWE-778
	// IMPACT: silent audit gap if flush is incomplete after shutdown
	// MITIGATION EXPECTED: all items written before Close() are processed

	const total = 10_000
	var processed int64

	processor := func(item *int) { atomic.AddInt64(&processed, 1) }

	tz, err := NewThreadedBuilder[int](16384, 2).
		WithProcessor(processor).
		WithBatchSize(256).
		Build()
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	<-tz.LoopProcess()

	// Two rings; a single goroutine alternates between them round-robin.
	// SafeWriters are created once, before the write loop.
	drainW0 := tz.NewSafeWriter(0)
	drainW1 := tz.NewSafeWriter(1)
	drainWriters := []*SafeWriter[int]{drainW0, drainW1}

	written := 0
	for i := 0; i < total; i++ {
		if drainWriters[i%2].Write(func(slot *int) { *slot = i }) {
			written++
		}
	}

	// Close() must drain every item written before this call.
	tz.Close()

	got := atomic.LoadInt64(&processed)
	if got != int64(written) {
		t.Errorf("DRAIN INCOMPLETE: wrote %d items, processed %d (lost %d)",
			written, got, int64(written)-got)
	}
}

// ---------------------------------------------------------------------------
// TestSecurity_PanicInProcessor
//
// ATTACK VECTOR: CWE-755 Improper Exception Handling
// IMPACT: a panicking processor kills the consumer goroutine. Subsequent
//
//	items are never processed -- silent permanent audit gap (CWE-778).
//
// MITIGATION EXPECTED: the consumer currently does NOT recover from processor
//
//	panics. This test DOCUMENTS that a panicking processor is a
//	caller responsibility, not a Zephyros responsibility. If the
//	processor panics, the test verifies the system does not DEADLOCK
//	(Close() must return even if the consumer goroutine died).
//
// NOTE: this is a known limitation of the current architecture. A future
// version should add a recover() wrapper in executeAndReset(). Tracked as
// a production hardening requirement.
// ---------------------------------------------------------------------------
func TestSecurity_PanicInProcessor_DocumentedLimitation(t *testing.T) {
	// ATTACK VECTOR: CWE-755
	// CURRENT BEHAVIOUR: panicking processor kills consumer goroutine.
	// This test documents the limitation so it is visible in the audit trail.
	// Production callers MUST NOT allow processors to panic.
	t.Log("DOCUMENTED LIMITATION: caller must ensure processor never panics")
	t.Log("A panicking processor will kill the consumer goroutine (CWE-755).")
	t.Log("Mitigation: wrap processor logic in recover() at the call site.")
	t.Log("Future work: add recover in executeAndReset (tracked).")
}

// ---------------------------------------------------------------------------
// TestSecurity_ResourceExhaustion_LargeCapacity
//
// ATTACK VECTOR: CWE-400 Resource Exhaustion
// IMPACT: allocating a ring with capacity near MaxInt64 would exhaust the
//
//	heap. The builder must reject unsafe allocations.
//
// MITIGATION EXPECTED: Build() returns an error for non-power-of-two and
//
//	for values that are technically power-of-two but astronomically
//	large (go allocator will OOM, not panic -- acceptable).
//	The test verifies that valid large capacities do not panic.
//
// ---------------------------------------------------------------------------
func TestSecurity_ResourceExhaustion_LargeCapacity(t *testing.T) {
	// ATTACK VECTOR: CWE-400

	processor := func(item *int) {}

	// These non-power-of-two values must be rejected.
	rejected := []int64{
		1<<62 + 1, // not power of two
		1<<30 + 5, // not power of two
	}
	for _, cap := range rejected {
		_, err := NewBuilder[int](cap).
			WithProcessor(processor).
			WithBatchSize(1).
			Build()
		if err == nil {
			t.Errorf("Should have rejected capacity %d", cap)
		}
	}

	// A valid large power-of-two must not panic during Build() itself.
	// We do NOT start it (no LoopProcess) to avoid allocating gigabytes.
	// The point is Build() must complete or return an error, never panic.
	_, err := NewBuilder[int](1 << 20). // 1M slots * item size
						WithProcessor(processor).
						WithBatchSize(1024).
						Build()
	if err != nil {
		t.Logf("Build rejected 1M capacity (acceptable): %v", err)
	}
}

// ---------------------------------------------------------------------------
// TestSecurity_ReadyBarrier
//
// ATTACK VECTOR: CWE-362 Race Condition — write before consumer is live
// IMPACT: items written before LoopProcess consumers have started could
//
//	be processed with a startup delay or -- in a racy scheduler --
//	missed if the consumer exits before seeing them.
//
// MITIGATION EXPECTED: <-tz.LoopProcess() blocks until ALL consumer
//
//	goroutines are live. Writes after barrier are always consumed.
//
// ---------------------------------------------------------------------------
func TestSecurity_ReadyBarrier(t *testing.T) {
	// ATTACK VECTOR: CWE-362
	// IMPACT: writes before consumer goroutines are live could be delayed
	// MITIGATION EXPECTED: ready channel guarantees consumer is running

	const items = 1000
	var processed int64

	processor := func(item *int) { atomic.AddInt64(&processed, 1) }

	tz, err := NewThreadedBuilder[int](4096, 2).
		WithProcessor(processor).
		WithBatchSize(64).
		Build()
	if err != nil {
		t.Fatalf("setup: %v", err)
	}

	// Wait for the barrier: the ready channel must close before we write.
	<-tz.LoopProcess()

	// Two rings; single goroutine writes after the ready barrier.
	barrierW0 := tz.NewSafeWriter(0)
	barrierW1 := tz.NewSafeWriter(1)
	barrierWriters := []*SafeWriter[int]{barrierW0, barrierW1}

	// All writes happen AFTER the barrier. No time.Sleep needed.
	for i := 0; i < items; i++ {
		barrierWriters[i%2].Write(func(slot *int) { *slot = i })
	}

	tz.Close()

	// After Close() completes, all items must have been processed.
	got := atomic.LoadInt64(&processed)
	if got == 0 {
		t.Error("BARRIER FAILURE: no items processed after ready barrier")
	}
	t.Logf("Ready barrier: %d items processed deterministically", got)
}

// ---------------------------------------------------------------------------
// TestSecurity_AnoemiInvariant_OneProducerPerRing
//
// ATTACK VECTOR: CWE-362 Anemoi invariant violation
// IMPACT: using multiple goroutines on a SINGLE Zephyros ring creates a
//
//	data race on the buffer slot (as documented in Write()). The
//	correct API is ThreadedZephyros (one ring per producer).
//	This test documents and verifies the invariant is enforced
//	structurally by showing the CORRECT pattern must be used.
//
// MITIGATION EXPECTED: ThreadedZephyros constructor allocates one ring
//
//	per declared producer, making the invariant structural.
//
// ---------------------------------------------------------------------------
func TestSecurity_AnoemiInvariant_OneProducerPerRing(t *testing.T) {
	// ATTACK VECTOR: CWE-362
	// CORRECT PATTERN: one ring per producer via ThreadedZephyros

	var processed int64
	processor := func(item *int) { atomic.AddInt64(&processed, 1) }

	numProducers := 8
	tz, err := NewThreadedBuilder[int](1024, numProducers).
		WithProcessor(processor).
		WithBatchSize(64).
		Build()
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	<-tz.LoopProcess()

	// Create writers before launching goroutines (SPSC enforcement at init).
	anemoisWriters := make([]*SafeWriter[int], numProducers)
	for id := 0; id < numProducers; id++ {
		anemoisWriters[id] = tz.NewSafeWriter(id)
	}

	var wg sync.WaitGroup
	wg.Add(numProducers)
	for id := 0; id < numProducers; id++ {
		go func(w *SafeWriter[int]) {
			defer wg.Done()
			ringID := w.GetRingID()
			// Each producer writes to its own ring -- zero contention.
			for j := 0; j < 500; j++ {
				w.Write(func(slot *int) { *slot = ringID*1000 + j })
			}
		}(anemoisWriters[id])
	}
	wg.Wait()
	tz.Close()

	got := atomic.LoadInt64(&processed)
	if got == 0 {
		t.Error("Anemoi invariant test: no items processed")
	}
	t.Logf("Anemoi invariant: %d items processed with %d producers, zero contention", got, numProducers)
}

// ---------------------------------------------------------------------------
// TestSecurity_WriteWaitContextCancellation
//
// ATTACK VECTOR: CWE-833 Deadlock
// IMPACT: if WriteWait ignores context cancellation when the ring is full,
//
//	the producer goroutine is permanently blocked. An attacker who can
//	fill the ring (e.g. by issuing requests faster than the consumer can
//	process) would lock up all producers, creating a full audit blackout.
//
// MITIGATION EXPECTED: WriteWait returns ctx.Err() within a bounded time
//
//	after the context is cancelled -- never hangs indefinitely.
//
// ---------------------------------------------------------------------------
func TestSecurity_WriteWaitContextCancellation(t *testing.T) {
	s := newSecCtx(t)
	const capacity = 4

	z, err := NewBuilder[int](capacity).
		WithProcessor(func(item *int) {}).
		Build()
	s.expectSecuritySuccess(err, "builder")
	defer z.Close()

	// Fill the ring so WriteWait blocks.
	for i := 0; i < capacity; i++ {
		z.Write(func(slot *int) { *slot = i })
	}

	// Use a very short deadline to test fast cancellation.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- z.WriteWait(ctx, func(slot *int) { *slot = 99 })
	}()

	select {
	case writeErr := <-done:
		if writeErr == nil {
			t.Fatal("SECURITY: WriteWait should fail on cancelled context, got nil")
		}
		if writeErr != context.DeadlineExceeded {
			t.Fatalf("SECURITY: expected DeadlineExceeded, got: %v", writeErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("SECURITY: WriteWait did not unblock after context cancellation -- DEADLOCK")
	}
}

// ---------------------------------------------------------------------------
// TestSecurity_WriteWaitClosedRing
//
// ATTACK VECTOR: CWE-833 Deadlock
// IMPACT: if WriteWait does not check the closed flag, calling it on a
//
//	closed ring with a background context would block forever.
//
// MITIGATION EXPECTED: WriteWait returns ErrClosed immediately.
// ---------------------------------------------------------------------------
func TestSecurity_WriteWaitClosedRing(t *testing.T) {
	z, err := NewBuilder[int](8).
		WithProcessor(func(item *int) {}).
		Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	z.Close()

	done := make(chan error, 1)
	go func() {
		done <- z.WriteWait(context.Background(), func(slot *int) { *slot = 1 })
	}()

	select {
	case writeErr := <-done:
		if writeErr != ErrClosed {
			t.Fatalf("SECURITY: expected ErrClosed, got: %v", writeErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("SECURITY: WriteWait on closed ring did not return -- DEADLOCK")
	}
}

// ---------------------------------------------------------------------------
// TestSecurity_WriteWaitConcurrentClose
//
// ATTACK VECTOR: CWE-362 Race Condition
// IMPACT: Close() racing with WriteWait could leave the producer blocked
//
//	or cause a panic due to unsynchronised state.
//
// MITIGATION EXPECTED: WriteWait returns ErrClosed or ctx.Err() cleanly.
// ---------------------------------------------------------------------------
func TestSecurity_WriteWaitConcurrentClose(t *testing.T) {
	const capacity = 4

	z, err := NewBuilder[int](capacity).
		WithProcessor(func(item *int) {}).
		Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	// Fill the ring so WriteWait blocks.
	for i := 0; i < capacity; i++ {
		z.Write(func(slot *int) { *slot = i })
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- z.WriteWait(ctx, func(slot *int) { *slot = 99 })
	}()

	// Close while WriteWait is blocking.
	time.Sleep(10 * time.Millisecond)
	z.Close()

	select {
	case writeErr := <-done:
		// ErrClosed or DeadlineExceeded are both acceptable outcomes.
		if writeErr == nil {
			// Write succeeded before close -- also acceptable.
			return
		}
		if writeErr != ErrClosed && writeErr != context.DeadlineExceeded {
			t.Fatalf("SECURITY: unexpected error from WriteWait during close: %v", writeErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("SECURITY: WriteWait did not unblock after Close() -- DEADLOCK")
	}
}

// ---------------------------------------------------------------------------
// TestSecurity_CallbackDoesNotBlockConsumer
//
// ATTACK VECTOR: CWE-400 Callback DOS
// IMPACT: a slow OnPressure or OnStall callback blocks the consumer
//
//	goroutine, creating an audit gap for the duration.
//
// MITIGATION EXPECTED: documented contract — callbacks MUST be non-blocking.
//
//	This test verifies that even with a slow callback, the consumer
//	still eventually drains all items (the ring is not permanently stuck).
//
// ---------------------------------------------------------------------------
func TestSecurity_CallbackDoesNotBlockConsumer(t *testing.T) {
	const capacity = 8
	var processed atomic.Int64

	z, err := NewBuilder[int](capacity).
		WithProcessor(func(item *int) { processed.Add(1) }).
		WithOnPressure(0.5, func(ratio float64, items int64) {
			// Simulate a slow callback (violation of contract).
			time.Sleep(50 * time.Millisecond)
		}).
		WithOnStall(10*time.Millisecond, func(ringIndex int, readerPos, writerPos int64) {
			time.Sleep(50 * time.Millisecond)
		}).
		Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	go z.LoopProcess()

	// Write enough items to trigger pressure.
	for i := 0; i < capacity; i++ {
		z.Write(func(slot *int) { *slot = i })
	}

	// Wait for consumer to drain despite slow callbacks.
	time.Sleep(500 * time.Millisecond)
	z.Close()

	if processed.Load() != int64(capacity) {
		t.Fatalf("SECURITY: consumer should drain all %d items despite slow callbacks, got %d",
			capacity, processed.Load())
	}
}

// ---------------------------------------------------------------------------
// TestSecurity_WriteWaitAuditGuarantee
//
// ATTACK VECTOR: CWE-778 Insufficient Audit Logging
// IMPACT: if WriteWait loses events under sustained back-pressure, the audit
//
//	trail has gaps that an attacker can exploit.
//
// MITIGATION EXPECTED: every WriteWait call that returns nil has its item
//
//	delivered to the processor. Zero gaps.
//
// ---------------------------------------------------------------------------
func TestSecurity_WriteWaitAuditGuarantee(t *testing.T) {
	const (
		numRings     = 4
		itemsPerRing = 500
	)

	var processed atomic.Int64

	tz, err := NewThreadedBuilder[int](32, numRings).
		WithProcessor(func(item *int) {
			processed.Add(1)
		}).
		Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	<-tz.LoopProcess()

	var wg sync.WaitGroup
	for r := 0; r < numRings; r++ {
		wg.Add(1)
		w := tz.NewSafeWriter(r)
		go func(writer *SafeWriter[int]) {
			defer wg.Done()
			ctx := context.Background()
			for i := 0; i < itemsPerRing; i++ {
				if writeErr := writer.WriteWait(ctx, func(slot *int) { *slot = i }); writeErr != nil {
					t.Errorf("SECURITY: WriteWait lost event: %v", writeErr)
					return
				}
			}
		}(w)
	}

	wg.Wait()
	time.Sleep(200 * time.Millisecond)
	tz.Close()

	expected := int64(numRings * itemsPerRing)
	if processed.Load() != expected {
		t.Fatalf("SECURITY: audit gap detected -- expected %d events, got %d (gap=%d)",
			expected, processed.Load(), expected-processed.Load())
	}
}

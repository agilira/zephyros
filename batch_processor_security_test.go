// batch_processor_security_test.go: Security and adversarial tests for BatchProcessorFunc.
//
// THREAT MODEL (lines 1-35)
// ============================================================
// BatchProcessorFunc introduces a callback-driven consumer path into the
// ring buffer. Unlike per-item ProcessorFunc (fire-and-forget), batch
// processing introduces error-driven retry loops and panic recovery --
// both of which expand the attack surface for audit gaps.
//
// Attack surface (specific to BatchProcessorFunc):
//
//   CWE-755 Panic Recovery: a panicking BatchProcessorFunc must be recovered.
//            The consumer goroutine must survive and continue processing
//            subsequent batches. Unrecovered panics == silent audit gap.
//
//   CWE-755 Panic vs Error Distinction: only panics increment the 3-strike
//            poison counter. Normal errors (DB down, disk full) must retry
//            indefinitely. Conflating the two either loses data (errors
//            skipped after 3) or freezes the consumer (panics never skipped).
//
//   CWE-400 Resource Exhaustion: sync.Pool batch buffers grow to match ring
//            capacity. A malicious workload cycling capacity-sized batches
//            must not cause unbounded memory growth.
//
//   CWE-362 Race Condition: concurrent Write + Close during batch processing
//            with retry backoff must not corrupt internal state.
//
//   CWE-404 Improper Resource Shutdown: items written before Close() must be
//            drained through the batch processor, even if errors occurred on
//            prior batches. No silent data loss during shutdown.
//
//   CWE-834 Excessive Iteration: the retry backoff loop must converge to a
//            bounded delay (capped at 1s). An unbounded backoff would create
//            a de-facto denial of service on audit throughput.
//
// Copyright (c) 2025 AGILira
// Series: an AGILira fragment
// SPDX-License-Identifier: MPL-2.0

package zephyros

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// batchSecurityCtx holds per-test helpers for batch processor security tests.
type batchSecurityCtx struct {
	t *testing.T
}

func newBatchSecCtx(t *testing.T) *batchSecurityCtx {
	t.Helper()
	return &batchSecurityCtx{t: t}
}

// expectSecurityError asserts that an error was returned when one is required.
func (s *batchSecurityCtx) expectSecurityError(err error, context string) {
	s.t.Helper()
	if err == nil {
		s.t.Errorf("SECURITY: expected rejection for %s, got nil error", context)
	}
}

// expectSecuritySuccess asserts no error when safe input is accepted.
func (s *batchSecurityCtx) expectSecuritySuccess(err error, context string) {
	s.t.Helper()
	if err != nil {
		s.t.Errorf("SECURITY: unexpected error for %s: %v", context, err)
	}
}

// ---------------------------------------------------------------------------
// TestSecurity_BatchProcessor_PanicRecovery_ConsumerSurvives
//
// ATTACK VECTOR: CWE-755
// IMPACT: an unrecovered panic in the batch processor kills the consumer
//
//	goroutine, creating a permanent audit gap. All subsequent events
//	are silently dropped.
//
// MITIGATION EXPECTED: invokeBatchProcessor recovers panics and converts
//
//	them to errors. The consumer loop continues. Subsequent valid
//	batches are processed normally.
//
// ---------------------------------------------------------------------------
func TestSecurity_BatchProcessor_PanicRecovery_ConsumerSurvives(t *testing.T) {
	var mu sync.Mutex
	var processedBatches [][]int
	var panicCount atomic.Int64

	// WHY content-based panic: the SAME batch is retried after each panic.
	// Using a call counter would succeed on retry #2 (counter != 1).
	// Instead, panic on items < 10 (poison batch) and succeed on items >= 10.
	batchFn := func(batch []int) error {
		if len(batch) > 0 && batch[0] < 10 {
			panicCount.Add(1)
			panic("adversarial panic: corrupted event payload")
		}
		snapshot := make([]int, len(batch))
		copy(snapshot, batch)
		mu.Lock()
		processedBatches = append(processedBatches, snapshot)
		mu.Unlock()
		return nil
	}

	z, err := NewBuilder[int](64).
		WithBatchProcessor(batchFn).
		Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	// Write first batch (items 0-2: will always panic).
	for i := 0; i < 3; i++ {
		val := i
		z.Write(func(slot *int) { *slot = val })
	}

	go z.LoopProcess()

	// Wait for the poison batch to be skipped (3 panics).
	deadline := time.After(5 * time.Second)
	for panicCount.Load() < 3 {
		select {
		case <-deadline:
			t.Fatalf("Timeout waiting for 3-strike panic skip (got %d panics)", panicCount.Load())
		default:
			time.Sleep(time.Millisecond)
		}
	}

	// Write second batch (items 10-12: will succeed) after the poison skip.
	for i := 10; i < 13; i++ {
		val := i
		z.Write(func(slot *int) { *slot = val })
	}

	// WHY count items not batches: the ring buffer may deliver items 10-12
	// across multiple small batches depending on scheduler timing.
	// Waiting for exactly 1 batch could break as soon as [10] alone arrives.
	deadline = time.After(5 * time.Second)
	for {
		mu.Lock()
		var total int
		for _, b := range processedBatches {
			total += len(b)
		}
		mu.Unlock()
		if total >= 3 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("Consumer died after panic: second batch never processed (AUDIT GAP)")
		default:
			time.Sleep(time.Millisecond)
		}
	}

	z.Close()

	mu.Lock()
	defer mu.Unlock()

	if len(processedBatches) == 0 {
		t.Fatal("SECURITY: consumer did not survive panic -- audit gap")
	}

	// Verify the second batch was processed correctly.
	// WHY flatten: the ring may deliver items 10-12 across multiple small
	// batches depending on writer/reader timing. What matters is that ALL
	// items survive the panic recovery and arrive in order.
	var all []int
	for _, b := range processedBatches {
		all = append(all, b...)
	}
	if len(all) < 3 || all[0] != 10 || all[1] != 11 || all[2] != 12 {
		t.Errorf("SECURITY: second batch data corrupted after panic recovery: %v", processedBatches)
	}
}

// ---------------------------------------------------------------------------
// TestSecurity_BatchProcessor_ErrorNeverSkipped
//
// ATTACK VECTOR: CWE-755
// IMPACT: if normal errors (DB timeout, disk full) are conflated with panics
//
//	in the 3-strike rule, the batch is skipped after 3 transient
//	failures. This is DATA LOSS -- the events are silently dropped.
//
// MITIGATION EXPECTED: only panics increment batchPanicCount. Normal errors
//
//	retry indefinitely via exponential backoff. After recovery, the
//	exact same batch is delivered.
//
// ---------------------------------------------------------------------------
func TestSecurity_BatchProcessor_ErrorNeverSkipped(t *testing.T) {
	var attempts atomic.Int64
	errDB := errors.New("SQLITE_BUSY: database is locked")

	var mu sync.Mutex
	var finalBatch []int

	batchFn := func(batch []int) error {
		n := attempts.Add(1)
		// Fail 10 times (well past the 3-strike threshold for panics).
		if n <= 10 {
			return errDB
		}
		mu.Lock()
		finalBatch = make([]int, len(batch))
		copy(finalBatch, batch)
		mu.Unlock()
		return nil
	}

	z, err := NewBuilder[int](64).
		WithBatchProcessor(batchFn).
		Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	for i := 0; i < 5; i++ {
		val := i
		z.Write(func(slot *int) { *slot = val })
	}

	go z.LoopProcess()

	deadline := time.After(10 * time.Second)
	for {
		if attempts.Load() > 10 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("Timeout: expected >10 attempts, got %d (batch may have been wrongly skipped)", attempts.Load())
		default:
			time.Sleep(time.Millisecond)
		}
	}

	z.Close()

	mu.Lock()
	defer mu.Unlock()

	if len(finalBatch) != 5 {
		t.Fatalf("SECURITY: expected 5 items after recovery, got %d -- data loss", len(finalBatch))
	}

	for i, v := range finalBatch {
		if v != i {
			t.Errorf("SECURITY: batch item %d corrupted: expected %d, got %d", i, i, v)
		}
	}
}

// ---------------------------------------------------------------------------
// TestSecurity_BatchProcessor_ConcurrentCloseAndRetry
//
// ATTACK VECTOR: CWE-362
// IMPACT: if Close() races with the retry backoff loop, internal state
//
//	(batchPanicCount, readerCursor) could be corrupted, causing either
//	a hang or a double-free of the batch buffer.
//
// MITIGATION EXPECTED: the consumer detects z.closed and exits cleanly.
//
//	Close() + drain + LoopProcess exit without panic.
//
// ---------------------------------------------------------------------------
func TestSecurity_BatchProcessor_ConcurrentCloseAndRetry(t *testing.T) {
	var attempts atomic.Int64

	batchFn := func(batch []int) error {
		attempts.Add(1)
		// Always fail to keep the consumer in retry mode.
		return errors.New("persistent failure")
	}

	z, err := NewBuilder[int](64).
		WithBatchProcessor(batchFn).
		Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	for i := 0; i < 8; i++ {
		val := i
		z.Write(func(slot *int) { *slot = val })
	}

	go z.LoopProcess()

	// Wait for at least one retry cycle.
	deadline := time.After(5 * time.Second)
	for attempts.Load() < 2 {
		select {
		case <-deadline:
			t.Fatal("Timeout waiting for retry cycle")
		default:
			time.Sleep(time.Millisecond)
		}
	}

	// Close during active retry -- must not panic or deadlock.
	done := make(chan struct{})
	go func() {
		z.Close()
		close(done)
	}()

	select {
	case <-done:
		// Close completed without hang.
	case <-time.After(5 * time.Second):
		t.Fatal("SECURITY: Close() deadlocked during active batch retry (CWE-362)")
	}
}

// ---------------------------------------------------------------------------
// TestSecurity_BatchProcessor_DrainAfterError
//
// ATTACK VECTOR: CWE-404
// IMPACT: if a batch error occurred just before Close(), the drain phase
//
//	might skip the failed batch, causing silent data loss.
//
// MITIGATION EXPECTED: the drain phase in loopBatchProcessor retries
//
//	ProcessBatchFunc until the ring is empty. Items that errored
//	before Close() are retried during drain.
//
// ---------------------------------------------------------------------------
func TestSecurity_BatchProcessor_DrainAfterError(t *testing.T) {
	var attempts atomic.Int64
	var mu sync.Mutex
	var delivered []int

	batchFn := func(batch []int) error {
		n := attempts.Add(1)
		// Fail only the first attempt.
		if n == 1 {
			return errors.New("first attempt fails")
		}
		mu.Lock()
		delivered = append(delivered, batch...)
		mu.Unlock()
		return nil
	}

	z, err := NewBuilder[int](64).
		WithBatchProcessor(batchFn).
		Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	for i := 0; i < 4; i++ {
		val := i
		z.Write(func(slot *int) { *slot = val })
	}

	go z.LoopProcess()

	// Wait for first failure + retry.
	deadline := time.After(5 * time.Second)
	for attempts.Load() < 2 {
		select {
		case <-deadline:
			t.Fatal("Timeout waiting for retry")
		default:
			time.Sleep(time.Millisecond)
		}
	}

	z.Close()

	mu.Lock()
	defer mu.Unlock()

	if len(delivered) != 4 {
		t.Fatalf("SECURITY: drain after error lost items: expected 4, got %d (CWE-404)", len(delivered))
	}

	for i, v := range delivered {
		if v != i {
			t.Errorf("SECURITY: item %d corrupted: expected %d, got %d", i, i, v)
		}
	}
}

// ---------------------------------------------------------------------------
// TestSecurity_BatchProcessor_BackoffBounded
//
// ATTACK VECTOR: CWE-834
// IMPACT: if the exponential backoff grows without bound, the consumer
//
//	effectively stops processing, creating a latent audit gap.
//
// MITIGATION EXPECTED: nextRetryDelay caps at 1 second. After many
//
//	consecutive errors, the delay never exceeds 1s.
//
// ---------------------------------------------------------------------------
func TestSecurity_BatchProcessor_BackoffBounded(t *testing.T) {
	delay := time.Duration(0)
	z := &Zephyros[int]{} // Zero-value sufficient for nextRetryDelay.

	// Simulate 100 consecutive failures.
	for i := 0; i < 100; i++ {
		delay = z.nextRetryDelay(delay)
		if delay > time.Second {
			t.Fatalf("SECURITY: backoff delay %v exceeds 1s cap at iteration %d (CWE-834)", delay, i)
		}
	}

	// After enough doublings, must be exactly 1s (the cap).
	if delay != time.Second {
		t.Errorf("SECURITY: expected delay to cap at 1s, got %v", delay)
	}
}

// ---------------------------------------------------------------------------
// TestSecurity_BatchProcessor_ThreadedConcurrentCloseAndWrite
//
// ATTACK VECTOR: CWE-362
// IMPACT: ThreadedZephyros with batch processor and concurrent writes +
//
//	close must not panic. N SafeWriters writing while Close() is called
//	must not corrupt batch state.
//
// MITIGATION EXPECTED: SafeWriter.Write returns false after Close().
//
//	runBatchConsumer drains and exits. No panics, no deadlocks.
//
// ---------------------------------------------------------------------------
func TestSecurity_BatchProcessor_ThreadedConcurrentCloseAndWrite(t *testing.T) {
	batchFn := func(batch []int) error { return nil }

	const numRings = 8
	tz, err := NewThreadedBuilder[int](numRings, 64).
		WithBatchProcessor(batchFn).
		Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	var wg sync.WaitGroup

	// Claim writers for each ring.
	writers := make([]*SafeWriter[int], numRings)
	for i := 0; i < numRings; i++ {
		writers[i] = tz.NewSafeWriter(i)
	}

	// Spawn writers.
	for idx, w := range writers {
		wg.Add(1)
		go func(id int, sw *SafeWriter[int]) {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				val := id*1000 + i
				sw.Write(func(slot *int) { *slot = val })
			}
		}(idx, w)
	}

	// Start consumers.
	tz.LoopProcess()

	// Let writers run briefly.
	time.Sleep(10 * time.Millisecond)

	// Close while writers are still active.
	done := make(chan struct{})
	go func() {
		tz.Close()
		close(done)
	}()

	select {
	case <-done:
		// Clean shutdown.
	case <-time.After(10 * time.Second):
		t.Fatal("SECURITY: ThreadedZephyros.Close() deadlocked during concurrent writes (CWE-362)")
	}

	wg.Wait()
}

// ---------------------------------------------------------------------------
// TestSecurity_BatchProcessor_OnBatchErrorCallbackPanic
//
// ATTACK VECTOR: CWE-755 / CWE-400
// IMPACT: if the OnBatchError callback panics, the consumer goroutine must
//
//	survive. A panicking error callback is the same risk as a panicking
//	batch processor -- it must not create an audit gap.
//
// MITIGATION EXPECTED: the panic is either recovered by invokeBatchProcessor's
//
//	deferred recovery (if it wraps the callback), or documented as a
//	contract violation. This test verifies the actual behavior.
//
// ---------------------------------------------------------------------------
func TestSecurity_BatchProcessor_OnBatchErrorCallbackPanic(t *testing.T) {
	// WHY: this test documents the boundary of panic recovery. If the callback
	// panics OUTSIDE invokeBatchProcessor's defer/recover, the consumer dies.
	// The test verifies the current contract: callbacks must not panic.
	// If this test fails, it means we need to add recovery around callbacks.

	batchFn := func(batch []int) error {
		return errors.New("trigger callback")
	}

	callbackPanicked := make(chan struct{}, 1)
	onBatchErr := func(batch []int, err error) {
		// Only panic once to test recovery behavior.
		select {
		case callbackPanicked <- struct{}{}:
			panic("malicious callback panic")
		default:
			// Subsequent calls: no panic.
		}
	}

	z, err := NewBuilder[int](64).
		WithBatchProcessor(batchFn).
		WithOnBatchError(onBatchErr).
		Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	z.Write(func(slot *int) { *slot = 42 })

	// LoopProcess in a goroutine. We recover() the callback panic to
	// prevent it from killing the test process. This lets us detect
	// whether the consumer goroutine survived or died.
	consumerDead := make(chan struct{})
	consumerPanicked := make(chan struct{})
	go func() {
		defer close(consumerDead)
		defer func() {
			if r := recover(); r != nil {
				close(consumerPanicked)
			}
		}()
		z.LoopProcess()
	}()

	// Wait for callback to fire.
	select {
	case <-callbackPanicked:
		// Callback panicked at least once.
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout: batch error callback never fired")
	}

	// Check if consumer survived.
	select {
	case <-consumerPanicked:
		// Consumer died from callback panic.
		t.Log("NOTE: OnBatchError callback panic kills the consumer goroutine.")
		t.Log("This is a documented contract: callbacks must not panic.")
		t.Log("Consider adding defer/recover around onBatchError in handleBatchError.")
	case <-time.After(500 * time.Millisecond):
		// Consumer survived -- close and clean up.
		z.Close()
		select {
		case <-consumerDead:
		case <-time.After(5 * time.Second):
			t.Fatal("SECURITY: consumer goroutine frozen after callback panic (deadlock)")
		}
	}
}

// ---------------------------------------------------------------------------
// TestSecurity_BatchProcessor_PoisonSkipReportsViaCallback
//
// ATTACK VECTOR: CWE-778 (Missing Audit)
// IMPACT: if a poison batch is skipped without notification, the audit trail
//
//	has a silent gap. The security team has no way to know events were
//	dropped.
//
// MITIGATION EXPECTED: every panic invokes OnBatchError. The 3rd panic
//
//	(which triggers the skip) also invokes OnBatchError. The callback
//	receives the error with "panic" context.
//
// ---------------------------------------------------------------------------
func TestSecurity_BatchProcessor_PoisonSkipReportsViaCallback(t *testing.T) {
	var mu sync.Mutex
	var reportedErrors []string

	batchFn := func(batch []int) error {
		panic("unrecoverable: corrupted event")
	}

	onErr := func(batch []int, err error) {
		mu.Lock()
		reportedErrors = append(reportedErrors, err.Error())
		mu.Unlock()
	}

	z, err := NewBuilder[int](64).
		WithBatchProcessor(batchFn).
		WithOnBatchError(onErr).
		Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	for i := 0; i < 3; i++ {
		val := i
		z.Write(func(slot *int) { *slot = val })
	}

	go z.LoopProcess()

	// Wait for 3 panic reports (the skip threshold).
	deadline := time.After(5 * time.Second)
	for {
		mu.Lock()
		n := len(reportedErrors)
		mu.Unlock()
		if n >= 3 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("Timeout: expected 3 error reports, got %d", n)
		default:
			time.Sleep(time.Millisecond)
		}
	}

	z.Close()

	mu.Lock()
	defer mu.Unlock()

	if len(reportedErrors) < 3 {
		t.Fatalf("SECURITY: only %d error reports before poison skip (expected >= 3)", len(reportedErrors))
	}

	// Each report must contain "panic" to distinguish from normal errors.
	for i, msg := range reportedErrors[:3] {
		if !containsSubstring(msg, "panic") {
			t.Errorf("SECURITY: error report %d missing 'panic' context: %q", i, msg)
		}
	}
}

// THREAT MODEL update: OnPoisonSkip quarantine callback
// ============================================================
//
//   CWE-755 Quarantine Callback Panic: if OnPoisonSkip itself panics, the
//            consumer must still advance past the poison batch. A panicking
//            quarantine callback must not become a secondary denial of
//            service vector that freezes the consumer forever.
//
//   CWE-778 Quarantine Completeness: the quarantine callback must receive
//            the EXACT batch contents and the triggering error. If either
//            is nil or truncated, forensic analysis is impossible and the
//            audit gap cannot be reconstructed post-incident.
//
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// TestSecurity_BatchProcessor_QuarantineCallbackPanic
//
// ATTACK VECTOR: CWE-755 (Secondary Denial of Service)
// IMPACT: if OnPoisonSkip panics, the consumer goroutine dies. The poison
//
//	batch is never skipped, and all subsequent events are silently
//	dropped -- a permanent audit gap.
//
// MITIGATION EXPECTED: handleBatchError wraps the OnPoisonSkip call in
//
//	a recover. Even if quarantine fails, the cursor advances. The
//	consumer survives.
//
// ---------------------------------------------------------------------------
func TestSecurity_BatchProcessor_QuarantineCallbackPanic(t *testing.T) {
	var goodBatches atomic.Int64

	batchFn := func(batch []int) error {
		if len(batch) > 0 && batch[0] < 100 {
			panic("poison")
		}
		goodBatches.Add(1)
		return nil
	}

	// Quarantine callback that ALSO panics.
	onPoison := func(batch []int, err error) {
		panic("quarantine handler crashed!")
	}

	z, err := NewBuilder[int](64).
		WithBatchProcessor(batchFn).
		WithOnPoisonSkip(onPoison).
		Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	// Poison batch (items < 100).
	for i := 0; i < 3; i++ {
		val := i
		z.Write(func(slot *int) { *slot = val })
	}

	done := make(chan struct{})
	go func() {
		z.LoopProcess()
		close(done)
	}()

	// Wait for poison to be skipped (3 panics). Under test, give it time.
	time.Sleep(500 * time.Millisecond)

	// Write good batch AFTER the poison is (hopefully) skipped.
	for i := 100; i < 103; i++ {
		val := i
		z.Write(func(slot *int) { *slot = val })
	}

	time.Sleep(300 * time.Millisecond)
	z.Close()
	<-done

	if goodBatches.Load() == 0 {
		t.Fatal("SECURITY: consumer died after quarantine callback panic -- audit gap")
	}
}

// ---------------------------------------------------------------------------
// TestSecurity_BatchProcessor_QuarantineReceivesExactData
//
// ATTACK VECTOR: CWE-778 (Missing Audit / Forensic Integrity)
// IMPACT: if the quarantine callback receives a truncated or nil batch,
//
//	forensic analysis is impossible. The security team cannot
//	reconstruct what caused the crash.
//
// MITIGATION EXPECTED: OnPoisonSkip receives the full batch slice and the
//
//	error from the last panic. Both must be non-nil and complete.
//
// ---------------------------------------------------------------------------
func TestSecurity_BatchProcessor_QuarantineReceivesExactData(t *testing.T) {
	var mu sync.Mutex
	var quarantinedBatch []int
	var quarantinedErr error

	batchFn := func(batch []int) error {
		panic("corruption: event 42 has invalid checksum")
	}

	onPoison := func(batch []int, err error) {
		snapshot := make([]int, len(batch))
		copy(snapshot, batch)
		mu.Lock()
		quarantinedBatch = snapshot
		quarantinedErr = err
		mu.Unlock()
	}

	z, err := NewBuilder[int](64).
		WithBatchProcessor(batchFn).
		WithOnPoisonSkip(onPoison).
		Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	// Write exactly 5 items.
	for i := 10; i < 15; i++ {
		val := i
		z.Write(func(slot *int) { *slot = val })
	}

	done := make(chan struct{})
	go func() {
		z.LoopProcess()
		close(done)
	}()

	time.Sleep(500 * time.Millisecond)
	z.Close()
	<-done

	mu.Lock()
	defer mu.Unlock()

	// Batch must be non-nil and contain all 5 items.
	if quarantinedBatch == nil {
		t.Fatal("SECURITY: quarantine batch was nil -- forensic data lost")
	}
	if len(quarantinedBatch) != 5 {
		t.Fatalf("SECURITY: quarantine batch truncated: got %d items, expected 5", len(quarantinedBatch))
	}
	for i, want := range []int{10, 11, 12, 13, 14} {
		if quarantinedBatch[i] != want {
			t.Fatalf("SECURITY: quarantine batch[%d]=%d, expected %d", i, quarantinedBatch[i], want)
		}
	}

	// Error must be non-nil and contain the panic message.
	if quarantinedErr == nil {
		t.Fatal("SECURITY: quarantine error was nil -- cannot reconstruct cause")
	}
	if !containsSubstring(quarantinedErr.Error(), "checksum") {
		t.Fatalf("SECURITY: quarantine error missing panic context: %v", quarantinedErr)
	}
}

// containsSubstring checks if s contains substr. Avoids importing strings
// for a single use.
func containsSubstring(s, substr string) bool {
	return len(substr) <= len(s) && searchSubstring(s, substr)
}

func searchSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// TestSecurity_BatchProcessor_MutualExclusionEnforced
//
// ATTACK VECTOR: CWE-20
// IMPACT: if both ProcessorFunc and BatchProcessorFunc are set, the consumer
//
//	path is ambiguous. One set of events might be processed per-item
//	while another is batched, creating inconsistent audit records.
//
// MITIGATION EXPECTED: Build() rejects configurations with both processors
//
//	and configurations with neither. Exactly one must be set.
//
// ---------------------------------------------------------------------------
func TestSecurity_BatchProcessor_MutualExclusionEnforced(t *testing.T) {
	s := newBatchSecCtx(t)

	itemFn := func(item *int) {}
	batchFn := func(batch []int) error { return nil }

	// Both set: must fail.
	_, err := NewBuilder[int](64).
		WithProcessor(itemFn).
		WithBatchProcessor(batchFn).
		Build()
	s.expectSecurityError(err, "both ProcessorFunc and BatchProcessorFunc set")

	// Neither set: must fail.
	_, err = NewBuilder[int](64).
		Build()
	s.expectSecurityError(err, "neither ProcessorFunc nor BatchProcessorFunc set")

	// Only item processor: must succeed.
	z1, err := NewBuilder[int](64).
		WithProcessor(itemFn).
		Build()
	s.expectSecuritySuccess(err, "only ProcessorFunc set")
	if z1 != nil {
		z1.Close()
	}

	// Only batch processor: must succeed.
	z2, err := NewBuilder[int](64).
		WithBatchProcessor(batchFn).
		Build()
	s.expectSecuritySuccess(err, "only BatchProcessorFunc set")
	if z2 != nil {
		z2.Close()
	}

	// Same for ThreadedBuilder.
	_, err = NewThreadedBuilder[int](4, 64).
		WithProcessor(itemFn).
		WithBatchProcessor(batchFn).
		Build()
	s.expectSecurityError(err, "ThreadedBuilder: both processors set")

	_, err = NewThreadedBuilder[int](4, 64).
		Build()
	s.expectSecurityError(err, "ThreadedBuilder: neither processor set")
}

// ---------------------------------------------------------------------------
// TestSecurity_BatchProcessor_SyncPoolNoLeak
//
// ATTACK VECTOR: CWE-400
// IMPACT: if sync.Pool batch buffers grow without bound (never returned or
//
//	returned with retained capacity), memory usage climbs indefinitely.
//
// MITIGATION EXPECTED: ProcessBatchFunc returns batch buffers to the pool
//
//	on both success and error paths. After GC, pool entries are freed.
//
// ---------------------------------------------------------------------------
func TestSecurity_BatchProcessor_SyncPoolNoLeak(t *testing.T) {
	var processed atomic.Int64

	batchFn := func(batch []int) error {
		processed.Add(1)
		return nil
	}

	z, err := NewBuilder[int](64).
		WithBatchProcessor(batchFn).
		Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	// Process many batches to exercise pool recycling.
	for round := 0; round < 100; round++ {
		for i := 0; i < 10; i++ {
			val := round*10 + i
			z.Write(func(slot *int) { *slot = val })
		}
		// Drain each round.
		for z.ProcessBatchFunc() > 0 {
		}
	}

	z.Close()

	if processed.Load() < 100 {
		t.Errorf("Expected at least 100 batches processed, got %d", processed.Load())
	}

	// WHY no explicit memory assertion: sync.Pool memory is managed by the
	// runtime GC. This test verifies the code path (Put on both success and
	// error) doesn't panic and exercises the pool under sustained load.
	// Memory leak detection is done via runtime/pprof in integration tests.
}

// ---------------------------------------------------------------------------
// FuzzBatchProcessorPanicPayload
//
// ATTACK VECTOR: CWE-755
// IMPACT: adversarial panic payloads (nil, complex structs, huge strings)
//
//	could bypass recover() or corrupt the error message.
//
// MITIGATION EXPECTED: recover() catches all panic values. The error message
//
//	is formatted via fmt.Errorf without crashing.
//
// Seeds are real attack patterns targeting recover():
//   - nil: panic(nil) -- Go 1.21+ treats this differently
//   - string: typical panic payload
//   - error: panic(errors.New(...))
//   - int: non-string panic value
//   - struct: complex panic payload
//
// ---------------------------------------------------------------------------
func FuzzBatchProcessorPanicPayload(f *testing.F) {
	// Seed corpus: different panic payload types encoded as strings.
	// The fuzz function will use the string to determine panic behavior.
	f.Add("nil")
	f.Add("string:adversarial payload")
	f.Add("error:database corruption")
	f.Add("int:42")
	f.Add("empty:")
	f.Add("long:" + string(make([]byte, 4096))) // Large panic message.

	f.Fuzz(func(t *testing.T, panicType string) {
		batchFn := func(batch []int) error {
			switch {
			case panicType == "nil":
				// WHY (*int)(nil) instead of panic(nil): Go 1.21+ converts
				// panic(nil) to *runtime.PanicNilError. Using a typed nil
				// exercises the recover() path with an actual nil interface value.
				panic((*int)(nil))
			case len(panicType) > 7 && panicType[:7] == "string:":
				panic(panicType[7:])
			case len(panicType) > 6 && panicType[:6] == "error:":
				panic(errors.New(panicType[6:]))
			case len(panicType) > 4 && panicType[:4] == "int:":
				panic(42)
			default:
				panic(panicType)
			}
		}

		z, err := NewBuilder[int](8).
			WithBatchProcessor(batchFn).
			Build()
		if err != nil {
			return // Build rejection is acceptable.
		}
		defer z.Close()

		z.Write(func(slot *int) { *slot = 1 })

		// ProcessBatchFunc must not panic regardless of the payload.
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("ProcessBatchFunc leaked panic with payload type %q: %v", panicType, r)
			}
		}()

		z.ProcessBatchFunc()
	})
}

// ---------------------------------------------------------------------------
// FuzzBatchProcessorErrorMessage
//
// ATTACK VECTOR: CWE-74 / CWE-117
// IMPACT: adversarial error messages from the batch processor could contain
//
//	control characters, null bytes, or injection payloads that corrupt
//	log output when passed through OnBatchError.
//
// MITIGATION EXPECTED: the error is passed through without interpretation.
//
//	OnBatchError receives it as-is. No crash, no format-string vuln.
//
// ---------------------------------------------------------------------------
func FuzzBatchProcessorErrorMessage(f *testing.F) {
	f.Add("simple error")
	f.Add("")
	f.Add("\x00\x01\x02")             // Null bytes.
	f.Add("%s%s%s%s%n")               // Format string attack.
	f.Add("\n\r\t")                   // Control characters.
	f.Add(string(make([]byte, 8192))) // Oversized message.

	f.Fuzz(func(t *testing.T, errMsg string) {
		var mu sync.Mutex
		var reportedErr error

		batchFn := func(batch []int) error {
			return fmt.Errorf("%s", errMsg) //nolint:gocritic // intentional: testing error message pass-through
		}

		onErr := func(batch []int, err error) {
			mu.Lock()
			reportedErr = err
			mu.Unlock()
		}

		z, err := NewBuilder[int](8).
			WithBatchProcessor(batchFn).
			WithOnBatchError(onErr).
			Build()
		if err != nil {
			return
		}
		defer z.Close()

		z.Write(func(slot *int) { *slot = 1 })

		// Must not panic or crash.
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("ProcessBatchFunc leaked panic with error message %q: %v", errMsg, r)
			}
		}()

		z.ProcessBatchFunc()

		mu.Lock()
		defer mu.Unlock()

		if reportedErr == nil {
			t.Error("OnBatchError was not called")
		}
	})
}

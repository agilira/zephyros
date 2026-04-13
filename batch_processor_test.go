// batch_processor_test.go: TDD tests for BatchProcessorFunc
//
// Written BEFORE implementation. Every test here defines expected behavior.
// Tests will fail until the production code is written.
//
// Covers:
//   - BatchProcessorFunc receives contiguous batches (never empty)
//   - Error semantics: cursor not advanced, same batch retried
//   - Exponential backoff on retry (1ms -> capped at 1s)
//   - Panic recovery: 3-strike rule, then skip poison batch
//   - OnBatchError callback for monitoring
//   - Builder validation: exactly one of ProcessorFunc or BatchProcessorFunc
//   - Single-ring (Zephyros) and multi-ring (ThreadedZephyros) paths
//   - sync.Pool buffer for wrap-around copy
//   - Graceful shutdown drains all items via batch processor
//
// Copyright (c) 2025 AGILira
// Series: an AGLIra fragment
// SPDX-License-Identifier: MPL-2.0

package zephyros

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Builder validation: mutually exclusive processors
// ---------------------------------------------------------------------------

// TestBatchProcessor_BuilderRequiresExactlyOneProcessor verifies that Build()
// rejects configurations with neither ProcessorFunc nor BatchProcessorFunc,
// and also rejects configurations with BOTH set.
func TestBatchProcessor_BuilderRequiresExactlyOneProcessor(t *testing.T) {
	// Neither processor: must fail.
	_, err := NewBuilder[int](64).Build()
	if err == nil {
		t.Fatal("Build() should fail when neither ProcessorFunc nor BatchProcessorFunc is set")
	}

	// Both processors: must fail.
	_, err = NewBuilder[int](64).
		WithProcessor(func(item *int) {}).
		WithBatchProcessor(func(batch []int) error { return nil }).
		Build()
	if err == nil {
		t.Fatal("Build() should fail when BOTH ProcessorFunc and BatchProcessorFunc are set")
	}

	// Only BatchProcessorFunc: must succeed.
	z, err := NewBuilder[int](64).
		WithBatchProcessor(func(batch []int) error { return nil }).
		Build()
	if err != nil {
		t.Fatalf("Build() should succeed with only BatchProcessorFunc: %v", err)
	}
	z.Close()

	// Only ProcessorFunc: must succeed (backward compat).
	z2, err := NewBuilder[int](64).
		WithProcessor(func(item *int) {}).
		Build()
	if err != nil {
		t.Fatalf("Build() should succeed with only ProcessorFunc: %v", err)
	}
	z2.Close()
}

// TestBatchProcessor_ThreadedBuilderRequiresExactlyOneProcessor verifies the
// same mutual exclusion on ThreadedBuilder.
func TestBatchProcessor_ThreadedBuilderRequiresExactlyOneProcessor(t *testing.T) {
	// Neither processor: must fail.
	_, err := NewThreadedBuilder[int](64, 2).Build()
	if err == nil {
		t.Fatal("Build() should fail when neither processor is set")
	}

	// Both processors: must fail.
	_, err = NewThreadedBuilder[int](64, 2).
		WithProcessor(func(item *int) {}).
		WithBatchProcessor(func(batch []int) error { return nil }).
		Build()
	if err == nil {
		t.Fatal("Build() should fail when BOTH processors are set on ThreadedBuilder")
	}

	// Only BatchProcessorFunc: must succeed.
	tz, err := NewThreadedBuilder[int](64, 2).
		WithBatchProcessor(func(batch []int) error { return nil }).
		Build()
	if err != nil {
		t.Fatalf("ThreadedBuilder should accept BatchProcessorFunc alone: %v", err)
	}
	tz.Close()
}

// ---------------------------------------------------------------------------
// Batch correctness: items delivered in order
// ---------------------------------------------------------------------------

// TestBatchProcessor_ReceivesItemsInOrder verifies that the BatchProcessorFunc
// receives contiguous items in FIFO order and that the batch is never empty.
func TestBatchProcessor_ReceivesItemsInOrder(t *testing.T) {
	const total = 100

	var mu sync.Mutex
	var received []int

	batchFn := func(batch []int) error {
		if len(batch) == 0 {
			t.Error("INV-201: batch must never be empty")
			return nil
		}
		mu.Lock()
		received = append(received, batch...)
		mu.Unlock()
		return nil
	}

	z, err := NewBuilder[int](128).
		WithBatchProcessor(batchFn).
		Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	go z.LoopProcess()

	for i := 0; i < total; i++ {
		val := i
		ok := z.Write(func(slot *int) { *slot = val })
		if !ok {
			t.Fatalf("Write %d failed", i)
		}
	}

	// Allow consumer to process.
	time.Sleep(50 * time.Millisecond)
	z.Close()

	mu.Lock()
	defer mu.Unlock()

	if len(received) != total {
		t.Fatalf("Expected %d items, got %d", total, len(received))
	}
	for i, v := range received {
		if v != i {
			t.Fatalf("Item %d: expected %d, got %d", i, i, v)
		}
	}
}

// TestBatchProcessor_ThreadedReceivesAllItems verifies that ThreadedZephyros
// with BatchProcessorFunc delivers all items (no silent drops).
func TestBatchProcessor_ThreadedReceivesAllItems(t *testing.T) {
	const itemsPerRing = 50
	const numRings = 4

	var totalProcessed atomic.Int64

	batchFn := func(batch []int) error {
		totalProcessed.Add(int64(len(batch)))
		return nil
	}

	tz, err := NewThreadedBuilder[int](128, numRings).
		WithBatchProcessor(batchFn).
		Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	<-tz.LoopProcess()

	writers := make([]*SafeWriter[int], numRings)
	for i := 0; i < numRings; i++ {
		writers[i] = tz.NewSafeWriter(i)
	}

	for i := 0; i < numRings; i++ {
		for j := 0; j < itemsPerRing; j++ {
			val := i*1000 + j
			writers[i].Write(func(slot *int) { *slot = val })
		}
	}

	time.Sleep(50 * time.Millisecond)
	tz.Close()

	got := totalProcessed.Load()
	expected := int64(numRings * itemsPerRing)
	if got != expected {
		t.Fatalf("Expected %d items total, got %d (lost %d)", expected, got, expected-got)
	}
}

// ---------------------------------------------------------------------------
// Error semantics: retry on failure
// ---------------------------------------------------------------------------

// TestBatchProcessor_ErrorRetry verifies that when BatchProcessorFunc returns
// an error, the cursor is NOT advanced and the SAME batch is retried.
func TestBatchProcessor_ErrorRetry(t *testing.T) {
	var attempts atomic.Int64
	var mu sync.Mutex
	var finalBatch []int

	errTransient := errors.New("transient DB failure")

	batchFn := func(batch []int) error {
		n := attempts.Add(1)
		if n <= 3 {
			// First 3 attempts fail.
			return errTransient
		}
		// 4th attempt succeeds.
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

	// Write some items.
	for i := 0; i < 5; i++ {
		val := i
		z.Write(func(slot *int) { *slot = val })
	}

	go z.LoopProcess()

	// Wait for retries + success.
	deadline := time.After(5 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("Timeout waiting for batch to succeed after retries")
		default:
			if attempts.Load() >= 4 {
				z.Close()
				// Verify the same batch was retried.
				mu.Lock()
				fb := finalBatch
				mu.Unlock()
				if len(fb) == 0 {
					t.Fatal("Final batch is empty")
				}
				// Items should be the same that were written.
				for i, v := range fb {
					if v != i {
						t.Fatalf("Retry delivered wrong item at %d: expected %d, got %d", i, i, v)
					}
				}
				if attempts.Load() < 4 {
					t.Fatalf("Expected at least 4 attempts, got %d", attempts.Load())
				}
				return
			}
			time.Sleep(time.Millisecond)
		}
	}
}

// TestBatchProcessor_ErrorBackpressure verifies that when the batch processor
// returns errors continuously, the ring fills up and WriteWait blocks the
// producer (natural backpressure).
func TestBatchProcessor_ErrorBackpressure(t *testing.T) {
	const capacity = 8

	errDB := errors.New("DB down")
	var failCount atomic.Int64

	batchFn := func(batch []int) error {
		failCount.Add(1)
		return errDB
	}

	z, err := NewBuilder[int](capacity).
		WithBatchProcessor(batchFn).
		Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	go z.LoopProcess()

	// Fill the ring completely.
	for i := 0; i < capacity; i++ {
		val := i
		z.Write(func(slot *int) { *slot = val })
	}

	// WriteWait should block because ring is full and consumer is stuck retrying.
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	writeErr := z.WriteWait(ctx, func(slot *int) { *slot = 99 })
	if writeErr == nil {
		t.Fatal("WriteWait should have timed out (ring full, consumer stuck)")
	}
	if !errors.Is(writeErr, context.DeadlineExceeded) {
		t.Fatalf("Expected DeadlineExceeded, got: %v", writeErr)
	}

	z.Close()

	// Consumer must have retried at least once.
	if failCount.Load() == 0 {
		t.Fatal("BatchProcessorFunc was never called")
	}
}

// ---------------------------------------------------------------------------
// OnBatchError callback
// ---------------------------------------------------------------------------

// TestBatchProcessor_OnBatchErrorCallback verifies that OnBatchError is
// called when BatchProcessorFunc returns an error.
func TestBatchProcessor_OnBatchErrorCallback(t *testing.T) {
	errDB := errors.New("DB failure")
	var callbackCalled atomic.Int64
	var lastErrMsg atomic.Value

	batchFn := func(batch []int) error {
		return errDB
	}

	onErr := func(batch []int, err error) {
		callbackCalled.Add(1)
		lastErrMsg.Store(err.Error())
	}

	z, err := NewBuilder[int](64).
		WithBatchProcessor(batchFn).
		WithOnBatchError(onErr).
		Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	z.Write(func(slot *int) { *slot = 42 })

	go z.LoopProcess()
	time.Sleep(100 * time.Millisecond)
	z.Close()

	if callbackCalled.Load() == 0 {
		t.Fatal("OnBatchError was never called")
	}
	msg, ok := lastErrMsg.Load().(string)
	if !ok || msg != "DB failure" {
		t.Fatalf("OnBatchError received wrong error: %v", msg)
	}
}

// ---------------------------------------------------------------------------
// Panic recovery: 3-strike rule
// ---------------------------------------------------------------------------

// TestBatchProcessor_PanicRecovery_SinglePanic verifies that a single panic
// in BatchProcessorFunc is recovered and the batch is retried.
func TestBatchProcessor_PanicRecovery_SinglePanic(t *testing.T) {
	var attempts atomic.Int64
	var processed atomic.Int64

	batchFn := func(batch []int) error {
		n := attempts.Add(1)
		if n == 1 {
			panic("nil pointer in DB layer")
		}
		processed.Add(int64(len(batch)))
		return nil
	}

	var panicReported atomic.Int64
	onErr := func(batch []int, err error) {
		panicReported.Add(1)
	}

	z, err := NewBuilder[int](64).
		WithBatchProcessor(batchFn).
		WithOnBatchError(onErr).
		Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	z.Write(func(slot *int) { *slot = 1 })
	z.Write(func(slot *int) { *slot = 2 })

	go z.LoopProcess()
	time.Sleep(200 * time.Millisecond)
	z.Close()

	if processed.Load() == 0 {
		t.Fatal("Batch was never successfully processed after panic recovery")
	}
	if panicReported.Load() == 0 {
		t.Fatal("OnBatchError was not called for the panic")
	}
}

// TestBatchProcessor_PoisonBatch_3StrikeSkip verifies that a batch causing
// 3 consecutive panics is skipped (not retried forever). The skip is
// reported via OnBatchError for audit visibility.
func TestBatchProcessor_PoisonBatch_3StrikeSkip(t *testing.T) {
	var panicCount atomic.Int64
	var batchErrors atomic.Int64

	// This batch ALWAYS panics -- poison.
	batchFn := func(batch []int) error {
		panicCount.Add(1)
		panic(fmt.Sprintf("poison batch, attempt %d", panicCount.Load()))
	}

	onErr := func(batch []int, err error) {
		batchErrors.Add(1)
	}

	z, err := NewBuilder[int](64).
		WithBatchProcessor(batchFn).
		WithOnBatchError(onErr).
		Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	// Write items that will form the poison batch.
	for i := 0; i < 5; i++ {
		val := i
		z.Write(func(slot *int) { *slot = val })
	}

	go z.LoopProcess()
	time.Sleep(500 * time.Millisecond)
	z.Close()

	// After 3 panics the batch MUST be skipped.
	if panicCount.Load() < 3 {
		t.Fatalf("Expected at least 3 panic attempts, got %d", panicCount.Load())
	}
	// The consumer must NOT be stuck in an infinite loop.
	// 3 panics -> skip -> consumer moves on. No more panicCount increases
	// after the 3rd (within reason -- timing dependent, so we check <= 5).
	if panicCount.Load() > 5 {
		t.Fatalf("Poison batch not skipped: panicCount=%d (expected 3, maybe 4-5 due to timing)",
			panicCount.Load())
	}
	if batchErrors.Load() == 0 {
		t.Fatal("OnBatchError not called for poison batch skip")
	}
}

// ---------------------------------------------------------------------------
// OnPoisonSkip quarantine callback
// ---------------------------------------------------------------------------

// TestBatchProcessor_OnPoisonSkip_CalledOnce verifies the quarantine callback
// fires EXACTLY ONCE, at the moment the batch is permanently dropped. This is
// the last-chance hook for the application to preserve forensic evidence.
func TestBatchProcessor_OnPoisonSkip_CalledOnce(t *testing.T) {
	var quarantined [][]int
	var mu sync.Mutex
	var panicCount atomic.Int64

	batchFn := func(batch []int) error {
		panicCount.Add(1)
		panic("always poison")
	}

	onPoison := func(batch []int, err error) {
		snapshot := make([]int, len(batch))
		copy(snapshot, batch)
		mu.Lock()
		quarantined = append(quarantined, snapshot)
		mu.Unlock()
	}

	z, err := NewBuilder[int](64).
		WithBatchProcessor(batchFn).
		WithOnPoisonSkip(onPoison).
		Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	for i := 0; i < 3; i++ {
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

	if len(quarantined) != 1 {
		t.Fatalf("OnPoisonSkip called %d times, expected exactly 1", len(quarantined))
	}
	// WHY verify contents: the quarantine must contain the exact batch that
	// was poisoned, so the operator can reproduce the failure.
	if len(quarantined[0]) < 3 || quarantined[0][0] != 0 || quarantined[0][1] != 1 || quarantined[0][2] != 2 {
		t.Fatalf("Quarantined batch data wrong: %v", quarantined[0])
	}
}

// TestBatchProcessor_OnPoisonSkip_ReceivesError verifies the quarantine
// callback receives the panic-wrapped error, not a generic message.
func TestBatchProcessor_OnPoisonSkip_ReceivesError(t *testing.T) {
	var capturedErr error
	var mu sync.Mutex

	batchFn := func(batch []int) error {
		panic("corruption: checksum mismatch on event 42")
	}

	onPoison := func(batch []int, err error) {
		mu.Lock()
		capturedErr = err
		mu.Unlock()
	}

	z, err := NewBuilder[int](64).
		WithBatchProcessor(batchFn).
		WithOnPoisonSkip(onPoison).
		Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	z.Write(func(slot *int) { *slot = 1 })

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

	if capturedErr == nil {
		t.Fatal("OnPoisonSkip error was nil")
	}
	if !strings.Contains(capturedErr.Error(), "checksum mismatch") {
		t.Fatalf("OnPoisonSkip error missing panic message: %v", capturedErr)
	}
}

// TestBatchProcessor_OnPoisonSkip_NotCalledOnNormalError verifies the
// quarantine callback is NOT invoked for normal errors (DB timeout etc).
// Normal errors retry indefinitely -- they are never "skipped".
func TestBatchProcessor_OnPoisonSkip_NotCalledOnNormalError(t *testing.T) {
	var poisonCalls atomic.Int64
	var attempts atomic.Int64

	batchFn := func(batch []int) error {
		n := attempts.Add(1)
		if n <= 5 {
			return fmt.Errorf("SQLITE_BUSY")
		}
		return nil // Recover on 6th attempt.
	}

	onPoison := func(batch []int, err error) {
		poisonCalls.Add(1)
	}

	z, err := NewBuilder[int](64).
		WithBatchProcessor(batchFn).
		WithOnPoisonSkip(onPoison).
		Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	z.Write(func(slot *int) { *slot = 1 })

	done := make(chan struct{})
	go func() {
		z.LoopProcess()
		close(done)
	}()

	time.Sleep(500 * time.Millisecond)
	z.Close()
	<-done

	if poisonCalls.Load() != 0 {
		t.Fatalf("OnPoisonSkip called %d times for normal errors, expected 0", poisonCalls.Load())
	}
}

// TestBatchProcessor_OnPoisonSkip_NilIsOptional verifies the system works
// correctly when OnPoisonSkip is not set. This is backward compatibility:
// existing code without the callback must not break.
func TestBatchProcessor_OnPoisonSkip_NilIsOptional(t *testing.T) {
	var panicCount atomic.Int64

	batchFn := func(batch []int) error {
		panicCount.Add(1)
		panic("poison without quarantine handler")
	}

	z, err := NewBuilder[int](64).
		WithBatchProcessor(batchFn).
		// NO WithOnPoisonSkip -- must not panic or crash.
		Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	z.Write(func(slot *int) { *slot = 1 })

	done := make(chan struct{})
	go func() {
		z.LoopProcess()
		close(done)
	}()

	time.Sleep(500 * time.Millisecond)
	z.Close()
	<-done

	if panicCount.Load() < 3 {
		t.Fatalf("Expected at least 3 panics, got %d", panicCount.Load())
	}
}

// TestBatchProcessor_OnPoisonSkip_Threaded verifies quarantine works on
// ThreadedZephyros, where N rings each have their own consumer goroutine.
func TestBatchProcessor_OnPoisonSkip_Threaded(t *testing.T) {
	var quarantineCount atomic.Int64

	batchFn := func(batch []int) error {
		panic("threaded poison")
	}

	onPoison := func(batch []int, err error) {
		quarantineCount.Add(1)
	}

	tz, err := NewThreadedBuilder[int](64, 2).
		WithBatchProcessor(batchFn).
		WithOnPoisonSkip(onPoison).
		Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	// Write to both rings via SafeWriters.
	for i := 0; i < 2; i++ {
		w := tz.NewSafeWriter(i)
		val := i + 1
		w.Write(func(slot *int) { *slot = val })
	}

	done := tz.LoopProcess()
	time.Sleep(500 * time.Millisecond)
	tz.Close()
	<-done

	// Both rings should quarantine their poison batch.
	if quarantineCount.Load() < 2 {
		t.Fatalf("Expected quarantine on both rings, got %d", quarantineCount.Load())
	}
}

// ---------------------------------------------------------------------------
// Drain completeness with batch processor
// ---------------------------------------------------------------------------

// TestBatchProcessor_DrainOnClose verifies that Close() drains all buffered
// items through the BatchProcessorFunc before returning (INV-203).
func TestBatchProcessor_DrainOnClose(t *testing.T) {
	const total = 500
	var processed atomic.Int64

	batchFn := func(batch []int) error {
		processed.Add(int64(len(batch)))
		return nil
	}

	z, err := NewBuilder[int](1024).
		WithBatchProcessor(batchFn).
		Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	// Write all items BEFORE starting consumer -- they sit in the ring.
	for i := 0; i < total; i++ {
		val := i
		ok := z.Write(func(slot *int) { *slot = val })
		if !ok {
			t.Fatalf("Write %d failed", i)
		}
	}

	go z.LoopProcess()
	// Immediately close -- consumer must drain everything first.
	time.Sleep(10 * time.Millisecond)
	z.Close()

	got := processed.Load()
	if got != int64(total) {
		t.Fatalf("DRAIN INCOMPLETE: wrote %d, processed %d (lost %d)", total, got, int64(total)-got)
	}
}

// TestBatchProcessor_ThreadedDrainOnClose verifies drain completeness for
// ThreadedZephyros with BatchProcessorFunc.
func TestBatchProcessor_ThreadedDrainOnClose(t *testing.T) {
	const itemsPerRing = 200
	const numRings = 4

	var processed atomic.Int64

	batchFn := func(batch []int) error {
		processed.Add(int64(len(batch)))
		return nil
	}

	tz, err := NewThreadedBuilder[int](1024, numRings).
		WithBatchProcessor(batchFn).
		Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	<-tz.LoopProcess()

	writers := make([]*SafeWriter[int], numRings)
	for i := 0; i < numRings; i++ {
		writers[i] = tz.NewSafeWriter(i)
	}

	totalWritten := 0
	for i := 0; i < numRings; i++ {
		for j := 0; j < itemsPerRing; j++ {
			val := i*1000 + j
			if writers[i].Write(func(slot *int) { *slot = val }) {
				totalWritten++
			}
		}
	}

	tz.Close()

	got := processed.Load()
	if got != int64(totalWritten) {
		t.Fatalf("DRAIN INCOMPLETE: wrote %d, processed %d (lost %d)",
			totalWritten, got, int64(totalWritten)-got)
	}
}

// ---------------------------------------------------------------------------
// Batch never empty (INV-201)
// ---------------------------------------------------------------------------

// TestBatchProcessor_NeverEmptyBatch validates that the BatchProcessorFunc
// is never called with an empty slice.
func TestBatchProcessor_NeverEmptyBatch(t *testing.T) {
	var emptyBatchSeen atomic.Int64

	batchFn := func(batch []int) error {
		if len(batch) == 0 {
			emptyBatchSeen.Add(1)
		}
		return nil
	}

	z, err := NewBuilder[int](64).
		WithBatchProcessor(batchFn).
		Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	// Write items one at a time with pauses to create many small batches.
	go z.LoopProcess()

	for i := 0; i < 50; i++ {
		val := i
		z.Write(func(slot *int) { *slot = val })
		time.Sleep(time.Millisecond)
	}

	z.Close()

	if emptyBatchSeen.Load() > 0 {
		t.Fatal("INV-201 VIOLATION: BatchProcessorFunc received an empty batch")
	}
}

// ---------------------------------------------------------------------------
// WriteWait + BatchProcessor integration
// ---------------------------------------------------------------------------

// TestBatchProcessor_WriteWaitIntegration verifies that WriteWait works
// correctly with BatchProcessorFunc (the Metis primary write path).
func TestBatchProcessor_WriteWaitIntegration(t *testing.T) {
	const total = 100
	var processed atomic.Int64

	batchFn := func(batch []int) error {
		processed.Add(int64(len(batch)))
		return nil
	}

	z, err := NewBuilder[int](128).
		WithBatchProcessor(batchFn).
		Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	go z.LoopProcess()

	ctx := context.Background()
	for i := 0; i < total; i++ {
		val := i
		if writeErr := z.WriteWait(ctx, func(slot *int) { *slot = val }); writeErr != nil {
			t.Fatalf("WriteWait %d failed: %v", i, writeErr)
		}
	}

	time.Sleep(50 * time.Millisecond)
	z.Close()

	got := processed.Load()
	if got != int64(total) {
		t.Fatalf("Expected %d items, got %d", total, got)
	}
}

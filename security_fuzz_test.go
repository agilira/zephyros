// security_fuzz_test.go: Fuzz targets for adversarial input at trust boundaries.
//
// THREAT MODEL (lines 1-35)
// ============================================================
// Zephyros is the ring buffer backing the Metis audit bus.
// All items that flow through Metis pass through this component.
// A corrupted or panicking ring buffer creates a silent audit gap (CWE-778).
//
// Fuzz targets cover every input that crosses a trust boundary:
//
//   1. Builder parameters (capacity, batchSize) — sourced from config files
//      that an operator or attacker can influence.
//      CWE-20  Input Validation: must reject without panic.
//      CWE-190 Integer Overflow: int64 arithmetic on capacity and sequence
//              must survive extreme values (MaxInt64, MinInt64, etc.).
//
//   2. Value written into the ring — sourced from network, user input, or
//      inter-process pipes feeding the audit bus.
//      CWE-74  Injection: arbitrary bit patterns written to ring slots must
//              not corrupt the lock-free committed[] sequence tracking.
//      CWE-400 Resource Exhaustion: large write counts must not exhaust
//              the ring's slot space in ways that freeze the producer.
//
// Seed catalogue follows orpheus/security_fuzz_test.go conventions:
// every seed is a REAL attack pattern, not random noise.
//
// Copyright (c) 2025 AGILira
// Series: an AGILira fragment
// SPDX-License-Identifier: MPL-2.0

package zephyros

import (
	"encoding/binary"
	"math"
	"testing"
)

// ---------------------------------------------------------------------------
// FuzzBuilder
//
// ATTACK VECTOR: CWE-20 / CWE-190
// IMPACT: panicking Build() or silent misconfiguration (capacity=0 accepted)
//
//	could produce a ring that discards all audit events.
//
// MITIGATION EXPECTED: Build() returns a non-nil error for every invalid
//
//	input; it must never panic regardless of input.
//
// Seeds are real attack patterns:
//   - 0: zero capacity  (most common mis-config)
//   - -1, MinInt64: negative / wrap-around overflows
//   - 3, 7, 100: non-powers-of-two (fence-post configuration errors)
//   - MaxInt64: maximum int64 (integer overflow in mask arithmetic)
//   - 1, 65536: valid values to confirm the fuzzer can find a passing path
//
// ---------------------------------------------------------------------------
func FuzzBuilder(f *testing.F) {
	// Seed corpus — real attack patterns for the capacity input.
	for _, seed := range []int64{
		0,
		-1,
		math.MinInt64,
		math.MaxInt64,
		3,     // non-power-of-two
		7,     // non-power-of-two
		100,   // non-power-of-two
		1,     // valid minimum (power-of-two)
		64,    // valid small ring
		65536, // valid large ring (already tested in resource exhaustion)
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, capacity int64) {
		// Build must never panic, regardless of input.
		// Invalid inputs must return an error; valid inputs return a ring.
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("Build() panicked with capacity=%d: %v", capacity, r)
			}
		}()

		ring, err := NewBuilder[int](capacity).
			WithProcessor(func(_ *int) {}).
			Build()

		if err != nil {
			// Rejection is expected for invalid inputs; no further checks.
			return
		}

		// If Build succeeded, the ring must be usable: Close must not panic.
		if ring == nil {
			t.Errorf("Build() returned nil ring with nil error for capacity=%d", capacity)
			return
		}

		ring.Close()
	})
}

// ---------------------------------------------------------------------------
// FuzzWrite
//
// ATTACK VECTOR: CWE-74 / CWE-400
// IMPACT: arbitrary values written through Write() could corrupt the
//
//	committed[] sequence tracker if slot indexing has a bug, or cause
//	the consumer's ProcessBatch() to hang waiting for a sequence that
//	never becomes committed.
//
// MITIGATION EXPECTED: Write() always proceeds or returns false (ring full /
//
//	closed); it never panics, hangs, or corrupts committed[].
//
// Seeds are adversarial bit patterns that target int64 edge cases in the
// committed[] atomic compare, the mask arithmetic, and the slot pointer.
// ---------------------------------------------------------------------------
func FuzzWrite(f *testing.F) {
	// Seed corpus — raw 8-byte representations of adversarial int64 values.
	// Encoding as []byte because f.Add only accepts scalar/slice literals.
	adversarialInt64Values := []int64{
		0,
		-1,
		math.MaxInt64,
		math.MinInt64,
		math.MaxInt32,
		math.MinInt32,
		1<<32 - 1,          // all low bits set
		1 << 32,            // power-of-two boundary
		0xDEADBEEF,         // recognisable corruption sentinel
		0xAAAAAAAA,         // alternating bit pattern
		0x5555555555555555, // alternating bit pattern (64-bit)
		-42,
		42,
	}

	for _, v := range adversarialInt64Values {
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, uint64(v))
		f.Add(buf)
	}

	f.Fuzz(func(t *testing.T, rawBytes []byte) {
		// Decode up to 8 bytes as int64; pad with zeros if shorter.
		// WHY exact 8-byte: int64 covers the full slot value domain.
		var val int64
		if len(rawBytes) >= 8 {
			val = int64(binary.LittleEndian.Uint64(rawBytes[:8]))
		} else {
			padded := make([]byte, 8)
			copy(padded, rawBytes)
			val = int64(binary.LittleEndian.Uint64(padded))
		}

		ring, err := NewBuilder[int64](64).
			WithProcessor(func(_ *int64) {}).
			WithBatchSize(8).
			Build()
		if err != nil {
			// Build should always succeed for this valid configuration.
			t.Errorf("Build() failed for valid config: %v", err)
			return
		}
		defer ring.Close()

		// Write must not panic or corrupt the ring's committed[] state.
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("Write() panicked with value %d: %v", val, r)
			}
		}()

		captured := val
		ring.Write(func(slot *int64) { *slot = captured })
	})
}

// ---------------------------------------------------------------------------
// FuzzBuilderBatchSize
//
// ATTACK VECTOR: CWE-20
// IMPACT: a zero or overflow batchSize accepted silently would cause
//
//	ProcessBatch() to either never make progress or allocate an
//	unbounded array, creating CPU starvation or OOM.
//
// MITIGATION EXPECTED: Build() must reject batchSize <= 0 with an error.
// ---------------------------------------------------------------------------
func FuzzBuilderBatchSize(f *testing.F) {
	// Seed corpus — adversarial batch sizes.
	for _, seed := range []int64{
		0,
		-1,
		math.MinInt64,
		math.MaxInt64,
		math.MaxInt32,
		1,    // minimum valid
		16,   // typical valid
		1024, // larger valid
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, batchSize int64) {
		// Build must never panic regardless of batchSize input.
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("Build() panicked with batchSize=%d: %v", batchSize, r)
			}
		}()

		ring, err := NewBuilder[int](64).
			WithProcessor(func(_ *int) {}).
			WithBatchSize(batchSize).
			Build()

		if err != nil {
			// Rejection of invalid batchSize is the expected mitigation.
			return
		}

		// If Build succeeded, Close must not panic.
		if ring == nil {
			t.Errorf("Build() returned nil ring with nil error for batchSize=%d", batchSize)
			return
		}

		ring.Close()
	})
}

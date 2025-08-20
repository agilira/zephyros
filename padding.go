// padding.go: Cache-line padding utilities for ZEPHYROS
//
// Copyright (c) 2025 AGILira
// Series: an AGLIra fragment
// SPDX-License-Identifier: MPL-2.0

package zephyros

import (
	"sync/atomic"
)

// AtomicPaddedInt64 is an atomic 64-bit int that is padded
// to prevent false sharing on 64-byte cache lines.
type AtomicPaddedInt64 struct {
	atomic.Int64
	_ [56]byte // 64-byte total alignment (8 + 56)
}

// PaddedInt64 is a 64-bit int that is padded
// to prevent false sharing on 64-byte cache lines.
type PaddedInt64 struct {
	Value int64
	_     [56]byte // 64-byte total alignment (8 + 56)
}

// objectpool.go: Object pooling for zephyros operation pool library
//
// Copyright (c) 2025 AGILira
// Licensed under the Business Source License (BSL). Change Date: NEVER

package zephyros

import (
	"sync"
	"time"
)

// ObjectPool provides memory pooling for Operation and OperationResult objects
type ObjectPool struct {
	operations      []*Operation
	results         []*OperationResult
	mu              sync.RWMutex
	closed          bool
	maxSize         int
	minSize         int
	resizeThreshold int
	hitCount        int
	missCount       int
}

// NewObjectPool creates a new object pool with the specified size
func NewObjectPool(size int) *ObjectPool {
	if size < 4 {
		size = 4
	}
	return &ObjectPool{
		operations:      make([]*Operation, 0, size),
		results:         make([]*OperationResult, 0, size),
		maxSize:         size,
		minSize:         4,
		resizeThreshold: size * 2, // after N hits/misses, check resize
	}
}

// GetOperation retrieves an Operation from the pool or creates a new one.
func (p *ObjectPool) GetOperation() *Operation {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return &Operation{Tags: make([]string, 0, 4), Metadata: make(map[string]interface{})}
	}
	if len(p.operations) > 0 {
		op := p.operations[len(p.operations)-1]
		p.operations = p.operations[:len(p.operations)-1]
		p.hitCount++
		if p.hitCount+p.missCount >= p.resizeThreshold {
			p.autoResize()
		}
		if op == nil {
			return &Operation{Tags: make([]string, 0, 4), Metadata: make(map[string]interface{})}
		}
		op.Type = ""
		op.Key = ""
		op.Value = ""
		op.Tags = op.Tags[:0]
		op.Metadata = nil
		op.Timestamp = time.Time{}
		op.ID = ""
		return op
	}
	p.missCount++
	if p.hitCount+p.missCount >= p.resizeThreshold {
		p.autoResize()
	}
	return &Operation{Tags: make([]string, 0, 4), Metadata: make(map[string]interface{})}
}

// PutOperation returns an Operation to the pool for reuse.
func (p *ObjectPool) PutOperation(op *Operation) {
	if op == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	if len(p.operations) < p.maxSize {
		p.operations = append(p.operations, op)
	}
}

// GetResult retrieves an OperationResult from the pool or creates a new one.
func (p *ObjectPool) GetResult() *OperationResult {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return &OperationResult{Metadata: make(map[string]interface{})}
	}
	if len(p.results) > 0 {
		result := p.results[len(p.results)-1]
		p.results = p.results[:len(p.results)-1]
		p.hitCount++
		if p.hitCount+p.missCount >= p.resizeThreshold {
			p.autoResize()
		}
		if result == nil {
			return &OperationResult{Metadata: make(map[string]interface{})}
		}
		result.OperationID = ""
		result.Success = false
		result.Data = nil
		result.Error = nil
		result.Duration = 0
		result.Metadata = nil
		return result
	}
	p.missCount++
	if p.hitCount+p.missCount >= p.resizeThreshold {
		p.autoResize()
	}
	return &OperationResult{Metadata: make(map[string]interface{})}
}

// PutResult returns an OperationResult to the pool for reuse.
func (p *ObjectPool) PutResult(result *OperationResult) {
	if result == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	if len(p.results) < p.maxSize {
		p.results = append(p.results, result)
	}
}

func (p *ObjectPool) autoResize() {
	// Semplice logica: se miss > 2*hit, cresce; se hit > 2*miss e pool grande, riduce
	if p.missCount > 2*p.hitCount && p.maxSize < 4096 {
		p.maxSize *= 2
		if p.maxSize > 4096 {
			p.maxSize = 4096
		}
	} else if p.hitCount > 2*p.missCount && p.maxSize > p.minSize {
		p.maxSize /= 2
		if p.maxSize < p.minSize {
			p.maxSize = p.minSize
		}
		if len(p.operations) > p.maxSize {
			p.operations = p.operations[:p.maxSize]
		}
		if len(p.results) > p.maxSize {
			p.results = p.results[:p.maxSize]
		}
	}
	p.hitCount = 0
	p.missCount = 0
	p.resizeThreshold = p.maxSize * 2
}

// Close releases all resources used by the ObjectPool.
func (p *ObjectPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.closed {
		p.closed = true
		p.operations = nil
		p.results = nil
	}
}

// Get retrieves a value from the pool by key.
func (p *ObjectPool) Get(id string) (*Operation, error) {
	return p.GetOperation(), nil
}

// Put stores a value in the pool with the given key.
func (p *ObjectPool) Put(op *Operation) {
	p.PutOperation(op)
}

// SetResizeThreshold is for testing only: allows to set the resize threshold for auto-resize logic
func (p *ObjectPool) SetResizeThreshold(threshold int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.resizeThreshold = threshold
}

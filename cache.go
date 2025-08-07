// cache.go: Strategic caching for zephyros operation pool library
//
// Copyright (c) 2025 AGILira
// Licensed under the Business Source License (BSL). Change Date: NEVER

package zephyros

import (
	"bytes"
	"compress/gzip"
	"container/list"
	"context"
	randc "crypto/rand"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"sync"
	"time"
)

func init() {
	// Register PrimitiveBox type for robust gob encoding/decoding
	gob.Register(PrimitiveBox{})
	// Register common primitive types that will be contained in PrimitiveBox.V
	gob.Register(int(0))
	gob.Register(int32(0))
	gob.Register(int64(0))
	gob.Register(uint(0))
	gob.Register(uint32(0))
	gob.Register(uint64(0))
	gob.Register(float32(0))
	gob.Register(float64(0))
	gob.Register(bool(false))
	gob.Register(string(""))
	gob.Register([]byte{})
}

// cacheShard represents a single shard of the cache, with its own map, mutex, and LRU/LFU list
type cacheShard struct {
	data map[string]*CacheEntry
	mu   sync.RWMutex
	ll   *list.List // Doubly-linked list for LRU/LFU
}

// EvictionPolicy defines the interface for cache eviction strategies
// The policy decides which key to evict when the cache is full
type EvictionPolicy interface {
	EvictKey(cache map[string]*CacheEntry) string
}

// LFUWithTimestampPolicy evicts the least frequently used entry, breaking ties with the oldest timestamp
// (default policy)
type LFUWithTimestampPolicy struct{}

// EvictKey selects the key to evict based on LFU with timestamp policy.
func (p *LFUWithTimestampPolicy) EvictKey(cache map[string]*CacheEntry) string {
	var oldestKey string
	var oldestTime time.Time
	var lowestAccess int64
	first := true
	for key, entry := range cache {
		if first {
			oldestKey = key
			oldestTime = entry.Timestamp
			lowestAccess = entry.AccessCount
			first = false
			continue
		}
		if entry.AccessCount < lowestAccess ||
			(entry.AccessCount == lowestAccess && entry.Timestamp.Before(oldestTime)) {
			oldestKey = key
			oldestTime = entry.Timestamp
			lowestAccess = entry.AccessCount
		}
	}
	return oldestKey
}

// LRUPolicy evicts the least recently used entry
// (uses Timestamp as last access time)
type LRUPolicy struct{}

// EvictKey selects the key to evict based on LRU policy.
func (p *LRUPolicy) EvictKey(cache map[string]*CacheEntry) string {
	var lruKey string
	var lruTime time.Time
	first := true
	for key, entry := range cache {
		if first || entry.Timestamp.Before(lruTime) {
			lruKey = key
			lruTime = entry.Timestamp
			first = false
		}
	}
	return lruKey
}

// AdmissionPolicy defines the interface for cache admission strategies
// The policy decides whether a new item should be admitted to the cache
type AdmissionPolicy interface {
	Allow(key string, value interface{}) bool
}

// AlwaysAdmitPolicy always allows admission (default)
type AlwaysAdmitPolicy struct{}

// Allow always returns true, admitting all items (default policy).
func (p *AlwaysAdmitPolicy) Allow(key string, value interface{}) bool { return true }

// ProbabilisticAdmissionPolicy admits items with a given probability (0.0-1.0)
type ProbabilisticAdmissionPolicy struct {
	Probability float64
}

// Allow returns true with the configured probability for probabilistic admission.
func (p *ProbabilisticAdmissionPolicy) Allow(key string, value interface{}) bool {
	if p.Probability >= 1.0 {
		return true
	}
	if p.Probability <= 0.0 {
		return false
	}
	return SecureFloat64() < p.Probability
}

// NeverAdmitPolicy never allows admission
// Used for AdmissionProbability == 0.0
type NeverAdmitPolicy struct{}

// Allow always returns false, never admitting any item.
func (p *NeverAdmitPolicy) Allow(key string, value interface{}) bool { return false }

// SecureFloat64 returns a float64 in [0,1) using crypto/rand
func SecureFloat64() float64 {
	var b [8]byte
	_, err := randc.Read(b[:])
	if err != nil {
		// log error, return 0
		return 0.0
	}
	return float64(binary.LittleEndian.Uint64(b[:])) / (1 << 64)
}

// PrimitiveBox is used to wrap primitive values for gob encoding
// to ensure type-safe roundtrip for int, float64, string, etc.
type PrimitiveBox struct {
	V interface{}
}

// StrategicCache provides a high-performance caching mechanism with TTL and eviction policies
// Now supports sharding for reduced lock contention
type StrategicCache struct {
	config     CacheConfig
	shards     []cacheShard
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	closed     bool
	policy     EvictionPolicy
	admission  AdmissionPolicy
	shardCount int
}

// getShard returns the shard for a given key
func (sc *StrategicCache) getShard(key string) *cacheShard {
	hash := crc32.ChecksumIEEE([]byte(key))
	return &sc.shards[int(hash)%sc.shardCount]
}

// NewStrategicCache creates a new strategic cache with the given configuration
func NewStrategicCache(config CacheConfig) *StrategicCache {
	if config.CacheSize <= 0 {
		config.CacheSize = 1000
	}
	if config.TTL <= 0 {
		config.TTL = 5 * time.Minute
	}
	if config.CleanupInterval <= 0 {
		config.CleanupInterval = 1 * time.Minute
	}
	if config.MaxKeySize <= 0 {
		config.MaxKeySize = 1024
	}
	if config.MaxValueSize <= 0 {
		config.MaxValueSize = 1024 * 1024
	}
	shardCount := config.ShardCount
	if shardCount <= 0 {
		shardCount = 32
	}
	if config.CacheSize < shardCount {
		config.CacheSize = shardCount
	}
	shards := make([]cacheShard, shardCount)
	for i := 0; i < shardCount; i++ {
		shards[i] = cacheShard{data: make(map[string]*CacheEntry), ll: list.New()}
	}
	ctx, cancel := context.WithCancel(context.Background())
	var policy EvictionPolicy
	if config.EvictionPolicy == "lru" {
		policy = &LRUPolicy{}
	} else {
		policy = &LFUWithTimestampPolicy{}
	}
	var admission AdmissionPolicy
	switch {
	case config.AdmissionProbability < 0:
		admission = &AlwaysAdmitPolicy{}
	case config.AdmissionProbability == 0.0:
		admission = &NeverAdmitPolicy{}
	case config.AdmissionProbability == 1.0:
		admission = &AlwaysAdmitPolicy{}
	case config.AdmissionProbability > 0.0 && config.AdmissionProbability < 1.0:
		admission = &ProbabilisticAdmissionPolicy{Probability: config.AdmissionProbability}
	default:
		admission = &AlwaysAdmitPolicy{}
	}
	sc := &StrategicCache{
		config:     config,
		shards:     shards,
		ctx:        ctx,
		cancel:     cancel,
		policy:     policy,
		admission:  admission,
		shardCount: shardCount,
	}
	if config.EnableCaching && config.CleanupInterval > 0 {
		sc.wg.Add(shardCount)
		for i := 0; i < shardCount; i++ {
			go sc.cleanupRoutine(i)
		}
	}
	return sc
}

// cleanupRoutine runs periodic cleanup for a single shard
func (sc *StrategicCache) cleanupRoutine(shardIdx int) {
	defer sc.wg.Done()
	ticker := time.NewTicker(sc.config.CleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			sc.cleanupExpired(shardIdx)
		case <-sc.ctx.Done():
			return
		}
	}
}

// cleanupExpired removes expired entries from a single shard
func (sc *StrategicCache) cleanupExpired(shardIdx int) {
	shard := &sc.shards[shardIdx]
	shard.mu.Lock()
	now := time.Now()
	for e := shard.ll.Back(); e != nil; {
		prev := e.Prev()
		ce := e.Value.(*CacheEntry)
		if now.Sub(ce.Timestamp) > sc.config.TTL {
			shard.ll.Remove(e)
			delete(shard.data, ce.Key)
		}
		e = prev
	}
	shard.mu.Unlock()
}

// Get retrieves a value from the cache. If compression is enabled, it will decompress and decode with gob for non-string values.
func (sc *StrategicCache) Get(key string) (interface{}, bool) {
	if !sc.config.EnableCaching {
		return nil, false
	}
	if len(key) > sc.config.MaxKeySize {
		return nil, false
	}
	shard := sc.getShard(key)
	shard.mu.Lock()
	entry, exists := shard.data[key]
	if !exists {
		shard.mu.Unlock()
		return nil, false
	}
	if time.Since(entry.Timestamp) > sc.config.TTL {
		shard.ll.Remove(entry.llElem)
		delete(shard.data, key)
		shard.mu.Unlock()
		return nil, false
	}
	entry.AccessCount++
	// Move to front for LRU
	if _, ok := sc.policy.(*LRUPolicy); ok {
		shard.ll.MoveToFront(entry.llElem)
		entry.Timestamp = time.Now()
	}
	shard.mu.Unlock()

	// Unify decompression/decoding logic
	if compressed, ok := entry.Value.([]byte); ok {
		var payload []byte
		var err error
		var header string

		if sc.config.EnableCompression {
			header, payload, err = decompressGzipWithHeader(compressed)
		} else {
			// If not compressed, but it's a byte slice, assume it's gob-encoded without header
			// or a raw byte slice.
			payload = compressed
			header = "RAW_BYTES_OR_GOB:" // A placeholder header for internal logic
		}

		if err == nil {
			switch header {
			case "STR:":
				return string(payload), true
			case "PRIM:", "GOB:", "RAW_BYTES_OR_GOB:": // Handle gob-encoded or raw bytes that might be gob
				var out PrimitiveBox // Always decode into PrimitiveBox
				dec := gob.NewDecoder(bytes.NewReader(payload))
				if err := dec.Decode(&out); err == nil {
					return out.V, true // Return the unwrapped value
				}
				// If gob decoding fails, and it's not a known header,
				// it might be raw bytes that were not gob-encoded (e.g., if Set stored []byte directly).
				// In this case, return the raw payload.
				fmt.Printf("DEBUG: gob.Decode failed for key '%s', error: %v, payload length: %d\n", key, err, len(payload))
				return payload, true
			default:
				// If unknown header but it's a byte slice, return as is.
				return payload, true
			}
		}
		return nil, false
	}
	return entry.Value, true // Return directly if not a byte slice (original non-string/non-byte value)
}

// Set stores a value in the cache with the given key
func (sc *StrategicCache) Set(key string, value interface{}) bool {
	if !sc.config.EnableCaching {
		return false
	}
	if len(key) > sc.config.MaxKeySize {
		return false
	}
	if sc.admission != nil && !sc.admission.Allow(key, value) {
		return false
	}

	var storeValue interface{}
	var valueBytes []byte // Intermediate for gob-encoded or original string/bytes
	var isString bool = false

	if str, ok := value.(string); ok {
		valueBytes = []byte(str)
		isString = true
	} else if _, ok := value.([]byte); ok {
		// If it's a raw byte slice, we still gob-encode it to handle its type consistently
		// and calculate size accurately through gob.
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		if err := enc.Encode(PrimitiveBox{V: value}); err != nil {
			fmt.Printf("GOB encoding failed for key '%s', value type %T, error: %v\n", key, value, err) // Debug print
			return false
		}
		valueBytes = buf.Bytes()
	} else { // Handle all other interface{} types (primitives, structs etc.)
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		if err := enc.Encode(PrimitiveBox{V: value}); err != nil {
			fmt.Printf("GOB encoding failed for key '%s', value type %T, error: %v\n", key, value, err) // Debug print
			return false
		}
		valueBytes = buf.Bytes()
	}

	valueSize := len(valueBytes)

	if sc.config.EnableCompression {
		var header string
		if isString {
			header = "STR:"
		} else { // Everything else is now gob-encoded (original []byte or other interface{})
			header = "GOB:"
		}

		compressed, err := compressGzipWithHeader(valueBytes, header)
		if err != nil {
			return false
		}
		storeValue = compressed
		valueSize = len(compressed)
	} else {
		if isString {
			storeValue = string(valueBytes) // Store as original string if not compressing
		} else {
			storeValue = valueBytes // Store as gob-encoded bytes if not compressing (includes original []byte values)
		}
	}

	if valueSize > sc.config.MaxValueSize {
		return false
	}

	shard := sc.getShard(key)
	shard.mu.Lock()
	if entry, exists := shard.data[key]; exists {
		entry.Value = storeValue
		entry.Timestamp = time.Now()
		entry.Size = valueSize
		shard.ll.MoveToFront(entry.llElem)
		shard.mu.Unlock()
		return true
	}
	minPerShard := sc.config.CacheSize / sc.shardCount
	if minPerShard < 1 {
		minPerShard = 1
	}
	if len(shard.data) >= minPerShard {
		var evictElem *list.Element
		if sc.policy != nil {
			if _, ok := sc.policy.(*LRUPolicy); ok {
				evictElem = shard.ll.Back()
			} else if _, ok := sc.policy.(*LFUWithTimestampPolicy); ok {
				minElem := shard.ll.Back()
				minAccess := int64(1<<63 - 1)
				for e := shard.ll.Back(); e != nil; e = e.Prev() {
					ce := e.Value.(*CacheEntry)
					if ce.AccessCount < minAccess {
						minAccess = ce.AccessCount
						minElem = e
					}
				}
				evictElem = minElem
			}
		}
		if evictElem != nil {
			ce := evictElem.Value.(*CacheEntry)
			delete(shard.data, ce.Key)
			shard.ll.Remove(evictElem)
		}
	}
	entry := &CacheEntry{
		Key:         key,
		Value:       storeValue,
		Timestamp:   time.Now(),
		AccessCount: 0,
		Size:        valueSize,
	}
	entry.llElem = shard.ll.PushFront(entry)
	shard.data[key] = entry
	shard.mu.Unlock()
	return true
}

// Delete removes a value from the cache by key
func (sc *StrategicCache) Delete(key string) {
	shard := sc.getShard(key)
	shard.mu.Lock()
	if entry, exists := shard.data[key]; exists {
		shard.ll.Remove(entry.llElem)
		delete(shard.data, key)
	}
	shard.mu.Unlock()
}

// Clear removes all entries from the cache
func (sc *StrategicCache) Clear() {
	for i := 0; i < sc.shardCount; i++ {
		shard := &sc.shards[i]
		shard.mu.Lock()
		shard.data = make(map[string]*CacheEntry)
		shard.ll.Init()
		shard.mu.Unlock()
	}
}

// CacheStats contains statistics about the cache performance
type CacheStats struct {
	Size               int
	MaxSize            int
	Enabled            bool
	TotalSizeBytes     int
	TotalAccessCount   int64
	AverageAccessCount int64
}

// GetStats returns the current cache statistics
func (sc *StrategicCache) GetStats() CacheStats {
	totalSize := 0
	totalAccess := int64(0)
	totalEntries := 0
	for i := 0; i < sc.shardCount; i++ {
		shard := &sc.shards[i]
		shard.mu.RLock()
		for _, entry := range shard.data {
			totalSize += entry.Size
			totalAccess += entry.AccessCount
			totalEntries++
		}
		shard.mu.RUnlock()
	}
	stats := CacheStats{
		Size:             totalEntries,
		MaxSize:          sc.config.CacheSize,
		Enabled:          sc.config.EnableCaching,
		TotalSizeBytes:   totalSize,
		TotalAccessCount: totalAccess,
	}
	if totalEntries > 0 {
		stats.AverageAccessCount = totalAccess / int64(totalEntries)
	}
	return stats
}

// Close closes the cache and stops the cleanup goroutines
func (sc *StrategicCache) Close() {
	if sc.closed {
		return
	}
	sc.closed = true
	sc.cancel()
	done := make(chan struct{})
	go func() {
		sc.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
	}
	sc.Clear()
}

// Compression helpers
func compressGzipWithHeader(data []byte, header string) ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteString(header)
	w := gzip.NewWriter(&buf)
	_, err := w.Write(data)
	if err != nil {
		if closeErr := w.Close(); closeErr != nil {
			return nil, fmt.Errorf("write error: %v, close error: %v", err, closeErr)
		}
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decompressGzipWithHeader(data []byte) (header string, payload []byte, err error) {
	if len(data) < 4 {
		return "", nil, fmt.Errorf("data too short for header")
	}
	header = string(data[:4])
	r, err := gzip.NewReader(bytes.NewReader(data[4:]))
	if err != nil {
		return header, nil, err
	}
	defer r.Close()
	out, err := ioutil.ReadAll(r)
	if err != nil {
		return header, nil, err
	}
	return header, out, nil
}

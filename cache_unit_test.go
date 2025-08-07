// cache_unit_test.go: Unit tests for zephyros strategic caching
//
// Copyright (c) 2025 AGILira
// Licensed under the Business Source License (BSL). Change Date: NEVER

package zephyros

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func init() {
	// Register all primitive types used in tests for gob serialization
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
	gob.Register(PrimitiveBox{})
}

func TestStrategicCache_BasicOperations(t *testing.T) {
	config := CacheConfig{
		EnableCaching:        true,
		CacheSize:            10,
		TTL:                  1 * time.Minute,
		CleanupInterval:      30 * time.Second,
		MaxKeySize:           100,
		MaxValueSize:         1000,
		EnableCompression:    false,
		AdmissionProbability: -1,
	}

	cache := NewStrategicCache(config)
	defer cache.Close()

	// Test Set and Get
	key := "test_key"
	value := "test_value"

	success := cache.Set(key, value)
	if !success {
		t.Error("Set() should succeed for valid key/value")
	}

	retrieved, exists := cache.Get(key)
	if !exists {
		t.Error("Get() should find cached value")
	}
	if retrieved != value {
		t.Errorf("Expected value %v, got %v", value, retrieved)
	}

	// Test non-existent key
	_, exists = cache.Get("non_existent")
	if exists {
		t.Error("Get() should return false for non-existent key")
	}
}

func TestStrategicCache_TTLExpiration(t *testing.T) {
	config := CacheConfig{
		EnableCaching:        true,
		CacheSize:            10,
		TTL:                  50 * time.Millisecond, // Short TTL for testing
		CleanupInterval:      10 * time.Millisecond,
		MaxKeySize:           100,
		MaxValueSize:         1000,
		EnableCompression:    false,
		AdmissionProbability: -1,
	}

	cache := NewStrategicCache(config)
	defer cache.Close()

	// Set value
	key := "test_key"
	value := "test_value"
	cache.Set(key, value)

	// Should exist immediately
	_, exists := cache.Get(key)
	if !exists {
		t.Error("Value should exist immediately after Set")
	}

	// Wait for TTL to expire
	time.Sleep(100 * time.Millisecond)

	// Should not exist after TTL
	_, exists = cache.Get(key)
	if exists {
		t.Error("Value should not exist after TTL expiration")
	}
}

func TestStrategicCache_SizeLimits(t *testing.T) {
	config := CacheConfig{
		EnableCaching:        true,
		CacheSize:            2, // Small cache size
		TTL:                  1 * time.Minute,
		CleanupInterval:      30 * time.Second,
		MaxKeySize:           10,  // Small key size
		MaxValueSize:         100, // Small value size
		EnableCompression:    false,
		AdmissionProbability: -1,
		ShardCount:           1, // For deterministic eviction in tests
	}

	cache := NewStrategicCache(config)
	defer cache.Close()

	// Test key size limit
	largeKey := strings.Repeat("a", 20) // Larger than MaxKeySize
	success := cache.Set(largeKey, "value")
	if success {
		t.Error("Set() should fail for key too large")
	}

	// Test value size limit
	largeValue := strings.Repeat("b", 200) // Larger than MaxValueSize
	success = cache.Set("key", largeValue)
	if success {
		t.Error("Set() should fail for value too large")
	}

	// Test cache size limit
	cache.Set("key1", "value1")
	cache.Set("key2", "value2")
	cache.Set("key3", "value3") // Should evict oldest

	// Check that cache size is maintained
	stats := cache.GetStats()
	if stats.Size > config.CacheSize {
		t.Errorf("Cache size %d exceeds limit %d", stats.Size, config.CacheSize)
	}
}

func TestStrategicCache_EvictionPolicy(t *testing.T) {
	config := CacheConfig{
		EnableCaching:        true,
		CacheSize:            2, // Small cache size
		TTL:                  1 * time.Minute,
		CleanupInterval:      30 * time.Second,
		MaxKeySize:           100,
		MaxValueSize:         1000,
		EnableCompression:    false,
		AdmissionProbability: -1,
		ShardCount:           1, // For deterministic eviction in tests
	}

	cache := NewStrategicCache(config)
	defer cache.Close()

	// Add two items
	cache.Set("key1", "value1")
	cache.Set("key2", "value2")

	// Access key1 multiple times
	cache.Get("key1")
	cache.Get("key1")
	cache.Get("key1")

	// Add third item (should evict key2 which has lower access count)
	cache.Set("key3", "value3")

	// key2 should be evicted
	_, exists := cache.Get("key2")
	if exists {
		t.Error("key2 should be evicted due to lower access count")
	}

	// key1 and key3 should still exist
	_, exists = cache.Get("key1")
	if !exists {
		t.Error("key1 should still exist")
	}
	_, exists = cache.Get("key3")
	if !exists {
		t.Error("key3 should still exist")
	}
}

func TestStrategicCache_EvictionPolicy_LRU(t *testing.T) {
	cfg := CacheConfig{
		EnableCaching:        true,
		CacheSize:            2,
		EvictionPolicy:       "lru",
		AdmissionProbability: -1,
		ShardCount:           1, // For deterministic eviction in tests
	}
	sc := NewStrategicCache(cfg)
	sc.Set("a", "1")
	time.Sleep(10 * time.Millisecond) // ensure timestamp difference
	sc.Set("b", "2")
	time.Sleep(10 * time.Millisecond) // ensure timestamp difference
	// Access "a" to make "b" the least recently used
	_, _ = sc.Get("a")
	time.Sleep(10 * time.Millisecond) // ensure timestamp difference
	sc.Set("c", "3")                  // should evict "b"
	if _, ok := sc.Get("b"); ok {
		t.Error("expected 'b' to be evicted by LRU policy")
	}
	if _, ok := sc.Get("a"); !ok {
		t.Error("expected 'a' to remain in cache")
	}
	if _, ok := sc.Get("c"); !ok {
		t.Error("expected 'c' to be in cache")
	}
	sc.Close()
}

func TestStrategicCache_EvictionPolicy_LFU(t *testing.T) {
	cfg := CacheConfig{
		EnableCaching:        true,
		CacheSize:            2,
		EvictionPolicy:       "lfu",
		AdmissionProbability: -1,
		ShardCount:           1, // For deterministic eviction in tests
	}
	sc := NewStrategicCache(cfg)
	sc.Set("a", "1")
	sc.Set("b", "2")
	// Access "a" twice, "b" once
	_, _ = sc.Get("a")
	_, _ = sc.Get("a")
	_, _ = sc.Get("b")
	sc.Set("c", "3") // should evict "b" (lowest access count)
	if _, ok := sc.Get("b"); ok {
		t.Error("expected 'b' to be evicted by LFU policy")
	}
	if _, ok := sc.Get("a"); !ok {
		t.Error("expected 'a' to remain in cache")
	}
	if _, ok := sc.Get("c"); !ok {
		t.Error("expected 'c' to be in cache")
	}
	sc.Close()
}

func TestStrategicCache_Compression(t *testing.T) {
	cfg := CacheConfig{
		EnableCaching:        true,
		CacheSize:            2,
		EnableCompression:    true,
		AdmissionProbability: -1,
	}
	sc := NewStrategicCache(cfg)
	value := "this is a long string that should compress well"
	sc.Set("key", value)
	got, ok := sc.Get("key")
	if !ok {
		t.Fatal("expected to get value from cache")
	}
	if got != value {
		t.Errorf("expected decompressed value '%s', got '%v'", value, got)
	}
	sc.Close()
}

func TestStrategicCache_Compression_EdgeCases(t *testing.T) {
	gob.Register(PrimitiveBox{})
	cfg := CacheConfig{
		EnableCaching:        true,
		CacheSize:            1,
		EnableCompression:    true,
		AdmissionProbability: -1,
	}
	sc := NewStrategicCache(cfg)
	// Store and retrieve empty string
	sc.Set("empty", "")
	got, ok := sc.Get("empty")
	if !ok || got != "" {
		t.Errorf("expected empty string, got '%v'", got)
	}
	// Store and retrieve non-string (should not compress)
	sc.Set("int", 42)
	got, ok = sc.Get("int")
	if !ok {
		t.Errorf("expected to get value, got !ok")
	}
	gotInt := 0
	switch v := got.(type) {
	case int:
		gotInt = v
	case string:
		var err error
		gotInt, err = strconv.Atoi(v)
		if err != nil {
			t.Errorf("expected int 42, got string '%v' (type %T)", got, got)
		}
	default:
		t.Errorf("expected int 42, got '%v' (type %T)", got, got)
	}
	if gotInt != 42 {
		t.Errorf("expected int 42, got '%v' (type %T)", got, got)
	}
	sc.Close()
}

func TestStrategicCache_DeleteAndClear(t *testing.T) {
	config := CacheConfig{
		EnableCaching:        true,
		CacheSize:            10,
		TTL:                  1 * time.Minute,
		CleanupInterval:      30 * time.Second,
		MaxKeySize:           100,
		MaxValueSize:         1000,
		EnableCompression:    false,
		AdmissionProbability: -1,
	}

	cache := NewStrategicCache(config)
	defer cache.Close()

	// Add items
	cache.Set("key1", "value1")
	cache.Set("key2", "value2")

	// Delete specific key
	cache.Delete("key1")
	_, exists := cache.Get("key1")
	if exists {
		t.Error("key1 should not exist after Delete")
	}

	// key2 should still exist
	_, exists = cache.Get("key2")
	if !exists {
		t.Error("key2 should still exist")
	}

	// Clear all
	cache.Clear()
	_, exists = cache.Get("key2")
	if exists {
		t.Error("key2 should not exist after Clear")
	}
}

func TestStrategicCache_Disabled(t *testing.T) {
	config := CacheConfig{
		EnableCaching:        false, // Disabled
		CacheSize:            10,
		TTL:                  1 * time.Minute,
		CleanupInterval:      30 * time.Second,
		MaxKeySize:           100,
		MaxValueSize:         1000,
		EnableCompression:    false,
		AdmissionProbability: -1,
	}

	cache := NewStrategicCache(config)
	defer cache.Close()

	// Set should fail when disabled
	success := cache.Set("key", "value")
	if success {
		t.Error("Set() should fail when caching is disabled")
	}

	// Get should return false when disabled
	_, exists := cache.Get("key")
	if exists {
		t.Error("Get() should return false when caching is disabled")
	}
}

func TestStrategicCache_Stats(t *testing.T) {
	config := CacheConfig{
		EnableCaching:        true,
		CacheSize:            10,
		TTL:                  1 * time.Minute,
		CleanupInterval:      30 * time.Second,
		MaxKeySize:           100,
		MaxValueSize:         1000,
		EnableCompression:    false,
		AdmissionProbability: -1,
		ShardCount:           1, // Fix: Add ShardCount to prevent CacheSize from being adjusted
	}

	cache := NewStrategicCache(config)
	defer cache.Close()

	// Add items and access them
	cache.Set("key1", "value1")
	cache.Set("key2", "value2")
	cache.Get("key1") // Access once
	cache.Get("key1") // Access twice
	cache.Get("key2") // Access once

	stats := cache.GetStats()

	if stats.Size != 2 {
		t.Errorf("Expected cache size 2, got %d", stats.Size)
	}
	if stats.MaxSize != config.CacheSize {
		t.Errorf("Expected max size %d, got %d", config.CacheSize, stats.MaxSize)
	}
	if !stats.Enabled {
		t.Error("Cache should be enabled")
	}
	if stats.TotalAccessCount != 3 {
		t.Errorf("Expected total access count 3, got %d", stats.TotalAccessCount)
	}
}

func TestOperationPool_WithCaching(t *testing.T) {
	config := PoolConfig{
		WorkerCount: 2,
		QueueSize:   10,
		CacheConfig: CacheConfig{
			EnableCaching:        true,
			CacheSize:            100,
			TTL:                  1 * time.Minute,
			CleanupInterval:      30 * time.Second,
			MaxKeySize:           100,
			MaxValueSize:         1000,
			EnableCompression:    false,
			AdmissionProbability: -1,
		},
	}

	handler := &mockHandler{
		processFunc: func(ctx context.Context, op Operation) (OperationResult, error) {
			return OperationResult{
				OperationID: op.ID,
				Success:     true,
				Data:        op.Value,
				Duration:    time.Millisecond,
			}, nil
		},
	}

	pool, err := NewOperationPool(config, handler)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}
	defer pool.Close()

	// Test cache stats
	stats := pool.GetCacheStats()
	if !stats.Enabled {
		t.Error("Cache should be enabled")
	}
}

func TestOperationPool_WithoutCaching(t *testing.T) {
	config := PoolConfig{
		WorkerCount: 2,
		QueueSize:   10,
		CacheConfig: CacheConfig{
			EnableCaching:        false, // Disabled
			AdmissionProbability: -1,
		},
	}

	handler := &mockHandler{
		processFunc: func(ctx context.Context, op Operation) (OperationResult, error) {
			return OperationResult{
				OperationID: op.ID,
				Success:     true,
				Data:        op.Value,
				Duration:    time.Millisecond,
			}, nil
		},
	}

	pool, err := NewOperationPool(config, handler)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}
	defer pool.Close()

	// Test cache stats when disabled
	stats := pool.GetCacheStats()
	if stats.Enabled {
		t.Error("Cache should be disabled")
	}
}

func TestStrategicCache_Close_Idempotente(t *testing.T) {
	cache := NewStrategicCache(CacheConfig{EnableCaching: true})
	cache.Close()
	cache.Close() // should not panic
}

func TestStrategicCache_ClearAndDelete_Repeat(t *testing.T) {
	cache := NewStrategicCache(CacheConfig{EnableCaching: true})
	cache.Set("key", "value")
	cache.Delete("key")
	cache.Delete("key") // repeat, should not panic
	cache.Clear()
	cache.Clear() // repeat, should not panic
	cache.Close()
}

func TestStrategicCache_EmptyKeyAndNilValue(t *testing.T) {
	cache := NewStrategicCache(CacheConfig{EnableCaching: true})
	cache.Set("", nil) // should not panic, allowed by implementation
	_, _ = cache.Get("")
	cache.Close()
}

func TestStrategicCache_ConcurrentAccess(t *testing.T) {
	cache := NewStrategicCache(CacheConfig{EnableCaching: true, CacheSize: 50})
	done := make(chan struct{})
	go func() {
		for i := 0; i < 100; i++ {
			cache.Set("k", i)
			cache.Get("k")
			cache.Delete("k")
		}
		done <- struct{}{}
	}()
	for i := 0; i < 100; i++ {
		cache.Set("k2", i)
		cache.Get("k2")
		cache.Delete("k2")
	}
	<-done
	cache.Close()
}

func TestStrategicCache_CleanupGoroutine(t *testing.T) {
	cache := NewStrategicCache(CacheConfig{EnableCaching: true, CleanupInterval: 10 * time.Millisecond})
	cache.Set("key", "value")
	time.Sleep(20 * time.Millisecond)
	cache.Close() // should wait for cleanup goroutine
}

func TestStrategicCache_AdmissionPolicy_ProbabilityZero(t *testing.T) {
	config := CacheConfig{
		EnableCaching:        true,
		CacheSize:            10,
		TTL:                  1 * time.Minute,
		CleanupInterval:      30 * time.Second,
		MaxKeySize:           100,
		MaxValueSize:         1000,
		AdmissionProbability: 0.0,
	}
	cache := NewStrategicCache(config)
	defer cache.Close()
	admitted := 0
	for i := 0; i < 100; i++ {
		if cache.Set("k"+strconv.Itoa(i), "v") {
			admitted++
		}
	}
	if admitted != 0 {
		t.Errorf("Expected 0 admitted items, got %d", admitted)
	}
}

func TestStrategicCache_AdmissionPolicy_ProbabilityOne(t *testing.T) {
	config := CacheConfig{
		EnableCaching:        true,
		CacheSize:            10,
		TTL:                  1 * time.Minute,
		CleanupInterval:      30 * time.Second,
		MaxKeySize:           100,
		MaxValueSize:         1000,
		AdmissionProbability: 1.0,
	}
	cache := NewStrategicCache(config)
	defer cache.Close()
	admitted := 0
	for i := 0; i < 10; i++ {
		if cache.Set("k"+strconv.Itoa(i), "v") {
			admitted++
		}
	}
	if admitted != 10 {
		t.Errorf("Expected 10 admitted items, got %d", admitted)
	}
}

func TestStrategicCache_AdmissionPolicy_ProbabilityIntermediate(t *testing.T) {
	config := CacheConfig{
		EnableCaching:        true,
		CacheSize:            100,
		TTL:                  1 * time.Minute,
		CleanupInterval:      30 * time.Second,
		MaxKeySize:           100,
		MaxValueSize:         1000,
		AdmissionProbability: 0.3,
	}
	cache := NewStrategicCache(config)
	defer cache.Close()
	admitted := 0
	N := 1000
	for i := 0; i < N; i++ {
		if cache.Set("k"+strconv.Itoa(i), "v") {
			admitted++
		}
	}
	// Allow a margin of error due to randomness
	if admitted < 200 || admitted > 400 {
		t.Errorf("Expected admitted items around 300, got %d", admitted)
	}
}

func TestAdmissionPolicies(t *testing.T) {
	never := &NeverAdmitPolicy{}
	always := &AlwaysAdmitPolicy{}
	if never.Allow("k", "v") {
		t.Error("NeverAdmitPolicy should never admit")
	}
	if !always.Allow("k", "v") {
		t.Error("AlwaysAdmitPolicy should always admit")
	}
}

func TestCompressDecompressGzip_CorruptedData(t *testing.T) {
	// Decompression of corrupted data should return error, not panic
	_, _, err := decompressGzipWithHeader([]byte{0x00, 0x01, 0x02})
	if err == nil {
		t.Error("Expected error for corrupted gzip data")
	}
}

func TestCompressGzip_NonString(t *testing.T) {
	cache := NewStrategicCache(CacheConfig{
		EnableCaching:        true,
		EnableCompression:    false, // Disable compression for this test
		MaxValueSize:         2048,
		CacheSize:            100,
		ShardCount:           1,  // Single shard for deterministic behavior
		CleanupInterval:      0,  // No cleanup goroutine
		AdmissionProbability: -1, // Always admit for testing
	})
	defer cache.Close()

	// Test storing and retrieving integer values
	testValue := 12345
	ok := cache.Set("int", testValue)
	if !ok {
		t.Fatalf("Set failed for key 'int'")
	}

	v, ok := cache.Get("int")
	if !ok {
		t.Fatalf("Expected to get value, got !ok")
	}
	if v != testValue {
		t.Fatalf("Expected int %d, got '%v' (type %T)", testValue, v, v)
	}

	// Test additional primitive types to ensure robustness
	testCases := map[string]interface{}{
		"float32": float32(3.14),
		"bool":    true,
		"string":  "test string",
	}

	for key, value := range testCases {
		if !cache.Set(key, value) {
			t.Fatalf("Set failed for key '%s'", key)
		}

		got, ok := cache.Get(key)
		if !ok {
			t.Fatalf("Get failed for key '%s'", key)
		}
		if got != value {
			t.Fatalf("Expected %v for key '%s', got '%v' (type %T)", value, key, got, got)
		}
	}
}

func TestStrategicCache_PrimitiveTypes_Robustness(t *testing.T) {
	// Define comprehensive test cases for all primitive types
	testCases := map[string]interface{}{
		"int":       42,
		"int32":     int32(12345),
		"int64":     int64(9876543210),
		"uint":      uint(99),
		"uint32":    uint32(123456),
		"uint64":    uint64(9876543210),
		"float32":   float32(3.14),
		"float64":   float64(2.718281828),
		"boolTrue":  true,
		"boolFalse": false,
		"string":    "hello world",
	}

	// Test both with and without compression
	for _, compression := range []bool{false, true} {
		t.Run(fmt.Sprintf("compression_%v", compression), func(t *testing.T) {
			// Use larger cache size to avoid eviction during test
			cache := NewStrategicCache(CacheConfig{
				EnableCaching:        true,
				EnableCompression:    compression,
				CacheSize:            100, // Increased to accommodate all test cases
				ShardCount:           1,   // Single shard for deterministic behavior
				MaxValueSize:         4096,
				AdmissionProbability: -1, // Always admit for testing
			})
			defer cache.Close()

			// Store all values first
			for key, expectedValue := range testCases {
				if !cache.Set(key, expectedValue) {
					t.Fatalf("Set failed for key '%s' (compression=%v)", key, compression)
				}
			}

			// Then retrieve and validate all values
			for key, expectedValue := range testCases {
				got, ok := cache.Get(key)
				if !ok {
					t.Fatalf("Get failed for key '%s' (compression=%v)", key, compression)
				}

				// Validate the retrieved value based on type
				validatePrimitiveValue(t, key, expectedValue, got, compression)
			}
		})
	}
}

// validatePrimitiveValue performs type-aware validation of cached values
func validatePrimitiveValue(t *testing.T, key string, expected, got interface{}, compression bool) {
	switch orig := expected.(type) {
	case int, int32, int64, uint, uint32, uint64:
		validateIntegerValue(t, key, orig, got, compression)
	case float32, float64:
		validateFloatValue(t, key, orig, got, compression)
	case bool:
		validateBoolValue(t, key, orig, got, compression)
	case string:
		validateStringValue(t, key, orig, got, compression)
	default:
		t.Fatalf("Unknown type for key '%s': %T (compression=%v)", key, orig, compression)
	}
}

func validateIntegerValue(t *testing.T, key string, expected, got interface{}, compression bool) {
	// Convert expected to int64 for comparison
	var want int64
	switch x := expected.(type) {
	case int:
		want = int64(x)
	case int32:
		want = int64(x)
	case int64:
		want = x
	case uint:
		want = int64(x)
	case uint32:
		want = int64(x)
	case uint64:
		want = int64(x)
	}

	// Convert got to int64 for comparison
	var gotInt int64
	switch val := got.(type) {
	case int:
		gotInt = int64(val)
	case int32:
		gotInt = int64(val)
	case int64:
		gotInt = val
	case uint:
		gotInt = int64(val)
	case uint32:
		gotInt = int64(val)
	case uint64:
		gotInt = int64(val)
	case string:
		var err error
		gotInt, err = strconv.ParseInt(val, 10, 64)
		if err != nil {
			t.Fatalf("Expected integer for key '%s', got string '%v' (compression=%v)", key, val, compression)
		}
	default:
		t.Fatalf("Expected integer for key '%s', got '%v' (type %T, compression=%v)", key, got, got, compression)
	}

	if gotInt != want {
		t.Fatalf("Value mismatch for key '%s': want %d, got %d (compression=%v)", key, want, gotInt, compression)
	}
}

func validateFloatValue(t *testing.T, key string, expected, got interface{}, compression bool) {
	// Convert expected to float64 for comparison
	var want float64
	switch x := expected.(type) {
	case float32:
		want = float64(x)
	case float64:
		want = x
	}

	// Convert got to float64 for comparison
	var gotF float64
	switch val := got.(type) {
	case float32:
		gotF = float64(val)
	case float64:
		gotF = val
	case string:
		var err error
		gotF, err = strconv.ParseFloat(val, 64)
		if err != nil {
			t.Fatalf("Expected float for key '%s', got string '%v' (compression=%v)", key, val, compression)
		}
	default:
		t.Fatalf("Expected float for key '%s', got '%v' (type %T, compression=%v)", key, got, got, compression)
	}

	// Use small epsilon for float comparison
	const epsilon = 1e-6
	if math.Abs(gotF-want) > epsilon {
		t.Fatalf("Value mismatch for key '%s': want %f, got %f (compression=%v)", key, want, gotF, compression)
	}
}

func validateBoolValue(t *testing.T, key string, expected, got interface{}, compression bool) {
	wantB := expected.(bool)
	var gotB bool
	switch val := got.(type) {
	case bool:
		gotB = val
	case string:
		var err error
		gotB, err = strconv.ParseBool(val)
		if err != nil {
			t.Fatalf("Expected bool for key '%s', got string '%v' (compression=%v)", key, val, compression)
		}
	default:
		t.Fatalf("Expected bool for key '%s', got '%v' (type %T, compression=%v)", key, got, got, compression)
	}

	if gotB != wantB {
		t.Fatalf("Value mismatch for key '%s': want %v, got %v (compression=%v)", key, wantB, gotB, compression)
	}
}

func validateStringValue(t *testing.T, key string, expected, got interface{}, compression bool) {
	wantS := expected.(string)
	if got != wantS {
		t.Fatalf("Value mismatch for key '%s': want '%v', got '%v' (compression=%v)", key, wantS, got, compression)
	}
}

func TestStrategicCache_ShardPerformance(t *testing.T) {
	// Test performance comparison between 2 shards and 32 shards
	shardConfigs := []struct {
		name       string
		shardCount int
	}{
		{"2_shards", 2},
		{"32_shards", 32},
	}

	for _, config := range shardConfigs {
		t.Run(config.name, func(t *testing.T) {
			cache := NewStrategicCache(CacheConfig{
				EnableCaching:        true,
				EnableCompression:    false,
				CacheSize:            2000, // Larger cache to avoid eviction
				ShardCount:           config.shardCount,
				MaxValueSize:         4096,
				AdmissionProbability: -1, // Always admit for testing
			})
			defer cache.Close()

			// Measure concurrent write performance
			const numGoroutines = 8
			const operationsPerGoroutine = 50

			start := time.Now()
			var wg sync.WaitGroup

			for i := 0; i < numGoroutines; i++ {
				wg.Add(1)
				go func(goroutineID int) {
					defer wg.Done()
					for j := 0; j < operationsPerGoroutine; j++ {
						key := fmt.Sprintf("key_%d_%d", goroutineID, j)
						value := fmt.Sprintf("value_%d_%d", goroutineID, j)
						cache.Set(key, value)
					}
				}(i)
			}

			wg.Wait()
			duration := time.Since(start)

			totalOperations := numGoroutines * operationsPerGoroutine
			opsPerSecond := float64(totalOperations) / duration.Seconds()

			t.Logf("%s: %d operations in %v (%.2f ops/sec)",
				config.name, totalOperations, duration, opsPerSecond)

			// Verify all values were stored correctly
			correctCount := 0
			for i := 0; i < numGoroutines; i++ {
				for j := 0; j < operationsPerGoroutine; j++ {
					key := fmt.Sprintf("key_%d_%d", i, j)
					expectedValue := fmt.Sprintf("value_%d_%d", i, j)
					if value, ok := cache.Get(key); ok && value == expectedValue {
						correctCount++
					}
				}
			}

			// Should have at least 95% of values correct (some may be evicted)
			expectedMin := int(float64(totalOperations) * 0.95)
			if correctCount < expectedMin {
				t.Errorf("%s: Expected at least %d correct values, got %d",
					config.name, expectedMin, correctCount)
			}
		})
	}
}

func TestStrategicCache_DefaultShardCount(t *testing.T) {
	// Test that default configuration uses 32 shards
	cache := NewStrategicCache(CacheConfig{
		EnableCaching:     true,
		EnableCompression: false,
		CacheSize:         100,
		// ShardCount not specified - should default to 32
		MaxValueSize:         4096,
		AdmissionProbability: -1,
	})
	defer cache.Close()

	// Test basic operations work with default shard count
	testKey := "test_key"
	testValue := "test_value"

	if !cache.Set(testKey, testValue) {
		t.Fatal("Set failed with default shard count")
	}

	if value, ok := cache.Get(testKey); !ok || value != testValue {
		t.Fatalf("Get failed with default shard count: got %v, ok=%v", value, ok)
	}

	// Verify stats show correct configuration
	stats := cache.GetStats()
	if stats.MaxSize != 100 {
		t.Errorf("Expected MaxSize 100, got %d", stats.MaxSize)
	}
}

func TestEvictionPolicies_Comprehensive(t *testing.T) {
	// Test LFU with timestamp policy
	lfuPolicy := &LFUWithTimestampPolicy{}
	cache := map[string]*CacheEntry{
		"key1": {AccessCount: 5, Timestamp: time.Now().Add(-time.Hour)},
		"key2": {AccessCount: 3, Timestamp: time.Now().Add(-30 * time.Minute)},
		"key3": {AccessCount: 3, Timestamp: time.Now().Add(-time.Hour)},
	}

	evictedKey := lfuPolicy.EvictKey(cache)
	if evictedKey != "key3" {
		t.Errorf("Expected key3 to be evicted (lowest access count + oldest timestamp), got %s", evictedKey)
	}

	// Test LRU policy
	lruPolicy := &LRUPolicy{}
	cache2 := map[string]*CacheEntry{
		"key1": {Timestamp: time.Now().Add(-time.Hour)},
		"key2": {Timestamp: time.Now().Add(-30 * time.Minute)},
		"key3": {Timestamp: time.Now().Add(-2 * time.Hour)},
	}

	evictedKey2 := lruPolicy.EvictKey(cache2)
	if evictedKey2 != "key3" {
		t.Errorf("Expected key3 to be evicted (oldest timestamp), got %s", evictedKey2)
	}
}

func TestAdmissionPolicies_Comprehensive(t *testing.T) {
	// Test ProbabilisticAdmissionPolicy
	probPolicy := &ProbabilisticAdmissionPolicy{Probability: 0.5}

	// Test with probability 1.0
	probPolicy.Probability = 1.0
	if !probPolicy.Allow("key", "value") {
		t.Error("Expected admission with probability 1.0")
	}

	// Test with probability 0.0
	probPolicy.Probability = 0.0
	if probPolicy.Allow("key", "value") {
		t.Error("Expected no admission with probability 0.0")
	}

	// Test with probability 0.5 (should be probabilistic)
	probPolicy.Probability = 0.5
	admitted := 0
	total := 1000
	for i := 0; i < total; i++ {
		if probPolicy.Allow("key", "value") {
			admitted++
		}
	}

	// Should be roughly 50% (allowing for some variance)
	ratio := float64(admitted) / float64(total)
	if ratio < 0.4 || ratio > 0.6 {
		t.Errorf("Expected admission ratio around 0.5, got %f", ratio)
	}

	// Test NeverAdmitPolicy
	neverPolicy := &NeverAdmitPolicy{}
	if neverPolicy.Allow("key", "value") {
		t.Error("NeverAdmitPolicy should never allow admission")
	}
}

func TestSecureFloat64(t *testing.T) {
	// Test that SecureFloat64 returns values in [0,1)
	for i := 0; i < 100; i++ {
		val := SecureFloat64()
		if val < 0 || val >= 1.0 {
			t.Errorf("SecureFloat64 returned value outside [0,1): %f", val)
		}
	}
}

func TestCompressionDecompression_ErrorHandling(t *testing.T) {
	// Test compression with empty data
	_, err := compressGzipWithHeader([]byte{}, "TEST:")
	if err != nil {
		t.Errorf("Compression should not fail with empty data: %v", err)
	}

	// Test decompression with invalid data
	_, _, err = decompressGzipWithHeader([]byte{1, 2, 3}) // Too short
	if err == nil {
		t.Error("Expected error when decompressing data too short for header")
	}

	// Test decompression with invalid gzip data
	invalidData := []byte("TEST:invalid-gzip-data")
	_, _, err = decompressGzipWithHeader(invalidData)
	if err == nil {
		t.Error("Expected error when decompressing invalid gzip data")
	}
}

func TestCache_DecompressionErrorHandling(t *testing.T) {
	config := CacheConfig{
		CacheSize:            100,
		ShardCount:           1,
		EnableCaching:        true,
		EnableCompression:    true,
		MaxValueSize:         1024,
		AdmissionProbability: -1,
	}

	cache := NewStrategicCache(config)
	defer cache.Close()

	// Store a value that will cause decompression issues
	// We'll manually insert corrupted data into the cache
	shard := cache.getShard("test")
	shard.mu.Lock()
	shard.data["test"] = &CacheEntry{
		Value:     []byte("CORR:invalid-data"),
		Timestamp: time.Now(),
		Size:      20,
	}
	shard.mu.Unlock()

	// Try to get the corrupted value
	_, found := cache.Get("test")
	if found {
		t.Error("Expected Get to fail with corrupted compressed data")
	}
}

func TestCache_AdmissionPolicyIntegration(t *testing.T) {
	config := CacheConfig{
		CacheSize:            100,
		ShardCount:           1,
		EnableCaching:        true,
		MaxValueSize:         1024,
		AdmissionProbability: 0.0, // Never admit
	}

	cache := NewStrategicCache(config)
	defer cache.Close()

	// Should not be admitted due to NeverAdmitPolicy
	success := cache.Set("key1", "value1")
	if success {
		t.Error("Expected Set to fail with AdmissionProbability 0.0")
	}

	// Verify the value is not in cache
	_, found := cache.Get("key1")
	if found {
		t.Error("Value should not be in cache when admission is denied")
	}
}

func TestCache_EvictionPolicyIntegration(t *testing.T) {
	config := CacheConfig{
		CacheSize:            2, // Small cache to force eviction
		ShardCount:           1,
		EnableCaching:        true,
		MaxValueSize:         1024,
		AdmissionProbability: -1,
		EvictionPolicy:       "lfu", // Use LFU policy
	}

	cache := NewStrategicCache(config)
	defer cache.Close()

	// Fill the cache
	cache.Set("key1", "value1")
	cache.Set("key2", "value2")

	// Access key1 more times to make it more frequently used
	cache.Get("key1")
	cache.Get("key1")

	// Add a third key, should evict key2 (less frequently used)
	success := cache.Set("key3", "value3")
	if !success {
		t.Error("Expected Set to succeed")
	}

	// key2 should be evicted
	_, found := cache.Get("key2")
	if found {
		t.Error("Expected key2 to be evicted by LFU policy")
	}

	// key1 and key3 should still be there
	_, found = cache.Get("key1")
	if !found {
		t.Error("Expected key1 to still be in cache")
	}

	_, found = cache.Get("key3")
	if !found {
		t.Error("Expected key3 to still be in cache")
	}
}

func TestCache_CompressionIntegration(t *testing.T) {
	config := CacheConfig{
		CacheSize:            100,
		ShardCount:           1,
		EnableCaching:        true,
		EnableCompression:    true,
		MaxValueSize:         1024,
		AdmissionProbability: -1,
	}

	cache := NewStrategicCache(config)
	defer cache.Close()

	// Test compression with string
	success := cache.Set("str_key", "this is a test string that should be compressed")
	if !success {
		t.Error("Expected Set to succeed with compression")
	}

	value, found := cache.Get("str_key")
	if !found {
		t.Error("Expected to find compressed string")
	}

	if value != "this is a test string that should be compressed" {
		t.Errorf("Expected original string, got %v", value)
	}

	// Test compression with bytes
	testBytes := []byte("test bytes data")
	success = cache.Set("bytes_key", testBytes)
	if !success {
		t.Error("Expected Set to succeed with compressed bytes")
	}

	value, found = cache.Get("bytes_key")
	if !found {
		t.Error("Expected to find compressed bytes")
	}

	if !bytes.Equal(value.([]byte), testBytes) {
		t.Errorf("Expected original bytes, got %v", value)
	}
}

func TestCache_EdgeCases(t *testing.T) {
	config := CacheConfig{
		CacheSize:            100,
		ShardCount:           1,
		EnableCaching:        true,
		MaxKeySize:           100,
		MaxValueSize:         1024,
		AdmissionProbability: -1,
	}

	cache := NewStrategicCache(config)
	defer cache.Close()

	// Test with very large key
	largeKey := strings.Repeat("a", 1000)
	success := cache.Set(largeKey, "value")
	if success {
		t.Error("Expected Set to fail with very large key")
	}

	// Test with very large value
	largeValue := strings.Repeat("x", 2000)
	success = cache.Set("key", largeValue)
	if success {
		t.Error("Expected Set to fail with very large value")
	}

	// Test with nil value
	success = cache.Set("key", nil)
	if !success {
		t.Error("Expected Set to succeed with nil value")
	}

	value, found := cache.Get("key")
	if !found {
		t.Error("Expected to find nil value")
	}

	if value != nil {
		t.Errorf("Expected nil value, got %v", value)
	}
}

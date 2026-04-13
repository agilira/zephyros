// zephyros_integration_test.go: End 2 End tests for ultra-high performance MPSC lock-free ring buffer
//
// Copyright (c) 2025 AGILira
// Series: an AGLIra fragment
// SPDX-License-Identifier: MPL-2.0

package zephyros

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestZephyros_LoggingPipeline simulates a real logging pipeline
func TestZephyros_LoggingPipeline(t *testing.T) {
	t.Logf("END-TO-END LOGGING PIPELINE TEST")

	// Simulate log entries
	type LogEntry struct {
		Level     string
		Message   string
		Timestamp time.Time
		ThreadID  int
	}

	// Processed log storage
	var processedLogs []LogEntry
	var processingMutex sync.Mutex
	processedCount := int64(0)

	// Realistic log processor with formatting
	processor := func(entry *LogEntry) {
		// Simulate log formatting and storage
		processingMutex.Lock()
		processedLogs = append(processedLogs, LogEntry{
			Level:     entry.Level,
			Message:   fmt.Sprintf("[%s] %s", entry.Level, entry.Message),
			Timestamp: entry.Timestamp,
			ThreadID:  entry.ThreadID,
		})
		processingMutex.Unlock()
		atomic.AddInt64(&processedCount, 1)
	}

	// Build production-like configuration
	numRings := 4
	threaded, err := NewThreadedBuilder[LogEntry](262144, numRings).
		WithProcessor(processor).
		WithBatchSize(8192).
		WithWorkers(numRings).
		Build()

	if err != nil {
		t.Fatalf("Failed to build logging pipeline: %v", err)
	}
	defer threaded.Close()

	// Start processing
	threaded.LoopProcess()
	time.Sleep(10 * time.Millisecond) // Stabilize

	t.Logf("Simulating multi-threaded application logging...")

	// Simulate 4 application threads logging concurrently
	var wg sync.WaitGroup
	numThreads := 4
	logsPerThread := 10000
	totalLogs := numThreads * logsPerThread

	startTime := time.Now()

	// One writer per ring (numThreads == numRings, so thread→ring is 1:1).
	logWriters := make([]*SafeWriter[LogEntry], numRings)
	for id := 0; id < numRings; id++ {
		logWriters[id] = threaded.NewSafeWriter(id)
	}

	for threadID := 0; threadID < numThreads; threadID++ {
		wg.Add(1)
		go func(w *SafeWriter[LogEntry], tid int) {
			defer wg.Done()

			levels := []string{"INFO", "WARN", "ERROR", "DEBUG"}

			for i := 0; i < logsPerThread; i++ {
				level := levels[i%len(levels)]
				entry := LogEntry{
					Level:     level,
					Message:   fmt.Sprintf("Message %d from thread %d", i, tid),
					Timestamp: time.Now(),
					ThreadID:  tid,
				}

				// Write to pipeline
				success := w.Write(func(slot *LogEntry) {
					*slot = entry
				})

				if !success {
					// Retry on backpressure
					time.Sleep(time.Microsecond)
					i-- // Retry this log
				}
			}
		}(logWriters[threadID], threadID)
	}

	wg.Wait()
	writeTime := time.Since(startTime)

	t.Logf("Writing completed in %v", writeTime)

	// Wait for all processing to complete
	for atomic.LoadInt64(&processedCount) < int64(totalLogs) {
		time.Sleep(time.Millisecond)
	}

	totalTime := time.Since(startTime)
	finalProcessed := atomic.LoadInt64(&processedCount)

	// Validate results
	processingMutex.Lock()
	actualLogCount := len(processedLogs)
	processingMutex.Unlock()

	t.Logf("")
	t.Logf("LOGGING PIPELINE RESULTS:")
	t.Logf("  Expected logs: %d", totalLogs)
	t.Logf("  Processed logs: %d", finalProcessed)
	t.Logf("  Stored logs: %d", actualLogCount)
	t.Logf("  Total time: %v", totalTime)
	t.Logf("  Throughput: %.1fM logs/sec", float64(finalProcessed)/totalTime.Seconds()/1000000)

	// Assertions
	if finalProcessed != int64(totalLogs) {
		t.Errorf("expected %d processed logs, got %d", totalLogs, finalProcessed)
	}

	// Verify log distribution across threads.
	// WHY not also asserting len(processedLogs): finalProcessed measures the
	// same increments under the same mutex, so they are always equal at drain.
	threadCounts := make(map[int]int)
	processingMutex.Lock()
	for _, log := range processedLogs {
		threadCounts[log.ThreadID]++
	}
	processingMutex.Unlock()

	for threadID := 0; threadID < numThreads; threadID++ {
		if threadCounts[threadID] != logsPerThread {
			t.Errorf("thread %d: expected %d logs, got %d", threadID, logsPerThread, threadCounts[threadID])
		}
	}

	t.Logf("LOGGING PIPELINE TEST PASSED - All logs processed correctly!")
}

// TestZephyros_EventProcessing simulates event processing system
func TestZephyros_EventProcessing(t *testing.T) {
	t.Logf("END-TO-END EVENT PROCESSING TEST")

	// Event types
	type Event struct {
		ID        string
		Type      string
		Payload   map[string]interface{}
		Timestamp time.Time
	}

	// Event processing results
	var processedEvents []Event
	var eventMutex sync.Mutex
	processedCount := int64(0)

	// Event processor with business logic
	processor := func(event *Event) {
		// Simulate event processing (validation, enrichment, storage)
		time.Sleep(time.Microsecond) // Simulate processing overhead

		eventMutex.Lock()
		processedEvents = append(processedEvents, Event{
			ID:        event.ID,
			Type:      event.Type,
			Payload:   event.Payload,
			Timestamp: time.Now(), // Processing timestamp
		})
		eventMutex.Unlock()
		atomic.AddInt64(&processedCount, 1)
	}

	// Build event processing system.
	// numRings must equal numSources to satisfy the Anemoi invariant:
	// one producer goroutine per ring.
	numSources := 3
	numRings := numSources
	eventSystem, err := NewThreadedBuilder[Event](131072, numRings).
		WithProcessor(processor).
		WithBatchSize(4096).
		WithWorkers(numRings).
		Build()

	if err != nil {
		t.Fatalf("Failed to build event system: %v", err)
	}
	defer eventSystem.Close()

	eventSystem.LoopProcess()
	time.Sleep(10 * time.Millisecond)

	t.Logf("Simulating event ingestion from multiple sources...")

	// Simulate event sources
	eventTypes := []string{"user_click", "page_view", "purchase", "signup"}
	eventsPerSource := 5000
	totalEvents := numSources * eventsPerSource

	var wg sync.WaitGroup
	startTime := time.Now()

	// Create writers before goroutines start (SPSC enforcement at init).
	eventWriters := make([]*SafeWriter[Event], numSources)
	for id := 0; id < numSources; id++ {
		eventWriters[id] = eventSystem.NewSafeWriter(id)
	}

	for sourceID := 0; sourceID < numSources; sourceID++ {
		wg.Add(1)
		go func(w *SafeWriter[Event], sID int) {
			defer wg.Done()

			for i := 0; i < eventsPerSource; i++ {
				event := Event{
					ID:   fmt.Sprintf("evt_%d_%d", sID, i),
					Type: eventTypes[i%len(eventTypes)],
					Payload: map[string]interface{}{
						"source": sID,
						"index":  i,
						"data":   fmt.Sprintf("payload_%d", i),
					},
					Timestamp: time.Now(),
				}

				success := w.Write(func(slot *Event) {
					*slot = event
				})

				if !success {
					time.Sleep(time.Microsecond)
					i-- // Retry
				}
			}
		}(eventWriters[sourceID], sourceID)
	}

	wg.Wait()
	ingestionTime := time.Since(startTime)

	t.Logf("Event ingestion completed in %v", ingestionTime)

	// Wait for processing completion
	for atomic.LoadInt64(&processedCount) < int64(totalEvents) {
		time.Sleep(time.Millisecond)
	}

	totalTime := time.Since(startTime)
	finalProcessed := atomic.LoadInt64(&processedCount)

	// Validate processing
	eventMutex.Lock()
	// Count events by type for distribution verification.
	typeCounts := make(map[string]int)
	for _, event := range processedEvents {
		typeCounts[event.Type]++
	}
	eventMutex.Unlock()

	t.Logf("")
	t.Logf("EVENT PROCESSING RESULTS:")
	t.Logf("  Expected events: %d", totalEvents)
	t.Logf("  Processed events: %d", finalProcessed)
	t.Logf("  Processing time: %v", totalTime)
	t.Logf("  Throughput: %.1fM events/sec", float64(finalProcessed)/totalTime.Seconds()/1000000)

	// Assertions
	if finalProcessed != int64(totalEvents) {
		t.Errorf("expected %d processed events, got %d", totalEvents, finalProcessed)
	}

	// Verify each event type was processed
	expectedCountPerType := totalEvents / len(eventTypes)
	for _, eventType := range eventTypes {
		if typeCounts[eventType] != expectedCountPerType {
			t.Errorf("event type %s: expected ~%d events, got %d",
				eventType, expectedCountPerType, typeCounts[eventType])
		}
	}

	t.Logf("EVENT PROCESSING TEST PASSED - All events processed correctly!")
}

// TestZephyros_WorkerPool simulates a worker pool scenario
func TestZephyros_WorkerPool(t *testing.T) {
	t.Logf("⚙️ END-TO-END WORKER POOL TEST")

	// Task definition
	type Task struct {
		ID       int
		Input    int
		Result   int
		Duration time.Duration
	}

	// Task execution results
	var completedTasks []Task
	var taskMutex sync.Mutex
	completedCount := int64(0)

	// Worker processor (simulates CPU work)
	processor := func(task *Task) {
		// Simulate work by calculating factorial (CPU intensive)
		result := 1
		for i := 1; i <= task.Input; i++ {
			result *= i
			if result > 1000000 { // Prevent overflow
				result = result % 1000000
			}
		}

		task.Result = result
		task.Duration = time.Since(time.Now().Add(-time.Duration(task.Input) * time.Microsecond))

		taskMutex.Lock()
		completedTasks = append(completedTasks, *task)
		taskMutex.Unlock()
		atomic.AddInt64(&completedCount, 1)
	}

	// Build worker pool
	numWorkers := 4
	workerPool, err := NewThreadedBuilder[Task](65536, numWorkers).
		WithProcessor(processor).
		WithBatchSize(1024).
		WithWorkers(numWorkers).
		Build()

	if err != nil {
		t.Fatalf("Failed to build worker pool: %v", err)
	}
	defer workerPool.Close()

	workerPool.LoopProcess()
	time.Sleep(10 * time.Millisecond)

	t.Logf("Submitting tasks to worker pool...")

	// Submit tasks
	numTasks := 8000
	var wg sync.WaitGroup
	startTime := time.Now()

	// One writer per ring, created before the submission goroutine.
	taskWriters := make([]*SafeWriter[Task], numWorkers)
	for id := 0; id < numWorkers; id++ {
		taskWriters[id] = workerPool.NewSafeWriter(id)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()

		for i := 0; i < numTasks; i++ {
			task := Task{
				ID:    i,
				Input: (i % 10) + 1, // Factorial of 1-10
			}

			success := taskWriters[i%numWorkers].Write(func(slot *Task) {
				*slot = task
			})

			if !success {
				time.Sleep(time.Microsecond)
				i-- // Retry
			}
		}
	}()

	wg.Wait()
	submissionTime := time.Since(startTime)

	t.Logf("Task submission completed in %v", submissionTime)

	// Wait for all tasks to complete
	for atomic.LoadInt64(&completedCount) < int64(numTasks) {
		time.Sleep(time.Millisecond)
	}

	totalTime := time.Since(startTime)
	finalCompleted := atomic.LoadInt64(&completedCount)

	// Analyze results
	taskMutex.Lock()
	actualCompleted := len(completedTasks)

	// Sort by ID for verification
	sort.Slice(completedTasks, func(i, j int) bool {
		return completedTasks[i].ID < completedTasks[j].ID
	})

	// Verify task results (factorial correctness)
	correctResults := 0
	for _, task := range completedTasks {
		expected := 1
		for i := 1; i <= task.Input; i++ {
			expected *= i
			if expected > 1000000 {
				expected = expected % 1000000
			}
		}
		if task.Result == expected {
			correctResults++
		}
	}
	taskMutex.Unlock()

	t.Logf("")
	t.Logf("WORKER POOL RESULTS:")
	t.Logf("  Expected tasks: %d", numTasks)
	t.Logf("  Completed tasks: %d", finalCompleted)
	t.Logf("  Verified tasks: %d", actualCompleted)
	t.Logf("  Correct results: %d (%.1f%%)", correctResults, float64(correctResults)*100/float64(actualCompleted))
	t.Logf("  Total time: %v", totalTime)
	t.Logf("  Throughput: %.1fK tasks/sec", float64(finalCompleted)/totalTime.Seconds()/1000)

	// Assertions
	if finalCompleted != int64(numTasks) {
		t.Errorf("❌ Expected %d completed tasks, got %d", numTasks, finalCompleted)
	}

	if correctResults != actualCompleted {
		t.Errorf("❌ Expected all results to be correct, got %d/%d", correctResults, actualCompleted)
	}

	// Verify task IDs are complete and unique
	taskIDs := make(map[int]bool)
	taskMutex.Lock()
	for _, task := range completedTasks {
		if taskIDs[task.ID] {
			t.Errorf("❌ Duplicate task ID: %d", task.ID)
		}
		taskIDs[task.ID] = true
	}
	taskMutex.Unlock()

	if len(taskIDs) != numTasks {
		t.Errorf("Expected %d unique task IDs, got %d", numTasks, len(taskIDs))
	}

	t.Logf("WORKER POOL TEST PASSED - All tasks processed correctly!")
}

// TestZephyros_LongRunningStability tests system stability over time
func TestZephyros_LongRunningStability(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping long-running stability test in short mode")
	}

	t.Logf("END-TO-END LONG-RUNNING STABILITY TEST")

	type Message struct {
		ID        int64
		Timestamp time.Time
		Data      string
	}

	var processed int64
	processor := func(msg *Message) {
		atomic.AddInt64(&processed, 1)
	}

	// Build system for stability testing
	system, err := NewThreadedBuilder[Message](131072, 2).
		WithProcessor(processor).
		WithBatchSize(2048).
		WithWorkers(2).
		Build()

	if err != nil {
		t.Fatalf("Failed to build system: %v", err)
	}
	defer system.Close()

	system.LoopProcess()

	// Use shorter duration for regular tests to avoid hanging
	duration := 5 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	t.Logf("Running continuous load for %v...", duration)

	var wg sync.WaitGroup
	messageID := int64(0)

	// Producer goroutine — 2 rings, rotated by message ID.
	sysW0 := system.NewSafeWriter(0)
	sysW1 := system.NewSafeWriter(1)
	sysWriters := [2]*SafeWriter[Message]{sysW0, sysW1}

	wg.Add(1)
	go func() {
		defer wg.Done()

		for {
			select {
			case <-ctx.Done():
				return
			default:
				id := atomic.AddInt64(&messageID, 1)
				msg := Message{
					ID:        id,
					Timestamp: time.Now(),
					Data:      fmt.Sprintf("msg_%d", id),
				}

				success := sysWriters[id%2].Write(func(slot *Message) {
					*slot = msg
				})

				if !success {
					time.Sleep(time.Microsecond)
				}
			}
		}
	}()

	// Monitor goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()

		lastProcessed := int64(0)
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				current := atomic.LoadInt64(&processed)
				rate := float64(current-lastProcessed) / 5.0 / 1000000
				t.Logf("Processed: %d (+%.1fM/sec)", current, rate)
				lastProcessed = current

				// Check system health
				stats := system.Stats()
				t.Logf("   System stats: %+v", stats)
			}
		}
	}()

	wg.Wait()

	finalProcessed := atomic.LoadInt64(&processed)
	finalMessages := atomic.LoadInt64(&messageID)

	t.Logf("")
	t.Logf("STABILITY TEST RESULTS:")
	t.Logf("  Messages sent: %d", finalMessages)
	t.Logf("  Messages processed: %d", finalProcessed)
	t.Logf("  Processing rate: %.1f%%", float64(finalProcessed)*100/float64(finalMessages))
	t.Logf("  Duration: %v", duration)
	t.Logf("  Average throughput: %.1fM msgs/sec", float64(finalProcessed)/duration.Seconds()/1000000)

	// Final health check
	stats := system.Stats()
	t.Logf("Final system stats: %+v", stats)

	// WHY no separate finalProcessed == 0 check: if no messages are processed,
	// processingRate evaluates to 0.0 which is already < 0.95, so the single
	// rate check covers that case with a clearer failure message.
	processingRate := float64(finalProcessed) / float64(finalMessages)
	if processingRate < 0.95 { // 95% processing rate minimum
		t.Errorf("low processing rate: %.1f%% (expected >95%%)", processingRate*100)
	}

	t.Logf("STABILITY TEST PASSED - System stable under continuous load!")
}

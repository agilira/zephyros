package zephyros

import (
	"testing"
)

func TestPublicErrors_NotNilAndMessages(t *testing.T) {
	errorsList := []struct {
		err error
		msg string
	}{
		{ErrPoolClosed, "operation pool is closed"},
		{ErrBatchTimeout, "batch processing timeout"},
		{ErrCacheDisabled, "caching is not enabled"},
		{ErrContextNil, "context cannot be nil"},
		{ErrResultTimeout, "result retrieval timeout"},
		{ErrInvalidConfig, "invalid configuration"},
		{ErrTimeout, "operation timeout"},
		{ErrContextCancelled, "context cancelled"},
		{ErrQueueFull, "operation queue is full"},
		{ErrValidationFailed, "operation validation failed"},
		{ErrRateLimited, "operation rate limited"},
	}
	for _, e := range errorsList {
		if e.err == nil {
			t.Errorf("expected error to be non-nil: %v", e.msg)
		}
		if e.err.Error() == "" {
			t.Errorf("expected error message to be non-empty for: %v", e.msg)
		}
		if e.err.Error() != "zephyros: "+e.msg {
			t.Errorf("unexpected error message: got '%s', want 'zephyros: %s'", e.err.Error(), e.msg)
		}
	}
}

func TestErrorCodes_NotEmptyAndUnique(t *testing.T) {
	codes := []string{
		ErrCodePoolClosed,
		ErrCodeBatchTimeout,
		ErrCodeCacheDisabled,
		ErrCodeContextNil,
		ErrCodeResultTimeout,
		ErrCodeInvalidConfig,
		ErrCodeTimeout,
		ErrCodeContextCancelled,
		ErrCodeQueueFull,
		ErrCodeValidationFailed,
		ErrCodeRateLimited,
	}
	seen := map[string]bool{}
	for _, code := range codes {
		if code == "" {
			t.Error("error code should not be empty")
		}
		if seen[code] {
			t.Errorf("duplicate error code: %s", code)
		}
		seen[code] = true
	}
}

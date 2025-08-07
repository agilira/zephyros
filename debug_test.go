package zephyros

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"testing"
)

func TestDebugGobEncode(t *testing.T) {
	// Register types
	gob.Register(PrimitiveBox{})
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

	// Test the exact same scenario as the failing test
	value := 12345
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)

	fmt.Printf("About to encode value: %v (type %T)\n", value, value)

	if err := enc.Encode(PrimitiveBox{V: value}); err != nil {
		fmt.Printf("GOB encoding failed for value %v (type %T), error: %v\n", value, value, err)
		t.Fatalf("gob.Encode failed: %v", err)
	}

	fmt.Printf("Encoding successful, buffer size: %d\n", buf.Len())
}

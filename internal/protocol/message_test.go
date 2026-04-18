package protocol

import (
	"bytes"
	"testing"
	"time"
)

func TestMessageEncodeDecode(t *testing.T) {
	m := &Message{
		Offset:    123,
		Timestamp: time.Now().UnixMilli(),
		Key:       []byte("test-key"),
		Value:     []byte("test-value"),
	}

	data := m.Encode()
	decoded, err := Decode(data)
	if err != nil {
		t.Fatalf("Failed to decode: %v", err)
	}

	if decoded.Offset != m.Offset {
		t.Errorf("Offset mismatch: got %d, want %d", decoded.Offset, m.Offset)
	}
	if decoded.Timestamp != m.Timestamp {
		t.Errorf("Timestamp mismatch: got %d, want %d", decoded.Timestamp, m.Timestamp)
	}
	if !bytes.Equal(decoded.Key, m.Key) {
		t.Errorf("Key mismatch: got %s, want %s", decoded.Key, m.Key)
	}
	if !bytes.Equal(decoded.Value, m.Value) {
		t.Errorf("Value mismatch: got %s, want %s", decoded.Value, m.Value)
	}
	if decoded.CRC != m.CRC {
		t.Errorf("CRC mismatch: got %x, want %x", decoded.CRC, m.CRC)
	}
}

func TestMessageEmptyKey(t *testing.T) {
	m := &Message{
		Offset:    456,
		Timestamp: time.Now().UnixMilli(),
		Key:       []byte{},
		Value:     []byte("only-value"),
	}

	data := m.Encode()
	decoded, err := Decode(data)
	if err != nil {
		t.Fatalf("Failed to decode: %v", err)
	}

	if len(decoded.Key) != 0 {
		t.Errorf("Expected empty key, got %v", decoded.Key)
	}
	if !bytes.Equal(decoded.Value, m.Value) {
		t.Errorf("Value mismatch: got %s, want %s", decoded.Value, m.Value)
	}
}

func TestGzipCompression(t *testing.T) {
	m := &Message{
		Offset:      1011,
		Compression: CompressionGzip,
		Value:       []byte("this is a long value that should be compressed nicely"),
	}

	data := m.Encode()
	
	// Ensure it's not the same as original value (though it might be larger for short strings, 
	// but we just want to verify round-trip)
	decoded, err := Decode(data)
	if err != nil {
		t.Fatalf("Failed to decode: %v", err)
	}

	if decoded.Compression != CompressionGzip {
		t.Errorf("Compression mismatch: got %d, want %d", decoded.Compression, CompressionGzip)
	}
	if !bytes.Equal(decoded.Value, m.Value) {
		t.Errorf("Value mismatch: got %s, want %s", decoded.Value, m.Value)
	}
}

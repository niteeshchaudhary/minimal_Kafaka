package log

import (
	"encoding/json"
	"time"
)

// Message represents a single record in the log.
type Message struct {
	Offset    uint64 `json:"offset"`
	Timestamp int64  `json:"timestamp"`
	Key       string `json:"key,omitempty"`
	Value     string `json:"value"`
}

// NewMessage creates a new message with the current timestamp.
func NewMessage(key, value string) *Message {
	return &Message{
		Timestamp: time.Now().UnixMilli(),
		Key:       key,
		Value:     value,
	}
}

// Marshal encodes the message to JSON.
func (m *Message) Marshal() ([]byte, error) {
	return json.Marshal(m)
}

// UnmarshalMessage decodes a message from JSON.
func UnmarshalMessage(data []byte) (*Message, error) {
	var m Message
	err := json.Unmarshal(data, &m)
	return &m, err
}

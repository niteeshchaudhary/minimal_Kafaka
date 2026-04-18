package log

import (
	"github.com/niteesh/gokafka/internal/protocol"
)

// Message is an alias for protocol.Message to maintain backward compatibility in the log package if needed,
// but we will primarily use the binary protocol's Message.
type Message = protocol.Message

// NewMessage creates a new message with the current timestamp.
func NewMessage(key, value string) *Message {
	return &protocol.Message{
		Key:   []byte(key),
		Value: []byte(value),
	}
}

// Marshal encodes the message to binary.
func Marshal(m *Message) []byte {
	return m.Encode()
}

// UnmarshalMessage decodes a message from binary.
func UnmarshalMessage(data []byte) (*Message, error) {
	return protocol.Decode(data)
}

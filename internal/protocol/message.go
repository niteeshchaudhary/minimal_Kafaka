package protocol

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
)

const (
	CompressionNone uint8 = 0
	CompressionGzip uint8 = 1
)

// Message represents a single record in binary format.
type Message struct {
	Offset      uint64 `json:"offset"`
	Timestamp   int64  `json:"timestamp"`
	Compression uint8  `json:"compression"`
	Key         []byte `json:"key"`
	Value       []byte `json:"value"`
	CRC         uint32 `json:"crc"`
}

// Encode serializes the message into a byte slice.
func (m *Message) Encode() []byte {
	valToEncode := m.Value
	if m.Compression == CompressionGzip {
		var buf bytes.Buffer
		gw := gzip.NewWriter(&buf)
		gw.Write(m.Value)
		gw.Close()
		valToEncode = buf.Bytes()
	}

	keyLen := len(m.Key)
	valLen := len(valToEncode)
	
	// Size = 8(Off) + 8(Ts) + 1(Comp) + 4(KLen) + keyLen + 4(VLen) + valLen + 4(CRC)
	size := 8 + 8 + 1 + 4 + keyLen + 4 + valLen + 4
	buf := make([]byte, size)
	
	binary.BigEndian.PutUint64(buf[0:8], m.Offset)
	binary.BigEndian.PutUint64(buf[8:16], uint64(m.Timestamp))
	buf[16] = m.Compression
	
	binary.BigEndian.PutUint32(buf[17:21], uint32(keyLen))
	copy(buf[21:21+keyLen], m.Key)
	
	valStart := 21 + keyLen
	binary.BigEndian.PutUint32(buf[valStart:valStart+4], uint32(valLen))
	copy(buf[valStart+4:valStart+4+valLen], valToEncode)
	
	m.CRC = crc32.ChecksumIEEE(buf[:size-4])
	binary.BigEndian.PutUint32(buf[size-4:], m.CRC)
	
	return buf
}

// Decode deserializes a message from a byte slice.
func Decode(data []byte) (*Message, error) {
	if len(data) < 29 { 
		return nil, fmt.Errorf("data too short")
	}
	
	m := &Message{}
	m.Offset = binary.BigEndian.Uint64(data[0:8])
	m.Timestamp = int64(binary.BigEndian.Uint64(data[8:16]))
	m.Compression = data[16]
	
	keyLen := int(binary.BigEndian.Uint32(data[17:21]))
	if len(data) < 21+keyLen+4 {
		return nil, fmt.Errorf("missing key or value length")
	}
	m.Key = make([]byte, keyLen)
	copy(m.Key, data[21:21+keyLen])
	
	valStart := 21 + keyLen
	valLen := int(binary.BigEndian.Uint32(data[valStart : valStart+4]))
	if len(data) < valStart+4+valLen+4 {
		return nil, fmt.Errorf("missing value or CRC")
	}
	
	valPayload := data[valStart+4 : valStart+4+valLen]
	if m.Compression == CompressionGzip {
		gr, err := gzip.NewReader(bytes.NewReader(valPayload))
		if err != nil {
			return nil, err
		}
		defer gr.Close()
		m.Value, err = io.ReadAll(gr)
		if err != nil {
			return nil, err
		}
	} else {
		m.Value = make([]byte, valLen)
		copy(m.Value, valPayload)
	}
	
	m.CRC = binary.BigEndian.Uint32(data[valStart+4+valLen : valStart+4+valLen+4])
	expectedCRC := crc32.ChecksumIEEE(data[:len(data)-4])
	if m.CRC != expectedCRC {
		return nil, fmt.Errorf("CRC mismatch")
	}
	
	return m, nil
}

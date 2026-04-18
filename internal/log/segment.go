package log

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// Segment represents a log segment (log file + index file).
type Segment struct {
	baseOffset uint64
	logFile    *os.File
	index      *Index
	timeIndex  *TimeIndex
	dir        string
	maxSize    uint64
	size       uint64
}

// NewSegment creates a new segment. baseOffset is the first offset in this segment.
func NewSegment(dir string, baseOffset uint64, maxSize uint64) (*Segment, error) {
	logPath := filepath.Join(dir, fmt.Sprintf("%d.log", baseOffset))
	indexPath := filepath.Join(dir, fmt.Sprintf("%d.index", baseOffset))

	logFile, err := os.OpenFile(logPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	index, err := NewIndex(indexPath)
	if err != nil {
		logFile.Close()
		return nil, err
	}

	fi, err := logFile.Stat()
	if err != nil {
		return nil, err
	}

	timeIdxPath := filepath.Join(dir, fmt.Sprintf("%020d.timeindex", baseOffset))
	timeIdx, err := NewTimeIndex(timeIdxPath)
	if err != nil {
		return nil, err
	}

	return &Segment{
		baseOffset: baseOffset,
		logFile:    logFile,
		index:      index,
		timeIndex:  timeIdx,
		dir:        dir,
		maxSize:    maxSize,
		size:       uint64(fi.Size()),
	}, nil
}

// WriteRawAt streams raw bytes from the segment to the given writer.
func (s *Segment) WriteRawAt(w io.Writer, pos uint64, size uint32) (int64, error) {
	sr := io.NewSectionReader(s.logFile, int64(pos), int64(size))
	return io.CopyN(w, sr, int64(size))
}

// Write appends a message to the segment and updates the index.
func (s *Segment) Write(msg *Message) (uint64, error) {
	data := msg.Encode()
	dataSize := uint32(len(data))
	
	// Total bytes to write: 4 (size) + len(data)
	buf := make([]byte, 4+len(data))
	binary.BigEndian.PutUint32(buf[0:4], dataSize)
	copy(buf[4:], data)

	pos := s.size
	if _, err := s.logFile.Write(buf); err != nil {
		return 0, err
	}

	if err := s.index.Write(msg.Offset, pos); err != nil {
		return 0, err
	}
	if err := s.timeIndex.Write(msg.Timestamp, msg.Offset); err != nil {
		return 0, err
	}
	s.size += uint64(len(buf))
	return msg.Offset, nil
}

// ReadAt reads a message at a specific file position.
func (s *Segment) ReadAt(pos uint64) (*Message, error) {
	// Read size first
	sizeBuf := make([]byte, 4)
	if _, err := s.logFile.ReadAt(sizeBuf, int64(pos)); err != nil {
		return nil, err
	}
	dataSize := binary.BigEndian.Uint32(sizeBuf)

	// Read data
	data := make([]byte, dataSize)
	if _, err := s.logFile.ReadAt(data, int64(pos+4)); err != nil {
		return nil, err
	}

	return UnmarshalMessage(data)
}

// IsFull returns true if the segment has reached its size limit.
func (s *Segment) IsFull() bool {
	return s.size >= s.maxSize
}

// Close closes the segment files.
func (s *Segment) Close() error {
	s.index.Close()
	s.timeIndex.Close()
	return s.logFile.Close()
}

// Sync flushes files to disk.
func (s *Segment) Sync() error {
	if err := s.index.Sync(); err != nil {
		return err
	}
	return s.logFile.Sync()
}

// Paths returns the paths to the log and index files.
func (s *Segment) Paths() (string, string) {
	logPath := filepath.Join(s.dir, fmt.Sprintf("%d.log", s.baseOffset))
	indexPath := filepath.Join(s.dir, fmt.Sprintf("%d.index", s.baseOffset))
	return logPath, indexPath
}

// Remove closes and deletes the segment files.
func (s *Segment) Remove() error {
	s.Close()
	logPath, indexPath := s.Paths()
	os.Remove(logPath)
	return os.Remove(indexPath)
}

package log

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
)

// Segment represents a log segment (log file + index file).
type Segment struct {
	baseOffset uint64
	logFile    *os.File
	index      *Index
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

	return &Segment{
		baseOffset: baseOffset,
		logFile:    logFile,
		index:      index,
		dir:        dir,
		maxSize:    maxSize,
		size:       uint64(fi.Size()),
	}, nil
}

// Write appends a message to the segment and updates the index.
func (s *Segment) Write(msg *Message) (uint64, error) {
	data, err := msg.Marshal()
	if err != nil {
		return 0, err
	}

	pos := s.size
	if _, err := s.logFile.Write(append(data, '\n')); err != nil {
		return 0, err
	}

	if err := s.index.Write(msg.Offset, pos); err != nil {
		return 0, err
	}

	s.size += uint64(len(data) + 1)
	return msg.Offset, nil
}

// ReadAt reads a message at a specific file position.
func (s *Segment) ReadAt(pos uint64) (*Message, error) {
	// For simplicity, we use a scanner but seek to the position first.
	// In a high-performance system, we'd use pre-allocated buffers.
	if _, err := s.logFile.Seek(int64(pos), 0); err != nil {
		return nil, err
	}

	reader := bufio.NewReader(s.logFile)
	line, err := reader.ReadBytes('\n')
	if err != nil {
		return nil, err
	}

	return UnmarshalMessage(line)
}

// IsFull returns true if the segment has reached its size limit.
func (s *Segment) IsFull() bool {
	return s.size >= s.maxSize
}

// Close closes the segment files.
func (s *Segment) Close() error {
	if err := s.index.Close(); err != nil {
		return err
	}
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

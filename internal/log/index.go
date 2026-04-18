package log

import (
	"encoding/binary"
	"io"
	"os"
)

var (
	entryWidth uint64 = 16 // 8 bytes offset + 8 bytes position
)

// Index provides an offset-to-position mapping for a log segment.
type Index struct {
	file *os.File
	size uint64
}

// NewIndex creates a new index file or opens an existing one.
func NewIndex(path string) (*Index, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	fi, err := file.Stat()
	if err != nil {
		return nil, err
	}
	return &Index{
		file: file,
		size: uint64(fi.Size()),
	}, nil
}

// Write appends an offset and its corresponding position to the index.
func (i *Index) Write(offset, position uint64) error {
	buf := make([]byte, entryWidth)
	binary.BigEndian.PutUint64(buf[0:8], offset)
	binary.BigEndian.PutUint64(buf[8:16], position)

	if _, err := i.file.Write(buf); err != nil {
		return err
	}
	i.size += entryWidth
	return nil
}

// Read returns the file position for a given relative offset (index entry index).
// Note: This is NOT the absolute offset, but the N-th entry in the index.
func (i *Index) Read(n int64) (offset uint64, position uint64, err error) {
	if i.size == 0 || uint64(n*int64(entryWidth)) >= i.size {
		return 0, 0, io.EOF
	}

	buf := make([]byte, entryWidth)
	if _, err := i.file.ReadAt(buf, n*int64(entryWidth)); err != nil {
		return 0, 0, err
	}

	offset = binary.BigEndian.Uint64(buf[0:8])
	position = binary.BigEndian.Uint64(buf[8:16])
	return offset, position, nil
}

// Close closes the index file.
func (i *Index) Close() error {
	return i.file.Close()
}

// Sync flushes stays to disk.
func (i *Index) Sync() error {
	return i.file.Sync()
}

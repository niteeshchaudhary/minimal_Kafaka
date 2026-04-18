package log

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

const timeEntryWidth = 16 // 8 (Timestamp) + 8 (Offset)

type TimeIndex struct {
	file *os.File
	size uint64
}

func NewTimeIndex(path string) (*TimeIndex, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	return &TimeIndex{
		file: f,
		size: uint64(fi.Size()),
	}, nil
}

func (idx *TimeIndex) Write(timestamp int64, offset uint64) error {
	buf := make([]byte, timeEntryWidth)
	binary.BigEndian.PutUint64(buf[0:8], uint64(timestamp))
	binary.BigEndian.PutUint64(buf[8:16], offset)
	
	_, err := idx.file.WriteAt(buf, int64(idx.size))
	if err != nil {
		return err
	}
	idx.size += timeEntryWidth
	return nil
}

func (idx *TimeIndex) Read(i int64) (int64, uint64, error) {
	if int64(idx.size) < (i+1)*timeEntryWidth {
		return 0, 0, io.EOF
	}
	buf := make([]byte, timeEntryWidth)
	_, err := idx.file.ReadAt(buf, i*timeEntryWidth)
	if err != nil {
		return 0, 0, err
	}
	ts := int64(binary.BigEndian.Uint64(buf[0:8]))
	off := binary.BigEndian.Uint64(buf[8:16])
	return ts, off, nil
}

// Search returns the largest offset whose timestamp is less than or equal to the target.
func (idx *TimeIndex) Search(targetTs int64) (uint64, error) {
	numEntries := int64(idx.size / timeEntryWidth)
	if numEntries == 0 {
		return 0, fmt.Errorf("no entries")
	}

	var low int64 = 0
	var high int64 = numEntries - 1
	var bestOffset uint64 = 0

	for low <= high {
		mid := low + (high-low)/2
		ts, off, err := idx.Read(mid)
		if err != nil {
			return 0, err
		}

		if ts == targetTs {
			return off, nil
		} else if ts < targetTs {
			bestOffset = off
			low = mid + 1
		} else {
			high = mid - 1
		}
	}

	return bestOffset, nil
}

func (idx *TimeIndex) Close() error {
	return idx.file.Close()
}

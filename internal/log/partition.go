package log

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const (
	defaultMaxSegmentSize uint64 = 10 * 1024 * 1024 // 10MB
)

// Partition represents a log partition composed of segments.
type Partition struct {
	mu            sync.RWMutex
	dir           string
	topic         string
	ID            int
	segments      []*Segment
	activeSegment *Segment
	currentOffset uint64
	maxSegSize    uint64
}

// NewPartition initializes a partition and recovers its segments.
func NewPartition(baseDir, topic string, id int, maxSegSize uint64) (*Partition, error) {
	dir := filepath.Join(baseDir, topic, strconv.Itoa(id))
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	if maxSegSize == 0 {
		maxSegSize = defaultMaxSegmentSize
	}

	p := &Partition{
		dir:        dir,
		topic:      topic,
		ID:         id,
		maxSegSize: maxSegSize,
	}

	if err := p.loadSegments(); err != nil {
		return nil, err
	}

	return p, nil
}

func (p *Partition) loadSegments() error {
	entries, err := os.ReadDir(p.dir)
	if err != nil {
		return err
	}

	var baseOffsets []uint64
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".log") {
			offStr := strings.TrimSuffix(entry.Name(), ".log")
			off, err := strconv.ParseUint(offStr, 10, 64)
			if err == nil {
				baseOffsets = append(baseOffsets, off)
			}
		}
	}

	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})

	for _, off := range baseOffsets {
		seg, err := NewSegment(p.dir, off, p.maxSegSize)
		if err != nil {
			return err
		}
		p.segments = append(p.segments, seg)
	}

	if len(p.segments) == 0 {
		seg, err := NewSegment(p.dir, 0, p.maxSegSize)
		if err != nil {
			return err
		}
		p.segments = append(p.segments, seg)
	}

	p.activeSegment = p.segments[len(p.segments)-1]

	// Recover current offset from the count of entries in all segments
	// For simplicity in Phase 2, we scan segments to find the last offset.
	// In a real system, we'd store the last offset in a metadata file or just trust the latest segment index.
	var count uint64
	for _, seg := range p.segments {
		// Just a rough estimate for Phase 2: scan last segment if needed
		// But let's do it properly by checking index size and entries.
		entries := seg.index.size / entryWidth
		if entries > 0 {
			lastOff, _, _ := seg.index.Read(int64(entries - 1))
			count = lastOff + 1
		}
	}
	p.currentOffset = count

	return nil
}

// Append adds a new message, rolling the segment if full.
func (p *Partition) Append(key, value string) (uint64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.activeSegment.IsFull() {
		newSeg, err := NewSegment(p.dir, p.currentOffset, p.maxSegSize)
		if err != nil {
			return 0, err
		}
		p.segments = append(p.segments, newSeg)
		p.activeSegment = newSeg
	}

	msg := NewMessage(key, value)
	msg.Offset = p.currentOffset

	offset, err := p.activeSegment.Write(msg)
	if err != nil {
		return 0, err
	}

	p.currentOffset++
	return offset, nil
}

// CurrentOffset returns the next offset to be written.
func (p *Partition) CurrentOffset() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.currentOffset
}

// Fetch retrieves messages starting from a given offset.
func (p *Partition) Fetch(startOffset uint64, maxMessages uint64) ([]*Message, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	var messages []*Message
	for _, seg := range p.segments {
		// Check if startOffset is in this segment or later segments
		// This is a simple linear search; in production we'd use binary search on segments.
		
		// Find logical end offset of this segment (approx)
		numEntries := seg.index.size / entryWidth
		if numEntries == 0 {
			continue
		}
		
		lastOff, _, _ := seg.index.Read(int64(numEntries - 1))
		
		if startOffset > lastOff {
			continue
		}

		// Found the segment or segments to read from
		for i := int64(0); i < int64(numEntries); i++ {
			off, pos, err := seg.index.Read(i)
			if err != nil {
				continue
			}
			if off >= startOffset {
				msg, err := seg.ReadAt(pos)
				if err == nil {
					messages = append(messages, msg)
				}
				if uint64(len(messages)) >= maxMessages {
					return messages, nil
				}
			}
		}
	}

	return messages, nil
}

// TotalSize returns the total size of all segments in the partition.
func (p *Partition) TotalSize() uint64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	var total uint64
	for _, seg := range p.segments {
		total += seg.size
	}
	return total
}

// CheckRetention deletes segments if the total size exceeds maxSize.
func (p *Partition) CheckRetention(maxSize uint64) {
	if maxSize == 0 {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	var total uint64
	for _, seg := range p.segments {
		total += seg.size
	}

	// Delete from oldest (index 0) to newest, but ALWAYS keep at least one segment (active one)
	for len(p.segments) > 1 && total > maxSize {
		oldest := p.segments[0]
		total -= oldest.size
		
		fmt.Printf("🧹 Retention: Deleting old segment (BaseOffset: %d, Size: %d, Total: %d, Max: %d)\n", oldest.baseOffset, oldest.size, total, maxSize)
		oldest.Remove()
		
		p.segments = p.segments[1:]
	}
}

// Close closes all segments.
func (p *Partition) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, seg := range p.segments {
		seg.Close()
	}
	return nil
}

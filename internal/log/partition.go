package log

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/niteesh/gokafka/internal/protocol"
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

	// ISR management
	isrMu         sync.RWMutex
	ISR           []int // In-Sync Replicas (Broker IDs)
	ISRListeners  map[uint64]chan struct{} // Offset -> channel to notify
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
		ISRListeners: make(map[uint64]chan struct{}),
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
func (p *Partition) Append(key, value []byte) (uint64, error) {
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

	msg := &protocol.Message{
		Key:   key,
		Value: value,
	}
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

// Fetch retrieves messages starting from a given offset into a slice.
func (p *Partition) Fetch(startOffset uint64, maxMessages uint64) ([]*Message, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	var messages []*Message
	for _, seg := range p.segments {
		numEntries := seg.index.size / entryWidth
		if numEntries == 0 {
			continue
		}
		
		lastOff, _, _ := seg.index.Read(int64(numEntries - 1))
		if startOffset > lastOff {
			continue
		}

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

// FetchStream writes raw message bytes directly to the given writer for zero-copy streaming.
func (p *Partition) FetchStream(w io.Writer, startOffset uint64, maxMessages uint64) (int, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	count := 0
	for _, seg := range p.segments {
		numEntries := seg.index.size / entryWidth
		if numEntries == 0 {
			continue
		}

		lastOff, _, _ := seg.index.Read(int64(numEntries - 1))
		if startOffset > lastOff {
			continue
		}

		// Find start and end positions in this segment
		startPos := uint64(0)
		endPos := uint64(0)
		startFound := false

		for i := int64(0); i < int64(numEntries); i++ {
			off, pos, err := seg.index.Read(i)
			if err != nil {
				continue
			}

			if off >= startOffset {
				if !startFound {
					startPos = pos
					startFound = true
				}
				
				// Calculate size of this message: [Size(4) + Payload(var)]
				// We can read size from file, or just use index to find next pos.
				// For the last message in segment, we use seg.size.
				var nextPos uint64
				if i+1 < int64(numEntries) {
					_, nextPos, _ = seg.index.Read(i + 1)
				} else {
					nextPos = seg.size
				}
				endPos = nextPos
				count++
				
				if uint64(count) >= maxMessages {
					break
				}
			}
		}

		if startFound {
			size := uint32(endPos - startPos)
			if _, err := seg.WriteRawAt(w, startPos, size); err != nil {
				return count, err
			}
		}

		if uint64(count) >= maxMessages {
			break
		}
	}

	return count, nil
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

// UpdateISR updates the in-sync replicas list and notifies listeners if needed.
func (p *Partition) UpdateISR(newISR []int, minOffset uint64) {
	p.isrMu.Lock()
	p.ISR = newISR
	
	// Notify listeners that have been reached by this minOffset
	for off, ch := range p.ISRListeners {
		if off <= minOffset {
			close(ch)
			delete(p.ISRListeners, off)
		}
	}
	p.isrMu.Unlock()
}

// AwaitISR returns a channel that will be closed when the given offset is replicated to all ISR members.
func (p *Partition) AwaitISR(offset uint64) chan struct{} {
	p.isrMu.Lock()
	defer p.isrMu.Unlock()
	
	ch := make(chan struct{})
	p.ISRListeners[offset] = ch
	return ch
}

// SearchByTimestamp finds the earliest offset whose timestamp is >= targetTs.
func (p *Partition) SearchByTimestamp(targetTs int64) (uint64, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	for _, seg := range p.segments {
		off, err := seg.timeIndex.Search(targetTs)
		if err == nil {
			return off, nil
		}
	}

	return 0, fmt.Errorf("no offset found for timestamp")
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

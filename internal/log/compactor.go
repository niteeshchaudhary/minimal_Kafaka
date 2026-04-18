package log

import (
	"fmt"
	"os"
	"path/filepath"
)

// Compactor handles background log compaction by key.
type Compactor struct {
	dir string
}

func NewCompactor(dir string) *Compactor {
	return &Compactor{dir: dir}
}

// Compact merges segments in a partition, keeping only the latest message for each key.
func (c *Compactor) Compact(p *Partition) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// We only compact segments that are NOT the active one.
	if len(p.segments) <= 1 {
		return nil
	}

	segmentsToCompact := p.segments[:len(p.segments)-1]
	if len(segmentsToCompact) == 0 {
		return nil
	}

	// 1. Build a map of the latest offset for each key
	latestOffsets := make(map[string]uint64)
	for _, seg := range segmentsToCompact {
		numEntries := seg.index.size / entryWidth
		for i := int64(0); i < int64(numEntries); i++ {
			off, pos, err := seg.index.Read(i)
			if err != nil {
				continue
			}
			msg, err := seg.ReadAt(pos)
			if err != nil {
				continue
			}
			if len(msg.Key) > 0 {
				latestOffsets[string(msg.Key)] = off
			}
		}
	}

	// 2. Create a new compacted segment
	baseOffset := segmentsToCompact[0].baseOffset
	compactedPath := filepath.Join(p.dir, fmt.Sprintf("%020d.log.compact", baseOffset))
	indexPath := filepath.Join(p.dir, fmt.Sprintf("%020d.index.compact", baseOffset))
	
	newLogFile, err := os.OpenFile(compactedPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer newLogFile.Close()

	newIdx, err := NewIndex(indexPath)
	if err != nil {
		return err
	}
	defer newIdx.Close()

	var currentPos uint64 = 0
	for _, seg := range segmentsToCompact {
		numEntries := seg.index.size / entryWidth
		for i := int64(0); i < int64(numEntries); i++ {
			off, pos, err := seg.index.Read(i)
			if err != nil {
				continue
			}
			msg, err := seg.ReadAt(pos)
			if err != nil {
				continue
			}
			
			// Only keep if it's the latest offset for this key, or if it has no key (keep all non-keyed?)
			// In Kafka, compaction only applies to keyed messages.
			keep := false
			if len(msg.Key) == 0 {
				keep = true 
			} else if latestOffsets[string(msg.Key)] == off {
				keep = true
			}

			if keep {
				data := msg.Encode()
				if _, err := newLogFile.Write(data); err != nil {
					return err
				}
				if err := newIdx.Write(off, currentPos); err != nil {
					return err
				}
				currentPos += uint64(len(data))
			}
		}
	}

	// 3. Swap segments (This is tricky – need to ensure atomic swap or recovery logic)
	// For MVP Phase 3, we'll just remove old and add new.
	for _, seg := range segmentsToCompact {
		seg.Close()
		os.Remove(seg.logFile.Name())
		os.Remove(seg.index.file.Name())
	}

	// Rename compacted files to original names
	finalLogPath := filepath.Join(p.dir, fmt.Sprintf("%020d.log", baseOffset))
	finalIndexPath := filepath.Join(p.dir, fmt.Sprintf("%020d.index", baseOffset))
	os.Rename(compactedPath, finalLogPath)
	os.Rename(indexPath, finalIndexPath)

	// Re-load the new compacted segment
	newSeg, err := NewSegment(p.dir, baseOffset, p.maxSegSize)
	if err != nil {
		return err
	}

	p.segments = append([]*Segment{newSeg}, p.segments[len(segmentsToCompact):]...)
	
	return nil
}

package log

import (
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"sync"
)

// Topic manages a set of partitions.
type Topic struct {
	Name       string
	Partitions []*Partition
}

// NewTopic creates a new topic with a specified number of partitions.
func NewTopic(baseDir, name string, numPartitions int, maxSegSize uint64) (*Topic, error) {
	partitions := make([]*Partition, numPartitions)
	for i := 0; i < numPartitions; i++ {
		p, err := NewPartition(baseDir, name, i, maxSegSize)
		if err != nil {
			return nil, err
		}
		partitions[i] = p
	}
	return &Topic{
		Name:       name,
		Partitions: partitions,
	}, nil
}

// GetPartition selects a partition based on the key (hash-based).
// If key is empty, it defaults to partition 0 (for MVP simplicity, Phase 1).
func (t *Topic) GetPartition(key []byte) *Partition {
	if len(key) == 0 {
		return t.Partitions[0]
	}
	h := fnv.New32a()
	h.Write(key)
	idx := int(h.Sum32()) % len(t.Partitions)
	return t.Partitions[idx]
}

// StorageEngine manages multiple topics.
type StorageEngine struct {
	mu            sync.RWMutex
	baseDir       string
	topics        map[string]*Topic
	numPartitions int
	maxSegSize    uint64
}

// NewStorageEngine creates a new storage engine.
func NewStorageEngine(baseDir string, defaultPartitions int, maxSegSize uint64) *StorageEngine {
	if maxSegSize == 0 {
		maxSegSize = defaultMaxSegmentSize
	}
	return &StorageEngine{
		baseDir:       baseDir,
		topics:        make(map[string]*Topic),
		numPartitions: defaultPartitions,
		maxSegSize:    maxSegSize,
	}
}

func (s *StorageEngine) DataDir() string {
	return s.baseDir
}

// GetOrCreateTopic returns an existing topic or creates a new one.
func (s *StorageEngine) GetOrCreateTopic(name string) (*Topic, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if t, ok := s.topics[name]; ok {
		return t, nil
	}

	t, err := NewTopic(s.baseDir, name, s.numPartitions, s.maxSegSize)
	if err != nil {
		return nil, err
	}

	s.topics[name] = t
	return t, nil
}

// CreateTopic explicitly creates a topic with a specified number of partitions.
func (s *StorageEngine) CreateTopic(name string, numPartitions int) (*Topic, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.topics[name]; ok {
		return nil, fmt.Errorf("topic %s already exists", name)
	}

	t, err := NewTopic(s.baseDir, name, numPartitions, s.maxSegSize)
	if err != nil {
		return nil, err
	}

	s.topics[name] = t
	return t, nil
}

// DeleteTopic removes a topic and its data.
func (s *StorageEngine) DeleteTopic(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.topics[name]; !ok {
		return fmt.Errorf("topic %s not found", name)
	}

	topicDir := filepath.Join(s.baseDir, name)
	os.RemoveAll(topicDir)

	delete(s.topics, name)
	return nil
}

// ListTopics returns names of all active topics.
func (s *StorageEngine) ListTopics() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var names []string
	for name := range s.topics {
		names = append(names, name)
	}
	return names
}

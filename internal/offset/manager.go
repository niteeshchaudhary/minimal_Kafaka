package offset

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// OffsetManager handles storing and retrieving consumer offsets.
type OffsetManager struct {
	mu      sync.RWMutex
	path    string
	offsets map[string]uint64 // key format: group:topic:partition
}

// NewOffsetManager creates a new offset manager.
func NewOffsetManager(dataDir string) (*OffsetManager, error) {
	path := filepath.Join(dataDir, "offsets.json")
	om := &OffsetManager{
		path:    path,
		offsets: make(map[string]uint64),
	}
	if err := om.load(); err != nil {
		return nil, err
	}
	return om, nil
}

func (om *OffsetManager) load() error {
	data, err := os.ReadFile(om.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	return json.Unmarshal(data, &om.offsets)
}

func (om *OffsetManager) save() error {
	data, err := json.MarshalIndent(om.offsets, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(om.path, data, 0644)
}

func (om *OffsetManager) getKey(group, topic string, partition int) string {
	return fmt.Sprintf("%s:%s:%d", group, topic, partition)
}

// Commit stores a new offset.
func (om *OffsetManager) Commit(group, topic string, partition int, offset uint64) error {
	om.mu.Lock()
	defer om.mu.Unlock()

	key := om.getKey(group, topic, partition)
	om.offsets[key] = offset
	return om.save()
}

// Fetch retrieves the last committed offset.
func (om *OffsetManager) Fetch(group, topic string, partition int) uint64 {
	om.mu.RLock()
	defer om.mu.RUnlock()

	key := om.getKey(group, topic, partition)
	return om.offsets[key]
}

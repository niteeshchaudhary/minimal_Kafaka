package broker

import (
	"fmt"
	"sync"
	"time"

	"github.com/niteesh/gokafka/internal/log"
)

const (
	MaxReplicaLagMessages = 1000
	MaxReplicaLagTime     = 10 * time.Second
)

type ReplicaInfo struct {
	BrokerID      int
	LastOffset    uint64
	LastSeen      time.Time
}

type ReplicaManager struct {
	mu       sync.RWMutex
	replicas map[string]map[int]*ReplicaInfo // topic:partition -> brokerID -> info
	storage  *log.StorageEngine
}

func NewReplicaManager(storage *log.StorageEngine) *ReplicaManager {
	rm := &ReplicaManager{
		replicas: make(map[string]map[int]*ReplicaInfo),
		storage:  storage,
	}
	go rm.isrMonitor()
	return rm
}

func (rm *ReplicaManager) UpdateReplica(topic string, partitionID int, brokerID int, offset uint64) {
	key := fmt.Sprintf("%s:%d", topic, partitionID)
	rm.mu.Lock()
	if _, ok := rm.replicas[key]; !ok {
		rm.replicas[key] = make(map[int]*ReplicaInfo)
	}

	rm.replicas[key][brokerID] = &ReplicaInfo{
		BrokerID:   brokerID,
		LastOffset: offset,
		LastSeen:   time.Now(),
	}
	partitionReplicas := rm.replicas[key]
	rm.mu.Unlock()

	// Trigger immediate ISR check for this partition to reduce latency
	rm.checkPartitionISR(topic, partitionID, partitionReplicas)
}

func (rm *ReplicaManager) checkPartitionISR(topic string, partID int, partitionReplicas map[int]*ReplicaInfo) {
	t, _ := rm.storage.GetOrCreateTopic(topic)
	if partID >= len(t.Partitions) {
		return
	}
	p := t.Partitions[partID]
	currentOffset := p.CurrentOffset()

	var newISR []int
	minOffset := currentOffset

	for brokerID, info := range partitionReplicas {
		lagMessages := uint64(0)
		if currentOffset > info.LastOffset {
			lagMessages = currentOffset - info.LastOffset
		}
		lagTime := time.Since(info.LastSeen)

		if lagMessages <= MaxReplicaLagMessages && lagTime <= MaxReplicaLagTime {
			newISR = append(newISR, brokerID)
			if info.LastOffset < minOffset {
				minOffset = info.LastOffset
			}
		}
	}
	p.UpdateISR(newISR, minOffset)
}

func (rm *ReplicaManager) isrMonitor() {
	ticker := time.NewTicker(5 * time.Second)
	for range ticker.C {
		rm.mu.Lock()
		// Copy map to avoid holding lock during I/O/logic
		replicasCopy := make(map[string]map[int]*ReplicaInfo)
		for k, v := range rm.replicas {
			replicasCopy[k] = v
		}
		rm.mu.Unlock()

		for key, partitionReplicas := range replicasCopy {
			var topic string
			var partID int
			fmt.Sscanf(key, "%s:%d", &topic, &partID)
			rm.checkPartitionISR(topic, partID, partitionReplicas)
		}
	}
}

package coordinator

import (
	"sync"
)

type BrokerInfo struct {
	ID   int    `json:"id"`
	Addr string `json:"addr"`
}

type ClusterMetadata struct {
	mu      sync.RWMutex
	Brokers map[int]BrokerInfo
	Leaders map[string]map[int]int // topic -> partition -> leaderID
}

type Controller struct {
	BrokerID int
	Metadata *ClusterMetadata
}

func NewController(id int) *Controller {
	return &Controller{
		BrokerID: id,
		Metadata: &ClusterMetadata{
			Brokers: make(map[int]BrokerInfo),
			Leaders: make(map[string]map[int]int),
		},
	}
}

// In a real system, the controller would monitor health and trigger elections.
// For our Phase 2, we'll implement a centralized metadata registry.

func (c *Controller) UpdateMetadata(brokers []BrokerInfo) {
	c.Metadata.mu.Lock()
	defer c.Metadata.mu.Unlock()
	
	for _, b := range brokers {
		c.Metadata.Brokers[b.ID] = b
	}
}

func (c *Controller) ElectLeaders(topic string, partitions int, activeIDs []int) {
	c.Metadata.mu.Lock()
	defer c.Metadata.mu.Unlock()

	if _, ok := c.Metadata.Leaders[topic]; !ok {
		c.Metadata.Leaders[topic] = make(map[int]int)
	}

	for i := 0; i < partitions; i++ {
		leaderID := activeIDs[i%len(activeIDs)]
		c.Metadata.Leaders[topic][i] = leaderID
	}
}

package coordinator

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	defaultHeartbeatInterval = 3 * time.Second
	defaultSessionTimeout   = 10 * time.Second
)

// Member represents a consumer instance in a group.
type Member struct {
	ID                 string
	LastHeartbeat      time.Time
	AssignedPartitions []int
}

// Group represents a consumer group.
type Group struct {
	mu            sync.Mutex
	Name          string
	Topic         string
	Members       map[string]*Member
	NumPartitions int
}

// GroupCoordinator manages consumer groups and partition assignments.
type GroupCoordinator struct {
	mu     sync.RWMutex
	path   string
	groups map[string]*Group
}

// NewGroupCoordinator creates a new coordinator and loads existing state.
func NewGroupCoordinator(dataDir string) *GroupCoordinator {
	path := filepath.Join(dataDir, "coordinator.json")
	gc := &GroupCoordinator{
		path:   path,
		groups: make(map[string]*Group),
	}
	gc.load()
	go gc.cleanupStaleMembers()
	return gc
}

func (gc *GroupCoordinator) JoinGroup(groupName, memberID, topic string, numPartitions int) ([]int, string, error) {
	gc.mu.Lock()
	g, ok := gc.groups[groupName]
	if !ok {
		g = &Group{
			Name:    groupName,
			Topic:   topic,
			Members: make(map[string]*Member),
		}
		gc.groups[groupName] = g
	}
	g.Topic = topic
	g.NumPartitions = numPartitions
	gc.mu.Unlock()

	g.mu.Lock()
	defer g.mu.Unlock()

	if memberID == "" {
		memberID = fmt.Sprintf("%s-%d", groupName, time.Now().UnixNano())
	}

	g.Members[memberID] = &Member{
		ID:            memberID,
		LastHeartbeat: time.Now(),
	}

	gc.rebalance(g, numPartitions)
	gc.save()
	return g.Members[memberID].AssignedPartitions, memberID, nil
}

// Heartbeat refreshes a member's lease and returns current assigned partitions.
func (gc *GroupCoordinator) Heartbeat(groupName, memberID string) ([]int, bool) {
	gc.mu.RLock()
	g, ok := gc.groups[groupName]
	gc.mu.RUnlock()
	if !ok {
		return nil, false
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	if m, ok := g.Members[memberID]; ok {
		m.LastHeartbeat = time.Now()
		return m.AssignedPartitions, true
	}
	return nil, false
}

// LeaveGroup removes a member gracefully.
func (gc *GroupCoordinator) LeaveGroup(groupName, memberID, topic string, numPartitions int) {
	gc.mu.RLock()
	g, ok := gc.groups[groupName]
	gc.mu.RUnlock()
	if !ok {
		return
	}

	g.mu.Lock()
	delete(g.Members, memberID)
	// Trigger rebalance on leave
	gc.rebalance(g, numPartitions)
	g.mu.Unlock()
	gc.save()
}

func (gc *GroupCoordinator) ListGroups() []*Group {
	gc.mu.RLock()
	defer gc.mu.RUnlock()
	var list []*Group
	for _, g := range gc.groups {
		list = append(list, g)
	}
	return list
}

func (gc *GroupCoordinator) GetGroup(name string) *Group {
	gc.mu.RLock()
	defer gc.mu.RUnlock()
	return gc.groups[name]
}

func (gc *GroupCoordinator) rebalance(g *Group, numPartitions int) map[string][]int {
	// Simple Round-Robin Assignment
	memberIDs := make([]string, 0, len(g.Members))
	for id := range g.Members {
		memberIDs = append(memberIDs, id)
	}

	assignments := make(map[string][]int)
	if len(memberIDs) == 0 {
		return assignments
	}

	for i := 0; i < numPartitions; i++ {
		mID := memberIDs[i%len(memberIDs)]
		assignments[mID] = append(assignments[mID], i)
	}

	for id, m := range g.Members {
		m.AssignedPartitions = assignments[id]
	}

	return assignments
}

func (gc *GroupCoordinator) cleanupStaleMembers() {
	for {
		time.Sleep(defaultHeartbeatInterval)
		gc.mu.Lock()
		for _, g := range gc.groups {
			g.mu.Lock()
			rebalanceNeeded := false
			for id, m := range g.Members {
				if time.Since(m.LastHeartbeat) > defaultSessionTimeout {
					delete(g.Members, id)
					rebalanceNeeded = true
				}
			}
			if rebalanceNeeded {
				// We don't know numPartitions here easily without more state,
				// so for Phase 3 we'll assume topic info is passed or stored.
				// For the MVP, we'll just log or handle it when the next request comes.
				// Better approach: store numPartitions in the group for the active topic.
			}
			g.mu.Unlock()
		}
		gc.mu.Unlock()
	}
}

// GetAssignment returns current partitions for a member.
func (gc *GroupCoordinator) GetAssignment(groupName, memberID string) []int {
	gc.mu.RLock()
	g, ok := gc.groups[groupName]
	gc.mu.RUnlock()
	if !ok {
		return nil
	}

	g.mu.Lock()
	defer g.mu.Unlock()
	if m, ok := g.Members[memberID]; ok {
		return m.AssignedPartitions
	}
	return nil
}

func (gc *GroupCoordinator) save() error {
	gc.mu.RLock()
	defer gc.mu.RUnlock()

	data, err := json.MarshalIndent(gc.groups, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(gc.path, data, 0644)
}

func (gc *GroupCoordinator) load() error {
	data, err := os.ReadFile(gc.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	var groups map[string]*Group
	if err := json.Unmarshal(data, &groups); err != nil {
		return err
	}

	gc.mu.Lock()
	gc.groups = groups
	gc.mu.Unlock()
	return nil
}

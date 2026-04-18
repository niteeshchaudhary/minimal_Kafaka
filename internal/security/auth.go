package security

import (
	"fmt"
	"net/http"
	"sync"
)

type Permission string

const (
	PermRead  Permission = "Read"
	PermWrite Permission = "Write"
	PermAdmin Permission = "Admin"
)

type ACLInfo struct {
	Topic      string     `json:"topic"`
	User       string     `json:"user"`
	Permission Permission `json:"permission"`
}

type SecurityManager struct {
	mu   sync.RWMutex
	acls map[string]map[string]Permission
}

func NewSecurityManager() *SecurityManager {
	return &SecurityManager{
		acls: make(map[string]map[string]Permission),
	}
}

func (s *SecurityManager) AddACL(topic, user string, perm Permission) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.acls[topic]; !ok {
		s.acls[topic] = make(map[string]Permission)
	}
	s.acls[topic][user] = perm
}

func (s *SecurityManager) RemoveACL(topic, user string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if t, ok := s.acls[topic]; ok {
		delete(t, user)
		if len(t) == 0 {
			delete(s.acls, topic)
		}
	}
}

func (s *SecurityManager) ListACLs() []ACLInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	list := []ACLInfo{}
	for topic, users := range s.acls {
		for user, perm := range users {
			list = append(list, ACLInfo{
				Topic:      topic,
				User:       user,
				Permission: perm,
			})
		}
	}
	return list
}

func (s *SecurityManager) Authorize(r *http.Request, topic string, required Permission) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	// Simplified auth: check "X-User" header
	user := r.Header.Get("X-User")
	if user == "" {
		return fmt.Errorf("authentication required (X-User header missing)")
	}

	topicACLs, ok := s.acls[topic]
	if !ok {
		return nil 
	}

	perm, ok := topicACLs[user]
	if !ok {
		return fmt.Errorf("user %s not authorized for topic %s", user, topic)
	}

	if perm != required && perm != PermAdmin {
		return fmt.Errorf("insufficient permissions: required %s, got %s", required, perm)
	}

	return nil
}

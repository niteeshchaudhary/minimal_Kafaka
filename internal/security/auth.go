package security

import (
	"fmt"
	"net/http"
)

type Permission string

const (
	PermRead  Permission = "Read"
	PermWrite Permission = "Write"
)

type SecurityManager struct {
	// topic -> user -> permission
	acls map[string]map[string]Permission
}

func NewSecurityManager() *SecurityManager {
	return &SecurityManager{
		acls: make(map[string]map[string]Permission),
	}
}

func (s *SecurityManager) AddACL(topic, user string, perm Permission) {
	if _, ok := s.acls[topic]; !ok {
		s.acls[topic] = make(map[string]Permission)
	}
	s.acls[topic][user] = perm
}

func (s *SecurityManager) Authorize(r *http.Request, topic string, required Permission) error {
	// Simplified auth: check "X-User" header
	user := r.Header.Get("X-User")
	if user == "" {
		return fmt.Errorf("authentication required (X-User header missing)")
	}

	// For MVP, if no ACL exists for topic, allow all. 
	// In production, it would be "deny by default".
	topicACLs, ok := s.acls[topic]
	if !ok {
		return nil 
	}

	perm, ok := topicACLs[user]
	if !ok {
		return fmt.Errorf("user %s not authorized for topic %s", user, topic)
	}

	if perm != required && perm != "Admin" {
		return fmt.Errorf("insufficient permissions: required %s, got %s", required, perm)
	}

	return nil
}

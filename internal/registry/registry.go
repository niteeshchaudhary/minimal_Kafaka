package registry

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
)

type Schema struct {
	ID      int    `json:"id"`
	Subject string `json:"subject"`
	Version int    `json:"version"`
	Schema  string `json:"schema"`
}

type SchemaRegistry struct {
	mu      sync.RWMutex
	schemas map[string][]Schema // subject -> versions
	nextID  int
}

func NewSchemaRegistry() *SchemaRegistry {
	return &SchemaRegistry{
		schemas: make(map[string][]Schema),
		nextID:  1,
	}
}

func (r *SchemaRegistry) Register(subject, schemaText string) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	versions := r.schemas[subject]
	newVersion := len(versions) + 1
	
	s := Schema{
		ID:      r.nextID,
		Subject: subject,
		Version: newVersion,
		Schema:  schemaText,
	}
	
	r.schemas[subject] = append(versions, s)
	r.nextID++
	return s.ID, nil
}

func (r *SchemaRegistry) GetLatest(subject string) (*Schema, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	versions, ok := r.schemas[subject]
	if !ok || len(versions) == 0 {
		return nil, fmt.Errorf("subject not found")
	}
	return &versions[len(versions)-1], nil
}

func (r *SchemaRegistry) ListSubjects() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	list := []string{}
	for s := range r.schemas {
		list = append(list, s)
	}
	return list
}

func (r *SchemaRegistry) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	// Simple REST API
	if req.Method == http.MethodPost {
		var body struct {
			Subject string `json:"subject"`
			Schema  string `json:"schema"`
		}
		if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		id, err := r.Register(body.Subject, body.Schema)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		json.NewEncoder(w).Encode(map[string]int{"id": id})
		return
	}

	if req.Method == http.MethodGet {
		subject := req.URL.Query().Get("subject")
		if subject == "" {
			subjects := r.ListSubjects()
			json.NewEncoder(w).Encode(subjects)
			return
		}
		s, err := r.GetLatest(subject)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		json.NewEncoder(w).Encode(s)
		return
	}
}

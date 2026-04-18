package broker

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/niteesh/gokafka/internal/coordinator"
	"github.com/niteesh/gokafka/internal/log"
	"github.com/niteesh/gokafka/internal/metrics"
	"github.com/niteesh/gokafka/internal/offset"
	"github.com/niteesh/gokafka/internal/registry"
	"github.com/niteesh/gokafka/internal/security"
)

// Broker represents the HTTP server layer.
type Broker struct {
	ID          int
	Addr        string
	Peers       map[int]string
	activeBrokers map[int]bool
	muActive      sync.RWMutex
	storage     *log.StorageEngine
	coordinator *coordinator.GroupCoordinator
	offsets     *offset.OffsetManager
	replicas    *ReplicaManager
	security    *security.SecurityManager
	metrics     *metrics.MetricsManager
	registry    *registry.SchemaRegistry
	certFile    string
	keyFile     string
}

// NewBroker initializes a new broker with storage, coordinator, and offsets.
func NewBroker(id int, addr string, peers map[int]string, dataDir string, maxSegSize uint64) *Broker {
	storage := log.NewStorageEngine(dataDir, 3, maxSegSize)
	om, _ := offset.NewOffsetManager(dataDir)
	return &Broker{
		ID:            id,
		Addr:          addr,
		Peers:         peers,
		activeBrokers: make(map[int]bool),
		storage:       storage,
		coordinator:   coordinator.NewGroupCoordinator(dataDir),
		offsets:       om,
		replicas:      NewReplicaManager(storage),
		security:      security.NewSecurityManager(),
		metrics:       metrics.DefaultMetrics,
		registry:      registry.NewSchemaRegistry(),
	}
}

// SetTLS configures the broker to use TLS.
func (b *Broker) SetTLS(certFile, keyFile string) {
	b.certFile = certFile
	b.keyFile = keyFile
}

// ServeHTTP handles routing for the broker.
func (b *Broker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	mux := http.NewServeMux()
	
	// Middleware for Authorization
	authWrap := func(topicParam string, perm security.Permission, next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			topic := r.URL.Query().Get(topicParam)
			if topic == "" && r.Method == http.MethodPost {
				// Try reading from JSON body for some handlers? 
				// For simplicity, we assume topic is in query or we skip check if topic is unknown.
			}
			if topic != "" {
				if err := b.security.Authorize(r, topic, perm); err != nil {
					http.Error(w, err.Error(), http.StatusForbidden)
					return
				}
			}
			next(w, r)
		}
	}

	mux.HandleFunc("/consume", authWrap("topic", security.PermRead, b.handleConsume))
	mux.HandleFunc("/consume/binary", authWrap("topic", security.PermRead, b.handleConsumeBinary))
	mux.HandleFunc("/produce", authWrap("topic", security.PermWrite, b.handleProduce))
	mux.HandleFunc("/produce/binary", authWrap("topic", security.PermWrite, b.handleProduceBinary))
	mux.HandleFunc("/produce/batch", authWrap("topic", security.PermWrite, b.handleProduceBatch))
	
	mux.HandleFunc("/topics", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			b.handleCreateTopic(w, r)
		} else if r.Method == http.MethodDelete {
			b.handleDeleteTopic(w, r)
		} else {
			b.handleListTopics(w, r)
		}
	})
	mux.HandleFunc("/metadata", b.handleMetadata)
	mux.HandleFunc("/acls", b.handleACLs)
	mux.HandleFunc("/metrics", b.metrics.ServeHTTP)
	mux.HandleFunc("/subjects", b.registry.ServeHTTP)
	mux.HandleFunc("/ping", b.handlePing)
	mux.HandleFunc("/groups", b.handleGroups)
	mux.HandleFunc("/security/tls", b.handleTLSConfig)
	mux.ServeHTTP(w, r)
}

type MetadataResponse struct {
	Brokers map[int]string           `json:"brokers"`
	Topics  map[string]TopicMetadata `json:"topics"`
}

type TopicMetadata struct {
	Name       string              `json:"name"`
	Partitions []PartitionMetadata `json:"partitions"`
}

type PartitionMetadata struct {
	ID       int   `json:"id"`
	Leader   int   `json:"leader"`
	Replicas []int `json:"replicas"`
	ISR      []int `json:"isr"`
}

type JSONMessage struct {
	Offset    uint64 `json:"offset"`
	Timestamp int64  `json:"timestamp"`
	Key       string `json:"key"`
	Value     string `json:"value"`
}

func (b *Broker) handleMetadata(w http.ResponseWriter, r *http.Request) {
	topics := b.storage.ListTopics()
	res := MetadataResponse{
		Brokers: make(map[int]string),
		Topics:  make(map[string]TopicMetadata),
	}

	res.Brokers[b.ID] = b.Addr
	
	b.muActive.RLock()
	var activeIDs []int
	activeIDs = append(activeIDs, b.ID)
	for id, addr := range b.Peers {
		if b.activeBrokers[id] {
			res.Brokers[id] = addr
			activeIDs = append(activeIDs, id)
		}
	}
	b.muActive.RUnlock()

	sort.Ints(activeIDs)
	numActive := len(activeIDs)

	for _, tName := range topics {
		t, _ := b.storage.GetOrCreateTopic(tName)
		tm := TopicMetadata{
			Name: tName,
		}
		for i := 0; i < len(t.Partitions); i++ {
			p := t.Partitions[i]
			// Dynamic leadership: Select from ACTIVE brokers only
			leaderIdx := i % numActive
			tm.Partitions = append(tm.Partitions, PartitionMetadata{
				ID:       i,
				Leader:   activeIDs[leaderIdx],
				Replicas: activeIDs,
				ISR:      p.ISR,
			})
		}
		res.Topics[tName] = tm
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(res)
}

func (b *Broker) startReplication() {
	ticker := time.NewTicker(2 * time.Second)
	for range ticker.C {
		topics := b.storage.ListTopics()
		
		b.muActive.RLock()
		var activeIDs []int
		activeIDs = append(activeIDs, b.ID)
		for id := range b.Peers {
			if b.activeBrokers[id] {
				activeIDs = append(activeIDs, id)
			}
		}
		b.muActive.RUnlock()
		
		sort.Ints(activeIDs)
		numActive := len(activeIDs)
		if numActive == 0 { continue }

		for _, tName := range topics {
			t, _ := b.storage.GetOrCreateTopic(tName)
			for i := 0; i < len(t.Partitions); i++ {
				leaderID := activeIDs[i%numActive]
				if leaderID != b.ID {
					leaderAddr, ok := b.Peers[leaderID]
					if ok {
						go b.syncPartition(tName, i, leaderAddr)
					}
				}
			}
		}
	}
}

func (b *Broker) syncPartition(topic string, partition int, leaderAddr string) {
	t, _ := b.storage.GetOrCreateTopic(topic)
	p := t.Partitions[partition]

	// Fetch from our current high-water mark
	offset := p.CurrentOffset()

	resp, err := http.Get(fmt.Sprintf("http://%s/consume/binary?topic=%s&partition=%d&offset=%d&replica_id=%d", leaderAddr, topic, partition, offset, b.ID))
	if err != nil {
		return
	}
	defer resp.Body.Close()

	// Since it's binary, we might need to decode multiple messages.
	// For simplicity in Phase 2, we skip full batch decoding here and just append row by row if we can.
	// But let's do it properly by reading the binary stream.
	for {
		dataSizeBuf := make([]byte, 4)
		_, err := io.ReadFull(resp.Body, dataSizeBuf)
		if err != nil {
			break
		}
		dataSize := binary.BigEndian.Uint32(dataSizeBuf)
		data := make([]byte, dataSize)
		_, err = io.ReadFull(resp.Body, data)
		if err != nil {
			break
		}
		
		msg, err := log.UnmarshalMessage(data)
		if err == nil {
			p.Append(msg.Key, msg.Value)
		}
	}
}

type OffsetCommitRequest struct {
	Group     string `json:"group"`
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Offset    uint64 `json:"offset"`
}

type OffsetFetchResponse struct {
	Offset uint64 `json:"offset"`
}

func (b *Broker) handleOffsetCommit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req OffsetCommitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := b.offsets.Commit(req.Group, req.Topic, req.Partition, req.Offset); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (b *Broker) handleOffsetFetch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	group := r.URL.Query().Get("group")
	topicName := r.URL.Query().Get("topic")
	partitionStr := r.URL.Query().Get("partition")
	partitionID, _ := strconv.Atoi(partitionStr)

	offset := b.offsets.Fetch(group, topicName, partitionID)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(OffsetFetchResponse{
		Offset: offset,
	})
}

type GroupJoinRequest struct {
	Group  string `json:"group"`
	Member string `json:"member,omitempty"`
	Topic  string `json:"topic"`
}

type GroupJoinResponse struct {
	Member             string `json:"member"`
	AssignedPartitions []int  `json:"assigned_partitions"`
}

type GroupHeartbeatRequest struct {
	Group  string `json:"group"`
	Member string `json:"member"`
}

func (b *Broker) handleGroupJoin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req GroupJoinRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	topic, err := b.storage.GetOrCreateTopic(req.Topic)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	assigned, memberID, err := b.coordinator.JoinGroup(req.Group, req.Member, req.Topic, len(topic.Partitions))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(GroupJoinResponse{
		Member:             memberID,
		AssignedPartitions: assigned,
	})
}

func (b *Broker) handleGroupHeartbeat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req GroupHeartbeatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	assigned, ok := b.coordinator.Heartbeat(req.Group, req.Member)
	if !ok {
		http.Error(w, "Member not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"assigned_partitions": assigned,
	})
}

func (b *Broker) handleGroupLeave(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req GroupJoinRequest // reuse struct for convenience
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	topic, err := b.storage.GetOrCreateTopic(req.Topic)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	b.coordinator.LeaveGroup(req.Group, req.Member, req.Topic, len(topic.Partitions))
	w.WriteHeader(http.StatusNoContent)
}

func (b *Broker) handleGroups(w http.ResponseWriter, r *http.Request) {
	groups := b.coordinator.ListGroups()
	
	type GroupInfo struct {
		Name       string         `json:"name"`
		Topic      string         `json:"topic"`
		Members    []string       `json:"members"`
		TotalLag   uint64         `json:"total_lag"`
		Partitions map[int]uint64 `json:"partition_lag"`
	}

	res := []GroupInfo{}
	for _, g := range groups {
		info := GroupInfo{
			Name:       g.Name,
			Topic:      g.Topic,
			Members:    make([]string, 0),
			Partitions: make(map[int]uint64),
		}
		for id := range g.Members {
			info.Members = append(info.Members, id)
		}

		// Calculate lag
		if g.Topic != "" {
			t, _ := b.storage.GetOrCreateTopic(g.Topic)
			for i := 0; i < len(t.Partitions); i++ {
				current := t.Partitions[i].CurrentOffset()
				committed := b.offsets.Fetch(g.Name, g.Topic, i)
				lag := uint64(0)
				if current > committed {
					lag = current - committed
				}
				info.Partitions[i] = lag
				info.TotalLag += lag
			}
		}
		res = append(res, info)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(res)
}

func (b *Broker) handleProduceBatch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req ProduceBatchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if req.Topic == "" || len(req.Messages) == 0 {
		http.Error(w, "Topic and messages are required", http.StatusBadRequest)
		return
	}

	topic, err := b.storage.GetOrCreateTopic(req.Topic)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var offsets []uint64
	var partitionID int
	for _, m := range req.Messages {
		keyBytes := []byte(m.Key)
		valBytes := []byte(m.Value)
		partition := topic.GetPartition(keyBytes)
		partitionID = partition.ID 
		offset, err := partition.Append(keyBytes, valBytes)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		offsets = append(offsets, offset)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(ProduceResponse{
		Topic:       req.Topic,
		PartitionID: partitionID,
		Offsets:     offsets,
	})
	b.metrics.IncProduce()
}

type ProduceRequest struct {
	Topic string `json:"topic"`
	Key   string `json:"key"`
	Value string `json:"value"`
	Acks  string `json:"acks,omitempty"`
}

type ProduceBatchRequest struct {
	Topic    string           `json:"topic"`
	Messages []ProduceRequest `json:"messages"`
}

type ProduceResponse struct {
	Topic       string   `json:"topic"`
	PartitionID int      `json:"partition"`
	Offset      uint64   `json:"offset"`
	Offsets     []uint64 `json:"offsets,omitempty"`
}

func (b *Broker) handleProduce(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req ProduceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if req.Topic == "" || req.Value == "" {
		http.Error(w, "Topic and Value are required", http.StatusBadRequest)
		return
	}

	topic, err := b.storage.GetOrCreateTopic(req.Topic)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	keyBytes := []byte(req.Key)
	valBytes := []byte(req.Value)
	partition := topic.GetPartition(keyBytes)
	offset, err := partition.Append(keyBytes, valBytes)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if req.Acks == "all" {
		select {
		case <-partition.AwaitISR(offset):
		case <-time.After(5 * time.Second):
			// Timeout to prevent UI hang if ISR is not met
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(ProduceResponse{
		Topic:       req.Topic,
		PartitionID: partition.ID,
		Offset:      offset,
	})
	b.metrics.IncProduce()
}

func (b *Broker) handleConsume(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	query := r.URL.Query()
	topicName := query.Get("topic")
	partitionStr := query.Get("partition")
	offsetStr := query.Get("offset")

	if topicName == "" || partitionStr == "" || offsetStr == "" {
		http.Error(w, "topic, partition, and offset are required", http.StatusBadRequest)
		return
	}

	partitionID, _ := strconv.Atoi(partitionStr)
	offset, _ := strconv.ParseUint(offsetStr, 10, 64)

	topic, err := b.storage.GetOrCreateTopic(topicName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if partitionID < 0 || partitionID >= len(topic.Partitions) {
		http.Error(w, "Invalid partition", http.StatusBadRequest)
		return
	}

	messages, err := topic.Partitions[partitionID].Fetch(offset, 100)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	jsonMessages := make([]JSONMessage, 0, len(messages))
	for _, m := range messages {
		jsonMessages = append(jsonMessages, JSONMessage{
			Offset:    m.Offset,
			Timestamp: m.Timestamp,
			Key:       string(m.Key),
			Value:     string(m.Value),
		})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(jsonMessages)
	b.metrics.IncConsume()
}

func (b *Broker) handleConsumeBinary(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	topicName := query.Get("topic")
	partitionStr := query.Get("partition")
	offsetStr := query.Get("offset")
	limitStr := query.Get("limit")

	partitionID, _ := strconv.Atoi(partitionStr)
	offset, _ := strconv.ParseUint(offsetStr, 10, 64)
	limit, _ := strconv.ParseUint(limitStr, 10, 64)
	if limit == 0 {
		limit = 100
	}

	topic, err := b.storage.GetOrCreateTopic(topicName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	p := topic.Partitions[partitionID]
	w.Header().Set("Content-Type", "application/octet-stream")
	
	replicaIDStr := query.Get("replica_id")
	if replicaIDStr != "" {
		replicaID, _ := strconv.Atoi(replicaIDStr)
		b.replicas.UpdateReplica(topicName, partitionID, replicaID, offset)
	}

	count, err := p.FetchStream(w, offset, limit)
	if err != nil {
		fmt.Printf("Error streaming: %v\n", err)
	}
	fmt.Printf("Streamed %d messages for %s:%d\n", count, topicName, partitionID)
}

func (b *Broker) handleProduceBinary(w http.ResponseWriter, r *http.Request) {
	topicName := r.URL.Query().Get("topic")
	partitionStr := r.URL.Query().Get("partition") // Optional, defaults to key-based
	
	topic, err := b.storage.GetOrCreateTopic(topicName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Read binary message from body
	defer r.Body.Close()
	data, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read body", http.StatusBadRequest)
		return
	}

	msg, err := log.UnmarshalMessage(data)
	if err != nil {
		http.Error(w, "Invalid binary message format", http.StatusBadRequest)
		return
	}

	acks := r.URL.Query().Get("acks")

	var p *log.Partition
	if partitionStr != "" {
		pID, _ := strconv.Atoi(partitionStr)
		p = topic.Partitions[pID]
	} else {
		p = topic.GetPartition(msg.Key)
	}

	offset, err := p.Append(msg.Key, msg.Value)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if acks == "all" {
		<-p.AwaitISR(offset)
	}

	b.metrics.IncProduce()

	w.Header().Set("X-Offset", strconv.FormatUint(offset, 10))
	w.WriteHeader(http.StatusCreated)
}

func (b *Broker) handleListTopics(w http.ResponseWriter, r *http.Request) {
	topics := b.storage.ListTopics()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(topics)
}

type CreateTopicRequest struct {
	Name       string `json:"name"`
	Partitions int    `json:"partitions"`
}

func (b *Broker) handleCreateTopic(w http.ResponseWriter, r *http.Request) {
	var req CreateTopicRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if req.Name == "" {
		http.Error(w, "name is required", http.StatusBadRequest)
		return
	}
	if req.Partitions <= 0 {
		req.Partitions = 3
	}

	_, err := b.storage.CreateTopic(req.Name, req.Partitions)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
}

func (b *Broker) handleDeleteTopic(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("name")
	if name == "" {
		http.Error(w, "name is required", http.StatusBadRequest)
		return
	}

	err := b.storage.DeleteTopic(name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (b *Broker) handleACLs(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		acls := b.security.ListACLs()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(acls)
	case http.MethodPost:
		var req struct {
			Topic      string              `json:"topic"`
			User       string              `json:"user"`
			Permission security.Permission `json:"permission"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		b.security.AddACL(req.Topic, req.User, req.Permission)
		w.WriteHeader(http.StatusCreated)
	case http.MethodDelete:
		topic := r.URL.Query().Get("topic")
		user := r.URL.Query().Get("user")
		if topic == "" || user == "" {
			http.Error(w, "topic and user are required", http.StatusBadRequest)
			return
		}
		b.security.RemoveACL(topic, user)
		w.WriteHeader(http.StatusNoContent)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (b *Broker) handleTLSConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Cert string `json:"cert"`
		Key  string `json:"key"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// For MVP: Save to data directory and update state
	certPath := filepath.Join(b.storage.DataDir(), "server.crt")
	keyPath := filepath.Join(b.storage.DataDir(), "server.key")

	if err := os.WriteFile(certPath, []byte(req.Cert), 0644); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := os.WriteFile(keyPath, []byte(req.Key), 0600); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	b.SetTLS(certPath, keyPath)
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "TLS configuration updated. Restart broker for full effect.")
}

// Run starts the HTTP server.
func (b *Broker) Run(port int) error {
	addr := fmt.Sprintf(":%d", port)
	fmt.Printf("Broker %d starting on port %d...\n", b.ID, port)
	go b.startHealthCheck()
	go b.startReplication()
	go b.startRetentionChecker()

	if b.certFile != "" && b.keyFile != "" {
		fmt.Printf("Broker %d running with TLS (HTTPS)\n", b.ID)
		return http.ListenAndServeTLS(addr, b.certFile, b.keyFile, b)
	}
	return http.ListenAndServe(addr, b)
}

func (b *Broker) startRetentionChecker() {
	ticker := time.NewTicker(30 * time.Second)
	// Small retention size for MVP/Testing: 50MB
	maxSize := uint64(50 * 1024 * 1024)
	for range ticker.C {
		for _, tName := range b.storage.ListTopics() {
			t, _ := b.storage.GetOrCreateTopic(tName)
			for _, p := range t.Partitions {
				p.CheckRetention(maxSize)
			}
		}
	}
}

func (b *Broker) handlePing(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func (b *Broker) startHealthCheck() {
	ticker := time.NewTicker(2 * time.Second)
	client := &http.Client{Timeout: 1 * time.Second}
	for range ticker.C {
		for id, addr := range b.Peers {
			resp, err := client.Get(fmt.Sprintf("http://%s/ping", addr))
			b.muActive.Lock()
			if err == nil && resp.StatusCode == http.StatusOK {
				b.activeBrokers[id] = true
			} else {
				if b.activeBrokers[id] {
					fmt.Printf("⚠️ Broker %d detected as DOWN\n", id)
				}
				b.activeBrokers[id] = false
			}
			if resp != nil {
				resp.Body.Close()
			}
			b.muActive.Unlock()
		}
	}
}

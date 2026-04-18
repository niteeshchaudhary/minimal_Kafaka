package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"hash/fnv"
)

type ProduceRequest struct {
	Topic string `json:"topic"`
	Key   string `json:"key"`
	Value string `json:"value"`
}

type ProduceBatchRequest struct {
	Topic    string           `json:"topic"`
	Messages []ProduceRequest `json:"messages"`
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
	ID     int   `json:"id"`
	Leader int   `json:"leader"`
}

func main() {
	brokerURL := flag.String("broker", "http://localhost:9092", "Initial Broker URL")
	topic := flag.String("topic", "", "Topic name")
	key := flag.String("key", "", "Message key (optional)")
	value := flag.String("value", "", "Message value")
	batchSize := flag.Int("batch-size", 1, "Number of messages to send in a batch")
	flag.Parse()

	if *topic == "" || (*value == "" && *batchSize == 1) {
		log.Fatal("Topic and Value are required")
	}

	// 1. Fetch Metadata
	metadata := fetchMetadata(*brokerURL)
	topicMeta, ok := metadata.Topics[*topic]
	if !ok {
		log.Fatalf("Topic %s not found in metadata", *topic)
	}

	// 2. Select Partition and Leader
	partitionID := selectPartition(*key, len(topicMeta.Partitions))
	leaderID := topicMeta.Partitions[partitionID].Leader
	leaderAddr, ok := metadata.Brokers[leaderID]
	if !ok {
		log.Fatalf("Leader %d not found in broker list", leaderID)
	}

	fmt.Printf("🎯 Routing to Leader Broker %d (%s) for Partition %d\n", leaderID, leaderAddr, partitionID)
	produceURL := fmt.Sprintf("http://%s", leaderAddr)

	// 3. Produce
	var urlPath string
	var reqBody []byte

	if *batchSize > 1 {
		urlPath = "/produce/batch"
		var messages []ProduceRequest
		for i := 0; i < *batchSize; i++ {
			val := fmt.Sprintf("%s - %d", *value, i)
			messages = append(messages, ProduceRequest{
				Key:   *key,
				Value: val,
			})
		}
		reqBody, _ = json.Marshal(ProduceBatchRequest{
			Topic:    *topic,
			Messages: messages,
		})
	} else {
		urlPath = "/produce"
		reqBody, _ = json.Marshal(ProduceRequest{
			Topic: *topic,
			Key:   *key,
			Value: *value,
		})
	}

	resp, err := http.Post(produceURL+urlPath, "application/json", bytes.NewBuffer(reqBody))
	if err != nil {
		log.Fatalf("Failed to produce: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		log.Fatalf("Production failed (%d): %s", resp.StatusCode, string(body))
	}

	fmt.Printf("✅ Produced -> %s\n", string(body))
}

func fetchMetadata(brokerURL string) *MetadataResponse {
	resp, err := http.Get(brokerURL + "/metadata")
	if err != nil {
		log.Fatalf("Failed to fetch metadata: %v", err)
	}
	defer resp.Body.Close()

	var res MetadataResponse
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		log.Fatalf("Failed to decode metadata: %v", err)
	}
	return &res
}

func selectPartition(key string, numPartitions int) int {
	if key == "" {
		return 0
	}
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32()) % numPartitions
}

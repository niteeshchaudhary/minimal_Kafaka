package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"
)

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
	partition := flag.Int("partition", 0, "Partition ID (ignored if -group is set)")
	offset := flag.Uint64("offset", 0, "Offset to start fetching from")
	group := flag.String("group", "", "Consumer group name")
	flag.Parse()

	if *topic == "" {
		log.Fatal("Topic is required")
	}

	if *group != "" {
		runGroupConsumer(*brokerURL, *topic, *group)
	} else {
		runSimpleConsumer(*brokerURL, *topic, *partition, *offset)
	}
}

func runSimpleConsumer(brokerURL, topic string, partition int, offset uint64) {
	// Simple consumer also needs to find the leader
	metadata := fetchMetadata(brokerURL)
	leaderAddr := findLeaderAddr(metadata, topic, partition)

	for {
		msgs := fetchMessages(fmt.Sprintf("http://%s", leaderAddr), topic, partition, offset)
		if len(msgs) > 0 {
			fmt.Printf("✅ Consumed %d messages from P%d\n", len(msgs), partition)
			for _, m := range msgs {
				off := uint64(m["offset"].(float64))
				fmt.Printf("  [%d] %v\n", off, m["value"])
				offset = off + 1
			}
		} else {
			time.Sleep(1 * time.Second)
		}
	}
}

func runGroupConsumer(brokerURL, topic, group string) {
	// 1. Join Group
	memberID, assigned := joinGroup(brokerURL, topic, group, "")
	fmt.Printf("👤 Joined group %s as %s. Assigned partitions: %v\n", group, memberID, assigned)

	// 2. Initial Metadata
	metadata := fetchMetadata(brokerURL)

	// 3. Start Heartbeat
	stopHeartbeat := make(chan bool)
	assignmentChan := make(chan []int)
	go func() {
		ticker := time.NewTicker(3 * time.Second)
		for {
			select {
			case <-ticker.C:
				newAssigned := heartbeat(brokerURL, group, memberID)
				if newAssigned != nil {
					assignmentChan <- newAssigned
				}
			case <-stopHeartbeat:
				return
			}
		}
	}()

	// 4. Handle signals for graceful leave
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// 5. Polling Loop
	offsets := make(map[int]uint64)
	for _, p := range assigned {
		offsets[p] = fetchOffset(brokerURL, group, topic, p)
	}

	for {
		select {
		case sig := <-sigChan:
			fmt.Printf("\n👋 Caught signal %v, leaving group...\n", sig)
			leaveGroup(brokerURL, topic, group, memberID)
			stopHeartbeat <- true
			return
		case newAssigned := <-assignmentChan:
			if fmt.Sprintf("%v", newAssigned) != fmt.Sprintf("%v", assigned) {
				fmt.Printf("🔄 Assignment changed: %v -> %v\n", assigned, newAssigned)
				assigned = newAssigned
				for _, p := range assigned {
					if _, ok := offsets[p]; !ok {
						offsets[p] = fetchOffset(brokerURL, group, topic, p)
					}
				}
			}
		default:
			if len(assigned) == 0 {
				time.Sleep(1 * time.Second)
				continue
			}

			foundAny := false
			for _, p := range assigned {
				leaderAddr := findLeaderAddr(metadata, topic, p)
				msgs := fetchMessages(fmt.Sprintf("http://%s", leaderAddr), topic, p, offsets[p])
				if len(msgs) > 0 {
					foundAny = true
					fmt.Printf("✅ [P%d] Consumed %d messages\n", p, len(msgs))
					for _, m := range msgs {
						off := uint64(m["offset"].(float64))
						fmt.Printf("  [%d] %v\n", off, m["value"])
						offsets[p] = off + 1
					}
					commitOffset(brokerURL, group, topic, p, offsets[p])
				}
			}
			if !foundAny {
				time.Sleep(1 * time.Second)
				// Refresh metadata occasionally
				metadata = fetchMetadata(brokerURL)
			}
		}
	}
}

func findLeaderAddr(metadata *MetadataResponse, topic string, partition int) string {
	tm, ok := metadata.Topics[topic]
	if !ok {
		log.Fatalf("Topic %s not found in metadata", topic)
	}
	for _, pm := range tm.Partitions {
		if pm.ID == partition {
			addr, ok := metadata.Brokers[pm.Leader]
			if !ok {
				log.Fatalf("Leader %d for P%d not found", pm.Leader, partition)
			}
			return addr
		}
	}
	log.Fatalf("Partition %d not found for topic %s", partition, topic)
	return ""
}

func fetchMetadata(brokerURL string) *MetadataResponse {
	resp, err := http.Get(brokerURL + "/metadata")
	if err != nil {
		log.Fatalf("Failed to fetch metadata: %v", err)
	}
	defer resp.Body.Close()
	var res MetadataResponse
	json.NewDecoder(resp.Body).Decode(&res)
	return &res
}

func joinGroup(brokerURL, topic, group, memberID string) (string, []int) {
	reqBody, _ := json.Marshal(GroupJoinRequest{
		Topic:  topic,
		Group:  group,
		Member: memberID,
	})
	resp, err := http.Post(brokerURL+"/group/join", "application/json", bytes.NewBuffer(reqBody))
	if err != nil {
		log.Fatalf("Join failed: %v", err)
	}
	defer resp.Body.Close()
	var res GroupJoinResponse
	json.NewDecoder(resp.Body).Decode(&res)
	return res.Member, res.AssignedPartitions
}

func heartbeat(brokerURL, group, memberID string) []int {
	reqBody, _ := json.Marshal(GroupHeartbeatRequest{
		Group:  group,
		Member: memberID,
	})
	resp, err := http.Post(brokerURL+"/group/heartbeat", "application/json", bytes.NewBuffer(reqBody))
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		var res struct {
			AssignedPartitions []int `json:"assigned_partitions"`
		}
		json.NewDecoder(resp.Body).Decode(&res)
		return res.AssignedPartitions
	}
	return nil
}

func leaveGroup(brokerURL, topic, group, memberID string) {
	reqBody, _ := json.Marshal(GroupJoinRequest{
		Topic:  topic,
		Group:  group,
		Member: memberID,
	})
	resp, err := http.Post(brokerURL+"/group/leave", "application/json", bytes.NewBuffer(reqBody))
	if err == nil {
		resp.Body.Close()
	}
}

func fetchOffset(brokerURL, group, topic string, partition int) uint64 {
	params := url.Values{}
	params.Add("group", group)
	params.Add("topic", topic)
	params.Add("partition", fmt.Sprintf("%d", partition))
	resp, err := http.Get(fmt.Sprintf("%s/offset/fetch?%s", brokerURL, params.Encode()))
	if err != nil {
		return 0
	}
	defer resp.Body.Close()
	var res struct {
		Offset uint64 `json:"offset"`
	}
	json.NewDecoder(resp.Body).Decode(&res)
	return res.Offset
}

func commitOffset(brokerURL, group, topic string, partition int, offset uint64) {
	reqBody, _ := json.Marshal(map[string]interface{}{
		"group":     group,
		"topic":     topic,
		"partition": partition,
		"offset":    offset,
	})
	resp, err := http.Post(brokerURL+"/offset/commit", "application/json", bytes.NewBuffer(reqBody))
	if err == nil {
		resp.Body.Close()
	}
}

func fetchMessages(brokerURL, topic string, partition int, offset uint64) []map[string]interface{} {
	params := url.Values{}
	params.Add("topic", topic)
	params.Add("partition", fmt.Sprintf("%d", partition))
	params.Add("offset", fmt.Sprintf("%d", offset))
	resp, err := http.Get(fmt.Sprintf("%s/consume?%s", brokerURL, params.Encode()))
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	var messages []map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&messages)
	return messages
}

package main

import (
	"flag"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/niteesh/gokafka/internal/broker"
)

func main() {
	id := flag.Int("id", 0, "Broker ID")
	port := flag.Int("port", 9092, "Port to run the broker on")
	dataDir := flag.String("data-dir", "data", "Directory to store log files")
	maxSegSize := flag.Uint64("max-seg-size", 0, "Max segment size in bytes (0 for default)")
	peersStr := flag.String("peers", "", "Comma-separated list of peer brokers (id:addr)")
	flag.Parse()

	addr := fmt.Sprintf("localhost:%d", *port)
	peers := make(map[int]string)
	if *peersStr != "" {
		for _, p := range strings.Split(*peersStr, ",") {
			parts := strings.Split(p, ":")
			if len(parts) >= 2 {
				pID, _ := strconv.Atoi(parts[0])
				pAddr := strings.Join(parts[1:], ":")
				peers[pID] = pAddr
			}
		}
	}

	b := broker.NewBroker(*id, addr, peers, *dataDir, *maxSegSize)
	if err := b.Run(*port); err != nil {
		log.Fatalf("Broker failed: %v", err)
	}
}

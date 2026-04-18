#!/bin/bash
# scripts/start_cluster.sh
# Start a 3-broker Kafka cluster

# Kill any existing broker processes
pkill -f "go run cmd/broker"

# Create data directories
mkdir -p data/b1 data/b2 data/b3

echo "🚀 Starting Broker 1 (Port 9092)..."
go run cmd/broker/main.go -id 1 -port 9092 -data-dir data/b1 -peers "2:localhost:9093,3:localhost:9094" > broker1.log 2>&1 &

echo "🚀 Starting Broker 2 (Port 9093)..."
go run cmd/broker/main.go -id 2 -port 9093 -data-dir data/b2 -peers "1:localhost:9092,3:localhost:9094" > broker2.log 2>&1 &

echo "🚀 Starting Broker 3 (Port 9094)..."
go run cmd/broker/main.go -id 3 -port 9094 -data-dir data/b3 -peers "1:localhost:9092,2:localhost:9093" > broker3.log 2>&1 &

echo "✅ Cluster started! Check logs (broker*.log) for status."
echo "UI is available at http://localhost:3000 (if running)"

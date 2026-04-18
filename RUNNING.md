# Running the Minimal Kafka System

This guide explains how to run the various components of your distributed Kafka system.

## 1. Prerequisites
- **Go**: Version 1.25+ recommended.
- **Node.js**: For the UI dashboard.
- **OpenSSL**: (Optional) For generating TLS certificates.

---

## 2. Start the Broker Cluster

You can start a cluster of 3 brokers using the automated script:

```bash
./scripts/start_cluster.sh
```

Alternatively, to start a single broker manually:
```bash
go run cmd/broker/main.go -id 1 -port 9092 -data-dir data/b1
```

### Broker Options:
- `-id`: Unique ID for the broker (default: 0)
- `-port`: HTTP port to run on (default: 9092)
- `-data-dir`: Directory for log storage (default: "data")
- `-peers`: Comma-separated list of peer brokers (`id:addr`)

---

## 3. Start the UI Dashboard

The UI is built with React and provides real-time monitoring of topics and brokers.

```bash
cd UIKafka
npm install  # First time only
npm run dev
```
The dashboard will be available at [http://localhost:3000](http://localhost:3000).

---

## 4. Produce Messages

Use the producer CLI to send messages to a topic. The system will automatically route messages to the leader of the relevant partition.

```bash
go run cmd/producer/main.go -broker http://localhost:9092 -topic my-topic -value "Hello Kafka"
```

### Producer Options:
- `-broker`: Initial broker connection URL
- `-topic`: Topic name (Required)
- `-value`: Message content (Required)
- `-key`: Optional message key (used for partitioning)
- `-batch-size`: Number of messages to send in a batch

---

## 5. Consume Messages

You can consume messages in two modes:

### Simple Mode (Single Partition)
Read from a specific partition starting from an offset.
```bash
go run cmd/consumer/main.go -topic my-topic -partition 0 -offset 0
```

### Group Mode (Consumer Group)
Automatically join a group and balance partitions across members.
```bash
go run cmd/consumer/main.go -topic my-topic -group my-group
```

---

## 6. Advanced Features

### Generating TLS Certificates
If you want to run the broker with TLS:
1. Generate certificates: `./scripts/gen_certs.sh`
2. Update the broker code or launch with certificate paths (requires code modification to expose flags).

### Metrics
Prometheus metrics are available at `http://localhost:9092/metrics`.

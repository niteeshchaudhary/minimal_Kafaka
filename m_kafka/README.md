# Minimal Kafka Clone

A minimal, log-based event streaming system written in Python.

## Features
- Append-only log storage on disk.
- Topic and partition support.
- Configurable number of partitions per topic.
- Producer and Consumer HTTP API.
- Sequential offset-based fetching.

## Requirements
- Python 3.9+ (No external dependencies)

## Running the Broker
```bash
python3 broker.py
```
The server will start on `http://localhost:8080`.

## Testing the System
You can use the provided `client.py` script which produces 3 messages to the `logs` topic and then consumes them from their respective partitions.

```bash
python3 client.py
```

### Manual Testing with cURL
#### Produce Message
```bash
curl -X POST http://localhost:8080/produce \
  -H "Content-Type: application/json" \
  -d '{"topic": "events", "key": "user1", "value": "login"}'
```

#### Consume Message
```bash
curl "http://localhost:8080/consume?topic=events&partition=0&offset=0"
```

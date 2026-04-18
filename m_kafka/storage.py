import os
import json
import time
import threading

class Partition:
    def __init__(self, data_dir: str, topic: str, partition_id: int):
        self.topic = topic
        self.partition_id = partition_id
        self.file_path = os.path.join(data_dir, topic, f"{partition_id}.log")
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(self.file_path), exist_ok=True)
        
        self.lock = threading.Lock()
        self.current_offset = self._load_current_offset()
        
    def _load_current_offset(self) -> int:
        """Reads the last line of the log file to determine the next offset."""
        if not os.path.exists(self.file_path):
            return 0
        
        # Simple scan to find the last offset. For large files this is inefficient,
        # but for an MVP it's perfectly fine. We just read lines and track the last one.
        # Alternatively, we could read backwards or just count lines. We'll count lines.
        offset = 0
        with open(self.file_path, 'r') as f:
            for line in f:
                if line.strip():
                    offset += 1
        return offset

    def append(self, key: str, value: str) -> int:
        """Appends a new message to the partition log and returns the assigned offset."""
        with self.lock:
            offset = self.current_offset
            self.current_offset += 1
            
            message = {
                "offset": offset,
                "timestamp": int(time.time() * 1000),
                "key": key,
                "value": value
            }
            
            # Append atomic-ish.
            with open(self.file_path, 'a') as f:
                f.write(json.dumps(message) + '\n')
                f.flush()
                os.fsync(f.fileno())
                
            return offset

    def fetch(self, start_offset: int, max_messages: int = 100) -> list:
        """Fetches messages starting from start_offset."""
        if not os.path.exists(self.file_path):
            return []
            
        messages = []
        with open(self.file_path, 'r') as f:
            for i, line in enumerate(f):
                if i >= start_offset:
                    try:
                        messages.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass
                
                if len(messages) >= max_messages:
                    break
        return messages


class Topic:
    def __init__(self, data_dir: str, name: str, num_partitions: int):
        self.name = name
        self.num_partitions = num_partitions
        self.partitions = [Partition(data_dir, name, i) for i in range(num_partitions)]
        
    def get_partition(self, key: str) -> Partition:
        if key is not None and key != "":
            partition_idx = hash(key) % self.num_partitions
        else:
            # simple round robin by pseudo-random or first partition for simplicity MVP
            partition_idx = int(time.time() * 1000) % self.num_partitions
        return self.partitions[partition_idx]


class StorageEngine:
    def __init__(self, data_dir: str = "data"):
        self.data_dir = data_dir
        self.topics = {}
        self.lock = threading.Lock()
        
    def get_or_create_topic(self, topic_name: str, num_partitions: int = 3) -> Topic:
        with self.lock:
            if topic_name not in self.topics:
                self.topics[topic_name] = Topic(self.data_dir, topic_name, num_partitions)
            return self.topics[topic_name]

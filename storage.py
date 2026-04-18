import os
import json
import time
import threading


class Partition:
    def __init__(self, data_dir: str, topic: str, partition_id: int):
        self.topic = topic
        self.partition_id = partition_id
        self.file_path = os.path.join(data_dir, topic, f"{partition_id}.log")
        os.makedirs(os.path.dirname(self.file_path), exist_ok=True)
        self.lock = threading.Lock()
        self.current_offset = self._load_current_offset()

    def _load_current_offset(self) -> int:
        if not os.path.exists(self.file_path):
            return 0
        offset = 0
        with open(self.file_path, 'r') as f:
            for line in f:
                if line.strip():
                    offset += 1
        return offset

    def append(self, key: str, value: str) -> int:
        with self.lock:
            offset = self.current_offset
            self.current_offset += 1
            message = {
                "offset": offset,
                "timestamp": int(time.time() * 1000),
                "key": key,
                "value": value
            }
            with open(self.file_path, 'a') as f:
                f.write(json.dumps(message) + '\n')
                f.flush()
                os.fsync(f.fileno())
            return offset

    def fetch(self, start_offset: int, max_messages: int = 100) -> list:
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
            partition_idx = int(time.time() * 1000) % self.num_partitions
        return self.partitions[partition_idx]


# ---------------------------------------------------------------------------
# Consumer Group Manager – tracks committed offsets per group
# ---------------------------------------------------------------------------
class ConsumerGroupManager:
    def __init__(self):
        self.lock = threading.Lock()
        # { group_id: { "topics": { topic: { partition_id_str: committed_offset } }, "members": int } }
        self.groups = {}

    def create_group(self, group_id: str):
        with self.lock:
            if group_id not in self.groups:
                self.groups[group_id] = {"topics": {}, "members": 0}

    def add_member(self, group_id: str):
        with self.lock:
            self._ensure(group_id)
            self.groups[group_id]["members"] += 1

    def remove_member(self, group_id: str):
        with self.lock:
            self._ensure(group_id)
            self.groups[group_id]["members"] = max(0, self.groups[group_id]["members"] - 1)

    def commit_offset(self, group_id: str, topic: str, partition: int, offset: int):
        with self.lock:
            self._ensure(group_id)
            topics = self.groups[group_id]["topics"]
            if topic not in topics:
                topics[topic] = {}
            topics[topic][str(partition)] = offset

    def get_committed_offset(self, group_id: str, topic: str, partition: int) -> int:
        with self.lock:
            return self.groups.get(group_id, {}).get("topics", {}).get(topic, {}).get(str(partition), 0)

    def list_groups(self, storage_topics: dict) -> list:
        with self.lock:
            result = []
            for gid, gdata in self.groups.items():
                total_lag = 0
                for topic_name, partitions in gdata["topics"].items():
                    if topic_name in storage_topics:
                        topic_obj = storage_topics[topic_name]
                        for pid_str, committed in partitions.items():
                            pid = int(pid_str)
                            if pid < topic_obj.num_partitions:
                                head = topic_obj.partitions[pid].current_offset
                                total_lag += max(0, head - committed)
                members = gdata["members"]
                state = "Stable" if members > 0 else "Empty"
                result.append({
                    "groupId": gid,
                    "state": state,
                    "members": members,
                    "lag": total_lag,
                    "topics": gdata["topics"]
                })
            return result

    def delete_group(self, group_id: str):
        with self.lock:
            self.groups.pop(group_id, None)

    def _ensure(self, group_id):
        if group_id not in self.groups:
            self.groups[group_id] = {"topics": {}, "members": 0}


# ---------------------------------------------------------------------------
# ACL Manager – simple in-memory ACL store
# ---------------------------------------------------------------------------
class ACLManager:
    def __init__(self):
        self.lock = threading.Lock()
        self.acls = []

    def add_acl(self, principal: str, resource_type: str, resource_name: str, operation: str):
        with self.lock:
            entry = {
                "principal": principal,
                "resourceType": resource_type,
                "resourceName": resource_name,
                "operation": operation
            }
            self.acls.append(entry)
            return entry

    def list_acls(self) -> list:
        with self.lock:
            return list(self.acls)

    def delete_acl(self, index: int):
        with self.lock:
            if 0 <= index < len(self.acls):
                return self.acls.pop(index)
            return None


# ---------------------------------------------------------------------------
# Storage Engine
# ---------------------------------------------------------------------------
class StorageEngine:
    def __init__(self, data_dir: str = "data"):
        self.data_dir = data_dir
        self.topics = {}
        self.lock = threading.Lock()
        self.consumer_groups = ConsumerGroupManager()
        self.acl_manager = ACLManager()

    def get_or_create_topic(self, topic_name: str, num_partitions: int = 3) -> Topic:
        with self.lock:
            if topic_name not in self.topics:
                self.topics[topic_name] = Topic(self.data_dir, topic_name, num_partitions)
            return self.topics[topic_name]

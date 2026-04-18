import json
import urllib.parse
from http.server import BaseHTTPRequestHandler, HTTPServer
from storage import StorageEngine

storage = StorageEngine()


class BrokerHandler(BaseHTTPRequestHandler):
    def _send_response(self, status, payload):
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, DELETE, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()
        if payload is not None:
            self.wfile.write(json.dumps(payload).encode('utf-8'))

    def do_OPTIONS(self):
        self._send_response(200, None)

    def _read_body(self):
        length = int(self.headers.get('Content-Length', 0))
        return json.loads(self.rfile.read(length).decode('utf-8')) if length else {}

    # ── POST ─────────────────────────────────────────────────────────────
    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)

        if parsed.path == '/produce':
            try:
                data = self._read_body()
                topic_name = data.get('topic')
                key = data.get('key', '')
                value = data.get('value')
                if not topic_name or value is None:
                    return self._send_response(400, {'error': 'Missing topic or value'})
                topic = storage.get_or_create_topic(topic_name)
                partition = topic.get_partition(key)
                offset = partition.append(key, value)
                self._send_response(200, {'partition': partition.partition_id, 'offset': offset})
            except Exception as e:
                self._send_response(500, {'error': str(e)})

        elif parsed.path == '/consumer-groups':
            try:
                data = self._read_body()
                group_id = data.get('groupId')
                if not group_id:
                    return self._send_response(400, {'error': 'Missing groupId'})
                storage.consumer_groups.create_group(group_id)
                self._send_response(200, {'created': group_id})
            except Exception as e:
                self._send_response(500, {'error': str(e)})

        elif parsed.path == '/consumer-groups/member':
            try:
                data = self._read_body()
                group_id = data.get('groupId')
                if not group_id:
                    return self._send_response(400, {'error': 'Missing groupId'})
                storage.consumer_groups.add_member(group_id)
                self._send_response(200, {'added_member': group_id})
            except Exception as e:
                self._send_response(500, {'error': str(e)})

        elif parsed.path == '/consumer-groups/commit':
            try:
                data = self._read_body()
                group_id = data.get('groupId')
                topic = data.get('topic')
                partition = int(data.get('partition', 0))
                offset = int(data.get('offset', 0))
                if not group_id or not topic:
                    return self._send_response(400, {'error': 'Missing groupId or topic'})
                storage.consumer_groups.commit_offset(group_id, topic, partition, offset)
                self._send_response(200, {'committed': True})
            except Exception as e:
                self._send_response(500, {'error': str(e)})

        elif parsed.path == '/acls':
            try:
                data = self._read_body()
                principal = data.get('principal')
                resource_type = data.get('resourceType')
                resource_name = data.get('resourceName')
                operation = data.get('operation')
                if not all([principal, resource_type, resource_name, operation]):
                    return self._send_response(400, {'error': 'Missing ACL fields'})
                entry = storage.acl_manager.add_acl(principal, resource_type, resource_name, operation)
                self._send_response(200, entry)
            except Exception as e:
                self._send_response(500, {'error': str(e)})

        else:
            self._send_response(404, {'error': 'Not Found'})

    # ── GET ──────────────────────────────────────────────────────────────
    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        query = urllib.parse.parse_qs(parsed.query)

        if parsed.path == '/topics':
            topics_data = []
            for t_name, t_obj in storage.topics.items():
                partitions_detail = []
                for p in t_obj.partitions:
                    partitions_detail.append({
                        "id": p.partition_id,
                        "offset": p.current_offset
                    })
                topics_data.append({
                    "name": t_name,
                    "partitions": t_obj.num_partitions,
                    "messages_count": sum(p.current_offset for p in t_obj.partitions),
                    "partitions_detail": partitions_detail
                })
            self._send_response(200, topics_data)

        elif parsed.path == '/consumer-groups':
            groups = storage.consumer_groups.list_groups(storage.topics)
            self._send_response(200, groups)

        elif parsed.path == '/acls':
            self._send_response(200, storage.acl_manager.list_acls())

        elif parsed.path == '/consume':
            try:
                topic_name = query.get('topic', [None])[0]
                partition_id = int(query.get('partition', [0])[0])
                offset = int(query.get('offset', [0])[0])
                if not topic_name:
                    return self._send_response(400, {'error': 'Missing topic'})
                topic = storage.get_or_create_topic(topic_name)
                if partition_id >= topic.num_partitions or partition_id < 0:
                    return self._send_response(400, {'error': 'Invalid partition'})
                partition = topic.partitions[partition_id]
                messages = partition.fetch(offset, max_messages=100)
                self._send_response(200, messages)
            except Exception as e:
                self._send_response(500, {'error': str(e)})

        else:
            self._send_response(404, {'error': 'Not Found'})

    # ── DELETE ───────────────────────────────────────────────────────────
    def do_DELETE(self):
        parsed = urllib.parse.urlparse(self.path)
        query = urllib.parse.parse_qs(parsed.query)

        if parsed.path == '/consumer-groups':
            group_id = query.get('groupId', [None])[0]
            if not group_id:
                return self._send_response(400, {'error': 'Missing groupId'})
            storage.consumer_groups.delete_group(group_id)
            self._send_response(200, {'deleted': group_id})

        elif parsed.path == '/acls':
            try:
                index = int(query.get('index', ['-1'])[0])
                removed = storage.acl_manager.delete_acl(index)
                if removed:
                    self._send_response(200, {'deleted': removed})
                else:
                    self._send_response(400, {'error': 'Invalid index'})
            except Exception as e:
                self._send_response(500, {'error': str(e)})

        else:
            self._send_response(404, {'error': 'Not Found'})


def run(port=8080):
    httpd = HTTPServer(('', port), BrokerHandler)
    print(f"Broker starting on port {port}...")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    httpd.server_close()
    print("Broker stopped.")


if __name__ == '__main__':
    run()

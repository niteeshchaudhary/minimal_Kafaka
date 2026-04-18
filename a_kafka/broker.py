import json
import urllib.parse
from http.server import BaseHTTPRequestHandler, HTTPServer
from storage import StorageEngine

# Initialize the storage engine, data dir defaults to './data'
storage = StorageEngine()

class BrokerHandler(BaseHTTPRequestHandler):
    def _send_response(self, status, payload):
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(payload).encode('utf-8'))

    def do_POST(self):
        """Handle Produce requests"""
        if self.path == '/produce':
            content_length = int(self.headers.get('Content-Length', 0))
            post_data = self.rfile.read(content_length)
            
            try:
                data = json.loads(post_data.decode('utf-8'))
                topic_name = data.get('topic')
                key = data.get('key', '')
                value = data.get('value')
                
                if not topic_name or value is None:
                    self._send_response(400, {'error': 'Missing topic or value'})
                    return
                
                topic = storage.get_or_create_topic(topic_name)
                partition = topic.get_partition(key)
                
                offset = partition.append(key, value)
                
                self._send_response(200, {
                    'partition': partition.partition_id,
                    'offset': offset
                })
            except Exception as e:
                self._send_response(500, {'error': str(e)})
        else:
            self._send_response(404, {'error': 'Not Found'})

    def do_GET(self):
        """Handle Consume requests"""
        parsed_path = urllib.parse.urlparse(self.path)
        if parsed_path.path == '/consume':
            query = urllib.parse.parse_qs(parsed_path.query)
            
            try:
                topic_name = query.get('topic', [None])[0]
                partition_id = int(query.get('partition', [0])[0])
                offset = int(query.get('offset', [0])[0])
                
                if not topic_name:
                    self._send_response(400, {'error': 'Missing topic'})
                    return
                
                topic = storage.get_or_create_topic(topic_name)
                
                if partition_id >= topic.num_partitions or partition_id < 0:
                    self._send_response(400, {'error': 'Invalid partition'})
                    return
                    
                partition = topic.partitions[partition_id]
                messages = partition.fetch(offset, max_messages=100)
                
                self._send_response(200, messages)
            except Exception as e:
                self._send_response(500, {'error': str(e)})
        else:
            self._send_response(404, {'error': 'Not Found'})


def run(server_class=HTTPServer, handler_class=BrokerHandler, port=8080):
    server_address = ('', port)
    httpd = server_class(server_address, handler_class)
    print(f"Broker starting on port {port}...")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    httpd.server_close()
    print("Broker stopped.")

if __name__ == '__main__':
    run()

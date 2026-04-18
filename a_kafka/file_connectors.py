import os
from connect import SourceConnector, SinkConnector

class FileSourceConnector(SourceConnector):
    """Monitors a file and produces each new line to Kafka"""
    def __init__(self, name, config):
        super().__init__(name, config)
        self.file_path = config.get('file_path')
        if not self.file_path:
            raise ValueError("FileSourceConnector requires 'file_path'")
        self.file_handle = None
        self.last_position = 0

    def start(self):
        # Create file if it doesn't exist
        if not os.path.exists(self.file_path):
            with open(self.file_path, 'w') as f:
                pass
        self.file_handle = open(self.file_path, 'r')
        # Start from the end of the file or beginning? 
        # For simplicity, let's start from the beginning.
        self.last_position = 0

    def stop(self):
        if self.file_handle:
            self.file_handle.close()

    def poll(self):
        self.file_handle.seek(self.last_position)
        line = self.file_handle.readline()
        if line:
            self.produce_message(key=self.file_path, value=line.strip())
            self.last_position = self.file_handle.tell()

class FileSinkConnector(SinkConnector):
    """Consumes from Kafka and writes to a file"""
    def __init__(self, name, config):
        super().__init__(name, config)
        self.file_path = config.get('file_path')
        if not self.file_path:
            raise ValueError("FileSinkConnector requires 'file_path'")

    def start(self):
        # Ensure the file exists
        if not os.path.exists(self.file_path):
            with open(self.file_path, 'w') as f:
                pass

    def stop(self):
        pass

    def poll(self):
        messages = self.consume_messages()
        if messages:
            with open(self.file_path, 'a') as f:
                for msg in messages:
                    f.write(f"{msg['value']}\n")
            print(f"[{self.name}] Wrote {len(messages)} messages to {self.file_path}")

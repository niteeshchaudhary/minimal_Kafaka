import time
from .client import KafkaClient

class Stream:
    def __init__(self, client, source_topic):
        self.client = client
        self.source_topic = source_topic
        self.operations = []

    def map(self, func):
        self.operations.append(('map', func))
        return self

    def filter(self, func):
        self.operations.append(('filter', func))
        return self

    def to(self, sink_topic):
        print(f"Starting stream: {self.source_topic} -> {sink_topic}")
        offset = 0
        while True:
            messages = self.client.consume(self.source_topic, partition=0, offset=offset)
            if not messages:
                time.sleep(1)
                continue

            for msg in messages:
                val = msg['value']
                
                # Apply operations
                skip = False
                for op_type, op_func in self.operations:
                    if op_type == 'map':
                        val = op_func(val)
                    elif op_type == 'filter':
                        if not op_func(val):
                            skip = True
                            break
                
                if not skip:
                    self.client.produce(sink_topic, val)
                
                offset = msg['offset'] + 1

def builder(client, source_topic):
    return Stream(client, source_topic)

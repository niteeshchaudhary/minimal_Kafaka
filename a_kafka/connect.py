import threading
import time
import abc
from client import produce, consume

class Connector(abc.ABC):
    def __init__(self, name, config):
        self.name = name
        self.config = config
        self.running = False

    @abc.abstractmethod
    def start(self):
        """Initialize the connector"""
        pass

    @abc.abstractmethod
    def stop(self):
        """Shut down the connector"""
        pass

    @abc.abstractmethod
    def poll(self):
        """Perform a single unit of work"""
        pass

class SourceConnector(Connector):
    """Reads from an external source and produces to Kafka"""
    def __init__(self, name, config):
        super().__init__(name, config)
        self.topic = config.get('topic')
        if not self.topic:
            raise ValueError("SourceConnector requires a 'topic' in config")

    def produce_message(self, key, value):
        print(f"[{self.name}] Producing message: {key}={value}")
        produce(self.topic, key, value)

class SinkConnector(Connector):
    """Consumes from Kafka and writes to an external sink"""
    def __init__(self, name, config):
        super().__init__(name, config)
        self.topic = config.get('topic')
        self.partition = config.get('partition', 0)
        self.offset = config.get('offset', 0)
        if not self.topic:
            raise ValueError("SinkConnector requires a 'topic' in config")

    def consume_messages(self):
        messages = consume(self.topic, self.partition, self.offset)
        if messages:
            self.offset += len(messages)
            return messages
        return []

class ConnectRunner:
    """Manages the execution of connectors in separate threads"""
    def __init__(self):
        self.connectors = []
        self.threads = []

    def add_connector(self, connector):
        self.connectors.append(connector)

    def _run_connector(self, connector):
        print(f"Starting connector: {connector.name}")
        connector.start()
        connector.running = True
        try:
            while connector.running:
                connector.poll()
                time.sleep(1) # Poll interval
        except Exception as e:
            print(f"Error in connector {connector.name}: {e}")
        finally:
            connector.stop()
            print(f"Stopped connector: {connector.name}")

    def run_all(self):
        for connector in self.connectors:
            thread = threading.Thread(target=self._run_connector, args=(connector,))
            thread.daemon = True
            thread.start()
            self.threads.append(thread)

    def stop_all(self):
        for connector in self.connectors:
            connector.running = False
        for thread in self.threads:
            thread.join()

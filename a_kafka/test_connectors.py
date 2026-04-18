import time
import subprocess
import os
import signal
from file_connectors import FileSourceConnector, FileSinkConnector
from connect import ConnectRunner

def test_connectors():
    # 1. Start Broker
    print("Starting Broker...")
    broker_process = subprocess.Popen(["python3", "broker.py"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    time.sleep(2) # Wait for broker to start

    try:
        # 2. Setup files
        source_file = "test_input.txt"
        sink_file = "test_output.txt"
        topic = "connector-test"
        
        if os.path.exists(source_file): os.remove(source_file)
        if os.path.exists(sink_file): os.remove(sink_file)
        
        with open(source_file, 'w') as f:
            f.write("Line 1: Hello Kafka\n")

        # 3. Setup Connectors
        runner = ConnectRunner()
        
        source_config = {'topic': topic, 'file_path': source_file}
        source_conn = FileSourceConnector("Source1", source_config)
        
        sink_config = {'topic': topic, 'file_path': sink_file}
        sink_conn = FileSinkConnector("Sink1", sink_config)
        
        runner.add_connector(source_conn)
        runner.add_connector(sink_conn)
        
        # 4. Run Connectors
        print("Starting Connectors...")
        runner.run_all()
        
        time.sleep(3) # Wait for initial poll
        
        # 5. Add more data
        print("Adding more data to source...")
        with open(source_file, 'a') as f:
            f.write("Line 2: Connector works!\n")
            f.write("Line 3: End to end test\n")
            
        time.sleep(5) # Wait for connectors to process
        
        # 6. Verify Results
        if os.path.exists(sink_file):
            with open(sink_file, 'r') as f:
                content = f.read()
            print("\n--- Sink File Content ---")
            print(content)
            print("--------------------------")
            
            lines = content.strip().split('\n')
            if len(lines) >= 3:
                print("✅ End-to-end verification SUCCESS!")
            else:
                print(f"❌ Expected 3 lines, found {len(lines)}")
        else:
            print("❌ Sink file not created!")

    finally:
        # 7. Cleanup
        print("Stopping Connectors...")
        runner.stop_all()
        print("Stopping Broker...")
        broker_process.send_signal(signal.SIGINT)
        broker_process.wait()
        
        # Remove test files
        # os.remove(source_file)
        # os.remove(sink_file)

if __name__ == "__main__":
    test_connectors()

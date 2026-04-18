import streamlit as st
import requests
import os
import json

BROKER_URL = "http://localhost:8080"
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')

st.set_page_config(layout="wide", page_title="Minimal Kafka Visualizer")

st.title("Minimal Kafka Stream Visualizer")

col1, col_broker, col3 = st.columns([1, 1.5, 1])

# --- SENDER (Producer) ---
with col1:
    st.header("📤 Sender")
    st.markdown("Produce messages to the broker.")
    
    with st.form("produce_form"):
        prod_topic = st.text_input("Topic", value="events")
        prod_key = st.text_input("Key (Optional)", value="user1")
        prod_value = st.text_input("Value", value="login")
        submit_produce = st.form_submit_button("Send Message 🚀")
        
        if submit_produce:
            payload = {
                "topic": prod_topic,
                "key": prod_key,
                "value": prod_value
            }
            try:
                res = requests.post(f"{BROKER_URL}/produce", json=payload)
                if res.status_code == 200:
                    st.success(f"Produced! Partition: {res.json()['partition']}, Offset: {res.json()['offset']}")
                else:
                    st.error(f"Error: {res.text}")
            except Exception as e:
                st.error(f"Failed to connect to Broker: {e}")

# --- BROKER LOGS ---
with col_broker:
    st.header("🗄️ Broker Storage")
    st.markdown("Visualize the physical `data/` directories and logs.")
    
    if st.button("🔄 Refresh Broker State"):
        pass # Streamlit natively refreshes on button click
        
    if not os.path.exists(DATA_DIR):
        st.warning("No data directory found. Produce a message first!")
    else:
        topics = [d for d in os.listdir(DATA_DIR) if os.path.isdir(os.path.join(DATA_DIR, d))]
        if not topics:
            st.info("No topics created yet.")
        else:
            for topic in topics:
                st.subheader(f"Topic: `{topic}`")
                topic_path = os.path.join(DATA_DIR, topic)
                partitions = sorted([f for f in os.listdir(topic_path) if f.endswith('.log')])
                
                for partition in partitions:
                    part_id = partition.split('.')[0]
                    with st.expander(f"Partition {part_id}", expanded=True):
                        log_path = os.path.join(topic_path, partition)
                        try:
                            with open(log_path, 'r') as f:
                                lines = f.readlines()
                                if not lines:
                                    st.write("*Empty*")
                                else:
                                    # Show last 5 msgs
                                    for line in lines[-5:]:
                                        msg = json.loads(line)
                                        st.code(f"Offset: {msg['offset']} | Key: {msg['key']} | Value: {msg['value']}")
                        except Exception as e:
                            st.error(f"Could not read logs: {e}")


# --- RECEIVER (Consumer) ---
with col3:
    st.header("📥 Receiver")
    st.markdown("Consume messages starting from an offset.")
    
    with st.form("consume_form"):
        cons_topic = st.text_input("Topic", value="events")
        cons_part = st.number_input("Partition", min_value=0, value=0, step=1)
        cons_offset = st.number_input("Start Offset", min_value=0, value=0, step=1)
        submit_consume = st.form_submit_button("Fetch Messages 🎣")
        
        if submit_consume:
            try:
                res = requests.get(f"{BROKER_URL}/consume?topic={cons_topic}&partition={cons_part}&offset={cons_offset}")
                if res.status_code == 200:
                    messages = res.json()
                    if not messages:
                        st.info("No new messages found.")
                    else:
                        st.success(f"Fetched {len(messages)} messages!")
                        for msg in messages:
                            st.json(msg)
                else:
                    st.error(f"Error: {res.text}")
            except Exception as e:
                st.error(f"Failed to connect to Broker: {e}")

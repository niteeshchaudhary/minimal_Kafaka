import requests
import struct
import zlib
import time
from pprint import pprint

BASE_URL = "http://localhost:9092"

def encode_message(offset, timestamp, compression, key, value):
    # Binary format:
    # 8(Off) + 8(Ts) + 1(Comp) + 4(KLen) + key + 4(VLen) + val + 4(CRC)
    
    val_to_encode = value
    if compression == 1: # Gzip
        # In Python we use zlib with gzip header or gzip module
        import gzip
        val_to_encode = gzip.compress(value)
    
    key_len = len(key)
    val_len = len(val_to_encode)
    
    # Pack initial part
    # Q: uint64, q: int64, B: uint8, i: int32
    header = struct.pack(">QqBi", offset, timestamp, compression, key_len)
    body = key + struct.pack(">i", val_len) + val_to_encode
    
    data_for_crc = header + body
    import binascii
    crc = binascii.crc32(data_for_crc) & 0xFFFFFFFF
    
    return data_for_crc + struct.pack(">I", crc)

def decode_message(data):
    if len(data) < 29:
        return None, data
    
    offset, timestamp, compression, key_len = struct.unpack(">QqBi", data[0:21])
    key = data[21:21+key_len]
    
    val_start = 21 + key_len
    val_len = struct.unpack(">i", data[val_start:val_start+4])[0]
    val_payload = data[val_start+4 : val_start+4+val_len]
    
    value = val_payload
    if compression == 1:
        import gzip
        value = gzip.decompress(val_payload)
        
    crc = struct.unpack(">I", data[val_start+4+val_len : val_start+4+val_len+4])[0]
    
    # Total size read
    total_read = val_start + 4 + val_len + 4
    
    return {
        "offset": offset,
        "timestamp": timestamp,
        "compression": compression,
        "key": key.decode('utf-8'),
        "value": value.decode('utf-8')
    }, data[total_read:]

def test_binary_produce_consume():
    topic = "test_binary"
    msg_bytes = encode_message(0, int(time.time()*1000), 1, b"k1", b"v1 shiny binary content")
    
    print(f"--- Producing Binary Message to {topic} ---")
    res = requests.post(f"{BASE_URL}/produce/binary?topic={topic}", data=msg_bytes)
    print(f"Status: {res.status_code}, Offset: {res.headers.get('X-Offset')}")
    
    print(f"\n--- Consuming Binary Stream from {topic} ---")
    res = requests.get(f"{BASE_URL}/consume/binary?topic={topic}&partition=0&offset=0&limit=1")
    if res.status_code == 200:
        data = res.content
        msg, remaining = decode_message(data)
        pprint(msg)
    else:
        print(f"Error: {res.text}")

if __name__ == "__main__":
    test_binary_produce_consume()

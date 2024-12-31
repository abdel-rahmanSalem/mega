# ---------------------------------------------------------------
# ---------------- STILL UNDER DEVELOPMENT -----------------------
# ---------------------------------------------------------------


import socket
import struct
import threading
import time
from enum import Enum
from typing import Optional
import random

class MessageType(Enum):
    PRODUCE = 0x01
    CONSUME = 0x02
    CREATE_TOPIC = 0x03

class Message:
    def __init__(self, correlation_id: int, message_type: MessageType, 
                 topic: str, timestamp: int, offset: Optional[int], payload: bytes):
        self.correlation_id = correlation_id
        self.message_type = message_type
        self.topic = topic
        self.timestamp = timestamp
        self.offset = offset
        self.payload = payload

    @classmethod
    def from_bytes(cls, data: bytes):
        # Parse binary message format
        correlation_id = struct.unpack('>i', data[0:4])[0]
        message_type = MessageType(data[4])
        topic_length = struct.unpack('>i', data[5:9])[0]
        topic = data[9:9+topic_length].decode('utf-8')
        timestamp = struct.unpack('>q', data[9+topic_length:17+topic_length])[0]
        
        current_pos = 17 + topic_length
        if message_type == MessageType.CONSUME:
            offset = struct.unpack('>i', data[current_pos:current_pos+4])[0]
            current_pos += 4
        else:
            offset = None
            
        payload_length = struct.unpack('>i', data[current_pos:current_pos+4])[0]
        payload = data[current_pos+4:current_pos+4+payload_length]
        
        return cls(correlation_id, message_type, topic, timestamp, offset, payload)

class BrokerClient:
    def __init__(self, host: str = 'localhost', port: int = 8080):
        self.host = host
        self.port = port
        self.socket = None
        self.correlation_id = 0

    def connect(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((self.host, self.port))

    def close(self):
        if self.socket:
            self.socket.close()

    def _pack_message(self, msg_type: MessageType, topic: str, payload: bytes, offset: Optional[int] = -1) -> bytes:
        self.correlation_id += 1
        timestamp = int(time.time() * 1000)
        
        message = struct.pack('>i', self.correlation_id)  # correlation_id (4 bytes)
        message += struct.pack('b', msg_type.value)      # message_type (1 byte)
        
        topic_bytes = topic.encode()
        message += struct.pack('>i', len(topic_bytes))   # topic length (4 bytes)
        message += topic_bytes                           # topic
        
        message += struct.pack('>q', timestamp)          # timestamp (8 bytes)
        
        if msg_type == MessageType.CONSUME:
            message += struct.pack('>i', offset)         # offset (4 bytes)
        
        message += struct.pack('>i', len(payload))       # payload length (4 bytes)
        message += payload                               # payload
        
        return message

    def _receive_response(self, response_type: MessageType):
        # Read and unpack response header
        header = self.socket.recv(4 + 1 + 8)  # correlationId (4 bytes), success (1 byte), timestamp (8 bytes)
        correlation_id, success, timestamp = struct.unpack('>Ibq', header)

        if response_type == MessageType.CREATE_TOPIC:
            # Read UTF topic
            topic_length_data = self.socket.recv(2)  # Length of the string (2 bytes)
            (topic_length,) = struct.unpack('>H', topic_length_data)
            topic = self.socket.recv(topic_length).decode('utf-8')
            return {
                'correlation_id': correlation_id,
                'success': bool(success),
                'timestamp': timestamp,
                'topic': topic
            }
        elif response_type == MessageType.PRODUCE:
            # Read offset
            offset_data = self.socket.recv(4)  # Offset (4 bytes)
            (offset,) = struct.unpack('>i', offset_data)
            return {
                'correlation_id': correlation_id,
                'success': bool(success),
                'timestamp': timestamp,
                'offset': offset
            }
        elif response_type == MessageType.CONSUME:
            # Read nextOffset and payload
            consume_header = self.socket.recv(4 + 4)  # nextOffset (4 bytes), payloadLength (4 bytes)
            next_offset, payload_length = struct.unpack('>ii', consume_header)
            payload = self.socket.recv(payload_length)
            return {
                'correlation_id': correlation_id,
                'success': bool(success),
                'timestamp': timestamp,
                'next_offset': next_offset,
                'payload': payload
            }
        else:
            raise ValueError("Unknown response type")

    def create_topic(self, topic: str):
        packed = self._pack_message(MessageType.CREATE_TOPIC, topic, b'')
        self.socket.sendall(packed)
        return self._receive_response(MessageType.CREATE_TOPIC)

    def produce(self, topic: str, message: str):
        packed = self._pack_message(MessageType.PRODUCE, topic, message.encode())
        self.socket.sendall(packed)
        return self._receive_response(MessageType.PRODUCE)

    def consume(self, topic: str, offset: int):
        packed = self._pack_message(MessageType.CONSUME, topic, b'', offset)
        self.socket.sendall(packed)
        return self._receive_response(MessageType.CONSUME)

def producer_thread(client_id: int, messages: int):
    client = BrokerClient()
    try:
        client.connect()
        for i in range(messages):
            msg = f"Message {i} from producer {client_id}"
            response = client.produce("test-topic", msg)
            print(f"Producer {client_id}: {response}")
            time.sleep(random.uniform(0.1, 0.5))
    except Exception as e:
        print(f"Producer {client_id} error: {e}")
    finally:
        client.close()

def consumer_thread(client_id: int, start_offset: int, duration: int):
    client = BrokerClient()
    try:
        client.connect()
        current_offset = start_offset
        end_time = time.time() + duration
        
        while time.time() < end_time:
            response = client.consume("test-topic", current_offset)
            if response and response['success']:
                print(f"Consumer {client_id} at offset {current_offset}: {response['payload'].decode('utf-8')}")
                current_offset = response['next_offset']
            time.sleep(random.uniform(0.2, 0.8))
    except Exception as e:
        print(f"Consumer {client_id} error: {e}")
    finally:
        client.close()

def run_test(num_producers: int, num_consumers: int, messages_per_producer: int, consumer_duration: int):
    # Create topic first
    setup_client = BrokerClient()
    try:
        setup_client.connect()
        response = setup_client.create_topic("test-topic")
        print(f"Topic creation response: {response}")
    finally:
        setup_client.close()
    
    # Wait a bit after topic creation
    time.sleep(1)
    
    threads = []
    
    # Start producers
    for i in range(num_producers):
        t = threading.Thread(target=producer_thread, args=(i, messages_per_producer))
        threads.append(t)
        t.start()

    # Wait for some messages to be produced
    time.sleep(2)
    
    # Start consumers
    for i in range(num_consumers):
        t = threading.Thread(target=consumer_thread, args=(i, 0, consumer_duration))
        threads.append(t)
        t.start()

    # Wait for all threads to complete
    for t in threads:
        t.join()

if __name__ == "__main__":
    run_test(
        num_producers=2,
        num_consumers=1,
        messages_per_producer=3,
        consumer_duration=5
    )

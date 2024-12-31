import socket
import struct
import time
from enum import Enum
from typing import Dict, Any, Optional

class MessageType(Enum):
    PRODUCE = 0x01
    CONSUME = 0x02
    CREATE_TOPIC = 0x03

class ErrorCode(Enum):
    INVALID_REQUEST = 1
    INVALID_MESSAGE_TYPE = 2
    MESSAGE_TOO_LARGE = 3
    TOPIC_NOT_FOUND = 4
    TOPIC_ALREADY_EXISTS = 5
    INVALID_OFFSET = 6
    INTERNAL_ERROR = 7
    NETWORK_ERROR = 8
    RESOURCE_EXHAUSTED = 9

class MessageClient:
    def __init__(self, host: str = 'localhost', port: int = 8080):
        self.host = host
        self.port = port
        self.socket = None

    def connect(self) -> None:
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((self.host, self.port))
        except socket.error as e:
            raise ConnectionError(f"Failed to connect to {self.host}:{self.port}: {e}")

    def close(self) -> None:
        if self.socket:
            self.socket.close()
            self.socket = None

    def create_topic(self, topic_name: str, correlation_id: int = 1) -> Dict[str, Any]:
        message = self._build_message(
            correlation_id=correlation_id,
            message_type=MessageType.CREATE_TOPIC,
            topic_name=topic_name,
            payload=""
        )
        
        try:
            self.socket.sendall(message)
            response_data = self._receive_data()
            return self._parse_create_topic_response(response_data)
        except Exception as e:
            raise RuntimeError(f"Error in create_topic operation: {e}")

    def produce(self, topic_name: str, payload: str, correlation_id: int = 1) -> Dict[str, Any]:
        message = self._build_message(
            correlation_id=correlation_id,
            message_type=MessageType.PRODUCE,
            topic_name=topic_name,
            payload=payload
        )
        
        try:
            self.socket.sendall(message)
            response_data = self._receive_data()
            return self._parse_produce_response(response_data)
        except Exception as e:
            raise RuntimeError(f"Error in produce operation: {e}")

    def consume(self, topic_name: str, offset: int, correlation_id: int = 1) -> Dict[str, Any]:
        message = self._build_message(
            correlation_id=correlation_id,
            message_type=MessageType.CONSUME,
            topic_name=topic_name,
            payload="",
            offset=offset
        )
        
        try:
            self.socket.sendall(message)
            response_data = self._receive_data()
            return self._parse_consume_response(response_data)
        except Exception as e:
            raise RuntimeError(f"Error in consume operation: {e}")

    def _build_message(self, correlation_id: int, message_type: MessageType, 
                      topic_name: str, payload: str, offset: Optional[int] = None) -> bytes:
        topic_bytes = topic_name.encode('utf-8')
        payload_bytes = payload.encode('utf-8') if payload else b''
        timestamp = int(time.time() * 1000)

        message = struct.pack('>i', correlation_id)  # Correlation ID (4 bytes)
        message += struct.pack('b', message_type.value)  # Message Type (1 byte)
        message += struct.pack('>i', len(topic_bytes))  # Topic length (4 bytes)
        message += topic_bytes  # Topic name (variable)
        message += struct.pack('>q', timestamp)  # Timestamp (8 bytes)
        
        if message_type == MessageType.CONSUME and offset is not None:
            message += struct.pack('>i', offset)  # Offset (4 bytes)
            
        message += struct.pack('>i', len(payload_bytes))  # Payload length (4 bytes)
        if payload_bytes:
            message += payload_bytes  # Payload (variable)

        return message

    def _receive_data(self, buffer_size: int = 4096) -> bytes:
        try:
            data = self.socket.recv(buffer_size)
            if not data:
                raise ConnectionError("Connection closed by server")
            return data
        except socket.error as e:
            raise ConnectionError(f"Error receiving data: {e}")

    def _parse_create_topic_response(self, data: bytes) -> Dict[str, Any]:
        try:
            correlation_id = struct.unpack('>i', data[:4])[0]
            success = struct.unpack('b', data[4:5])[0]
            
            if not success:
                error_code = struct.unpack('b', data[5:6])[0]
                return {
                    'correlation_id': correlation_id,
                    'success': False,
                    'error_code': ErrorCode(error_code).name
                }
                
            timestamp = struct.unpack('>q', data[5:13])[0]
            topic = data[13:].decode('utf-8')
            
            return {
                'correlation_id': correlation_id,
                'success': True,
                'timestamp': timestamp,
                'topic': topic
            }
        except Exception as e:
            raise ValueError(f"Error parsing create topic response: {e}")

    def _parse_produce_response(self, data: bytes) -> Dict[str, Any]:
        try:
            correlation_id = struct.unpack('>i', data[:4])[0]
            success = struct.unpack('b', data[4:5])[0]
            
            if not success:
                error_code = struct.unpack('b', data[5:6])[0]
                return {
                    'correlation_id': correlation_id,
                    'success': False,
                    'error_code': ErrorCode(error_code).name
                }
                
            timestamp = struct.unpack('>q', data[5:13])[0]
            offset = struct.unpack('>i', data[13:17])[0]
            
            return {
                'correlation_id': correlation_id,
                'success': True,
                'timestamp': timestamp,
                'offset': offset
            }
        except Exception as e:
            raise ValueError(f"Error parsing produce response: {e}")

    def _parse_consume_response(self, data: bytes) -> Dict[str, Any]:
        try:
            correlation_id = struct.unpack('>i', data[:4])[0]
            success = struct.unpack('b', data[4:5])[0]
            
            if not success:
                error_code = struct.unpack('b', data[5:6])[0]
                return {
                    'correlation_id': correlation_id,
                    'success': False,
                    'error_code': ErrorCode(error_code).name
                }
                
            timestamp = struct.unpack('>q', data[5:13])[0]
            next_offset = struct.unpack('>i', data[13:17])[0]
            payload_length = struct.unpack('>i', data[17:21])[0]
            payload = data[21:21+payload_length].decode('utf-8') if payload_length > 0 else ""
            
            return {
                'correlation_id': correlation_id,
                'success': True,
                'timestamp': timestamp,
                'next_offset': next_offset,
                'payload': payload
            }
        except Exception as e:
            raise ValueError(f"Error parsing consume response: {e}")
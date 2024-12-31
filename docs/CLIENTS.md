# Client Documentation

## Client Libraries as Abstraction Layers

MEGA's client libraries are designed to serve as abstraction layers between applications and the MEGA broker. This approach provides several benefits:

### Purpose

- Hide the complexity of the binary protocol
- Provide language-native interfaces for different platforms
- Enable consistent behavior across different programming languages
- Make it easier to modify the underlying protocol without impacting applications

### Implementation Requirements

1. **Protocol Abstraction**

   - Handle binary message packing/unpacking
   - Manage connection lifecycle
   - Implement error handling and retries

2. **Language-Specific Interfaces**

   - Follow language conventions and best practices
   - Provide idiomatic APIs for the target language
   - Use appropriate data types for the language

3. **Core Features**
   - Topic management
   - Message production
   - Message consumption
   - Connection handling
   - Error management

### Example Architecture

```
Application Layer
      ↓
Client Library (Abstraction Layer)
      ↓
Binary Protocol Implementation
      ↓
Network Communication
      ↓
MEGA Broker
```

## Python Client

### Basic Usage

```python
from message_client import MessageClient

def example_usage():
    client = MessageClient(host='localhost', port=8080)

    try:
        # Connect to broker
        client.connect()

        # Create a topic
        create_response = client.create_topic("test-topic")
        print(f"Topic created: {create_response}")

        # Produce a message
        produce_response = client.produce("test-topic", "Hello, MEGA!")
        print(f"Message produced at offset: {produce_response['offset']}")

        # Consume the message
        consume_response = client.consume("test-topic", 0)
        print(f"Consumed message: {consume_response['payload']}")

    finally:
        client.close()
```

## Creating New Client Libraries

To implement a new client library:

1. Implement the binary protocol as specified in [PROTOCOL.md](PROTOCOL.md)
2. Support all message types (CREATE_TOPIC, PRODUCE, CONSUME)
3. Handle error responses appropriately
4. Provide connection management
5. Follow the existing client patterns for consistency

### Minimum Requirements

- Connection management (connect/disconnect)
- Topic creation
- Message production
- Message consumption
- Error handling
- Resource cleanup

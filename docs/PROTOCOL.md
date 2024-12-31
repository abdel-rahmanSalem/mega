# MEGA Protocol Documentation

## Message Format

MEGA uses a binary protocol for all communications. Each message follows this general structure:

### Header Format

```
+----------------+---------------+---------------+-------------+--------------+----------------+
|  Correlation   |   Message     |    Topic      |  Topic Name |  Timestamp   |    Optional    |
|      ID        |     Type      |    Length     |             |              |     Fields     |
+----------------+---------------+---------------+-------------+--------------+----------------+
|     4 bytes    |    1 byte     |    4 bytes    |   Variable  |    8 bytes   |    Variable    |
```

### Message Types

1. CREATE_TOPIC (0x03)

```
Header
```

2. PRODUCE (0x01)

```
Header + Payload Length (4 bytes) + Payload
```

3. CONSUME (0x02)

```
Header + Offset (4 bytes)
```

### Response Format

#### Success Response Structure

```
+----------------+---------------+----------------+------------------+
|  Correlation   |   Success     |   Timestamp   |     Payload     |
|      ID        |     Flag      |               |                 |
+----------------+---------------+----------------+------------------+
|     4 bytes    |    1 byte     |    8 bytes    |    Variable     |
```

#### Error Response Structure

```
+----------------+---------------+----------------+
|  Correlation   |   Success     |  Error Code   |
|      ID        |     Flag      |               |
+----------------+---------------+----------------+
|     4 bytes    |    1 byte     |    1 byte     |
```

### Error Codes

- INVALID_REQUEST (1)
- INVALID_MESSAGE_TYPE (2)
- MESSAGE_TOO_LARGE (3)
- TOPIC_NOT_FOUND (4)
- TOPIC_ALREADY_EXISTS (5)
- INVALID_OFFSET (6)
- INTERNAL_ERROR (7)
- NETWORK_ERROR (8)
- RESOURCE_EXHAUSTED (9)

## Message Flow Examples

### Creating a Topic

```sequence
Client -> Broker: CREATE_TOPIC Request
Broker -> Client: Success/Error Response
```

### Producing a Message

```sequence
Client -> Broker: PRODUCE Request
Broker -> Client: Success Response with Offset
```

### Consuming a Message

```sequence
Client -> Broker: CONSUME Request with Offset
Broker -> Client: Success Response with Message
```

from message_client import MessageClient

def test_consume():
    client = MessageClient()
    try:
        client.connect()
        response = client.consume("test-topic", 0)
        print("\nConsume Response:")
        print(f"Correlation ID: {response['correlation_id']}")
        print(f"Success: {response['success']}")
        if response['success']:
            print(f"Timestamp: {response['timestamp']}")
            print(f"Next Offset: {response['next_offset']}")
            print(f"Payload: {response['payload']}")
        else:
            print(f"Error: {response['error_code']}")
    finally:
        client.close()

if __name__ == "__main__":
    test_consume()
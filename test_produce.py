from message_client import MessageClient

def test_produce():
    client = MessageClient()
    try:
        client.connect()
        response = client.produce("test-topic", "Hello, World!")
        print("\nProduce Response:")
        print(f"Correlation ID: {response['correlation_id']}")
        print(f"Success: {response['success']}")
        if response['success']:
            print(f"Timestamp: {response['timestamp']}")
            print(f"Offset: {response['offset']}")
        else:
            print(f"Error: {response['error_code']}")
    finally:
        client.close()

if __name__ == "__main__":
    test_produce()
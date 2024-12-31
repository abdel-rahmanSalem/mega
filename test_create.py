from message_client import MessageClient

def test_create_topic():
    client = MessageClient()
    try:
        client.connect()
        response = client.create_topic("test-topic")
        print("\nCreate Topic Response:")
        print(f"Correlation ID: {response['correlation_id']}")
        print(f"Success: {response['success']}")
        if response['success']:
            print(f"Timestamp: {response['timestamp']}")
            print(f"Topic: {response['topic']}")
        else:
            print(f"Error: {response['error_code']}")
    finally:
        client.close()

if __name__ == "__main__":
    test_create_topic()

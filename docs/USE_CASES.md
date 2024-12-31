# Use Cases

## Common Use Cases

### 1. Event Broadcasting

Broadcast events from one service to multiple consumers.

```python
# Producer (Service A)
client.produce("user-events", "user.created:123")

# Consumer 1 (Email Service)
response = client.consume("user-events", last_offset)
# Send welcome email

# Consumer 2 (Analytics Service)
response = client.consume("user-events", last_offset)
# Record user creation event
```

### 2. Work Queue

Distribute tasks across multiple workers.

```python
# Task Producer
client.produce("image-processing", "process:image123.jpg")

# Worker 1
while True:
    response = client.consume("image-processing", current_offset)
    # Process image
    current_offset += 1

# Worker 2
while True:
    response = client.consume("image-processing", current_offset)
    # Process image
    current_offset += 1
```

### 3. Log Aggregation

Collect logs from multiple services.

```python
# Service A
client.produce("system-logs", "ServiceA:Info:Operation completed")

# Service B
client.produce("system-logs", "ServiceB:Error:Database connection failed")

# Log Aggregator
while True:
    logs = client.consume("system-logs", current_offset)
    # Store logs in centralized system
```

## Example Implementation: E-commerce Order Processing

```python
# Order Service
def process_order(order_data):
    client.produce("new-orders", json.dumps(order_data))

# Inventory Service
def handle_inventory():
    while True:
        response = client.consume("new-orders", current_offset)
        order = json.loads(response['payload'])
        update_inventory(order)

# Shipping Service
def handle_shipping():
    while True:
        response = client.consume("new-orders", current_offset)
        order = json.loads(response['payload'])
        create_shipping_label(order)
```

package mega;

import java.io.DataInputStream;
import java.io.IOException;

public class Message {
    private int correlationId;
    private MessageType messageType;
    private String topic;
    private long timestamp;
    private int offset = -1; // Default -1 for non-Consume messages
    private byte[] payload;

    public Message(DataInputStream dataInputStream) throws IOException {
        parseMessage(dataInputStream);
    }

    private void parseMessage(DataInputStream dataInputStream) throws IOException {

        // Parse Correlation ID (4 bytes)
        correlationId = dataInputStream.readInt();

        // Parse Message Type (1 byte)
        byte messageTypeCode = dataInputStream.readByte();
        messageType = MessageType.fromCode(messageTypeCode);

        // Parse Topic (String)
        int topicLength = dataInputStream.readInt();
        byte[] topicBytes = new byte[topicLength];
        dataInputStream.readFully(topicBytes);
        topic = new String(topicBytes);

        // Parse Timestamp (8 bytes)
        timestamp = dataInputStream.readLong();

        // Parse Offset (4 bytes, for Consume messages)
        if (messageType == MessageType.CONSUME) {
            offset = dataInputStream.readInt();
        }

        // Parse Payload (remaining bytes)
        int payloadLength = dataInputStream.readInt();
        payload = new byte[payloadLength];
        dataInputStream.readFully(payload);
    }

    // Getters
    public int getCorrelationId() {
        return correlationId;
    }

    public MessageType getMessageType() {
        return messageType;
    }

    public String getTopic() {
        return topic;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public int getOffset() {
        return offset;
    }

    public byte[] getPayload() {
        return payload;
    }

    // String representation for testing
    @Override
    public String toString() {
        return "{" +
                "correlationId=" + correlationId +
                ", messageType=" + messageType +
                ", topic='" + topic + '\'' +
                ", timestamp=" + timestamp +
                ", offset=" + offset +
                ", payload=" + new String(payload) +
                '}';
    }
}

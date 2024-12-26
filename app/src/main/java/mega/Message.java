package mega;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

public class Message {
    private int correlationId;
    private byte messageType;
    private String topic;
    private long timestamp;
    private byte[] payload;

    public Message(byte[] rawData) throws IOException {
        parseMessage(rawData);
    }

    private void parseMessage(byte[] rawData) throws IOException {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(rawData);
        DataInputStream dataInputStream = new DataInputStream(byteArrayInputStream);

        // Parse Correlation ID (4 bytes)
        correlationId = dataInputStream.readInt();

        // Parse Message Type (1 byte)
        messageType = dataInputStream.readByte();

        // Parse Topic (String)
        int topicLength = dataInputStream.readInt();
        byte[] topicBytes = new byte[topicLength];
        dataInputStream.readFully(topicBytes);
        topic = new String(topicBytes);

        // Parse Timestamp (8 bytes)
        timestamp = dataInputStream.readLong();

        // Parse Payload (remaining bytes)
        int payloadLength = dataInputStream.readInt();
        payload = new byte[payloadLength];
        dataInputStream.readFully(payload);
    }

    // Getters
    public int getCorrelationId() {
        return correlationId;
    }

    public byte getMessageType() {
        return messageType;
    }

    public String getTopic() {
        return topic;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public byte[] getPayload() {
        return payload;
    }

    // String representation for testing
    @Override
    public String toString() {
        return "Message{" +
                "correlationId=" + correlationId +
                ", messageType=" + messageType +
                ", topic='" + topic + '\'' +
                ", timestamp=" + timestamp +
                ", payload=" + new String(payload) +
                '}';
    }
}

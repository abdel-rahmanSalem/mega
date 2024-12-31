package mega;

import java.io.DataInputStream;
import java.io.IOException;

public class Message {
    private int correlationId;
    private MessageType messageType;
    private String topic;
    private long timestamp;
    private int offset = -1; // Default -1 for non-Consume messages
    private int payloadLength = -1;
    private byte[] payload;

    public Message(DataInputStream dataInputStream) throws IOException {
        parseMessage(dataInputStream);
    }

    private void parseMessage(DataInputStream dataInputStream) throws IOException {

        // Parse Correlation ID (4 bytes)
        this.correlationId = dataInputStream.readInt();

        // Parse Message Type (1 byte)
        byte messageTypeCode = dataInputStream.readByte();
        this.messageType = MessageType.fromCode(messageTypeCode);

        // Parse Topic (String)
        int topicLength = dataInputStream.readInt();
        byte[] topicBytes = new byte[topicLength];
        dataInputStream.readFully(topicBytes);
        this.topic = new String(topicBytes);

        // Parse Timestamp (8 bytes)
        this.timestamp = dataInputStream.readLong();

        // Parse Offset (4 bytes, for Consume messages)
        if (this.messageType == MessageType.CONSUME) {
            this.offset = dataInputStream.readInt();
        }

        // Parse Payload (remaining bytes)
        this.payloadLength = dataInputStream.readInt();
        this.payload = new byte[payloadLength];
        dataInputStream.readFully(this.payload);
    }

    // Getters
    public int getCorrelationId() {
        return this.correlationId;
    }

    public MessageType getMessageType() {
        return this.messageType;
    }

    public String getTopic() {
        return this.topic;
    }

    public long getTimestamp() {
        return this.timestamp;
    }

    public int getOffset() {
        return this.offset;
    }

    public int getPayloadLength() {
        return this.payloadLength;
    }

    public byte[] getPayload() {
        return this.payload;
    }

    // String representation for testing
    @Override
    public String toString() {
        return "{" +
                "correlationId=" + getCorrelationId() +
                ", messageType=" + getMessageType() +
                ", topic='" + getTopic() + '\'' +
                ", timestamp=" + getTimestamp() +
                ", offset=" + getOffset() +
                ", payload=" + new String(getPayload()) +
                '}';
    }
}

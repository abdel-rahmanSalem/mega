package mega;

public enum MessageType {
    PRODUCE((byte) 0x01), // Produce message
    CONSUME((byte) 0x02); // Consume message

    private final byte code;

    MessageType(byte code) {
        this.code = code; // Assign the byte value to the enum constant
    }

    public static MessageType fromCode(byte code) {
        for (MessageType type : values()) { // Iterate through all enum values
            if (type.code == code) {
                return type; // Match found, return it
            }
        }
        throw new IllegalArgumentException("Unknown message type code: " + code);
    }

    public byte getCode() {
        return code; // Return the byte value of the enum
    }
}

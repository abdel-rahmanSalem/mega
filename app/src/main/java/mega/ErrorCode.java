package mega;

public enum ErrorCode {
    INVALID_REQUEST((byte) 1),
    INVALID_MESSAGE_TYPE((byte) 2),
    MESSAGE_TOO_LARGE((byte) 3),
    TOPIC_NOT_FOUND((byte) 4),
    TOPIC_ALREADY_EXISTS((byte) 5),
    INVALID_OFFSET((byte) 6),
    INTERNAL_ERROR((byte) 7),
    NETWORK_ERROR((byte) 8),
    RESOURCE_EXHAUSTED((byte) 9);

    private final byte code;

    ErrorCode(byte code) {
        this.code = code;
    }

    public byte getCode() {
        return code;
    }
}
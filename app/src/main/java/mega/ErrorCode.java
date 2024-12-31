package mega;

public enum ErrorCode {
    TOPIC_NOT_FOUND((byte) 1),
    INVALID_OFFSET((byte) 2),
    INTERNAL_ERROR((byte) 3),
    INVALID_MESSAGE_TYPE((byte) 4),
    NETWORK_ERROR((byte) 5),
    MESSAGE_TOO_LARGE((byte) 6),
    INVALID_REQUEST((byte) 7),
    RESOURCE_EXHAUSTED((byte) 8);

    private final byte code;

    ErrorCode(byte code) {
        this.code = code;
    }

    public byte getCode() {
        return code;
    }
}
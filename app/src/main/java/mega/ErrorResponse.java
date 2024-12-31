package mega;

import java.io.DataOutputStream;
import java.io.IOException;

public class ErrorResponse {
    private final int correlationId;
    private final ErrorCode errorCode;

    public ErrorResponse(int correlationId, ErrorCode errorCode) {
        this.correlationId = correlationId;
        this.errorCode = errorCode;
    }

    public void writeTo(DataOutputStream output) throws IOException {
        output.writeInt(correlationId);
        output.writeByte(0);
        output.writeByte(errorCode.getCode());
    }
}
package exception;

import enums.ErrorCode;

public class ProtocolException extends RuntimeException {

    private final ErrorCode errorCode;
    private final int correlationId;

    public ProtocolException(ErrorCode errorCode, int correlationId) {
        super(errorCode.name());
        this.errorCode = errorCode;
        this.correlationId = correlationId;
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }

    public int getCorrelationId() {
        return correlationId;
    }
}

package enums;

public enum ErrorCode {
    NONE(0),
    UNKNOWN_SERVER_ERROR(-1),
    UNKNOWN_TOPIC_OR_PARTITION(3),
    UNSUPPORTED_VERSION(35),
    UNKNOWN_TOPIC_ID(100);

    private final short value;

    ErrorCode(int value) {
        this.value = (short) value;
    }

    public short getValue() {
        return value;
    }
}
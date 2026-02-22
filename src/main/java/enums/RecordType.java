package enums;

public enum RecordType {
    TOPIC(2),
    PARTITION(3),
    FEATURE_LEVEL(12);

    private final int value;

    RecordType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }
}

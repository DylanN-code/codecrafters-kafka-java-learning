package domain.message;

import java.util.Objects;

public record KeyVersion(
        short key,
        short version
) {

    public static final KeyVersion API_VERSIONS = of(18, 4);
    public static final KeyVersion DESCRIBE_TOPIC_PARTITIONS = of(75, 0);
    public static final KeyVersion FETCH = of(1, 16);
    public static final KeyVersion PRODUCE = of(0, 11);

    public static KeyVersion of(short key, short version) {
        return new KeyVersion(key, version);
    }

    public static KeyVersion of(int key, int version) {
        return new KeyVersion((short) key, (short) version);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        KeyVersion that = (KeyVersion) object;
        return key == that.key && version == that.version;
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, version);
    }
}

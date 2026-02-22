package io;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.Function;

public interface DataInput {

    ByteBuffer readNBytes(int n);

    byte peekByte();

    byte readSignedByte();

    short readSignedShort();

    int readSignedInt();

    long readSignedLong();

    default UUID readUuid() {
        return new UUID(readSignedLong(), readSignedLong());
    }

    default long readUnsignedVarInt() {
        return VarInt.readLong(this);
    }

    default ByteBuffer readBytes() {
        final int length = readSignedInt();
        if (length == -1) {
            return null;
        }
        return readNBytes(length);
    }

    default ByteBuffer readCompactBytes() {
        final long length = readUnsignedVarInt();
        if (length == 0) {
            return null;
        }
        return readNBytes((int) length - 1);
    }

    default String readString() {
        final short length = readSignedShort();
        if (length == -1) {
            return null;
        }
        return asString(readNBytes(length));
    }

    default String readCompactString() {
        return asString(readCompactBytes());
    }

    default <T> List<T> readArray(Function<DataInput, T> deserializer) {
        final int length = readSignedInt();
        if (length == -1) {
            return null;
        }
        return readArray0(deserializer, length);
    }

    default <T> List<T> readCompactArray(Function<DataInput, T> deserializer) {
        long length = readUnsignedVarInt();
        if (length == 0) {
            return null;
        }
        length--;
        return readArray0(deserializer, (int) length);
    }

    default List<Integer> readCompactIntArray() {
        return readCompactArray((DataInput::readSignedInt));
    }

    default List<UUID> readCompactUUIDArray() {
        return readCompactArray(DataInput::readUuid);
    }

    default <K, V> Map<K, V> readCompactDict(Function<DataInput, K> keyDeserializer, Function<DataInput, V> valueDeserializer) {
        long length = readUnsignedVarInt();
        if (length == 0) {
            return null;
        }
        length--;
        final Map<K, V> map = new HashMap<>();
        for (int i = 0; i < length; i++) {
            map.put(keyDeserializer.apply(this), valueDeserializer.apply(this));
        }
        return map;
    }

    default void skipEmptyTaggedFieldArray() {
        readUnsignedVarInt();
    }

    private String asString(ByteBuffer byteBuffer) {
        if (byteBuffer == null) {
            return null;
        }
        return new String(byteBuffer.array(), byteBuffer.arrayOffset(), byteBuffer.limit(), StandardCharsets.UTF_8);
    }

    private <T> List<T> readArray0(Function<DataInput, T> deserializer, int length) {
        final List<T> items = new ArrayList<>(length);
        for (int i = 0; i < length; i++) {
            items.add(deserializer.apply(this));
        }
        return items;
    }
}

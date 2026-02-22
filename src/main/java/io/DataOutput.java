package io;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.function.BiConsumer;

public interface DataOutput {

    void writeBytes(byte[] bytes);

    void writeByte(byte b);

    void writeShort(short s);

    void writeInt(int i);

    void writeLong(long l);

    default void writeBoolean(boolean b) {
        writeByte((byte) (b ? 1 : 0));
    }

    default void writeUuid(UUID uuid) {
        writeLong(uuid.getMostSignificantBits()); // write the most significant bits first
        writeLong(uuid.getLeastSignificantBits()); // write the least significant bits second
    }

    default void writeUnsignedVarInt(long l) {
        VarInt.writeLong(l, this);
    }

    default void writeCompactBytes(byte[] bytes) {
        if (bytes == null) {
            writeUnsignedVarInt(0L);
            return;
        }
        writeUnsignedVarInt(bytes.length + 1);
        writeBytes(bytes);
    }

    default void writeString(String s) {
        if (s == null) {
            writeShort((short) -1);
            return;
        }
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        writeShort((short) bytes.length);
        writeBytes(bytes);
    }

    default void writeCompactString(String s) {
        if (s == null) {
            writeShort((short) 0);
            return;
        }
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        writeShort((short) (bytes.length+1));
        writeBytes(bytes);
    }

    default <T> void writeCompactArray(List<T> items, BiConsumer<T, DataOutput> serializer) {
        if (items == null) {
            writeUnsignedVarInt(0);
            return;
        }
        writeUnsignedVarInt(items.size()+1);
        for (T item: items) {
            serializer.accept(item, this);
        }
    }

    default void writeCompactIntArray(List<Integer> items) {
        writeCompactArray(items, (value, output) -> output.writeInt(value));
    }

    default void skipEmptyTaggedFieldArray() {
        writeUnsignedVarInt(0);
    }
}

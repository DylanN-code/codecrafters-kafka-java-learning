package io;

import util.GeneralUtil;

import java.io.DataInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Optional;

public class KafkaDataInputStream implements DataInput {

    private final DataInputStream dataInputStream;

    public KafkaDataInputStream(InputStream inputStream) {
        this.dataInputStream = new DataInputStream(inputStream);
    }

    @Override
    public ByteBuffer readNBytes(int n) {
        return GeneralUtil.tryCatch(() -> ByteBuffer.wrap(getDataInputStream().readNBytes(n)));
    }

    @Override
    public byte peekByte() {
        getDataInputStream().mark(1);
        Byte value = GeneralUtil.tryCatch(dataInputStream::readByte);
        GeneralUtil.tryCatch(dataInputStream::reset);
        return Optional.ofNullable(value).orElse((byte) -1);
    }

    @Override
    public byte readSignedByte() {
        Byte value = GeneralUtil.tryCatch(dataInputStream::readByte);
        return Optional.ofNullable(value).orElse((byte) -1);
    }

    @Override
    public short readSignedShort() {
        Short value = GeneralUtil.tryCatch(dataInputStream::readShort);
        return Optional.ofNullable(value).orElse((short) -1);
    }

    @Override
    public int readSignedInt() {
        Integer value = GeneralUtil.tryCatch(dataInputStream::readInt);
        return Optional.ofNullable(value).orElse(-1);
    }

    @Override
    public long readSignedLong() {
        Long value = GeneralUtil.tryCatch(dataInputStream::readLong);
        return Optional.ofNullable(value).orElse((long) -1);
    }

    private DataInputStream getDataInputStream() {
        return dataInputStream;
    }
}

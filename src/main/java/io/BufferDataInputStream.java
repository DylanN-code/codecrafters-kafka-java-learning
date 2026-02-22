package io;

import util.GeneralUtil;

import java.nio.ByteBuffer;

public class BufferDataInputStream implements DataInput {

    private final ByteBuffer byteBuffer;

    public BufferDataInputStream(ByteBuffer byteBuffer) {
        this.byteBuffer = byteBuffer;
    }

    @Override
    public ByteBuffer readNBytes(int n) {
        byte[] bytes = new byte[n];
        GeneralUtil.tryCatch(() -> getByteBuffer().get(bytes));
        return ByteBuffer.wrap(bytes);
    }

    @Override
    public byte peekByte() {
        getByteBuffer().mark();
        byte b = getByteBuffer().get();
        getByteBuffer().reset();
        return b;
    }

    @Override
    public byte readSignedByte() {
        return getByteBuffer().get();
    }

    @Override
    public short readSignedShort() {
        return getByteBuffer().getShort();
    }

    @Override
    public int readSignedInt() {
        return getByteBuffer().getInt();
    }

    @Override
    public long readSignedLong() {
        return getByteBuffer().getLong();
    }

    private ByteBuffer getByteBuffer() {
        return byteBuffer;
    }
}

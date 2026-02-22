package io;

import util.GeneralUtil;

import java.io.DataOutputStream;
import java.io.OutputStream;

public class KafkaDataOutputStream implements DataOutput {

    private final DataOutputStream dataOutputStream;

    public KafkaDataOutputStream(OutputStream outputStream) {
        this.dataOutputStream = new DataOutputStream(outputStream);
    }

    @Override
    public void writeBytes(byte[] bytes) {
        GeneralUtil.tryCatch(() -> getDataOutputStream().write(bytes));
    }

    @Override
    public void writeByte(byte b) {
        GeneralUtil.tryCatch(() -> getDataOutputStream().write(b));
    }

    @Override
    public void writeShort(short s) {
        GeneralUtil.tryCatch(() -> getDataOutputStream().writeShort(s));
    }

    @Override
    public void writeInt(int i) {
        GeneralUtil.tryCatch(() -> getDataOutputStream().writeInt(i));
    }

    @Override
    public void writeLong(long l) {
        GeneralUtil.tryCatch(() -> getDataOutputStream().writeLong(l));
    }

    private DataOutputStream getDataOutputStream() {
        return dataOutputStream;
    }
}

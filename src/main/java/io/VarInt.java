package io;

public class VarInt {

    /**
     * @author https://github.com/bazelbuild/bazel/blob/b8073bbcaa63c9405824f94184014b19a2255a52/src/main/java/com/google/devtools/build/lib/util/VarInt.java#L149
     */
    public static long readLong(DataInput input) {
        long tmp;
        if ((tmp = input.readSignedByte()) >= 0) {
            return tmp;
        }
        long result = tmp & 0x7f;
        if ((tmp = input.readSignedByte()) >= 0) {
            result |= tmp << 7;
        } else {
            result |= (tmp & 0x7f) << 7;
            if ((tmp = input.readSignedByte()) >= 0) {
                result |= tmp << 14;
            } else {
                result |= (tmp & 0x7f) << 14;
                if ((tmp = input.readSignedByte()) >= 0) {
                    result |= tmp << 21;
                } else {
                    result |= (tmp & 0x7f) << 21;
                    if ((tmp = input.readSignedByte()) >= 0) {
                        result |= tmp << 28;
                    } else {
                        result |= (tmp & 0x7f) << 28;
                        if ((tmp = input.readSignedByte()) >= 0) {
                            result |= tmp << 35;
                        } else {
                            result |= (tmp & 0x7f) << 35;
                            if ((tmp = input.readSignedByte()) >= 0) {
                                result |= tmp << 42;
                            } else {
                                result |= (tmp & 0x7f) << 42;
                                if ((tmp = input.readSignedByte()) >= 0) {
                                    result |= tmp << 49;
                                } else {
                                    result |= (tmp & 0x7f) << 49;
                                    if ((tmp = input.readSignedByte()) >= 0) {
                                        result |= tmp << 56;
                                    } else {
                                        result |= (tmp & 0x7f) << 56;
                                        result |= ((long) input.readSignedByte()) << 63;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        return result;
    }

    /**
     * @author https://github.com/bazelbuild/bazel/blob/b8073bbcaa63c9405824f94184014b19a2255a52/src/main/java/com/google/devtools/build/lib/util/VarInt.java#L205
     */
    public static void writeLong(long l, DataOutput dataOutput) {
        while (true) {
            int bits = ((int) l) & 0x7f;
            l >>>= 7;
            if (l == 0) {
                dataOutput.writeByte((byte) bits);
                return;
            }
            dataOutput.writeByte((byte) (bits | 0x80));
        }
    }
}

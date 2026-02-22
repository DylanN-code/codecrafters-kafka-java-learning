package domain.logdata;

import domain.logdata.record.Record;
import io.DataInput;

import java.util.List;

public record Batch(
        long baseOffset,
        int partitionLeaderEpoch,
        byte magic,
        int crc,
        short attributes,
        int lastOffsetDelta,
        long baseTimestamp,
        long maxTimestamp,
        long producerId,
        short producerEpoch,
        int baseSequence,
        List<Record> records
) {

    public static Batch deserialize(DataInput dataInput) {
        long baseOffset = dataInput.readSignedLong();
        int partitionLeaderEpoch = dataInput.readSignedInt();
        byte magic = dataInput.readSignedByte();
        int crc = dataInput.readSignedInt();
        short attributes = dataInput.readSignedShort();
        int lastOffsetDelta = dataInput.readSignedInt();
        long baseTimestamp = dataInput.readSignedLong();
        long maxTimestamp = dataInput.readSignedLong();
        long producerId = dataInput.readSignedLong();
        short producerEpoch = dataInput.readSignedShort();
        int baseSequence = dataInput.readSignedInt();
        List<Record> recordList = dataInput.readArray(Record::deserialize);

        return new Batch(
                baseOffset,
                partitionLeaderEpoch,
                magic,
                crc,
                attributes,
                lastOffsetDelta,
                baseTimestamp,
                maxTimestamp,
                producerId,
                producerEpoch,
                baseSequence,
                recordList
        );
    }

    public Long getEndOffset() {
        return baseOffset + lastOffsetDelta;
    }

}

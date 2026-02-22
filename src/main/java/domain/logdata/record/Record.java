package domain.logdata.record;

import io.DataInput;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public interface Record {

    @SuppressWarnings("unused")
    static Record deserialize(DataInput dataInput) {
        long length = dataInput.readUnsignedVarInt();
        byte attributes = dataInput.readSignedByte();
        long timestampDelta = dataInput.readUnsignedVarInt();
        long offsetDelta = dataInput.readUnsignedVarInt();
        String key = dataInput.readCompactString();
        long valueLength = dataInput.readUnsignedVarInt();
        byte recordFrameVersion = dataInput.readSignedByte();
        byte recordType = dataInput.readSignedByte();
        byte recordVersion = dataInput.readSignedByte();

        Record record = switch (recordType) {
            case 2 -> Topic.deserialize(dataInput);
            case 3 -> Partition.deserialize(dataInput);
            case 12 -> FeatureLevel.deserialize(dataInput);
            default -> throw new IllegalArgumentException("unknown record type: %s".formatted(recordType));
        };

        Map<String, ByteBuffer> headers = dataInput.readCompactDict(
                DataInput::readCompactString,
                DataInput::readCompactBytes
        );

        return record;
    }

    record Topic(
            String name,
            UUID uuid
    ) implements Record {

        public static Topic deserialize(DataInput dataInput) {
            String name = dataInput.readCompactString();
            UUID uuid = dataInput.readUuid();

            dataInput.skipEmptyTaggedFieldArray();

            return new Topic(name, uuid);
        }

    }

    record Partition(
            int id,
            UUID topicId,
            List<Integer> replicas,
            List<Integer> inSyncReplicas,
            List<Integer> removingReplicas,
            List<Integer> addingReplicas,
            int leader,
            int leaderEpoch,
            int partitionEpoch,
            List<UUID> directories
    ) implements Record {

        public static Record deserialize(DataInput dataInput) {
            int id = dataInput.readSignedInt();
            UUID topicId = dataInput.readUuid();
            List<Integer> replicas = dataInput.readCompactIntArray();
            List<Integer> inSyncReplicas = dataInput.readCompactIntArray();
            List<Integer> removingReplicas = dataInput.readCompactIntArray();
            List<Integer> addingReplicas = dataInput.readCompactIntArray();
            int leader = dataInput.readSignedInt();
            int leaderEpoch = dataInput.readSignedInt();
            int partitionEpoch = dataInput.readSignedInt();
            List<UUID> directories = dataInput.readCompactUUIDArray();

            dataInput.skipEmptyTaggedFieldArray();

            return new Partition(
                    id,
                    topicId,
                    replicas,
                    inSyncReplicas,
                    removingReplicas,
                    addingReplicas,
                    leader,
                    leaderEpoch,
                    partitionEpoch,
                    directories
            );
        }

    }

    record FeatureLevel(
            String name,
            short featureLevel
    ) implements Record {

        public static Record deserialize(DataInput dataInput) {
            String name = dataInput.readCompactString();
            short featureLevel = dataInput.readSignedShort();

            dataInput.skipEmptyTaggedFieldArray();

            return new FeatureLevel(name, featureLevel);
        }

    }
}

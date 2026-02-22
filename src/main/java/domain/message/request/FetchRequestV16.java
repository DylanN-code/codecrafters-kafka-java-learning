package domain.message.request;

import domain.message.RequestBody;
import io.DataInput;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

public record FetchRequestV16(
        Duration maxWait,
        int minBytes,
        int maxBytes,
        byte isolationLevel,
        int sessionId,
        int sessionEpoch,
        List<Topic> topics,
        List<ForgottenTopic> forgottenTopics,
        String rackId
) implements RequestBody {

    public static RequestBody deserialize(DataInput dataInput) {
        Duration maxWait = Duration.ofMillis(dataInput.readSignedInt());
        int minBytes = dataInput.readSignedInt();
        int maxBytes = dataInput.readSignedInt();
        byte isolationLevel = dataInput.readSignedByte();
        int sessionId = dataInput.readSignedInt();
        int sessionEpoch = dataInput.readSignedInt();
        List<Topic> topics = dataInput.readCompactArray(Topic::deserialize);
        List<ForgottenTopic> forgottenTopics = dataInput.readCompactArray(ForgottenTopic::deserialize);
        String rackId = dataInput.readCompactString();

        dataInput.skipEmptyTaggedFieldArray();

        return new FetchRequestV16(
                maxWait,
                minBytes,
                maxBytes,
                isolationLevel,
                sessionId,
                sessionEpoch,
                topics,
                forgottenTopics,
                rackId
        );
    }

    public record Topic(
            UUID topicUuid,
            List<Partition> partitions
    ) {

        public static Topic deserialize(DataInput dataInput) {
            UUID topicUuid = dataInput.readUuid();
            List<Partition> partitions = dataInput.readCompactArray(Partition::deserialize);

            dataInput.skipEmptyTaggedFieldArray();

            return new Topic(topicUuid, partitions);
        }
    }

    public record Partition(
            int partition,
            int currentLeaderEpoch,
            long fetchOffset,
            int lastFetchedEpoch,
            long logStartOffset,
            int partitionMaxBytes
    ) {

        public static Partition deserialize(DataInput dataInput) {
            int partition = dataInput.readSignedInt();
            int currentLeaderEpoch = dataInput.readSignedInt();
            long fetchOffset = dataInput.readSignedLong();
            int lastFetchedEpoch = dataInput.readSignedInt();
            long logStartOffset = dataInput.readSignedLong();
            int partitionMaxBytes = dataInput.readSignedInt();

            dataInput.skipEmptyTaggedFieldArray();

            return new Partition(
                    partition,
                    currentLeaderEpoch,
                    fetchOffset,
                    lastFetchedEpoch,
                    logStartOffset,
                    partitionMaxBytes
            );
        }
    }

    public record ForgottenTopic(
            UUID topicId,
            List<Integer> partitions
    ) {

        public static ForgottenTopic deserialize(DataInput dataInput) {
            UUID topicId = dataInput.readUuid();
            List<Integer> partitions = dataInput.readCompactArray(DataInput::readSignedInt);
            return new ForgottenTopic(topicId, partitions);
        }
    }
}

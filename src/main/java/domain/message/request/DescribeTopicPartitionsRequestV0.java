package domain.message.request;

import domain.message.RequestBody;
import io.DataInput;

import java.util.List;

public record DescribeTopicPartitionsRequestV0(
        List<Topic> topics,
        int responsePartitionLimit,
        DescribeTopicPartitionsCursorV0 cursor
) implements RequestBody {

    public static RequestBody deserialize(DataInput dataInput) {
        final List<Topic> topics = dataInput.readCompactArray(Topic::deserialize);
        final int responsePartitionLimit = dataInput.readSignedInt();
        final DescribeTopicPartitionsCursorV0 cursor = DescribeTopicPartitionsCursorV0.deserialize(dataInput);
        return new DescribeTopicPartitionsRequestV0(topics, responsePartitionLimit, cursor);
    }

    public record Topic(
            String name
    ) {

        public static Topic deserialize(DataInput dataInput) {
            final String name = dataInput.readCompactString();

            dataInput.skipEmptyTaggedFieldArray();

            return new Topic(name);
        }
    }

    public record DescribeTopicPartitionsCursorV0(
            String topicName,
            int partitionIndex
    ) {

        public static DescribeTopicPartitionsCursorV0 deserialize(DataInput dataInput) {
            if (dataInput.peekByte() == (byte) 0xff) {
                return null;
            }

            final String topicName = dataInput.readCompactString();
            final int partitionIndex = dataInput.readSignedInt();

            dataInput.skipEmptyTaggedFieldArray();

            return new DescribeTopicPartitionsCursorV0(topicName, partitionIndex);
        }
    }
}

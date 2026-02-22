package domain.message.request;

import domain.message.RequestBody;
import io.DataInput;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;

public record ProduceRequestV11(
        String transactionId,
        short acks,
        Duration timeout,
        List<Topic> topics
) implements RequestBody {

    public static ProduceRequestV11 deserialize(DataInput dataInput) {
        String transactionId = dataInput.readCompactString();
        short ACKs = dataInput.readSignedShort();
        Duration timeout = Duration.ofMillis(dataInput.readSignedInt());
        List<Topic> topics = dataInput.readCompactArray(Topic::deserialize);

        dataInput.skipEmptyTaggedFieldArray();

        return new ProduceRequestV11(transactionId, ACKs, timeout, topics);
    }

    public record Topic(
            String name,
            List<Partition> partitions
    ) {

        public static Topic deserialize(DataInput dataInput) {
            String name = dataInput.readCompactString();
            List<Partition> partitions = dataInput.readCompactArray(Partition::deserialize);

            dataInput.skipEmptyTaggedFieldArray();

            return new Topic(name, partitions);
        }

    }

    public record Partition(
            int index,
            ByteBuffer byteBuffer
    ) {

        public static Partition deserialize(DataInput dataInput) {
            int index = dataInput.readSignedInt();
            ByteBuffer byteBuffer = dataInput.readCompactBytes();

            dataInput.skipEmptyTaggedFieldArray();

            return new Partition(index, byteBuffer);
        }
    }
}

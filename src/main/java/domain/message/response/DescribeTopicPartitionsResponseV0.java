package domain.message.response;

import constant.Constant;
import domain.logdata.record.Record;
import domain.message.Header;
import domain.message.RequestBody;
import domain.message.ResponseBody;
import domain.message.request.DescribeTopicPartitionsRequestV0;
import enums.ErrorCode;
import io.DataOutput;
import kafka.Kafka;

import java.time.Duration;
import java.util.*;

public record DescribeTopicPartitionsResponseV0(
        Duration throttleTime,
        List<Topic> topics,
        DescribeTopicPartitionsCursorV0 cursor
) implements ResponseBody {

    public static Header handleHeader(Header header) {
        Header.V2 headerV2 = (Header.V2) header;
        return new Header.V1(headerV2.correlationId());
    }

    public static DescribeTopicPartitionsResponseV0 handle(RequestBody requestBody) {
        DescribeTopicPartitionsRequestV0 apiRequest = (DescribeTopicPartitionsRequestV0) requestBody;
        List<Topic> topicList = new ArrayList<>();

        for (DescribeTopicPartitionsRequestV0.Topic requestTopic : apiRequest.topics()) {
            Record.Topic recordTopic = Kafka.getTopicByName(requestTopic.name());
            if (recordTopic == null) {
                topicList.add(new Topic(
                        ErrorCode.UNKNOWN_TOPIC_OR_PARTITION,
                        requestTopic.name(),
                        new UUID(Constant.DEFAULT_UUID_MOST_SIGN_BITS, Constant.DEFAULT_UUID_LEAST_SIG_BITS),
                        Constant.DEFAULT_TOPIC_IS_INTERNAL,
                        Collections.emptyList(),
                        Constant.DEFAULT_TOPIC_AUTHORIZED_OPERATIONS
                ));
            } else {
                List<Record.Partition> recordPartitions = Kafka.getPartitionListByTopicId(recordTopic.uuid());
                List<Partition> partitions = recordPartitions == null ? Collections.emptyList() : recordPartitions.stream()
                        .map(p -> new Partition(
                                ErrorCode.NONE,
                                p.id(),
                                p.leader(),
                                p.leaderEpoch(),
                                p.replicas(),
                                p.inSyncReplicas(),
                                Collections.emptyList(),
                                Collections.emptyList(),
                                Collections.emptyList()
                        ))
                        .toList();
                topicList.add(new Topic(
                        ErrorCode.NONE,
                        recordTopic.name(),
                        recordTopic.uuid(),
                        Constant.DEFAULT_TOPIC_IS_INTERNAL,
                        partitions,
                        Constant.DEFAULT_TOPIC_AUTHORIZED_OPERATIONS
                ));
            }
        }
        return new DescribeTopicPartitionsResponseV0(Duration.ZERO, topicList, null);
    }

    @Override
    public void serialize(DataOutput dataOutput) {
        dataOutput.writeInt((int) throttleTime.toMillis());
        dataOutput.writeCompactArray(topics, Topic::serialize);
        if (cursor == null) {
            dataOutput.writeLong(Constant.DEFAULT_NULL_NEXT_CURSOR);
        } else {
            cursor.serialize(dataOutput);
        }

        dataOutput.skipEmptyTaggedFieldArray();
    }

    public record Topic(
            ErrorCode errorCode,
            String name,
            UUID topicId,
            boolean isInternal,
            List<Partition> partitions,
            int topicAuthorizedOperations
    ) implements ResponseBody, Comparable<Topic> {

        @Override
        public void serialize(DataOutput dataOutput) {
            dataOutput.writeShort(errorCode.getValue());
            dataOutput.writeCompactString(name);
            dataOutput.writeUuid(topicId);
            dataOutput.writeBoolean(isInternal);
            dataOutput.writeCompactArray(partitions, Partition::serialize);
            dataOutput.writeInt(topicAuthorizedOperations);

            dataOutput.skipEmptyTaggedFieldArray();
        }

        @Override
        public int compareTo(Topic o) {
            return Objects.compare(o.name(), this.name(), String::compareTo);
        }
    }

    public record Partition(
            ErrorCode errorCode,
            int partitionIndex,
            int leaderId,
            int leaderEpoch,
            List<Integer> replicaNodes,
            List<Integer> inSyncReplicasNodes,
            List<Integer> eligibleLeaderReplicas,
            List<Integer> lastKnownElr,
            List<Integer> offlineReplicas
    ) implements ResponseBody {

        @Override
        public void serialize(DataOutput dataOutput) {
            dataOutput.writeShort(errorCode.getValue());
            dataOutput.writeInt(partitionIndex);
            dataOutput.writeInt(leaderId);
            dataOutput.writeInt(leaderEpoch);
            dataOutput.writeCompactIntArray(replicaNodes);
            dataOutput.writeCompactIntArray(inSyncReplicasNodes);
            dataOutput.writeCompactIntArray(eligibleLeaderReplicas);
            dataOutput.writeCompactIntArray(lastKnownElr);
            dataOutput.writeCompactIntArray(offlineReplicas);

            dataOutput.skipEmptyTaggedFieldArray();
        }
    }

    public record DescribeTopicPartitionsCursorV0(
            String topicName,
            int partitionIndex
    ) implements ResponseBody {

        @Override
        public void serialize(DataOutput dataOutput) {
            dataOutput.writeCompactString(topicName);
            dataOutput.writeInt(partitionIndex);

            dataOutput.skipEmptyTaggedFieldArray();
        }
    }
}

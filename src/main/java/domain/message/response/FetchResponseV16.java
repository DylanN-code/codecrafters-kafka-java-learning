package domain.message.response;

import constant.Constant;
import domain.logdata.record.Record;
import domain.message.Header;
import domain.message.RequestBody;
import domain.message.ResponseBody;
import domain.message.request.FetchRequestV16;
import enums.ErrorCode;
import io.DataOutput;
import kafka.Kafka;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public record FetchResponseV16(
        Duration throttleTime,
        ErrorCode errorCode,
        int sessionId,
        List<Response> responses
) implements ResponseBody {

    public static Header handleHeader(Header header) {
        Header.V2 headerV2 = (Header.V2) header;
        return new Header.V1(headerV2.correlationId());
    }

    public static FetchResponseV16 handle(RequestBody requestBody) {
        FetchRequestV16 apiRequest = (FetchRequestV16) requestBody;
        List<Response> responseList = new ArrayList<>();

        for (FetchRequestV16.Topic requestTopic : apiRequest.topics()) {
            UUID topicId = requestTopic.topicUuid();
            Record.Topic recordTopic = Kafka.getTopicById(topicId);

            if (recordTopic == null) {
                responseList.add(new Response(
                        requestTopic.topicUuid(),
                        List.of(
                                new Partition(
                                        Constant.DEFAULT_PARTITION_INDEX,
                                        ErrorCode.UNKNOWN_TOPIC_ID,
                                        Constant.DEFAULT_HIGH_WATER_MARK,
                                        Constant.DEFAULT_LAST_STABLE_OFFSET,
                                        Constant.DEFAULT_LOG_START_OFFSET,
                                        Collections.emptyList(),
                                        Constant.DEFAULT_PREFERRED_READ_REPLICA,
                                        new byte[0]
                                )
                        )
                ));
                continue;
            }

            String topicName = recordTopic.name();
            List<Partition> partitionList = new ArrayList<>();
            for (FetchRequestV16.Partition partitionRequest : requestTopic.partitions()) {
                int partitionIndex = partitionRequest.partition();
                partitionList.add(new Partition(
                        partitionIndex,
                        ErrorCode.NONE,
                        Constant.DEFAULT_HIGH_WATER_MARK,
                        Constant.DEFAULT_LAST_STABLE_OFFSET,
                        Constant.DEFAULT_LOG_START_OFFSET,
                        Collections.emptyList(),
                        Constant.DEFAULT_PREFERRED_READ_REPLICA,
                        Kafka.readRawBatchData(topicName, partitionIndex)
                ));
            }

            responseList.add(new Response(
                    topicId,
                    partitionList
            ));
        }

        return new FetchResponseV16(
                Duration.ZERO,
                ErrorCode.NONE,
                apiRequest.sessionId(),
                responseList
        );
    }

    @Override
    public void serialize(DataOutput dataOutput) {
        dataOutput.writeInt((int) throttleTime.toMillis());
        dataOutput.writeShort(errorCode.getValue());
        dataOutput.writeInt(sessionId);
        dataOutput.writeCompactArray(responses, Response::serialize);

        dataOutput.skipEmptyTaggedFieldArray();
    }

    public record Response(
            UUID topicId,
            List<Partition> partitions
    ) implements ResponseBody {
        @Override
        public void serialize(DataOutput dataOutput) {
            dataOutput.writeUuid(topicId);
            dataOutput.writeCompactArray(partitions, Partition::serialize);

            dataOutput.skipEmptyTaggedFieldArray();
        }
    }

    public record Partition(
            int partitionIndex,
            ErrorCode errorCode,
            long highWatermark,
            long lastStableOffset,
            long logStartOffset,
            List<AbortedTransaction> abortedTransactions,
            int preferredReadReplica,
            byte[] bytes
    ) implements ResponseBody {

        @Override
        public void serialize(DataOutput dataOutput) {
            dataOutput.writeInt(partitionIndex);
            dataOutput.writeShort(errorCode.getValue());
            dataOutput.writeLong(highWatermark);
            dataOutput.writeLong(lastStableOffset);
            dataOutput.writeLong(logStartOffset);
            dataOutput.writeCompactArray(abortedTransactions, AbortedTransaction::serialize);
            dataOutput.writeInt(preferredReadReplica);
            dataOutput.writeBytes(bytes);

            dataOutput.skipEmptyTaggedFieldArray();
        }
    }

    public record AbortedTransaction(
            long producerId,
            long firstOffset
    ) implements ResponseBody {

        @Override
        public void serialize(DataOutput dataOutput) {
            dataOutput.writeLong(producerId);
            dataOutput.writeLong(firstOffset);

            dataOutput.skipEmptyTaggedFieldArray();
        }
    }
}

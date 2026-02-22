package domain.message.response;

import constant.Constant;
import domain.logdata.record.Record;
import domain.message.Header;
import domain.message.RequestBody;
import domain.message.ResponseBody;
import domain.message.request.ProduceRequestV11;
import enums.ErrorCode;
import io.DataOutput;
import kafka.Kafka;

import java.time.Duration;
import java.util.*;

public record ProduceResponseV11(
        List<Response> responses,
        Duration throttleTime
) implements ResponseBody {

    public static Header handleHeader(Header header) {
        Header.V2 headerV2 = (Header.V2) header;
        return new Header.V1(headerV2.correlationId());
    }

    public static ProduceResponseV11 handle(RequestBody requestBody) {
        ProduceRequestV11 apiRequest = (ProduceRequestV11) requestBody;
        List<Response> responseList = new ArrayList<>();

        for (ProduceRequestV11.Topic requestTopic : apiRequest.topics()) {
            String topicName = requestTopic.name();
            Record.Topic recordTopic = Kafka.getTopicByName(topicName);

            if (recordTopic == null) {
                responseList.add(new Response(
                        topicName,
                        List.of(new Partition(
                                Constant.DEFAULT_PARTITION_INDEX,
                                ErrorCode.UNKNOWN_TOPIC_OR_PARTITION,
                                Constant.DEFAULT_BASE_OFFSET,
                                Constant.DEFAULT_LOG_APPEND_TIME_MS,
                                Constant.DEFAULT_LOG_START_OFFSET_V2,
                                Collections.emptyList(),
                                null
                        ))
                ));
                continue;
            }

            UUID recordTopicId = recordTopic.uuid();
            List<Partition> responsePartitionList = new ArrayList<>();
            for (ProduceRequestV11.Partition requestPartition : requestTopic.partitions()) {
                int partitionIndex = requestPartition.index();

                List<Record.Partition> partitionRecordList = Kafka.getPartitionListByTopicId(recordTopicId);
                if (partitionRecordList == null || partitionRecordList.isEmpty()) {
                    responsePartitionList.add(new Partition(
                            partitionIndex,
                            ErrorCode.UNKNOWN_TOPIC_OR_PARTITION,
                            Constant.DEFAULT_BASE_OFFSET,
                            Constant.DEFAULT_LOG_APPEND_TIME_MS,
                            Constant.DEFAULT_LOG_START_OFFSET_V2,
                            Collections.emptyList(),
                            null
                    ));
                    continue;
                }

                Record.Partition partitionRecord = partitionRecordList.stream()
                        .filter(partition -> Objects.equals(partition.id(), partitionIndex))
                        .findFirst()
                        .orElse(null);
                if (partitionRecord == null) {
                    responsePartitionList.add(new Partition(
                            partitionIndex,
                            ErrorCode.UNKNOWN_TOPIC_OR_PARTITION,
                            Constant.DEFAULT_BASE_OFFSET,
                            Constant.DEFAULT_LOG_APPEND_TIME_MS,
                            Constant.DEFAULT_LOG_START_OFFSET_V2,
                            Collections.emptyList(),
                            null
                    ));
                    continue;
                }

                NewOffsetResponse newOffsetResponse = Kafka.appendBatchData(topicName, partitionIndex, requestPartition.byteBuffer());
                responsePartitionList.add(new Partition(
                        partitionIndex,
                        ErrorCode.NONE,
                        Objects.nonNull(newOffsetResponse) ? newOffsetResponse.nextOffset() : Constant.DEFAULT_NEW_OFFSET,
                        Constant.DEFAULT_LOG_APPEND_TIME_MS,
                        Objects.nonNull(newOffsetResponse) ? newOffsetResponse.logStartOffset() : Constant.DEFAULT_NEW_OFFSET,
                        Collections.emptyList(),
                        null
                ));
            }

            responseList.add(new Response(
                    topicName,
                    responsePartitionList
            ));
        }

        return new ProduceResponseV11(responseList, Duration.ZERO);
    }

    @Override
    public void serialize(DataOutput dataOutput) {
        dataOutput.writeCompactArray(responses, Response::serialize);
        dataOutput.writeInt((int) throttleTime.toMillis());

        dataOutput.skipEmptyTaggedFieldArray();
    }

    public record Response(
            String name,
            List<Partition> partitions
    ) implements ResponseBody {
        @Override
        public void serialize(DataOutput dataOutput) {
            dataOutput.writeCompactString(name);
            dataOutput.writeCompactArray(partitions, Partition::serialize);

            dataOutput.skipEmptyTaggedFieldArray();
        }
    }

    public record Partition(
            int index,
            ErrorCode errorCode,
            long baseOffset,
            long logAppendTimeMs,
            long logStartOffset,
            List<RecordError> recordErrors,
            String errorMessage
    ) implements ResponseBody {

        @Override
        public void serialize(DataOutput dataOutput) {
            dataOutput.writeInt(index);
            dataOutput.writeShort(errorCode.getValue());
            dataOutput.writeLong(baseOffset);
            dataOutput.writeLong(logAppendTimeMs);
            dataOutput.writeLong(logStartOffset);
            dataOutput.writeCompactArray(recordErrors, RecordError::serialize);
            dataOutput.writeCompactString(errorMessage);

            dataOutput.skipEmptyTaggedFieldArray();
        }
    }

    public record RecordError(
            int batchIndex,
            String batchIndexErrorMessage
    ) implements ResponseBody {

        @Override
        public void serialize(DataOutput dataOutput) {
            dataOutput.writeInt(batchIndex);
            dataOutput.writeString(batchIndexErrorMessage);

            dataOutput.skipEmptyTaggedFieldArray();
        }
    }
}

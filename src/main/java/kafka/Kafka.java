package kafka;

import constant.Constant;
import domain.logdata.Batch;
import domain.logdata.record.Record;
import domain.message.response.NewOffsetResponse;
import io.KafkaDataInputStream;
import io.KafkaDataOutputStream;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Kafka {

    private static final Map<UUID, Record.Topic> TOPIC_PER_ID_MAP = new HashMap<>();
    private static final Map<String, Record.Topic> TOPIC_PER_NAME_MAP = new HashMap<>();
    private static final Map<UUID, List<Record.Partition>> LIST_PARTITION_PER_TOPIC_ID_MAP = new HashMap<>();
    private static File LOG_ROOT;

    /**
     * Step 1: Load local appended log files to in-memory data
     */
    public static void load(File logRoot) {
        List<Record.Topic> topicList = new ArrayList<>();
        List<Record.Partition> partitionList = new ArrayList<>();

        // step 1: init variables
        if (logRoot == null) {
            return;
        }
        if (LOG_ROOT == null) {
            LOG_ROOT = logRoot;
        }
        File firstLogFile = getLogFile(logRoot, Constant.DEFAULT_CLUSTER_METADATA_TOPIC_NAME, Constant.INITIAL_PARTITION_INDEX);
        if (!firstLogFile.exists()) {
            return;
        }

        // step 2: read log data from local appended log files
        try {
            FileInputStream fileInputStream = new FileInputStream(firstLogFile);
            KafkaDataInputStream kafkaDataInputStream = new KafkaDataInputStream(fileInputStream);
            while (fileInputStream.available() != Constant.EOF_INDICATOR) {
                Batch batch = Batch.deserialize(kafkaDataInputStream);
                if (batch.records() == null) {
                    break;
                }
                for (Record record : batch.records()) {
                    if (record == null) {
                        return;
                    }
                    if (record instanceof Record.Topic topic) {
                        topicList.add(topic);
                    } else if (record instanceof Record.Partition partition) {
                        partitionList.add(partition);
                    }
                }
            }
        } catch (IOException e) {
            System.out.printf("failed to read log file data due to %s%n", e.getMessage());
        }

        // step 3: fill in-memory data structures
        Map<UUID, Record.Topic> topicPerIdMap = topicList
                .stream()
                .collect(Collectors.toMap(Record.Topic::uuid, Function.identity()));
        TOPIC_PER_ID_MAP.putAll(topicPerIdMap);

        Map<String, Record.Topic> topicPerNameMap = topicList
                .stream()
                .collect(Collectors.toMap(Record.Topic::name, Function.identity()));
        TOPIC_PER_NAME_MAP.putAll(topicPerNameMap);

        Map<UUID, List<Record.Partition>> partitionPerTopicIdMap = partitionList
                .stream()
                .collect(Collectors.groupingBy(Record.Partition::topicId));
        LIST_PARTITION_PER_TOPIC_ID_MAP.putAll(partitionPerTopicIdMap);
    }

    private static File getLogFile(String topicName, int partitionIndex) {
        if (LOG_ROOT == null) {
            return null;
        }
        return getLogFile(LOG_ROOT, topicName, partitionIndex);
    }

    private static File getLogFile(File logRoot, String topicName, int partitionIndex) {
        /* Params: parent – The parent abstract pathname; child – The child pathname string */
        return new File(logRoot, String.format(Constant.FORMATTED_LOG_ROOT_FILE_PATH, topicName, partitionIndex));
    }


    /**
     * Step 2: Query in-memory appended log data
     */
    public static Record.Topic getTopicById(UUID uuid) {
        return TOPIC_PER_ID_MAP.get(uuid);
    }

    public static Record.Topic getTopicByName(String name) {
        return TOPIC_PER_NAME_MAP.get(name);
    }

    public static List<Record.Partition> getPartitionListByTopicId(UUID uuid) {
        return LIST_PARTITION_PER_TOPIC_ID_MAP.get(uuid);
    }


    /**
     * Step 3: Append data to local log files
     */
    public static NewOffsetResponse appendBatchData(String topicName, int partitionIndex, ByteBuffer recordData) {
        File logFile = getLogFile(topicName, partitionIndex);
        if (logFile == null) {
            return null;
        }
        logFile.getParentFile().mkdirs();

        List<Batch> batchList = readBatchData(topicName, partitionIndex);
        OptionalLong optCurrentEndOffset = batchList.stream().mapToLong(Batch::getEndOffset).max();
        Long nextOffset = optCurrentEndOffset.isPresent() ? optCurrentEndOffset.getAsLong() + Constant.NEXXT_OFFSET_INCREMENT : 0;

        try {
            // read
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(recordData.array(), recordData.arrayOffset(), recordData.remaining());
            KafkaDataInputStream kafkaDataInputStream = new KafkaDataInputStream(byteArrayInputStream);
            kafkaDataInputStream.readSignedLong(); /* skip nextOffset */

            // write
            FileOutputStream fileOutputStream = new FileOutputStream(logFile, Boolean.TRUE);
            KafkaDataOutputStream kafkaDataOutputStream = new KafkaDataOutputStream(fileOutputStream);
            kafkaDataOutputStream.writeLong(nextOffset);
            byteArrayInputStream.transferTo(fileOutputStream); /* transfer byteArrayInputStream to fileOutputStream */
        } catch (IOException e) {
            System.out.printf("failed to read log file data due to %s%n", e.getMessage());
        }

        return new NewOffsetResponse(nextOffset, Constant.DEFAULT_LOG_START_OFFSET);
    }


    /**
     * Step 4: Read data from local appended log files
     */
    public static byte[] readRawBatchData(String topicName, int partitionIndex) {
        File logFile = getLogFile(topicName, partitionIndex);
        if (logFile == null || !logFile.exists()) {
            return null;
        }
        try {
            FileInputStream fileInputStream = new FileInputStream(logFile);
            return fileInputStream.readAllBytes();
        } catch (IOException e) {
            System.out.printf("failed to read log file data due to %s%n", e.getMessage());
            return null;
        }
    }

    public static List<Batch> readBatchData(String topicName, int partitionIndex) {
        File logFile = getLogFile(topicName, partitionIndex);
        if (logFile == null || !logFile.exists()) {
            return null;
        }
        List<Batch> batchList = new ArrayList<>();
        try {
            FileInputStream fileInputStream = new FileInputStream(logFile);
            KafkaDataInputStream kafkaDataInputStream = new KafkaDataInputStream(fileInputStream);
            while (fileInputStream.available() != Constant.EOF_INDICATOR) {
                Batch batch = Batch.deserialize(kafkaDataInputStream);
                batchList.add(batch);
            }
            return batchList;
        } catch (IOException e) {
            System.out.printf("failed to read log file data due to %s%n", e.getMessage());
            return batchList;
        }
    }
}

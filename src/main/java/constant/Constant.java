package constant;

public class Constant {
    public static final Integer DEFAULT_PORT = 9092;
    public static final String LOG_DIRS = "log.dirs";
    public static final String DEFAULT_CLUSTER_METADATA_TOPIC_NAME = "__cluster_metadata";
    public static final Integer INITIAL_PARTITION_INDEX = 0;
    public static final Integer EOF_INDICATOR = 0;
    public static final String FORMATTED_LOG_ROOT_FILE_PATH = "%s-%s/00000000000000000000.log";
    public static final Long NEXXT_OFFSET_INCREMENT = 1L;
    public static final Long DEFAULT_LOG_START_OFFSET = 0L;
    public static final Long DEFAULT_LOG_START_OFFSET_V2 = -1L;
    public static final Integer DEFAULT_PARTITION_INDEX = 0;
    public static final Integer DEFAULT_HIGH_WATER_MARK = 0;
    public static final Integer DEFAULT_LAST_STABLE_OFFSET = 0;
    public static final Integer DEFAULT_PREFERRED_READ_REPLICA = 0;
    public static final Integer DEFAULT_BASE_OFFSET = -1;
    public static final Integer DEFAULT_LOG_APPEND_TIME_MS = -1;
    public static final Integer DEFAULT_NEW_OFFSET = -1;
}

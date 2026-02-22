package domain.message.response;

import domain.ExchangeMapper;
import domain.message.Header;
import domain.message.RequestBody;
import domain.message.ResponseBody;
import enums.ErrorCode;
import io.DataOutput;

import java.time.Duration;
import java.util.List;

public record ApiVersionsResponseV4(
        List<Key> keys,
        Duration throttleTime
) implements ResponseBody {

    public static Header handleHeader(Header header) {
        Header.V2 headerV2 = (Header.V2) header;
        return new Header.V0(headerV2.correlationId());
    }

    @SuppressWarnings("unused")
    public static ApiVersionsResponseV4 handle(RequestBody requestBody) {
        List<Key> keyList = ExchangeMapper.getEXCHANGE_FUNCTION_MAP()
                .keySet()
                .stream()
                .map(kv -> new Key(kv.key(), kv.version(), kv.version()))
                .toList();
        return new ApiVersionsResponseV4(keyList, Duration.ZERO);
    }

    @Override
    public void serialize(DataOutput dataOutput) {
        dataOutput.writeShort(ErrorCode.NONE.getValue());
        dataOutput.writeCompactArray(keys, Key::serialize);
        dataOutput.writeInt((int) throttleTime.toMillis());

        dataOutput.skipEmptyTaggedFieldArray();
    }

    public record Key(
            short apiKey,
            short minVersion,
            short maxVersion
    ) implements ResponseBody {

        @Override
        public void serialize(DataOutput dataOutput) {
            dataOutput.writeShort(apiKey);
            dataOutput.writeShort(minVersion);
            dataOutput.writeShort(maxVersion);
            dataOutput.skipEmptyTaggedFieldArray();
        }
    }
}

package domain.message;

import io.DataOutput;

public record Response(
        Header header,
        ResponseBody responseBody
) {

    public void serialize(DataOutput dataOutput) {
        header.serialize(dataOutput);
        responseBody.serialize(dataOutput);
    }
}

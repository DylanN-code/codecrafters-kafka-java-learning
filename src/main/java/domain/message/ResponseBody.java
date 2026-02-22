package domain.message;

import io.DataOutput;

public interface ResponseBody {

    void serialize(DataOutput dataOutput);
}

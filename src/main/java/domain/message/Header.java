package domain.message;

import io.DataInput;
import io.DataOutput;

public sealed interface Header {

    default void serialize(DataOutput dataOutput) {

    }

    record V0(int correlationId) implements Header {

        @Override
        public void serialize(DataOutput dataOutput) {
            dataOutput.writeInt(correlationId);
        }
    }

    record V1(int correlationId) implements Header {

        @Override
        public void serialize(DataOutput dataOutput) {
            dataOutput.writeInt(correlationId);
            dataOutput.skipEmptyTaggedFieldArray();
        }
    }

    record V2(KeyVersion keyVersion, int correlationId, String clientId) implements Header {

        public static V2 deserialize(DataInput dataInput) {
            KeyVersion deserializedKeyVersion = KeyVersion.of(
                    dataInput.readSignedShort(),
                    dataInput.readSignedShort()
            );
            int deserializedCorrelationId = dataInput.readSignedInt();
            String deserializedClientId = dataInput.readString();
            dataInput.skipEmptyTaggedFieldArray();
            return new V2(deserializedKeyVersion, deserializedCorrelationId, deserializedClientId);
        }
    }
}

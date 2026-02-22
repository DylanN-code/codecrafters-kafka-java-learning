package domain.message.request;

import domain.message.RequestBody;
import io.DataInput;

public record ApiVersionsRequestV4(
        ClientSoftware clientSoftware
) implements RequestBody {

    public static RequestBody deserialize(DataInput dataInput) {
        final String clientSoftwareName = dataInput.readCompactString();
        final String clientSoftwareVersion = dataInput.readCompactString();

        dataInput.skipEmptyTaggedFieldArray();

        return new ApiVersionsRequestV4(new ClientSoftware(clientSoftwareName, clientSoftwareVersion));
    }

    public record ClientSoftware(
            String name,
            String version
    ) {
    }
}

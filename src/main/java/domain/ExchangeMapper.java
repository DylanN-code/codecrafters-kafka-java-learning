package domain;

import domain.message.*;
import domain.message.request.ApiVersionsRequestV4;
import domain.message.request.DescribeTopicPartitionsRequestV0;
import domain.message.request.FetchRequestV16;
import domain.message.request.ProduceRequestV11;
import domain.message.response.ApiVersionsResponseV4;
import domain.message.response.DescribeTopicPartitionsResponseV0;
import domain.message.response.FetchResponseV16;
import domain.message.response.ProduceResponseV11;
import enums.ErrorCode;
import exception.ProtocolException;
import io.BufferDataInputStream;
import io.DataInput;
import io.DataOutput;
import io.KafkaDataOutputStream;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class ExchangeMapper {

    private static final Map<KeyVersion, ExchangeFunction> EXCHANGE_FUNCTION_MAP = new HashMap<>();

    static {
        // initialize function map
        EXCHANGE_FUNCTION_MAP.put(
                KeyVersion.API_VERSIONS,
                new ExchangeFunction.Builder()
                        .ofRequestDeserializer(ApiVersionsRequestV4::deserialize)
                        .ofHeaderHandler(ApiVersionsResponseV4::handleHeader)
                        .ofRequestResponseHandler(ApiVersionsResponseV4::handle)
                        .build());
        EXCHANGE_FUNCTION_MAP.put(
                KeyVersion.DESCRIBE_TOPIC_PARTITIONS,
                new ExchangeFunction.Builder()
                        .ofRequestDeserializer(DescribeTopicPartitionsRequestV0::deserialize)
                        .ofHeaderHandler(DescribeTopicPartitionsResponseV0::handleHeader)
                        .ofRequestResponseHandler(DescribeTopicPartitionsResponseV0::handle)
                        .build());
        EXCHANGE_FUNCTION_MAP.put(
                KeyVersion.FETCH,
                new ExchangeFunction.Builder()
                        .ofRequestDeserializer(FetchRequestV16::deserialize)
                        .ofHeaderHandler(FetchResponseV16::handleHeader)
                        .ofRequestResponseHandler(FetchResponseV16::handle)
                        .build());
        EXCHANGE_FUNCTION_MAP.put(
                KeyVersion.PRODUCE,
                new ExchangeFunction.Builder()
                        .ofRequestDeserializer(ProduceRequestV11::deserialize)
                        .ofHeaderHandler(ProduceResponseV11::handleHeader)
                        .ofRequestResponseHandler(ProduceResponseV11::handle)
                        .build());
    }

    public static Request extractRequest(DataInput dataInput) {
        // step 1: get messageSize and byteBuffer
        int messageSize = dataInput.readSignedInt();
        ByteBuffer byteBuffer = dataInput.readNBytes(messageSize);
        BufferDataInputStream bufferDataInputStream = new BufferDataInputStream(byteBuffer);

        // step 2: extract header V2
        Header.V2 header = Header.V2.deserialize(bufferDataInputStream);

        // step 3: get exchangeFunction by KeyVersion
        ExchangeFunction exchangeFunction = EXCHANGE_FUNCTION_MAP.get(header.keyVersion());
        if (exchangeFunction == null || exchangeFunction.getRequestDeserializer() == null) {
            throw new ProtocolException(ErrorCode.UNSUPPORTED_VERSION, header.correlationId());
        }

        // step 4: extract request body
        RequestBody requestBody = exchangeFunction.getRequestDeserializer().apply(bufferDataInputStream);
        return new Request(header, requestBody);
    }

    public static Response handle(Request request) {
        // step 1: get exchangeFunction by KeyVersion
        Header.V2 header = (Header.V2) request.header();
        ExchangeFunction exchangeFunction = EXCHANGE_FUNCTION_MAP.get(header.keyVersion());
        if (exchangeFunction == null
                || exchangeFunction.getHeaderHandler() == null
                || exchangeFunction.getRequestResponsehandler() == null) {
            throw new ProtocolException(ErrorCode.UNSUPPORTED_VERSION, header.correlationId());
        }

        // step 2: handle response header
        Header responseHeader = exchangeFunction.getHeaderHandler().apply(header);

        // step 3: handle response body
        ResponseBody responseBody = exchangeFunction.getRequestResponsehandler().apply(request.requestBody());
        return new Response(responseHeader, responseBody);
    }

    public static void writeResponse(DataOutput dataOutput, Request request, Response response) {
        if (response == null) {
            throw new ProtocolException(ErrorCode.UNKNOWN_SERVER_ERROR, ((Header.V2) request.header()).correlationId());
        }

        // step 1: create a byte buffer
        ByteArrayOutputStream byteBuffer = new ByteArrayOutputStream();
        KafkaDataOutputStream kafkaByteBuffer = new KafkaDataOutputStream(byteBuffer);

        // step 2: serialize response
        response.serialize(kafkaByteBuffer);

        // step 3: write length of output bytes then output bytes
        byte[] bytesToWrite = byteBuffer.toByteArray();
        dataOutput.writeInt(bytesToWrite.length);
        dataOutput.writeBytes(bytesToWrite);
    }

    public static void writeErrorResponse(DataOutput dataOutput, ProtocolException protocolException) {
        dataOutput.writeInt(Integer.BYTES + Short.BYTES);
        dataOutput.writeInt(protocolException.getCorrelationId());
        dataOutput.writeShort(protocolException.getErrorCode().getValue());
    }

    public static Map<KeyVersion, ExchangeFunction> getEXCHANGE_FUNCTION_MAP() {
        return EXCHANGE_FUNCTION_MAP;
    }
}

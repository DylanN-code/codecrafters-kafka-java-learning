package domain;

import domain.message.Header;
import domain.message.RequestBody;
import domain.message.ResponseBody;
import io.DataInput;

import java.util.function.Function;

/**
 * The following exchange functions facilitate the static methods that handle conversion between request and response
 */
public class ExchangeFunction {
    private Function<DataInput, RequestBody> requestDeserializer;
    private Function<Header, Header> headerHandler;
    private Function<RequestBody, ResponseBody> requestResponsehandler;

    public static class Builder {
        private final ExchangeFunction exchangeFunction;

        public Builder() {
            this.exchangeFunction = new ExchangeFunction();
        }

        public Builder ofRequestDeserializer(Function<DataInput, RequestBody> requestDeserializer) {
            exchangeFunction.requestDeserializer = requestDeserializer;
            return this;
        }

        public Builder ofHeaderHandler(Function<Header, Header> headerFunction) {
            exchangeFunction.headerHandler = headerFunction;
            return this;
        }

        public Builder ofRequestResponseHandler(Function<RequestBody, ResponseBody> requestResponsehandler) {
            exchangeFunction.requestResponsehandler = requestResponsehandler;
            return this;
        }

        public ExchangeFunction build() {
            return exchangeFunction;
        }
    }

    public Function<DataInput, RequestBody> getRequestDeserializer() {
        return requestDeserializer;
    }

    public Function<Header, Header> getHeaderHandler() {
        return headerHandler;
    }

    public Function<RequestBody, ResponseBody> getRequestResponsehandler() {
        return requestResponsehandler;
    }
}

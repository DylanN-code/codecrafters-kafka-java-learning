import domain.ExchangeMapper;
import domain.message.Request;
import domain.message.Response;
import exception.ProtocolException;
import io.KafkaDataInputStream;
import io.KafkaDataOutputStream;

import java.io.IOException;
import java.net.Socket;

public class Client implements Runnable {

    private final Socket socket;
    private final KafkaDataInputStream kafkaDataInputStream;
    private final KafkaDataOutputStream kafkaDataOutputStream;

    public Client(Socket socket) throws IOException {
        this.socket = socket;
        this.kafkaDataInputStream = new KafkaDataInputStream(socket.getInputStream());
        this.kafkaDataOutputStream = new KafkaDataOutputStream(socket.getOutputStream());
    }

    @Override
    public void run() {
        try {
            while (!socket.isClosed()) {
                exchange();
            }
        } catch (Exception e) {
            System.err.printf("failed to exchange for socket=%s due to error=%s", socket.getLocalAddress(), e.getMessage());
            e.printStackTrace();
        }
    }

    private void exchange() {
        try {
            // step 1: convert input byte stream into request object
            Request request = ExchangeMapper.extractRequest(kafkaDataInputStream);

            // step 2: handle request object to response object
            Response response = ExchangeMapper.handle(request);

            // step 3: convert response object to output byte stream
            ExchangeMapper.writeResponse(kafkaDataOutputStream, request, response);
        } catch (ProtocolException e) {
            ExchangeMapper.writeErrorResponse(kafkaDataOutputStream, e);
        }
    }

    public Socket getSocket() {
        return socket;
    }

    public KafkaDataInputStream getKafkaDataInputStream() {
        return kafkaDataInputStream;
    }

    public KafkaDataOutputStream getKafkaDataOutputStream() {
        return kafkaDataOutputStream;
    }
}

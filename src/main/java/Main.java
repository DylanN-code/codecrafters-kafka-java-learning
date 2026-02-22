import constant.Constant;
import kafka.Kafka;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.Properties;

public class Main {

    public static void main(String[] args) {
        // step 1: load properties given input file path
        final String serverPropertiesPath = args[0];
        final Properties properties = new Properties();
        try (final FileInputStream fileInputStream = new FileInputStream(serverPropertiesPath)) {
            properties.load(fileInputStream);
        } catch (IOException e) {
            System.err.printf("failed to set server properties path %s due to %s%n", serverPropertiesPath, e.getMessage());
        }
        for (final Map.Entry<Object, Object> entry : properties.entrySet()) {
            System.out.printf("properties: %s maps to %s%n", entry.getKey(), entry.getValue());
        }

        // step 2: load local appended log metadata to in-memory data structure
        Kafka.load(new File(properties.getProperty(Constant.LOG_DIRS)));

        // step 3: init websocket connection and forward to virtual thread
        try (final ServerSocket serverSocket = new ServerSocket(Constant.DEFAULT_PORT)) {
            serverSocket.setReuseAddress(Boolean.TRUE);
            while (true) {
                try {
                    final Socket clientSocket = serverSocket.accept();
                    System.out.printf("init connect from client %s%n", clientSocket.getRemoteSocketAddress());
                    Thread.ofVirtual().start(new Client(clientSocket));
                } catch (IOException e) {
                    System.err.printf("failed to start client socket connection due to %s%n", e.getMessage());
                    System.exit(-1);
                }
            }
        } catch (IOException e) {
            System.err.printf("failed to init server socket connection due to %s%n", e.getMessage());
        }
    }
}

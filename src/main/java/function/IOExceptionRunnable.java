package function;

import java.io.IOException;

@FunctionalInterface
public interface IOExceptionRunnable {
    void start() throws IOException;
}

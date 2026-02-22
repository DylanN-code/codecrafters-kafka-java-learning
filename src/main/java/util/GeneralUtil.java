package util;

import function.IOExceptionRunnable;
import function.IOExceptionSupplier;

import java.io.IOException;

public class GeneralUtil {

    public static <T> T tryCatch(IOExceptionSupplier<T> supplier) {
        try {
            return supplier.get();
        } catch (IOException e) {
            System.out.printf("failed to try-catch due to %s%n", e.getMessage());
            return null;
        }
    }

    public static void tryCatch(IOExceptionRunnable runnable) {
        try {
            runnable.start();
        } catch (IOException e) {
            System.out.printf("failed to try-catch due to %s%n", e.getMessage());
        }
    }
}

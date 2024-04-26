package com.samhad.spark.module4_sparkStreaming;

import com.samhad.spark.module4_sparkStreaming.lesson__30.IntroductionDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Creates a Demo Streaming Server emitting random logs to the localhost socket 8989.
 */
public class LoggingServer {

    private static final Logger LOGGER = LoggerFactory.getLogger(IntroductionDStream.class);

    public static void main(String[] args) throws IOException {
        ServerSocket serverSocket = new ServerSocket(8989);
        Socket socket = serverSocket.accept();
        final List<String> logLevels = List.of("INFO", "WARN", "DEBUG", "ERROR", "TRACE");
        final int size = logLevels.size();
        infiniteLoop:
        while (true) {
            childLoop:
            for (int i = 0; i < 1000; i++) {
                PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
                int index = ThreadLocalRandom.current().nextInt(0, size);
                String formatted = logLevels.get(index) + "," + LocalDateTime.now();
                writer.println(formatted);
                try {
                    TimeUnit.MILLISECONDS.sleep(10);
                } catch (InterruptedException e) {
                    LOGGER.error("----- childLoop :: Interrupted");
                    break childLoop;
                }
            }

            try {
                TimeUnit.MILLISECONDS.sleep(1000);
            } catch (InterruptedException e) {
                LOGGER.error("----- infiniteLoop :: Interrupted");
                break infiniteLoop;
            }
        }
    }
}

package com.samhad.spark.misc.java_practice;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

/**
 * A demo program to practice the initialisation of dependent services using CountDownLatch.
 */
public class PracticeLatch {

    private static final Logger LOGGER = LoggerFactory.getLogger(PracticeLatch.class);

    public static void main(String[] args) {
        int maxParallel = 5;
        int totalJobs = 20;
        ExecutorService service = Executors.newFixedThreadPool(maxParallel);
        CountDownLatch latch = new CountDownLatch(totalJobs);

        IntStream.range(0, totalJobs)
                .forEach(i -> service.submit(new DependentLatch(latch, i)));

        try {
            LOGGER.info("Waiting for all the jobs to  complete.");
            latch.await();

            // Now perform those operations that requires the above jobs to be completed.
            LOGGER.info("All jobs execution complete.");

        } catch (InterruptedException e) {
            LOGGER.error("Waiting latch interrupted.");
            throw new RuntimeException(e);
        }
    }

    /**
     * This can a simple or a heavy task that needs to be executed asynchronously or in parallel, so that we can delegate
     * their execution to a different thread.
     */
    private static class DependentLatch implements Runnable {

        private static final Logger LOGGER = LoggerFactory.getLogger(DependentLatch.class);

        private final CountDownLatch countDownLatch;
        private final int id;

        public DependentLatch(CountDownLatch countDownLatch, int id) {
            LOGGER.info("Initialising Dependent Latch. Id: {}.", id);
            this.id = id;
            this.countDownLatch = countDownLatch;
        }

        @Override
        public void run() {
            // perform the initialization tasks.
            try {
                Thread.sleep(3000);
                countDownLatch.countDown();
                LOGGER.info("CountDown Dependent Latch. Id: {}.", id);
            } catch (InterruptedException e) {
                LOGGER.error("Thread interrupted. Id: %s%n".formatted(id));
                throw new RuntimeException(e);
            }

            // perform some optional cleanup tasks.
        }
    }

}

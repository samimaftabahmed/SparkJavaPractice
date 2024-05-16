package com.samhad.spark.misc.java_practice;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

/**
 * A demo program that performs multiple tasks at exactly the same time using a Barrier.
 */
public class PracticeCyclicBarrier {

    private static final Logger LOGGER = LoggerFactory.getLogger(PracticeCyclicBarrier.class);

    public static void main(String[] args) {
        int maxParallel = 5;
        ExecutorService service = Executors.newFixedThreadPool(maxParallel);
        CyclicBarrier barrier = new CyclicBarrier(maxParallel);
        IntStream.range(0, maxParallel)
                .forEach(i -> {
                    int randomSeconds = ThreadLocalRandom.current().nextInt(1, 10);
                    service.submit(new Task(barrier, i, randomSeconds));
                });
        LOGGER.info("All Tasks submitted.");
    }

    /**
     * A Task to be executed at exactly the same time.
     */
    private static class Task implements Runnable {

        private static final Logger LOGGER = LoggerFactory.getLogger(Task.class);

        private final CyclicBarrier barrier;
        private final int id;
        private final int sleepTime;

        public Task(CyclicBarrier barrier, int id, int sleepTime) {
            LOGGER.info("Task initialised. Id: {}", id);
            this.barrier = barrier;
            this.id = id;
            this.sleepTime = sleepTime;
        }

        @Override
        public void run() {
            try {
                LOGGER.info("Sleeping task {} for {} seconds.", id, sleepTime);
                TimeUnit.SECONDS.sleep(sleepTime);
                // perform some unique initialisation tasks or checks or validations
                LOGGER.info("Issuing await for task: {}", id);
                barrier.await();

                // perform the tasks that needs to be executed at exactly the same time, like broadcasting messages etc.
                LOGGER.info("Executing after await: {}", id);
            } catch (InterruptedException | BrokenBarrierException e) {
                throw new RuntimeException(e);
            }
        }
    }

}

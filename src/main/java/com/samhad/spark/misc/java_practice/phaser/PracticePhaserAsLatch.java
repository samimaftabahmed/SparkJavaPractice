package com.samhad.spark.misc.java_practice.phaser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;
import java.util.stream.IntStream;

/**
 * A demo program to use a Phaser as a Latch.
 */
public class PracticePhaserAsLatch {

    private static final Logger LOGGER = LoggerFactory.getLogger(PracticePhaserAsLatch.class);

    public static void main(String[] args) {
        ExecutorService service = Executors.newFixedThreadPool(3);
        int maxTasks = 10;
        Phaser phaser = new Phaser(maxTasks);

        IntStream.range(0, maxTasks).forEach(i -> service.submit(new DependentService(phaser, i)));
        LOGGER.info("All tasks submitted.");

        phaser.awaitAdvance(0);
        LOGGER.info("Performing rest of the operations.");
    }

    private static class DependentService implements Runnable {

        private static final Logger LOGGER = LoggerFactory.getLogger(DependentService.class);

        private final Phaser phaser;
        private final int id;

        public DependentService(Phaser phaser, int id) {
            this.phaser = phaser;
            this.id = id;
        }

        @Override
        public void run() {
            try {
                LOGGER.info("Starting task: {}", id);
                Thread.sleep(3000);

                phaser.arrive();
                LOGGER.info("Arrived task: {}", id);

            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

}

package com.samhad.spark.misc.java_practice.phaser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

/**
 * A demo program to use a Phaser as a CyclicBarrier.
 */
public class PracticePhaserAsCyclicBarrier {

    private static final Logger LOGGER = LoggerFactory.getLogger(PracticePhaserAsCyclicBarrier.class);

    public static void main(String[] args) {
        int maxTasks = 10;
        ExecutorService service = Executors.newFixedThreadPool(maxTasks);
        Phaser phaser = new Phaser(maxTasks);

        IntStream.range(0, maxTasks).forEach(i -> {
            int randomInt = ThreadLocalRandom.current().nextInt(2, 9);
            service.submit(new Task(i, phaser, randomInt));
        });
        LOGGER.info("All tasks submitted.");
    }

    private static class Task implements Runnable {

        private static final Logger LOGGER = LoggerFactory.getLogger(Task.class);

        private final int id;
        private final Phaser phaser;
        private final int randomSleepSec;

        public Task(int id, Phaser phaser, int randomSleepSec) {
            this.id = id;
            this.phaser = phaser;
            this.randomSleepSec = randomSleepSec;
        }

        @Override
        public void run() {
            try {
                LOGGER.info("Executing before sleep(time={}) for task: {}", randomSleepSec, id);
                TimeUnit.SECONDS.sleep(randomSleepSec);

                LOGGER.info("Awaiting advancement for task: {}", id);
                phaser.arriveAndAwaitAdvance();

                LOGGER.info("Advancing operations for task: {}", id);

            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }


}

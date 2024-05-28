package com.samhad.spark.misc.java_practice.phaser;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

public class PhaserUsage implements Callable<String> {

    private static final int THREAD_POOL_SIZE = 10;
    private final Phaser phaser;

    private PhaserUsage(Phaser phaser) {
        this.phaser = phaser;
    }

    public static void main(String a[]) {
        ExecutorService execService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
        CompletionService<String> completionService = new ExecutorCompletionService<>(execService);

        // since we know beforehand how many tasks we have, initialize the
        // number of participants in the constructor; other wise register
        // *before* launching the task
        Phaser phaser = new Phaser(THREAD_POOL_SIZE);

        IntStream.range(0, THREAD_POOL_SIZE)
                .forEach(nbr -> completionService.submit(new PhaserUsage(phaser)));

        execService.shutdown();
        int count = 0;
        try {
            while (count != THREAD_POOL_SIZE) {
                count++;
                String result = completionService.take().get();
                System.out.printf("Result is: %s%n", result);
            }
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public String call() {
        String threadName = Thread.currentThread().getName();
        System.out.printf("Arrive and await advance...%s%n", threadName);
        phaser.arriveAndAwaitAdvance(); // await all creation
        int a = ThreadLocalRandom.current().nextInt(100000);
        int b = ThreadLocalRandom.current().nextInt(100000);
//        Random random = new Random();
//        int a = 0, b = 1;
//        for (int i = 0; i < random.nextInt(10000000); i++) {
//            a = a + b;
//            b = a - b;
//        }
        System.out.printf("De-registering...%s%n", threadName);
        phaser.arriveAndDeregister();
        return String.format("Thread %s results: a = %s, b = %s", threadName, a, b);
    }
}

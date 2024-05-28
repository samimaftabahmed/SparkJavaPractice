package com.samhad.spark.misc.java_practice.phaser;

import java.text.MessageFormat;
import java.util.concurrent.Phaser;

public class PhaserExample {

    public static void main(String[] args) {
        Phaser phaser = new Phaser();
        phaser.register();
        int currentPhase;

        System.out.println("Main: Starting");

        new MyThread(phaser, "A");
        new MyThread(phaser, "B");
        new MyThread(phaser, "C");

        // Wait for all threads to complete phase Zero.
        currentPhase = phaser.getPhase();
        phaser.arriveAndAwaitAdvance();
        System.out.println(MessageFormat.format("Main: Phase {0} Complete", currentPhase));
        System.out.println("Main: Phase Zero Ended\n");

        // Wait for all threads to complete phase One.
        currentPhase = phaser.getPhase();
        phaser.arriveAndAwaitAdvance();
        System.out.println(MessageFormat.format("Main: Phase {0} Complete", currentPhase));
        System.out.println("Main: Phase One Ended\n");

        currentPhase = phaser.getPhase();
        phaser.arriveAndAwaitAdvance();
        System.out.println(MessageFormat.format("Main: Phase {0} Complete", currentPhase));
        System.out.println("Main: Phase Two Ended\n");

        // Deregister the main thread.
        phaser.arriveAndDeregister();
        if (phaser.isTerminated()) {
            System.out.println("Main: Phaser is terminated\n");
        }
    }

    // A thread of execution that uses a phaser.
    private static class MyThread implements Runnable {
        Phaser phaser;
        String title;

        public MyThread(Phaser phaser, String title) {
            this.phaser = phaser;
            this.title = title;

            phaser.register();
            new Thread(this).start();
        }

        @Override
        public void run() {
            System.out.println("Thread: " + title + " Phase Zero Started");
            phaser.arriveAndAwaitAdvance();

            int sleepTime = 1000;
            // Stop execution to prevent jumbled output
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                System.out.println(e);
            }

            System.out.println("Thread: " + title + " Phase One Started");
            phaser.arriveAndAwaitAdvance();

            // Stop execution to prevent jumbled output
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                System.out.println(e);
            }

            System.out.println("Thread: " + title + " Phase Two Started");
            phaser.arriveAndDeregister();
        }
    }
}


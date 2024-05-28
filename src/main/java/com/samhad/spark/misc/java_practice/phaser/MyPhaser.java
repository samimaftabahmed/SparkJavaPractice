package com.samhad.spark.misc.java_practice.phaser;

import java.util.concurrent.Phaser;

public class MyPhaser {

    public static void main(String[] args) {
        Phaser myPhaser = new Phaser();
        myPhaser.register();

        System.out.println("let's start phaser example");

        new MyThread(myPhaser, "cat");
        new MyThread(myPhaser, "dog");
        new MyThread(myPhaser, "elephant");

        myPhaser.arriveAndAwaitAdvance();
        System.out.println("Ending phase one\n");

        myPhaser.arriveAndAwaitAdvance();
        System.out.println("Ending phase two\n");

        myPhaser.arriveAndAwaitAdvance();
        System.out.println("Ending phase three");
    }

    private static class MyThread implements Runnable {

        Phaser myPhaser;
        String threadName;

        MyThread(Phaser myPhaser, String threadName) {
            this.myPhaser = myPhaser;
            this.threadName = threadName;
            myPhaser.register();
            new Thread(this).start();
        }

        @Override
        public void run() {
            // phase 1 of our code.
            System.out.println("This is Phase one for : " + this.threadName);
            // creating a phaser barrier for all threads to sync
            myPhaser.arriveAndAwaitAdvance();

            int sleepTime = 1000;
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            // start new phase of execution, phase 2 of code
            System.out.println("This is Phase two for : " + this.threadName);
            // creating a barrier for all threads to sync
            myPhaser.arriveAndAwaitAdvance();

            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            // start new phase of execution, phase 3 of code
            System.out.println("This is Phase three for : " + this.threadName);
            myPhaser.arriveAndDeregister();
        }
    }
}

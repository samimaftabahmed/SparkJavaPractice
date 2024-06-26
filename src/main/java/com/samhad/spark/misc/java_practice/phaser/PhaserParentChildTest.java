package com.samhad.spark.misc.java_practice.phaser;

import java.util.concurrent.Phaser;

public class PhaserParentChildTest {

    public static void main(String[] args) {
        /*
         * Creates a new phaser with no registered unArrived parties.
         */
        Phaser parentPhaser = new Phaser();

        /*
         * Creates a new phaser with the given parent &
         * no registered unArrived parties.
         */
        Phaser childPhaser = new Phaser(parentPhaser, 0);

        childPhaser.register();

        System.out.println("parentPhaser isTerminated : " + parentPhaser.isTerminated());
        System.out.println("childPhaser isTerminated : " + childPhaser.isTerminated());

        childPhaser.arriveAndDeregister();
        System.out.println("\n--childPhaser has called arriveAndDeregister()-- \n");

        System.out.println("parentPhaser isTerminated : " + parentPhaser.isTerminated());
        System.out.println("childPhaser isTerminated : " + childPhaser.isTerminated());

    }
}


package com.samhad.spark;

import org.apache.spark.api.java.JavaSparkContext;

/**
 * Just a Marker interface
 */
public interface SparkTask {

    void execute(JavaSparkContext sc);
}

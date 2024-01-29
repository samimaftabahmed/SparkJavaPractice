package com.samhad.spark;

import org.apache.spark.api.java.JavaSparkContext;

/**
 * Any class implementing this interface will be executed.
 */
public interface SparkTask {

    void execute(JavaSparkContext sc);
}

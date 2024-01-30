package com.samhad.spark.common;

import org.apache.spark.api.java.JavaSparkContext;

/**
 * Any class implementing this interface will be executed.
 */
public interface SparkTask {

    /**
     * Executes the implementation in Spark
     *
     * @param sc - The spark context
     */
    void execute(JavaSparkContext sc);
}

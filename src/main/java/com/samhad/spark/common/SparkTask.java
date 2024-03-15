package com.samhad.spark.common;

import org.apache.spark.sql.SparkSession;

/**
 * Any class implementing this interface will be executed.
 */
public interface SparkTask {

    /**
     * Executes the implementation in Spark
     *
     * @param spark - The spark session
     */
    void execute(SparkSession spark);
}

package com.samhad.spark.common;

import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;

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

    /**
     * Logs a line for depicting the output separation between different files.
     *
     * @param logger The Logger instance to use for logging.
     * @param tClass The class from where this method is called.
     */
    default void logFileStart(Logger logger, Class tClass) {
        logger.info("\n-------------- {} --------------\n", tClass.getName());
    }
}

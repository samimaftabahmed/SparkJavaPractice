package com.samhad.spark.module1_basics;

import com.samhad.spark.common.Utility;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executes SparkTasks that are in the "com.samhad.spark.module1_basics" package
 */
public class Module1Main {

    private static final Logger LOGGER = LoggerFactory.getLogger(Module1Main.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("LearningSpark").setMaster("local[*]");
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            Utility.callWithClassGraph(sc, Module1Main.class.getPackageName());
//            Utility.pauseSparkApp();
        } catch (Exception e) {
            LOGGER.error("Module1 Main:: Exception caught during execution: ", e);
        }
    }
}

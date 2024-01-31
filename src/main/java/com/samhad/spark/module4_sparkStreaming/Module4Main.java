package com.samhad.spark.module4_sparkStreaming;

import com.samhad.spark.common.Utility;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Module4Main {

    private static final Logger LOGGER = LoggerFactory.getLogger(Module4Main.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("LearningSpark").setMaster("local[*]");
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            Utility.callWithClassGraph(sc, Module4Main.class.getPackageName());
        } catch (Exception e) {
            LOGGER.error("Module4 Main:: Exception caught during execution: ", e);
        }
    }
}

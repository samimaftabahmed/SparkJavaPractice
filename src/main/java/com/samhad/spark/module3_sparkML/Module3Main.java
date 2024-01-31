package com.samhad.spark.module3_sparkML;

import com.samhad.spark.common.Utility;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Module3Main {

    private static final Logger LOGGER = LoggerFactory.getLogger(Module3Main.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("LearningSpark").setMaster("local[*]");
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            Utility.callWithClassGraph(sc, Module3Main.class.getPackageName());
        } catch (Exception e) {
            LOGGER.error("Module3 Main:: Exception caught during execution: ", e);
        }
    }
}

package com.samhad.spark;

import com.samhad.spark.common.Utility;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("LearningSpark").setMaster("local[*]");
        JavaSparkContext sc = null;
        try {
            sc = new JavaSparkContext(conf);
            Utility.callWithClassGraph(sc, Main.class.getPackageName());
        } catch (Exception e) {
            LOGGER.error("Exception caught during execution: ", e);
        } finally {
            LOGGER.info("Execution completed. Closing Spark Context.");
            if (sc != null)
                sc.close();
        }
    }
}

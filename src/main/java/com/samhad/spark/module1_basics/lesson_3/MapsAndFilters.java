package com.samhad.spark.module1_basics.lesson_3;

import com.samhad.spark.common.SparkTask;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

/**
 * Concepts on Map, FlapMap and Filter
 * Section 7
 */
public class MapsAndFilters implements SparkTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(MapsAndFilters.class);

    @Override
    public void execute(SparkSession spark) {
        logFileStart(LOGGER, this.getClass());
        JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        List<String> inputData = Arrays.asList(
                "WARN: Tuesday 4 September 0405",
                "ERROR: Tuesday 4 September 0408",
                "FATAL: Tuesday 5 September 1632",
                "ERROR: Tuesday 7 September 1854",
                "WARN: Tuesday 4 September 0405");

        JavaRDD<String> logRdd = sc.parallelize(inputData)
                .flatMap(words -> Arrays.asList(words.split(" ")).iterator());

        LOGGER.info("\n\n");
        LOGGER.info("2. *** \t *** \t ***");
        LOGGER.info("FlapMap");
        // basically converting sentences to words
        logRdd.foreach(word -> LOGGER.info("flatMap word: {}", word));

        LOGGER.info("\n\n");
        LOGGER.info("3. *** \t *** \t ***");
        LOGGER.info("Filter");
        logRdd.filter(word -> !StringUtils.isNumeric(word.trim()))
                .foreach(word -> LOGGER.info("Filter word only: {}", word));

    }
}

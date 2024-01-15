package com.samhad.spark.lesson_3;

import com.samhad.spark.MyRunner;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

/**
 * Concepts on Map, FlapMap and Filter
 */
public class MapsAndFilters implements MyRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(MapsAndFilters.class);

    @Override
    public void execute(JavaSparkContext sc) {
        LOGGER.info("\n---------------------------------------------------------");
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
        logRdd.foreach(word -> LOGGER.info("flatMap word: {}", word));

        LOGGER.info("\n\n");
        LOGGER.info("3. *** \t *** \t ***");
        LOGGER.info("Filter");
        logRdd.filter(word -> !StringUtils.isNumeric(word.trim()))
                .foreach(word -> LOGGER.info("Filter word only: {}", word));

    }
}

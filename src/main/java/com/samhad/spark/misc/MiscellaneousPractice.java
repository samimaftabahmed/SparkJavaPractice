package com.samhad.spark.misc;

import com.samhad.spark.MyRunner;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

/**
 * Miscellaneous concepts learned from other sources. Some include: top(), take(), textFile()
 */
public class MiscellaneousPractice implements MyRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(MiscellaneousPractice.class);

    @Override
    public void execute(JavaSparkContext sc) {

        LOGGER.info("---------------------------------------------------------");
        List<Integer> integerList = Arrays.asList(1, 55, 2, 65, 7, 3);
        LOGGER.info("The Integer List: {}", integerList);

        LOGGER.info("*** \t *** \t ***");
        sc.parallelize(integerList).top(3).forEach(integer -> LOGGER.info("Top: {}", integer));

        LOGGER.info("*** \t *** \t ***");
        sc.parallelize(integerList).take(3).forEach(integer -> LOGGER.info("Take: {}", integer));

        LOGGER.info("*** \t *** \t ***");
        sc.textFile("D:\\words.txt").foreach(s -> LOGGER.info("text: {}", s));

    }
}

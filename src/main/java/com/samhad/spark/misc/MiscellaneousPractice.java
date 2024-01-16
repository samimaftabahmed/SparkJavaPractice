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
        LOGGER.info("\n---------------------------------------------------------");
        List<Integer> integerList = Arrays.asList(1, 55, 2, 65, 7, 3);
        LOGGER.info("The Integer List: {}", integerList);

        List<Integer> topList = sc.parallelize(integerList)
                .top(3) // returns the largest 3 candidates
                .stream().toList();
        LOGGER.info("1. Top List: {}", topList);

        List<Integer> takeList = sc.parallelize(integerList)
                .take(3) // returns 3 candidates based on their insertion order
                .stream().toList();
        LOGGER.info("2. Take List: {}", takeList);

        List<String> wordsFromFile = sc.textFile("D:\\words.txt").collect();
        LOGGER.info("3. WordsFromFile: {}", wordsFromFile);

    }
}

package com.samhad.spark.module1_basics.lesson_4;

import com.samhad.spark.common.SparkTask;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

/**
 * Miscellaneous concepts like top(), take(), textFile()
 * Section 8
 */
public class MiscellaneousPractice implements SparkTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(MiscellaneousPractice.class);

    @Override
    public void execute(SparkSession spark) {
        JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

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

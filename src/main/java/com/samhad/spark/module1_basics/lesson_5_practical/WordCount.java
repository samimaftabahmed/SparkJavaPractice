package com.samhad.spark.module1_basics.lesson_5_practical;

import com.samhad.spark.common.SparkTask;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;

/**
 * Word Count program exercise.
 * Section 22 to 24.
 */
public class WordCount implements SparkTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(WordCount.class);

    @Override
    public void execute(JavaSparkContext sc) {
        LOGGER.info("\n---------------------------------------------------------");
        JavaRDD<String> rdd = sc.textFile("src/main/resources/dataset/sample.srt").cache();
        rdd = rdd
                .map(s -> s.replaceAll("[^a-zA-Z\\s]", "").trim())
                .filter(s -> !s.isEmpty()); // cleaning out the RDD, leaving only words and sentences

        rdd = rdd.flatMap(s -> Arrays.asList(s.split(" ", -1)).iterator())
                .filter(s -> !s.trim().isEmpty()); // converting sentences to words

        JavaPairRDD<String, Integer> pairRdd = rdd
                .mapToPair(s -> new Tuple2<>(s, 1))
                .reduceByKey((integer, integer2) -> integer + integer2); // converting to pairRDD and reducing

        JavaPairRDD<Integer, String> transformedPairRdd = pairRdd
                .mapToPair(tuple2 -> new Tuple2<>(tuple2._2(), tuple2._1())); // transforming the RDD, in order to take advantage of reduceByKey

        int resultSize = 10;

        LOGGER.info("*** Top {} words using 'sortByKey()' ***", resultSize);
        transformedPairRdd
                .sortByKey(false)
                .take(resultSize) // sorting in descending order based on key and taking the top 10 highest occurrences of words
                .forEach(tuple2 -> LOGGER.info("word: count -- {}: {}", tuple2._2(), tuple2._1()));

        LOGGER.info("\n\n");
        LOGGER.info("*** Top {} words using 'top()' ***", resultSize);
        transformedPairRdd
                .top(resultSize, new TupleComparator())
                .forEach(tuple2 -> LOGGER.info("word: count -- {}: {}", tuple2._2(), tuple2._1()));
        /**
         * Note: On a separate Spark program, it was observed that top() was faster than sortByKey() and take().
         */
    }

    private static class TupleComparator implements Serializable, Comparator<Tuple2<Integer, String>> {
        @Override
        public int compare(Tuple2<Integer, String> o1, Tuple2<Integer, String> o2) {
            return Integer.compare(o1._1(), o2._1());
        }
    }
}

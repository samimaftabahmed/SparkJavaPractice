package com.samhad.spark.lesson_5_practical;

import com.samhad.spark.MyRunner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Arrays;

public class WordCount implements MyRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(WordCount.class);

    @Override
    public void execute(JavaSparkContext sc) {
        LOGGER.info("\n---------------------------------------------------------");
        JavaRDD<String> rdd = sc.textFile("src/main/resources/dataset/sample.srt").cache();
        rdd = rdd
                .map(s -> s.replaceAll("[^a-zA-Z\\s]", "").trim())
                .filter(s -> !s.isEmpty());
        rdd = rdd.flatMap(s -> Arrays.asList(s.split(" ", -1)).iterator())
                .filter(s -> !s.trim().isEmpty());

        JavaPairRDD<String, Integer> pairRdd = rdd
                .mapToPair(s -> new Tuple2<>(s, 1))
                .reduceByKey((integer, integer2) -> integer + integer2);

        JavaPairRDD<Integer, String> transformedPairRdd = pairRdd
                .mapToPair(tuple2 -> new Tuple2<>(tuple2._2(), tuple2._1()));

        LOGGER.info("*** Using sortByKey ***");
        transformedPairRdd
                .sortByKey(false)
                .take(10)
                .forEach(tuple2 -> LOGGER.info("word: count -- {}: {}", tuple2._2(), tuple2._1()));

//        LOGGER.info("*** Using top ***");
//        transformedPairRdd
//                .top(10, (o1, o2) -> Integer.compare(o1._1(), o2._1()));


    }
}

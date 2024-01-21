package com.samhad.spark.lesson_2;

import com.google.common.collect.Iterables;
import com.samhad.spark.MyRunner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * Concepts on PairRDD, groupByKey(), reduceByKey()
 * Section 6
 */
public class PairRDDsAndOperations implements MyRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(PairRDDsAndOperations.class);

    @Override
    public void execute(JavaSparkContext sc) {
        LOGGER.info("\n---------------------------------------------------------");

        List<String> inputData = Arrays.asList(
                "WARN: Tuesday 4 September 0405",
                "ERROR: Tuesday 4 September 0408",
                "ERROR: Tuesday 4 September 0408",
                "ERROR: Tuesday 4 September 0408",
                "ERROR: Tuesday 4 September 0408",
                "FATAL: Tuesday 5 September 1632",
                "ERROR: Tuesday 7 September 1854",
                "WARN: Tuesday 4 September 0405");

        JavaPairRDD<String, String> logsPairRDD = sc.parallelize(inputData)
                .mapToPair(data -> {
                    String[] split = data.split(":");
                    return new Tuple2<>(split[0], split[1]);
                });

        LOGGER.info("\n1. *** \t *** \t ***");
        logsPairRDD.foreach(tuple2 -> LOGGER.info("Level: {}, Message: {}", tuple2._1(), tuple2._2().trim()));

        LOGGER.info("\n2. *** \t *** \t ***");
        logsPairRDD.groupByKey()
                .foreach(tuple -> LOGGER.info("Key: {}, Iterable size: {}", tuple._1(), Iterables.size(tuple._2())));

        // reduceByKey() is preferred over groupByKey() due to performance and stability reasons
        LOGGER.info("\n3. *** \t *** \t ***");
        sc.parallelize(inputData)
                .mapToPair(data -> {
                    String[] split = data.split(":");
                    return new Tuple2<>(split[0], 1);
                })
                .reduceByKey((v1, v2) -> v1 + v2)
                .foreach(tuple2 -> LOGGER.info("Key: {}, Iterable size: {}", tuple2._1(), tuple2._2()));
    }
}

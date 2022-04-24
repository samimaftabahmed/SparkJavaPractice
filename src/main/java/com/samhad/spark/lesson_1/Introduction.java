package com.samhad.spark.lesson_1;

import com.samhad.spark.MyRunner;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * All introduction related topics of Spark related to RDD, map(), reduce() and Tuple2
 */
public class Introduction implements MyRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(Introduction.class);

    @Override
    public void execute(JavaSparkContext sc) {

        LOGGER.info("---------------------------------------------------------");
        List<Integer> inputData = Arrays.asList(36, 49, 100, 121);
        JavaRDD<Integer> myRdd = sc.parallelize(inputData);
        myRdd.foreach(integer -> LOGGER.info("myRdd: {}", integer));

        LOGGER.info("*** \t *** \t ***");
        Integer reduce = myRdd.reduce(((integer, integer2) -> integer + integer2));
        LOGGER.info("reduce of myRdd: {}", reduce); // summation

        LOGGER.info("*** \t *** \t ***");
        JavaRDD<Double> sqrtRdd = myRdd.map(integer -> Math.sqrt(integer));
        LOGGER.info("RDD foreach");
        // this will loop across the RDD to display the value. In a multi-socket CPU environment(not to confuse with multiple core CPU), this will produce error.
        sqrtRdd.foreach(aDouble -> LOGGER.info("root: {}", aDouble)); // RDD foreach

        LOGGER.info("*** \t *** \t ***");
        LOGGER.info("Collection forEach");
        // this will loop across the Collection created from the RDD to display the value.
        // In any environment, this will work fine.
        sqrtRdd.collect().forEach(aDouble -> LOGGER.info("root: {}", aDouble)); // Collections forEach

        LOGGER.info("*** \t *** \t ***");
        JavaRDD<Integer> singleValueRdd = myRdd.map(integer -> 1);
        LOGGER.info("foreach of singleValueRdd");
        singleValueRdd.foreach(integer -> LOGGER.info("value: {}", integer));
        Integer count = singleValueRdd.reduce((integer, integer2) -> integer + integer2);
        LOGGER.info("sum of singleValueRdd (count): {}", count);

        LOGGER.info("*** \t *** \t ***");
        LOGGER.info("Tuple forEach");
        JavaRDD<Tuple2<Integer, Double>> tuple2JavaRDD = myRdd.map(integer -> new Tuple2<>(integer, Math.sqrt(integer)));
        tuple2JavaRDD.foreach(tuple2 -> LOGGER.info("{} is square root of {}", tuple2._1(), tuple2._2().intValue()));

    }

}

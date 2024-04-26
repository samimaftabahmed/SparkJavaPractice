package com.samhad.spark.module4_sparkStreaming.lesson__30;

import com.samhad.spark.common.SparkTask;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

/**
 * Implements a DStream.
 * Note: DStream is deprecated.
 */
public class IntroductionDStream implements SparkTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(IntroductionDStream.class);

    @Override
    public void execute(SparkSession spark) {
        JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        JavaStreamingContext jsc = new JavaStreamingContext(sc, Durations.seconds(2));
        JavaReceiverInputDStream<String> textStream = jsc.socketTextStream("127.0.0.1", 8989);
//        JavaDStream<String> dStream = textStream.cache(); // similar behaviour observed as the below statement.
        JavaDStream<String> dStream = textStream.map(s -> s);
        JavaPairDStream<String, Integer> pairDStream = dStream.mapToPair(s -> {
            String[] split = s.split(",");
            String level = split[0];
//            String timestamp = split[1];
            return new Tuple2<>(level, 1);
        });

        pairDStream
                .reduceByKey(Integer::sum)
                .print(10);
//        dStream.print(10);

        try {
            jsc.start();
            jsc.awaitTermination();
        } catch (InterruptedException e) {
            LOGGER.error("JavaStreamingContext interrupted.");
            throw new RuntimeException(e);
        }
    }
}

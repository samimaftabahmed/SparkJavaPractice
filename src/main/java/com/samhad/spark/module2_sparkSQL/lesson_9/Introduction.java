package com.samhad.spark.module2_sparkSQL.lesson_9;

import com.samhad.spark.common.SparkTask;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Introduction implements SparkTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(Introduction.class);

    @Override
    public void execute(SparkSession spark) {
        Dataset<Row> dataset = spark.read().option("header", true)
                .csv("src/main/resources/dataset/students.csv");
        dataset.show(20);
        long count = dataset.count();
        LOGGER.info("Data Count: {}", count);
    }
}

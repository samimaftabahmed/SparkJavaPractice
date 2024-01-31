package com.samhad.spark.module1_basics.lesson_7;

import com.samhad.spark.common.SparkTask;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

public class Joins implements SparkTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(Joins.class);

    @Override
    public void execute(JavaSparkContext sc) {
        LOGGER.info("\n---------------------------------------------------------");
        JavaRDD<String> rdd = sc.textFile("src/main/resources/dataset/salary.csv").cache();
        rdd = rdd.filter(s -> !s.equals("Last Name,First Name,Status,Salary"));

        JavaPairRDD<String, String> firstNameSalaryPairRDD = rdd.mapToPair(s -> {
            String[] split = s.split(",");
            String firstname = split[1];
            String salary = split[3].split("\"")[1].trim();
            return new Tuple2<>(firstname, salary);
        });

        JavaPairRDD<String, String> firstNameStatusPairRDD = rdd.mapToPair(s -> {
            String[] split = s.split(",");
            String firstname = split[1];
            String status = split[2];
            return new Tuple2<>(firstname, status);
        });

        JavaPairRDD<String, Tuple2<String, String>> joinedPairRDD = firstNameSalaryPairRDD.join(firstNameStatusPairRDD);
        joinedPairRDD.foreach(jpr -> {
            LOGGER.info("Firstname: {}, Salary: {}, Status: {}", jpr._1(), jpr._2()._1(), jpr._2()._2());
        });
    }
}

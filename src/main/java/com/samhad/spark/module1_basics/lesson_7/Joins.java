package com.samhad.spark.module1_basics.lesson_7;

import com.samhad.spark.common.SparkTask;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.List;

public class Joins implements SparkTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(Joins.class);

    @Override
    public void execute(JavaSparkContext sc) {
        LOGGER.info("\n---------------------------------------------------------");
        List<String> data = sc.textFile("src/main/resources/dataset/salary.csv").collect();
        data = data.stream().filter(s -> !s.equals("Last Name,First Name,Status,Salary")).toList();
        List<String> nameStatusData = data.stream().map(s -> {
            String[] split = s.split(",");
            return split[1] + "," + split[2];
        }).toList();

        List<String> nameSalaryData = data.stream().map(s -> {
            String[] split = s.split(",");
            return split[1] + "," + split[3];
        }).toList();

        JavaRDD<String> nameStatusRDD = sc.parallelize(nameStatusData);
        JavaRDD<String> nameSalaryRDD = sc.parallelize(nameSalaryData);

        JavaPairRDD<String, String> nameSalaryPairRDD = nameSalaryRDD.mapToPair(s -> {
            String[] split = s.split(",");
            String name = split[0];
            String salary = split[1].split("\"")[1].trim();
            return new Tuple2<>(name, salary);
        });

        JavaPairRDD<String, String> nameStatusPairRDD = nameStatusRDD.mapToPair(s -> {
            String[] split = s.split(",");
            String name = split[0];
            String status = split[1];
            return new Tuple2<>(name, status);
        });

        JavaPairRDD<String, Tuple2<String, String>> joinedPairRDD = nameSalaryPairRDD.join(nameStatusPairRDD);
        joinedPairRDD.foreach(jpr -> {
            LOGGER.info("Name: {}, Salary: {}, Status: {}", jpr._1(), jpr._2()._1(), jpr._2()._2());
        });
    }
}

package com.samhad.spark.module1_basics.lesson_7;

import com.samhad.spark.common.SparkTask;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Concepts on join(), leftOuterJoin(), rightOuterJoin(), fullOuterJoin(), cartesian().
 * Section: 12.
 */
public class Joins implements SparkTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(Joins.class);

    @Override
    public void execute(SparkSession spark) {
        logFileStart(LOGGER, this.getClass());
        JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        List<String> data = sc.textFile("src/main/resources/dataset/salary.csv").collect();
        data = data.stream().filter(s -> !s.equals("Last Name,First Name,Status,Salary")).toList();

        List<String> nameStatusData = getData(data, 1, 2);
        // the following added values will be missing out from inner-joined and left-outer-joined pair RDD
        nameStatusData.add("Nehra,Full Time");
        nameStatusData.add("Shami,Full Time");
        nameStatusData.add("Brett,Full Time");
        nameStatusData.add("Shoaib,Full Time");

        List<String> nameSalaryData = getData(data, 1, 3);
        // the following added values will be missing out from inner-joined and right-outer-joined pair RDD
        nameSalaryData.add("Bruce,\"$500000\"");
        nameSalaryData.add("John,\"$70000\"");
        nameSalaryData.add("Tony,\"$500000\"");
        nameSalaryData.add("Jindal,\"$65000\"");

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

        LOGGER.info("\n\n*** INNER JOIN : nameSalary with nameStatus ***\n");
        JavaPairRDD<String, Tuple2<String, String>> joinedPairRDD = nameSalaryPairRDD.join(nameStatusPairRDD);
        joinedPairRDD.foreach(jpr -> {
            LOGGER.info("Name: {}, Salary: {}, Status: {}", jpr._1(), jpr._2()._1(), jpr._2()._2());
        });

        LOGGER.info("\n\n*** LEFT OUTER JOIN : nameSalary with nameStatus ***\n");
        JavaPairRDD<String, Tuple2<String, Optional<String>>> leftOuterJoinPairRDD = nameSalaryPairRDD.leftOuterJoin(nameStatusPairRDD);
        leftOuterJoinPairRDD.foreach(jpr -> {
            String name = jpr._1();
            String salary = jpr._2()._1();
            Optional<String> optionalStatus = jpr._2()._2();
            String status = optionalStatus.orElse("<UNKNOWN>");
            LOGGER.info("Name: {}, Salary: {}, Status: {}", name, salary, status);
        });

        LOGGER.info("\n\n*** RIGHT OUTER JOIN : nameSalary with nameStatus ***\n");
        JavaPairRDD<String, Tuple2<Optional<String>, String>> rightOuterJoinPairRDD = nameSalaryPairRDD.rightOuterJoin(nameStatusPairRDD);
        rightOuterJoinPairRDD.foreach(jpr -> {
            String name = jpr._1();
            String status = jpr._2()._2();
            Optional<String> optionalSalary = jpr._2()._1();
            String salary = optionalSalary.orElse("<UNKNOWN>");
            LOGGER.info("Name: {}, Salary: {}, Status: {}", name, salary, status);
        });

        LOGGER.info("\n\n*** FULL OUTER JOIN : nameSalary with nameStatus ***\n");
        JavaPairRDD<String, Tuple2<Optional<String>, Optional<String>>> fullOuterJoinPairRDD = nameSalaryPairRDD.fullOuterJoin(nameStatusPairRDD);
        fullOuterJoinPairRDD.foreach(jpr -> {
            String name = jpr._1();
            Optional<String> optionalStatus = jpr._2()._2();
            String status = optionalStatus.orElse("<UNKNOWN>");
            Optional<String> optionalSalary = jpr._2()._1();
            String salary = optionalSalary.orElse("<UNKNOWN>");
            LOGGER.info("Name: {}, Salary: {}, Status: {}", name, salary, status);
        });

        JavaRDD<String> take5RDD = sc.parallelize(nameSalaryRDD.take(5));
        JavaRDD<String> take2RDD = sc.parallelize(nameStatusRDD.take(2));

        LOGGER.info("\n\n*** CROSS JOIN / CARTESIAN PRODUCT : take2 with take5 ***\n");
        JavaPairRDD<String, String> cartesianRDD = take2RDD.cartesian(take5RDD);
        cartesianRDD.foreach(jpr -> {
            LOGGER.info("{} --- {}", jpr._1(), jpr._2());
        });
    }

    private List<String> getData(List<String> data, int columnA, int columnB) {
        return data.stream().map(s -> {
            String[] split = s.split(",");
            return split[columnA] + "," + split[columnB];
        }).collect(Collectors.toList());
    }
}

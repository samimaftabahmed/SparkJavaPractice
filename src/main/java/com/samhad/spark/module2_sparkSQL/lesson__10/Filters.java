package com.samhad.spark.module2_sparkSQL.lesson__10;

import com.samhad.spark.common.SparkTask;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.spark.sql.functions.col;

public class Filters implements SparkTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(Filters.class);

    @Override
    public void execute(SparkSession spark) {
        logFileStart(LOGGER, this.getClass());
//      header:  student_id,exam_center_id,subject,year,quarter,score,grade
        Dataset<Row> dataset = spark.read().option("header", true)
                .csv("src/main/resources/dataset/students.csv");

        // filtering records using SQL like expression. Using just needs the where clause.
        Dataset<Row> filter = dataset.filter("subject='Math' and grade='A+' and score >= 93");
        filter.show(5);

        // filtering records using Lambdas
        Dataset<Row> filter2 = dataset.filter((Row row) ->
                row.getAs("subject").equals("Math") &&
                        row.getAs("grade").equals("A+"));
        // greater than equal to is not possible using this API.
        filter2.show(5);

        // filtering records using a similar Spring's Criteria-like API.
        Column subjectCol = dataset.col("subject");
        Column gradeCol = dataset.col("grade");
        Column scoreCol = dataset.col("score");

        Dataset<Row> filter3 = dataset
                .filter(subjectCol.equalTo("Math")
                        .and(gradeCol.equalTo("A+"))
                        .and(scoreCol.geq(93)));
        filter3.show(5);

        // filtering records using a similar Spring's Criteria-like API but getting Column references using helper
        // methods of class 'functions'. Yes, this class name starts with a small case. Check the static import statement.
        Dataset<Row> filter4 = dataset
                .filter(col("subject").equalTo("Math").and(
                        col("grade").equalTo("A+")).and(
                        col("score").geq(93)));
        filter4.show(5);
    }
}

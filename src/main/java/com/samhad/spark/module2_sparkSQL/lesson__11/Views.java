package com.samhad.spark.module2_sparkSQL.lesson__11;

import com.samhad.spark.common.SparkTask;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Views implements SparkTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(Views.class);

    @Override
    public void execute(SparkSession spark) {
        logFileStart(LOGGER, this.getClass());
//      header:  student_id,exam_center_id,subject,year,quarter,score,grade
        Dataset<Row> dataset = spark.read().option("header", true)
                .csv("src/main/resources/dataset/students.csv");
        try {
            dataset.createTempView("student_table");
        } catch (AnalysisException e) {
            throw new RuntimeException(e);
        }

        spark.sql("select student_id,score from student_table where subject='Math'")
                .show(5);


    }
}
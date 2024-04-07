package com.samhad.spark.module2_sparkSQL.lesson__13;

import com.samhad.spark.common.SparkTask;
import com.samhad.spark.common.Utility;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.apache.spark.sql.functions.col;

/**
 * Exercise on Pivot, Aggregation and few Spark functions.
 * Section: 27.
 */
public class ExerciseOnPivotAndFunctions implements SparkTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExerciseOnPivotAndFunctions.class);

    @Override
    public void execute(SparkSession spark) {
        logFileStart(LOGGER, this.getClass());
//      header:  student_id,exam_center_id,subject,year,quarter,score,grade
        Dataset<Row> dataset = Utility.getStudentsDataset(spark);

        LOGGER.info("Exercise on creating a Pivot Table with Year as Pivot, with average of score and " +
                "standard deviation of score.");
        Column subject = col("subject");
        Column year = col("year");
        Column score = col("score");

        List<Object> pivotColumns = List.of("2004", "2005", "2011", "2012");

        dataset
                .groupBy(subject)
                .pivot(year, pivotColumns)
                .agg(
                        functions.round(functions.avg(score), 2).alias("Avg_Score"),
                        functions.round(functions.stddev(score), 2).alias("Std_Deviation")
                )
                .na().fill(0)
                .show();
    }
}

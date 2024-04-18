package com.samhad.spark.module2_sparkSQL.lesson__14;

import com.samhad.spark.common.SparkTask;
import com.samhad.spark.common.Utility;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Month;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;

/**
 * Creating and implementing User-Defined Functions in DataFrame API and SparkSQL. Tried some Performance optimisations
 * with SparkSQL and Dataset API. HashAggregation vs SortAggregation.
 * Spark Optimizer "Catalyst" performs optimisations on DataFrames or Datasets but not on RDD. So theoretically, RDDs are
 * faster than Datasets or DataFrames.
 * Section 28, 29, 30, 31.
 */
public class UserDefinedFunctions implements SparkTask, Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(UserDefinedFunctions.class);

    @Override
    public void execute(SparkSession spark) {
        logFileStart(LOGGER, this.getClass());
        /**
         To change the no. of partitions, we use the following config. More the no. of partitions, more the no. of Tasks
         in a Job. By default, in Spark 3, it is seen that the number of partitions = number of cores + virtual cores.
        */
//        spark.conf().set("spark.sql.shuffle.partitions", "24");

        /**
          By default, during group Hash Aggregation is used if the grouping value is mutable. We can force the usage of
          Hash Aggregation during sorting with the following config. But spark will fallback to Sort Aggregation if
          there is not enough memory for Hash Aggregation.
        */
//        spark.conf().set("spark.sql.execution.useObjectHashAggregateExec", "true");

        // header: level, datetime
        Dataset<Row> logsDataset = createLogsDataset(spark);
        // header: student_id,exam_center_id,subject,year,quarter,score,grade
        Dataset<Row> studentsDataset = Utility.getStudentsDataset(spark);

        singleColumnUDF(logsDataset, spark);
        multipleColumnUDF(studentsDataset, spark);
        usingUDFInSparkSQL(logsDataset, spark);
    }

    private void singleColumnUDF(Dataset<Row> logsDataset, SparkSession spark) {
        // Creating a column and setting value based on other column.
        logsDataset
                .withColumn("New Created Column",
                        functions.lit(
                                col("level").equalTo("INFO")))
                .show();

        String isInfoUDF = "isInfo";
        // Creating and registering a User-Defined Functions (UDF).
        spark.udf().register(isInfoUDF, (String level) -> level.equals("INFO"), DataTypes.BooleanType);

        // Using UDF "isInfo", creating a column and setting value based on other column.
        logsDataset
                .withColumn("UDF New Column",
                        functions.lit(
                                functions.callUDF(isInfoUDF, col("level"))))
                .show();
    }

    private void usingUDFInSparkSQL(Dataset<Row> logsDataset, SparkSession spark) {
        logsDataset.createOrReplaceTempView("log_table");

        // ordering the logs based on the month and level.
        // built-in SparkSQL functions first, date_format and cast is used.
        // first picks out the first element from a collection.
        // cast is used to cast to a particular datatype.
        // date_format is used to parse the date and return us in our specified format.
        String sqlQuery = "select level, date_format(datetime,'MMMM') as month, " +
                " cast(date_format(datetime,'M') as int) as monthNum, count(1) as total from log_table " +
                " group by level,month,monthNum " +
                " order by monthNum,level";
        LOGGER.info("Executing SQL Query: {}", sqlQuery);
        spark.sql(sqlQuery).show(100);

        String monthNumUDF = "monthNumUDF";
        // creating a new UDF
        spark.udf().register(monthNumUDF,
                (String month) -> Month.valueOf(month.toUpperCase()).getValue(),
                DataTypes.IntegerType);

        // executing the same query above but using UDF.
//        sqlQuery = "select level, date_format(datetime,'MMMM') as month, " +
//                " count(1) as total from log_table " +
//                " group by level,month " +
//                " order by monthNumUDF(month),level";
//        LOGGER.info("Executing SQL Query: {}", sqlQuery);
//        spark.sql(sqlQuery).show(100);

        // Executing the same query above but optimising the performance of the query.
        // When "group by" clause has the same columns or params as "order by" clause, then we are able to reduce a spark job,
        // thereby improving the performance.
        sqlQuery = "select level, date_format(datetime,'MMMM') as month, " +
                " monthNumUDF(month), count(1) as total from log_table " +
                " group by level,month,monthNumUDF(month) " +
                " order by monthNumUDF(month),level";
        LOGGER.info("Executing SQL Query: {}", sqlQuery);
        Dataset<Row> resultsSQL = spark.sql(sqlQuery);
        resultsSQL.show(100);
        /**
         Dataset.explain() shows the execution plan in textual format. In spark2, with sparkSQL when using "group by" clause
         it does a Sort Aggregation. In Spark 3, it uses Hash Aggregation. Hash Aggregation is faster than Sort Aggregation
         but Sort Aggregation is memory efficient than Hash Aggregation as it does in-memory sorting.
        */
        resultsSQL.explain();

        Column levelCol = col("level");
        Column datetimeCol = col("datetime");
        Column monthCol = col("month");

        // In Spark 2, programmatically written codes with the Dataset API are approximately 4 times faster than
        // SparkSQL codes during execution, but the same cannot be observed in Spark 3. Maybe in bigger operations this
        // might hold true.
        Dataset<Row> resultsJava = logsDataset
                .select(
                        levelCol,
                        date_format(datetimeCol, "MMMM").alias("month")
                )
                .groupBy(levelCol, monthCol, functions.callUDF(monthNumUDF, monthCol))
                .count().withColumnRenamed("count", "total")
                .orderBy(functions.callUDF(monthNumUDF, monthCol), levelCol);
        resultsJava.show(100);
        resultsJava.explain();
    }

    private void multipleColumnUDF(Dataset<Row> studentsDataset, SparkSession spark) {
        // header: student_id,exam_center_id,subject,year,quarter,score,grade
        studentsDataset.createOrReplaceTempView("student");
        spark.udf().register("remark",
                (String score, String grade) -> remarkUDF(score, grade),
                DataTypes.StringType);

        spark.sql("select student_id, subject, score, grade, remark(score, grade) from student")
                .show();

        UDF2<String, String, String> remarkUDF2 = (score, grade) -> remarkUDF(score, grade);
        spark.udf().register("remarkUDF2", remarkUDF2, DataTypes.StringType);
        spark.sql("select student_id, subject, score, grade, remarkUDF2(score, grade) from student")
                .show();
    }

    private String remarkUDF(String score, String grade) {
        int scoreNum = Integer.parseInt(score);
        String remark = "Fail";
        if (scoreNum > 75 && grade.startsWith("A")) {
            remark = "Excellent";
        } else if (scoreNum > 60 && grade.startsWith("B")) {
            remark = "Good";
        } else if (scoreNum > 45 && grade.startsWith("C")) {
            remark = "Needs Improvement";
        }
        return remark;
    }

    private Dataset<Row> createLogsDataset(SparkSession spark) {
        List<Row> inMemoryRows = Utility.generateDummyLogs(10000, 2023);
        StructField[] fields = {
                new StructField("level", DataTypes.StringType, false, Metadata.empty()),
                new StructField("datetime", DataTypes.TimestampType, false, Metadata.empty())
        };

        StructType structType = new StructType(fields);
        LOGGER.info("Creating DataFrame from a List of Row.");
        Dataset<Row> dataFrame = spark.createDataFrame(inMemoryRows, structType);
        return dataFrame;
    }

}

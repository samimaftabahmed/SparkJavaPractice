package com.samhad.spark.module2_sparkSQL.lesson__14;

import com.samhad.spark.common.SparkTask;
import com.samhad.spark.common.Utility;
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

/**
 * Creating and implementing User-Defined Functions in DataFrame API and SparkSQL.
 * Section 28.
 */
public class UserDefinedFunctions implements SparkTask, Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(UserDefinedFunctions.class);

    @Override
    public void execute(SparkSession spark) {
        logFileStart(LOGGER, this.getClass());
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
                " count(1) as total from log_table " +
                " group by level,month " +
                " order by cast( first( date_format(datetime,'M')) as int),level";
        LOGGER.info("Executing SQL Query: {}", sqlQuery);
        spark.sql(sqlQuery).show(100);

        // creating a new UDF
        spark.udf().register("monthNumUDF",
                (String month) -> Month.valueOf(month.toUpperCase()).getValue(),
                DataTypes.IntegerType);

        // executing the same query above but using UDF.
        sqlQuery = "select level, date_format(datetime,'MMMM') as month, " +
                " count(1) as total from log_table " +
                " group by level,month " +
                " order by monthNumUDF(month),level";
        LOGGER.info("Executing SQL Query: {}", sqlQuery);
        spark.sql(sqlQuery).show(100);
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

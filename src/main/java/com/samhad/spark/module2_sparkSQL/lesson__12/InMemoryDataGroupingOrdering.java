package com.samhad.spark.module2_sparkSQL.lesson__12;

import com.samhad.spark.common.SparkTask;
import com.samhad.spark.common.Utility;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;

/**
 * Creating in-memory DataFrame programmatically, using built-in Spark SQL functions, Grouping, Ordering.
 * Using the DataFrame API.
 * Creating a Pivot Table.
 * Section: 19 to 26.
 */
public class InMemoryDataGroupingOrdering implements SparkTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(InMemoryDataGroupingOrdering.class);

    @Override
    public void execute(SparkSession spark) {
        logFileStart(LOGGER, this.getClass());

        List<Row> inMemoryRows = Utility.generateDummyLogs(10000, 2023);
        StructField[] fields = {
                new StructField("level", DataTypes.StringType, false, Metadata.empty()),
                new StructField("datetime", DataTypes.TimestampType, false, Metadata.empty())
        };

        StructType structType = new StructType(fields);
        LOGGER.info("Creating DataFrame from a List of Row.");
        Dataset<Row> dataFrame = spark.createDataFrame(inMemoryRows, structType);
        dataFrame.show(10);

        dataFrame.createOrReplaceTempView("log_table");

        groupingOrderingUsingSparkSQL(spark);
        groupingOrderingUsingDatasetAPI(spark, dataFrame);
        usingPivotTable(spark, dataFrame);
        groupingAndAggregation(dataFrame);
    }

    private void groupingOrderingUsingSparkSQL(SparkSession spark) {
        LOGGER.info("Using SparkSQL for Grouping and Ordering.");

        String sqlQuery = "select level, count(level) as count from log_table group by level order by count desc";
        LOGGER.info("Executing SQL Query: {}", sqlQuery);
        Dataset<Row> resultDataset = spark.sql(sqlQuery);
        resultDataset.show();

        // Check https://spark.apache.org/docs/3.5.1/api/sql/ for more built-in SparkSQL functions.
        // group by 'key' negatively impacts performance of the JVM, just like groupByKey() in RDD
        sqlQuery = "select level, count(level) as count, collect_list(datetime) as dateTimes " +
                "from log_table group by level";
        LOGGER.info("Executing SQL Query: {}", sqlQuery);
        resultDataset = spark.sql(sqlQuery);
        resultDataset.show();

        LOGGER.info("Showing with truncate=false.");
//         if truncate parameter false is used then value in the column will not be truncated by ellipsis.
//         By default, values are truncated.
//        resultDataset.show(2, false); // commenting it as executing this messes the console log.

        List<Row> rows = resultDataset.collectAsList();
        printRows(rows, 10);

        // ordering the logs based on the month and level.
        // built-in SparkSQL functions first, date_format and cast is used.
        // first picks out the first element from a collection.
        // cast is used to cast to a particular datatype.
        // date_format is used to parse the date and return us in our specified format.
        sqlQuery = "select level, date_format(datetime,'MMMM') as month, " +
                " cast( first( date_format(datetime,'M')) as int) as monthNum, " +
                " count(1) as total from log_table " +
                " group by level,month " +
                " order by monthNum,level";
        LOGGER.info("Sorting results based on Month. Note: the 'monthNum' column is dropped from the DataFrame.");
        LOGGER.info("Executing SQL Query: {}", sqlQuery);
        resultDataset = spark
                .sql(sqlQuery)
                .drop("monthNum"); // dropping unnecessary column from the dataset

        resultDataset.show(100);
    }

    private void groupingOrderingUsingDatasetAPI(SparkSession spark, Dataset<Row> dataFrame) {
        LOGGER.info("Using DataSet / DataFrame API :: select() and selectExpr(), for Grouping and Ordering.");

        LOGGER.info("Selecting the column 'level' with select() method.");
        Dataset<Row> resultDataset = dataFrame.select("level"); // we can select the columns of the dataset.
        resultDataset.show();

        LOGGER.info("Selecting with selectExpr() for executing spark_sql functions during selection.");
        // To execute a sparkSQL built-in function selectExpr() needs to be used.
        // Also, columns can be used with selectExpr() as well.
        resultDataset = dataFrame.selectExpr("level", "date_format(datetime,'MMMM') as month");
        resultDataset.show();

        Column levelCol = col("level");
        Column datetimeCol = col("datetime");
        Column monthNumCol = col("monthNum");
        Column monthCol = col("month");

        LOGGER.info("Sorting results based on Month. The equivalent SQL Query was executed few moments ago. Check logs.");
        // selection using the Dataset API.
        resultDataset = dataFrame
                .select(
                        levelCol,
                        date_format(datetimeCol, "MMMM").alias("month"),
                        date_format(datetimeCol, "M").alias("monthNum").cast(DataTypes.IntegerType)
                )
                .groupBy(levelCol, monthCol, monthNumCol)
                .count().withColumnRenamed("count", "total")
                .orderBy(monthNumCol, levelCol)
                .drop(monthNumCol);
        resultDataset.show(100);
    }

    private void usingPivotTable(SparkSession spark, Dataset<Row> dataFrame) {
        LOGGER.info("Creating Pivot Table with Spark.");
        Column levelCol = col("level");
        Column datetimeCol = col("datetime");

        // spark creating pivot table with columns auto-detected
        Dataset<Row> resultDataset = dataFrame
                .select(
                        levelCol,
                        date_format(datetimeCol, "MMMM").alias("month"),
                        date_format(datetimeCol, "M").alias("monthNum").cast(DataTypes.IntegerType)
                )
                .groupBy(levelCol).pivot("month")
                .count();
        resultDataset.show(100);

        LOGGER.info("Creating Pivot table with the Column names provided.");
        List<Object> monthList = getPivotColumns();
        // spark creating pivot table with columns being user-provided. This helps in performance.
        resultDataset = dataFrame
                .select(
                        levelCol,
                        date_format(datetimeCol, "MMMM").alias("month"),
                        date_format(datetimeCol, "M").alias("monthNum").cast(DataTypes.IntegerType)
                )
                .groupBy(levelCol)
                .pivot("month", monthList)
                .count()
                .na().fill(0); // na() checks for null and allows us to fill the null values with a some value.
        resultDataset.show(100);
    }

    private void groupingAndAggregation(Dataset<Row> dataFrame) {
        Column datetime = col("datetime");
        Column level = col("level");
        String maxDateTimeAlias = "Max_DateTime";
        LOGGER.info("Grouping and Aggregation using various spark functions.");

        dataFrame.groupBy(level)
                .agg(
                        functions.max(datetime).alias(maxDateTimeAlias),
                        functions.min(datetime).alias("Min_DateTime"),
                        functions.current_user(),
                        functions.current_timestamp(),
                        functions.md5(functions.date_format(col(maxDateTimeAlias), "hh:mm:ss"))
                ).show(false); // the dataset used in lesson 9, 10 is better when used with these functions.
    }

    private void printRows(List<Row> rows, int numberOfRows) {
        int size = Math.min(numberOfRows, rows.size());
        LOGGER.info("Printing '{}' number of collected Rows.", size);
        for (int i = 0; i < size; i++) {
            Row row = rows.get(i);
            List<Timestamp> dateTimes = row.getList(2);
            String level = row.getAs("level");
            String count = row.getAs("count").toString();
            System.out.printf("level: %s, count: %s%n", level, count);
            int min = Math.min(dateTimes.size(), 10); // showing max 10 to prevent console log getting overflowed
            for (int j = 0; j < min; j++) {
                Timestamp dateTime = dateTimes.get(j);
                System.out.printf("dateTime: %s%n", dateTime);
            }

//            System.out.println(row.json() + "\n"); // prints the entire row in JSON format.
        }
    }

    private List<Object> getPivotColumns() {
        int totalMonths = 12;
        List<Object> monthList = new ArrayList<>(totalMonths);
        DateTimeFormatter pattern = DateTimeFormatter.ofPattern("MMMM");
        LocalDate dateTime = LocalDate.of(2024, 1, 1);
        for (int i = 0; i < totalMonths; i++) {
            if (i != 0) {
                dateTime = dateTime.plusMonths(1);
            }
            String calendarMonth = dateTime.format(pattern);
            monthList.add(calendarMonth);
        }

        LOGGER.info("Created Pivot columns: {}", monthList);
        return monthList;
    }
}

package com.samhad.spark.module2_sparkSQL.lesson__12;

import com.samhad.spark.common.SparkTask;
import com.samhad.spark.common.Utility;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;

/**
 * Creating in-memory DataFrame programmatically, using built-in Spark SQL functions, Grouping, Ordering.
 * Section: 19 to 24
 */
public class InMemoryDataGroupingOrdering implements SparkTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(InMemoryDataGroupingOrdering.class);

    @Override
    public void execute(SparkSession spark) {
        logFileStart(LOGGER, this.getClass());

        List<Row> inMemoryRows = generateRows(10000, 2023);
        StructField[] fields = {
                new StructField("level", DataTypes.StringType, false, Metadata.empty()),
                new StructField("datetime", DataTypes.StringType, false, Metadata.empty())
        };

        StructType structType = new StructType(fields);
        Dataset<Row> dataFrame = spark.createDataFrame(inMemoryRows, structType);
        dataFrame.show(10);

        dataFrame.createOrReplaceTempView("log_table");

        groupingOrderingUsingSparkSQL(spark);
        groupingOrderingUsingDatasetAPI(spark, dataFrame);
    }

    private void groupingOrderingUsingSparkSQL(SparkSession spark) {
        LOGGER.info("Using SparkSQL for Grouping and Ordering.");

        Dataset<Row> resultDataset =
                spark.sql("select level, count(level) as count from log_table group by level order by count desc");
        resultDataset.show();

        // Check https://spark.apache.org/docs/3.5.1/api/sql/ for more built-in SparkSQL functions.
        resultDataset =
                spark.sql("select level, count(level) as count, collect_list(datetime) as dateTimes " +
                        "from log_table group by level");
        // group by 'key' negatively impacts performance of the JVM, just like groupByKey() in RDD
        resultDataset.show();
//        resultDataset.show(2, false);

        List<Row> rows = resultDataset.collectAsList();
        printRows(rows, 10);

        // ordering the logs based on the month and level.
        // built-ib SparkSQL functions first, date_format and cast is used.
        // first picks out the first element from a collection.
        // cast is used to cast to a particular datatype.
        // date_format is used to parse the date and return us in our specified format.
        resultDataset = spark
                .sql("select level, date_format(datetime,'MMMM') as month, " +
                        " cast( first( date_format(datetime,'M')) as int) as monthNum, " +
                        "count(1) as total from log_table group by level,month order by monthNum,level")
                .drop("monthNum");

        resultDataset.show(100);
    }

    private void groupingOrderingUsingDatasetAPI(SparkSession spark, Dataset<Row> dataFrame) {
        LOGGER.info("Using DataSet / DataFrame API :: select() and selectExpr(), for Grouping and Ordering.");

        Dataset<Row> resultDataset = dataFrame.select("level"); // we can select the columns of the dataset.
        resultDataset.show();

        // To execute a sparkSQL built-in function selectExpr() needs to be used.
        // Also, columns can be used with selectExpr() as well.
        resultDataset = dataFrame.selectExpr("level", "date_format(datetime,'MMMM') as month");
        resultDataset.show();

        // selection using the Dataset API.
        resultDataset = dataFrame
                .select(
                        col("level"),
                        date_format(col("datetime"), "MMMM").alias("month"),
                        date_format(col("datetime"), "M").alias("monthNum").cast(DataTypes.IntegerType)
                )
                .groupBy(col("level"), col("month"), col("monthNum"))
                .count().withColumnRenamed("count", "total")
                .orderBy(col("monthNum"), col("level"))
                .drop(col("monthNum"));
        resultDataset.show(100);
    }

    private List<Row> generateRows(int recordCount, int startYear) {
        System.out.println("Creating Data start: " + LocalDateTime.now());
        List<Row> rows = new ArrayList<>(recordCount);
        final String[] logLevel = {"WARN", "INFO", "DEBUG", "ERROR", "TRACE"};
        int logLevelArraySize = logLevel.length;
        DateTimeFormatter pattern = DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss");
        for (int i = 0; i < recordCount; i++) {
            int index = ThreadLocalRandom.current().nextInt(0, logLevelArraySize);
            LocalDateTime randomDateTime = Utility.getRandomDateTime(
                    LocalDateTime.of(startYear, 1, 1, 0, 0),
                    LocalDateTime.now());

            Row row = RowFactory.create(logLevel[index], randomDateTime.format(pattern));
            rows.add(row);
        }

        System.out.println("Data Creation completed: " + LocalDateTime.now());
        return rows;
    }

    private void printRows(List<Row> rows, int numberOfRows) {
        int size = Math.min(numberOfRows, rows.size());
        for (int i = 0; i < size; i++) {
            Row row = rows.get(i);
            List<String> dateTimes = row.getList(2);
            String level = row.getAs("level");
            String count = row.getAs("count").toString();
            System.out.println("level: %s, count: %s".formatted(level, count));
            int min = Math.min(dateTimes.size(), 10); // showing max 10 to prevent console log getting overflowed
            for (int j = 0; j < min; j++) {
                String dateTime = dateTimes.get(j);
                System.out.println("dateTime: %s".formatted(dateTime));
            }

//            System.out.println(row.json() + "\n"); // prints the entire row in JSON format.
        }
    }
}

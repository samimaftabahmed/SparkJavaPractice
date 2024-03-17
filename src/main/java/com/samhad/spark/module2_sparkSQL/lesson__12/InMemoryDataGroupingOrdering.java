package com.samhad.spark.module2_sparkSQL.lesson__12;

import com.samhad.spark.common.SparkTask;
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

public class InMemoryDataGroupingOrdering implements SparkTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(InMemoryDataGroupingOrdering.class);

    @Override
    public void execute(SparkSession spark) {
        logFileStart(LOGGER, this.getClass());

        List<Row> inMemoryRows = generateRows();
        StructField[] fields = {
                new StructField("level", DataTypes.StringType, false, Metadata.empty()),
                new StructField("datetime", DataTypes.StringType, false, Metadata.empty())
        };

        StructType structType = new StructType(fields);
        Dataset<Row> dataFrame = spark.createDataFrame(inMemoryRows, structType);
        dataFrame.show(10);

        dataFrame.createOrReplaceTempView("log_table");
        Dataset<Row> resultDataset =
                spark.sql("select level, count(level) as count from log_table group by level order by count desc");
        resultDataset.show();

        // Check https://spark.apache.org/docs/3.5.1/api/sql/ for more build-in SparkSQL functions.
        resultDataset =
                spark.sql("select level, count(level) as count, collect_list(datetime) as dateTimes " +
                        "from log_table group by level");
        // group by 'key' negatively impacts performance of the JVM, just like groupByKey() in RDD
        resultDataset.show();

        List<Row> rows = resultDataset.collectAsList();
        printRows(rows);

    }

    private List<Row> generateRows() {
        List<Row> rows = new ArrayList<>();
        final String[] logLevel = {"WARN", "INFO", "DEBUG", "ERROR"};
        DateTimeFormatter pattern = DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss");
        for (int i = 0; i < 10; i++) {
            int index = ThreadLocalRandom.current().nextInt(0, 3);
            Row row = RowFactory.create(logLevel[index], LocalDateTime.now().format(pattern));
            rows.add(row);
        }
        return rows;
    }

    private void printRows(List<Row> rows) {
        for (Row row : rows) {
            List<String> dateTimes = row.getList(2);
            String level = row.getAs("level");
            String count = row.getAs("count").toString();
            System.out.println("level: %s, count: %s".formatted(level, count));
            for (String dateTime : dateTimes) {
                System.out.println("dateTime: %s".formatted(dateTime));
            }

            System.out.println(row.json() + "\n"); // prints the entire row in JSON format.
        }
    }
}

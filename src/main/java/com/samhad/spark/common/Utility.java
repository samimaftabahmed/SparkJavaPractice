package com.samhad.spark.common;

import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfo;
import io.github.classgraph.ScanResult;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ThreadLocalRandom;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * Class for all utility methods
 */
public class Utility {

    private static final Logger LOGGER = LoggerFactory.getLogger(Utility.class);

    /**
     * Reads a Zip file and extracts the files.
     *
     * @param file      - The zip file which files needs to be extracted
     * @param outputDir - The output directory
     */
    public static void readZipFile(String file, String outputDir) {
        try (ZipFile zipFile = new ZipFile(file)) {
            Enumeration<? extends ZipEntry> entries = zipFile.entries();
            while (entries.hasMoreElements()) {
                ZipEntry entry = entries.nextElement();
                File destinationFileOrOutput = new File(outputDir, entry.getName());
                if (entry.isDirectory()) {
                    boolean dirMade = destinationFileOrOutput.mkdirs();
                    LOGGER.info("Destination directory created: {}", dirMade);
                } else {
                    boolean dirMade = destinationFileOrOutput.getParentFile().mkdirs();
                    LOGGER.info("Destination file created: {}", dirMade);
                    try (
                            InputStream in = zipFile.getInputStream(entry);
                            OutputStream out = Files.newOutputStream(destinationFileOrOutput.toPath())
                    ) {
                        IOUtils.copy(in, out);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Creating instances and calling with Class Graph
     *
     * @param initializerVO - the Initializer VO
     */
    public static void callWithClassGraph(InitializerVO initializerVO) throws NoSuchMethodException,
            InvocationTargetException, InstantiationException, IllegalAccessException {

        SparkSession session = initializerVO.getSession();
        String packageName = initializerVO.getPackageName();
        LOGGER.info("Instantiating all implementations with Class Graph for package: \"{}\"", packageName);

        try (ScanResult scanResult = new ClassGraph().enableAllInfo().acceptPackages(packageName).scan()) {
            for (ClassInfo ci : scanResult.getClassesImplementing(SparkTask.class)) {
                SparkTask sparkTask = (SparkTask) ci.loadClass().getDeclaredConstructor().newInstance();
                sparkTask.execute(session);
            }
        }
    }

    /**
     * A hack to prevent the spark app from closing, as it gives us the window to check the Spark UI
     * that is accessible on port 4040. When running on local machine, check the spark UI here
     * <a href="http://localhost:4040">http://localhost:4040</a>
     */
    public static void pauseSparkApp() {
        new Scanner(System.in).nextLine();
    }

    /**
     * Creates a SparkSession.
     *
     * @param appName The name of the Session app.
     */
    public static SparkSession getSession(String appName) {
        String osName = SystemUtils.OS_NAME;
        String tempDirectory = osName.contains("Linux") ?
                "/tmp/spark_sql/" : "file:///p:/spark_sql/";
        return SparkSession.builder().appName(appName)
                .master("local[*]")
                .config("spark.storage.memoryFraction", "1")
                .config("rdd.compression", true)
                .config("spark.sql.warehouse.dir", tempDirectory)
                .getOrCreate();
    }

    /**
     * Generates Random Instant of time.
     */
    public static Instant getRandomInstant(Instant startInclusive, Instant endExclusive) {
        long startSeconds = startInclusive.getEpochSecond();
        long endSeconds = endExclusive.getEpochSecond();
        long random = ThreadLocalRandom.current()
                .nextLong(startSeconds, endSeconds);

        return Instant.ofEpochSecond(random);
    }

    /**
     * Generates Random LocalDateTime.
     */
    public static LocalDateTime getRandomDateTime(LocalDateTime startInclusive, LocalDateTime endExclusive) {
        Instant startEpochDay = startInclusive.toInstant(ZoneOffset.UTC);
        Instant endEpochDay = endExclusive.toInstant(ZoneOffset.UTC);
        Instant between = getRandomInstant(startEpochDay, endEpochDay);

        return LocalDateTime.ofInstant(between, ZoneOffset.UTC);
    }

    /**
     * Reads the 'students.csv' file.
     *
     * @param spark The SparkSession
     * @return Dataset<Row>
     */
    public static Dataset<Row> getStudentsDataset(SparkSession spark) {
        LOGGER.info("Reading Students Dataset file 'students.csv'");
        Dataset<Row> dataset = spark.read().option("header", true)
                .csv("src/main/resources/dataset/students.csv");
        return dataset;
    }

    /**
     * Generate Dummy logs with the given recordCount.
     *
     * @param recordCount The number of records to be generated.
     * @param minYear     The minimum year from which the log events should be created.
     * @return List<Row>
     */
    public static List<Row> generateDummyLogs(int recordCount, int minYear) {
        System.out.println("Creating Dummy Data start: " + LocalDateTime.now());
        List<Row> rows = new ArrayList<>(recordCount);
        final String[] logLevel = {"WARN", "INFO", "DEBUG", "ERROR", "TRACE"};
        int logLevelArraySize = logLevel.length;
//        DateTimeFormatter pattern = DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss");
        for (int i = 0; i < recordCount; i++) {
            int index = ThreadLocalRandom.current().nextInt(0, logLevelArraySize);
            LocalDateTime randomDateTime = getRandomDateTime(
                    LocalDateTime.of(minYear, 1, 1, 0, 0),
                    LocalDateTime.now());
            Timestamp timestamp = Timestamp.valueOf(randomDateTime);
            Row row = RowFactory.create(logLevel[index], timestamp);
            rows.add(row);
        }

        System.out.println("Dummy Data Creation completed: " + LocalDateTime.now());
        return rows;
    }
}

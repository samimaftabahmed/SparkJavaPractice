package com.samhad.spark.common;

import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfo;
import io.github.classgraph.ScanResult;
import org.apache.commons.io.IOUtils;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.util.Enumeration;
import java.util.Scanner;
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
        return SparkSession.builder().appName(appName)
                .master("local[*]")
                .config("spark.storage.memoryFraction","1")
                .config("rdd.compression", true)
                .config("spark.sql.warehouse.dir", "file:///p:/")
                .getOrCreate();
    }
}

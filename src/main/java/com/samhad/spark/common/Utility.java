package com.samhad.spark.common;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.Enumeration;
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

}

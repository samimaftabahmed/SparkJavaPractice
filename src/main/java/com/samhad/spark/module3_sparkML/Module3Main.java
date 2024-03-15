package com.samhad.spark.module3_sparkML;

import com.samhad.spark.common.InitializerVO;
import com.samhad.spark.common.Utility;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executes SparkTasks that are in the "com.samhad.spark.module3_sparkML" package
 */
public class Module3Main {

    private static final Logger LOGGER = LoggerFactory.getLogger(Module3Main.class);

    public static void main(String[] args) {
        final String appName = "Learning_Spark_Module_3";
        try (SparkSession spark = Utility.getSession(appName)) {
            InitializerVO initializerVO = new InitializerVO(spark, Module3Main.class.getPackageName());
            Utility.callWithClassGraph(initializerVO);
//            Utility.pauseSparkApp();
        } catch (Exception e) {
            LOGGER.error("Module3 Main:: Exception caught during execution: ", e);
        }
    }
}

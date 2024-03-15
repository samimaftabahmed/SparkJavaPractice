package com.samhad.spark.module2_sparkSQL;

import com.samhad.spark.common.InitializerVO;
import com.samhad.spark.common.Utility;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executes SparkTasks that are in the "com.samhad.spark.module2_sparkSQL" package
 */
public class Module2Main {

    private static final Logger LOGGER = LoggerFactory.getLogger(Module2Main.class);

    public static void main(String[] args) {
        final String appName = "Learning_Spark_Module_2";
        try (SparkSession spark = Utility.getSession(appName)) {
            InitializerVO initializerVO = new InitializerVO(spark, Module2Main.class.getPackageName());
            Utility.callWithClassGraph(initializerVO);
//            Utility.pauseSparkApp();
        } catch (Exception e) {
            LOGGER.error("Module2 Main:: Exception caught during execution: ", e);
        }
    }
}

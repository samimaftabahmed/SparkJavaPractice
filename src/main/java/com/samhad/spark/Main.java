package com.samhad.spark;

import com.samhad.spark.common.InitializerVO;
import com.samhad.spark.common.Utility;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        final String appName = "Learning_Spark_MAIN";
        SparkSession spark = null;
        try {
            spark = Utility.getSession(appName);
            InitializerVO initializerVO = new InitializerVO(spark, Main.class.getPackageName());
            Utility.callWithClassGraph(initializerVO);
//            Utility.pauseSparkApp();
        } catch (Exception e) {
            LOGGER.error("Exception caught during execution: ", e);
        } finally {
            LOGGER.info("Execution completed. Closing Spark Session.");
            if (spark != null)
                spark.close();
        }
    }
}

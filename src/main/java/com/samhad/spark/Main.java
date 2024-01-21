package com.samhad.spark;

import com.samhad.spark.lesson_1.Introduction;
import com.samhad.spark.lesson_2.PairRDDsAndOperations;
import com.samhad.spark.lesson_3.MapsAndFilters;
import com.samhad.spark.misc.MiscellaneousPractice;
import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfo;
import io.github.classgraph.ScanResult;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;

public class Main {

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws InvocationTargetException, NoSuchMethodException,
            InstantiationException, IllegalAccessException {
        SparkConf conf = new SparkConf().setAppName("LearningSpark").setMaster("local[*]");
        JavaSparkContext sc = null;
        try {
            sc = new JavaSparkContext(conf);
//            callManually(sc);
            callWithClassGraph(sc);
        } finally {
            if (sc != null)
                sc.close();
        }
    }

    /**
     * Creating instances and call manually
     * @param sc - the Spark Context
     */
    private static void callManually(JavaSparkContext sc) {
        LOGGER.info("Instantiating and calling manually.");
        new MiscellaneousPractice().execute(sc); // Miscellaneous Spark Practice
        new Introduction().execute(sc); // Lesson_1
        new PairRDDsAndOperations().execute(sc); // Lesson_2
        new MapsAndFilters().execute(sc); // Lesson_3
    }

    /**
     * Creating instances and calling with Class Graph
     * @param sc - the Spark Context
     */
    private static void callWithClassGraph(JavaSparkContext sc) throws NoSuchMethodException, InvocationTargetException,
            InstantiationException, IllegalAccessException {
        LOGGER.info("Instantiating all implementations with Class Graph");
        try (ScanResult scanResult = new ClassGraph().enableAllInfo().acceptPackages("com.samhad.spark").scan()) {
            for (ClassInfo ci : scanResult.getClassesImplementing("com.samhad.spark.MyRunner")) {
                MyRunner myRunner = (MyRunner) ci.loadClass().getDeclaredConstructor().newInstance();
                myRunner.execute(sc);
            }
        }
    }
}

package com.samhad.spark;

import com.samhad.spark.lesson_1.Introduction;
import com.samhad.spark.lesson_2.PairRDDsAndOperations;
import com.samhad.spark.lesson_3.MapsAndFilters;
import com.samhad.spark.misc.MiscellaneousPractice;
import org.apache.log4j.Level;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class Main {

    public static void main(String[] args) {

        System.setProperty("hadoop.home.dir", "C:\\winutils");
        org.apache.log4j.Logger.getLogger("org.apache").setLevel(Level.ERROR);

        SparkConf conf = new SparkConf().setAppName("LearningSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);


        new MiscellaneousPractice().execute(sc); // Miscellaneous Spark Practice
        new Introduction().execute(sc); // Lesson_1
        new PairRDDsAndOperations().execute(sc); // Lesson_2
        new MapsAndFilters().execute(sc); // Lesson_3

        sc.close();

    }

}

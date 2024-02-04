package com.samhad.spark.module1_basics.lesson_8;

import com.samhad.spark.common.SparkTask;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;

/**
 * Big Data Exercise on datasets to find out the popularity of various courses.
 * Section: 13.
 */
public class BigDataExercise implements SparkTask, Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(BigDataExercise.class);

    @Override
    public void execute(JavaSparkContext sc) {
        LOGGER.info("\n\n---------------------------");
        JavaRDD<String> rdd1 = sc.textFile("src/main/resources/exercise/chapter-course-data.txt").cache();
        JavaRDD<String> rdd2 = sc.textFile("src/main/resources/exercise/user-chapter-data.txt").cache();
        JavaRDD<String> rdd3 = sc.textFile("src/main/resources/exercise/course-title-data.txt").cache();

        JavaPairRDD<String, String> chapterCoursePair = rdd1.mapToPair(s -> {
            String[] split = s.split(",");
            String chapter = split[0];
            String course = split[1];
            return new Tuple2<>(chapter, course); // (96,1)
        });

        JavaPairRDD<String, String> chapterUserPair = rdd2.mapToPair(s -> {
            String[] split = s.split(",");
            String user = split[0];
            String chapter = split[1];
            return new Tuple2<>(chapter, user);
        }).distinct(); // (100,13)

        viewPairRDD(chapterCoursePair, "Chapter-Course Pair");
        viewPairRDD(chapterUserPair, "Chapter-User Pair");

        JavaPairRDD<String, Tuple2<String, String>> chapterUserCourseJoinPair =
                chapterUserPair.join(chapterCoursePair); // (100, (13,1))

        JavaPairRDD<Tuple2<String, String>, Integer> userCourseViewCountPair =
                chapterUserCourseJoinPair.mapToPair(tuple2 -> new Tuple2<>(tuple2._2, 1))
                        .reduceByKey(Integer::sum); // ((14,2), 2)

        viewPairRDD(chapterUserCourseJoinPair, "Chapter-(User-Course) Join Pair");
        viewPairRDD(userCourseViewCountPair, "(User-Course)-ViewCount Pair");

        JavaPairRDD<String, Integer> courseViewCountPair =
                userCourseViewCountPair.mapToPair(tuple2 -> new Tuple2<>(tuple2._1._2, tuple2._2)); // (2,1)

        JavaPairRDD<String, Integer> courseChapterCountPair =
                chapterCoursePair.mapToPair(tuple2 -> new Tuple2<>(tuple2._2, 1))
                        .reduceByKey(Integer::sum); // (3,10)

        viewPairRDD(courseViewCountPair, "Course-ViewCount Pair");
        viewPairRDD(courseChapterCountPair, "Course-ChapterCount Pair");

        JavaPairRDD<String, Tuple2<Integer, Integer>> courseViewCountChapterJoinPair =
                courseViewCountPair.join(courseChapterCountPair); // (3, (1,10))

        JavaPairRDD<String, Float> coursePercentagePair = courseViewCountChapterJoinPair.mapToPair(tuple2 -> {
            String courseId = tuple2._1;
            Tuple2<Integer, Integer> viewCountChapterTuple = tuple2._2;
            float percentage = (float) (viewCountChapterTuple._1 * 100) / viewCountChapterTuple._2;
            return new Tuple2<>(courseId, percentage); // (1,66.666664)
        });

        viewPairRDD(courseViewCountChapterJoinPair, "Course-(ViewCount-Chapter) Join Pair");
        viewPairRDD(coursePercentagePair, "Course-Percentage Pair");

        JavaPairRDD<String, Integer> courseTotalScorePair = coursePercentagePair.mapToPair(tuple2 -> {
            // All codes inside lambda must be Serializable, otherwise we get - Caused by: java.io.NotSerializableException:
            // So we implemented Serializable interface to this class.
            return new Tuple2<>(tuple2._1, this.getPoints(tuple2._2));
        }).reduceByKey(Integer::sum); // (2,10)

        JavaPairRDD<String, String> courseTitlePair = rdd3.mapToPair(s -> {
            String[] split = s.split(",", -1);
            String course = split[0];
            String title = split[1];
            return new Tuple2<>(course, title);
        });

        JavaPairRDD<String, Tuple2<String, Integer>> courseTitleTotalScoreJoinPair = courseTitlePair.join(courseTotalScorePair);

        viewPairRDD(courseTitlePair, "Course-Title Pair");
        viewPairRDD(courseTotalScorePair, "Course-TotalScore Pair");
        viewPairRDD(courseTitleTotalScoreJoinPair, "Course-(Title-TotalScore) Join Pair");

        List<Tuple2<String, Integer>> titleTotalScoreList = courseTitleTotalScoreJoinPair
                .mapToPair(tuple2 -> tuple2._2)
                .top(10, new TotalScoreComparator());
        // We have noticed that doing a foreach() operation after sortByKey() does not display the sorted results as expected,
        // as it was displaying  results from RDD.
        // So we switched to top() and then forEach().
        // Alternatively, we could have also done sortByKey() followed by take() and forEach().

        LOGGER.info("");
        titleTotalScoreList.forEach(tuple2 -> LOGGER.info("{} - {}", "Title-TotalScore List", tuple2));
    }

    /**
     * Returns points based on percentage.
     *
     * @param percentage - The percentage.
     * @return Point - Integer.
     */
    private int getPoints(float percentage) {
        int points;
        if (percentage > 90) {
            points = 10;
        } else if (percentage > 50 && percentage < 90) {
            points = 4;
        } else if (percentage > 25 && percentage < 50) {
            points = 2;
        } else {
            points = 0;
        }

        return points;
    }

    /**
     * Prints the elements of the JavaPairRDD.
     *
     * @param pairRDD     - The JavaPairRDD whose elements needs to be printed.
     * @param pairRddName - The JavaPairRDD name.
     */
    private void viewPairRDD(JavaPairRDD pairRDD, String pairRddName) {
        LOGGER.info("");
        pairRDD.foreach(o -> LOGGER.info("{} - {}", pairRddName, o));
    }

    /**
     * A Comparator comparing a Tuple of generic - (String, Integer).
     */
    private static class TotalScoreComparator implements Comparator<Tuple2<String, Integer>>, Serializable {
        @Override
        public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
            return Integer.compare(o1._2, o2._2);
        }
    }
}

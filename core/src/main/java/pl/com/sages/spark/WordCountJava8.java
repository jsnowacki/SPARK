package pl.com.sages.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import pl.com.sages.spark.conf.SparkConfBuilder;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Word Count Java 8
 */
public class WordCountJava8 {

    public static void main(String[] args) {
        SparkConf conf = SparkConfBuilder.buildLocal("word-count-java8");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> file = sc.textFile(args[0], 1);
        JavaRDD<String> words = file.flatMap(s -> Arrays.asList(s.split(" ")));
        JavaPairRDD<String, Integer> pairs = words.mapToPair(s -> new Tuple2<>(s, 1));
        JavaPairRDD<String, Integer> counts = pairs.reduceByKey((a, b) -> a + b).sortByKey();

        counts.saveAsTextFile(args[1]);
        //counts.coalesce(1).saveAsTextFile(args[1]);
    }
}

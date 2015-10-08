package pl.com.sages.spark.java;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import pl.com.sages.spark.java.conf.SparkConfBuilder;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Word Count Java
 */
public class WordCount {

    public static void main(String[] args) {
        SparkConf conf = SparkConfBuilder.buildLocal("word-count");

        JavaSparkContext sc = new JavaSparkContext(conf);

        sc.textFile(args[0], 1);
        JavaRDD<String> file = sc.textFile(args[0], 1);
        JavaRDD<String> words = file.flatMap(new FlatMapFunction<String, String>() {
            public Iterable<String> call(String s) { return Arrays.asList(s.split(" ")); }
        });
        JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) { return new Tuple2<String, Integer>(s, 1); }
        });
        JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer a, Integer b) { return a + b; }
        });
        counts.saveAsTextFile(args[1]);
        //counts.coalesce(1).saveAsTextFile(args[1]);
    }
}

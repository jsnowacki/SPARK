package pl.com.sages.spark;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import pl.com.sages.spark.conf.SparkConfBuilder;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Word Count Java 8
 */
public class WordClassCount {

    public static void main(String[] args) {
        SparkConf conf = SparkConfBuilder.buildLocal("word-count-java8");

        JavaSparkContext sc = new JavaSparkContext(conf);

        Map<String, Integer> classes = new HashMap<>();
        classes.put("starts_with_A", 1);
        classes.put("not_starts_with_A", -1);

        Broadcast<Map<String,Integer>> bc = sc.broadcast(classes);

        Accumulator<Integer> withA = sc.accumulator(0);
        Accumulator<Integer> withoutA = sc.accumulator(0);

        sc.textFile(args[0], 1);
        JavaRDD<String> file = sc.textFile(args[0], 1);
        JavaRDD<String> words = file.flatMap(s -> Arrays.asList(s.split(" ")));
        words.foreach(w -> {
            if (w.toLowerCase().startsWith("a")) {
                withA.add(bc.getValue().get("starts_with_A"));
            } else {
                withoutA.add(bc.getValue().get("not_starts_with_A"));
            }
        });

        System.out.printf("Starts with A count: %d\n", withA.value());
        System.out.printf("Does not start with A count: %d\n", withoutA.value());

    }
}

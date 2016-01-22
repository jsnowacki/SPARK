package pl.com.sages.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import pl.com.sages.spark.conf.SparkConfBuilder;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Word Count Java 8
 */
public class WordCountStreaming {

    public static void main(String[] args) {
        SparkConf conf = SparkConfBuilder.buildLocal("word-count-streaming");

        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(1));
        sc.checkpoint("checkpoint/");

        JavaDStream<String> file = sc.textFileStream(args[0]);
        JavaDStream<String> words = file.flatMap(s -> Arrays.asList(s.split(" ")));
        JavaPairDStream<String, Integer> pairs = words.mapToPair(s -> new Tuple2<>(s, 1));
        JavaPairDStream<String, Integer> counts = pairs.reduceByKey((a, b) -> a + b);
        counts.print();

        sc.start();
        sc.awaitTermination();
    }
}

package pl.com.sages.spark;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import pl.com.sages.spark.conf.SparkConfBuilder;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

/**
 * Simple table to tuple example.
 */
public class PeopleTuple {

    public static class PeopleJson {
        public String name;
        public String surname;
        public Integer age;
        public Integer children;
    }

    public static void main(String[] args) {
        SparkConf conf = SparkConfBuilder.buildLocal("people");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Tuple4<String, String, Integer, Integer>> rows = sc.textFile("data/people.json")
                .map(json -> {
                    Gson gson = new Gson();
                    PeopleJson peopleJson = gson.fromJson(json, PeopleJson.class);
                    return new Tuple4<String, String, Integer, Integer>(
                            peopleJson.name,
                            peopleJson.surname,
                            peopleJson.age,
                            peopleJson.children);
                });
        JavaPairRDD<String, Tuple2<Integer, Integer>> forStats =
                rows.mapToPair(r -> new Tuple2<>(r._2(), new Tuple2<>(r._3(), r._4())));
        List<Tuple2<String, Tuple2<Integer, Integer>>> result =
                forStats.reduceByKey((l,r) -> new Tuple2<>(Math.max(l._1(), r._1()), l._2()+r._2())).collect();

        result.forEach(System.out::println);
    }

}

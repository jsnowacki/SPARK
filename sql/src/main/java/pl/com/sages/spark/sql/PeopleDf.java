package pl.com.sages.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import pl.com.sages.spark.conf.SparkConfBuilder;
import scala.Tuple2;

import java.util.Arrays;

import static org.apache.spark.sql.functions.*;

/**
 * Simple Spark SQL example
 */
public class PeopleDf {
    public static void main(String[] args) {
        SparkConf conf = SparkConfBuilder.buildLocal("people-df");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        // Spark 1.3.1
        //DataFrame df = sqlContext.jsonFile("data/people.json");
        // Spark 1.4.0+
        DataFrame df = sqlContext.read().json("data/people.json");
        df.show();
        df.printSchema();

        // Select something
        df.select(df.col("surname"), df.col("age")).show();

        // Do some aggregations
        DataFrame result = df.groupBy("surname").agg(
                col("surname"),
                max("age").as("maxAge"),
                sum("children").as("sumChildren"));
        result.show();

        // Pivot example - works in Spark 1.6.0+
        //df.groupBy("surname").pivot("gender", Arrays.asList("male", "female")).avg("age").show();

        // Go back to using RDD if needed
        df.toJavaRDD().map(r -> {
            // Spark 1.4.0+
            //int iSurname = r.fieldIndex("surname");
            //int iAge = r.fieldIndex("age");
            // Spark 1.3.1
            int iSurname = 4;
            int iAge = 0;
            String surname = r.getString(iSurname);
            long age = r.getLong(iAge);
            return new Tuple2<>(surname, age);
        }).collect().forEach(System.out::println);
    }
}

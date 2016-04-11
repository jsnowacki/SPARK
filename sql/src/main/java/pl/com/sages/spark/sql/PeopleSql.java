package pl.com.sages.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import pl.com.sages.spark.conf.SparkConfBuilder;

/**
 * Simple Spark SQL example
 */
public class PeopleSql {
    public static void main(String[] args) {
        SparkConf conf = SparkConfBuilder.buildLocal("people-sql");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        // Spark 1.3.1
        //DataFrame df = sqlContext.jsonFile("data/people.json");
        // Spark 1.4.0+
        DataFrame df = sqlContext.read().json("data/people.json");
        df.registerTempTable("people");
        df.show();

        sqlContext.sql("SELECT surname, age FROM people").show();

        DataFrame result = sqlContext.sql(
                "SELECT surname, " +
                "MAX(age) AS maxAge, " +
                "SUM(children) as sumChildren " +
                "FROM people GROUP BY surname");
        result.show();

        //result.saveAsParquetFile("output/people.parquet");
    }
}

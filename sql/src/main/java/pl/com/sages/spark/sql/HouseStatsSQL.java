package pl.com.sages.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import pl.com.sages.spark.conf.SparkConfBuilder;

/**
 * House stats the SQL way
 */
public class HouseStatsSQL {

    public static void main(String[] args) {
        SparkConf conf = SparkConfBuilder.buildLocal("house-stats-sql");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        DataFrame housesDf = sqlContext.read().json("data/rollingsales_bronx.json");
        housesDf.registerTempTable("houses");
        housesDf.show();
        housesDf.printSchema();

        // The SQL way
        String query = "SELECT " +
                "houses.hood, " +
                "type, " +
                "count(*) as houseCount, " +
                "AVG(grossArea) AS avgGrossArea, " +
                "AVG(landArea) AS avgLandArea, " +
                "AVG(year) AS avgYear, " +
                "AVG(price) AS avgPrice " +
                "FROM houses " +
                "GROUP BY hood, type " +
                "ORDER BY hood, type";
        DataFrame stats = sqlContext.sql(query);
        stats.show();
    }
}

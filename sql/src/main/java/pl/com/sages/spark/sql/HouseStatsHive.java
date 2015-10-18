package pl.com.sages.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;
import pl.com.sages.spark.conf.SparkConfBuilder;

/**
 * House stats the SQL way
 */
public class HouseStatsHive {

    public static void main(String[] args) {
        SparkConf conf = SparkConfBuilder.buildLocal("house-stats-hive");
        JavaSparkContext sc = new JavaSparkContext(conf);
        HiveContext hiveContext = new HiveContext(JavaSparkContext.toSparkContext(sc));

        // Create table
        hiveContext.sql("CREATE TABLE houses_raw (lines STRING)");
        hiveContext.sql("LOAD DATA LOCAL INPATH data/rollingsales_bronx.csv OVERWRITE INTO TABLE houses_raw");
        hiveContext.sql("CREATE TABLE houses AS SELECT " +
                "regexp_extract(lines, '^(?:([^,]*)\\,?){2}', 1) hood," +
                "regexp_extract(lines, '^(?:([^,]*)\\,?){3}', 1) type," +
                "regexp_extract(lines, '^(?:([^,]*)\\,?){15}', 1) landArea," +
                "regexp_extract(lines, '^(?:([^,]*)\\,?){16}', 1) grossArea," +
                "regexp_extract(lines, '^(?:([^,]*)\\,?){17}', 1) year," +
                "regexp_extract(lines, '^(?:([^,]*)\\,?){20}', 1) price" +
                "FROM houses_raw;");
        hiveContext.sql("DROP TABLE houses_raw");

        // Do stats
        String query = "SELECT " +
                "hood, " +
                "type, " +
                "count(*) as houseCount, " +
                "AVG(grossArea) AS avgGrossArea, " +
                "AVG(landArea) AS avgLandArea, " +
                "AVG(year) AS avgYear, " +
                "AVG(price) AS avgPrice " +
                "FROM houses " +
                "GROUP BY hood, type " +
                "ORDER BY hood, type";
        DataFrame stats = hiveContext.sql(query);
        stats.show();
    }
}

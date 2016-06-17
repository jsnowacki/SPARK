package pl.com.sages.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.GroupedData;
import org.apache.spark.sql.SQLContext;
import pl.com.sages.spark.conf.SparkConfBuilder;

import java.io.Serializable;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.count;

/**
 * House stats the SQL way
 */
public class HouseStatsDf {

    public static class Houses implements Serializable {
        public static final int HOOD_COLUMN = 1;
        public static final int TYPE_COLUMN = 2;
        public static final int LAND_AREA_COLUMN = 14;
        public static final int GROSS_AREA_COLUMN = 15;
        public static final int YEAR_COLUMN = 16;
        public static final int PRICE_COLUMN = 19;

        public static final String NON_DIGIT_REGEX = "[^\\d]";
        public static final String EMPTY_STRING_REGEX = "";

        private String hood;
        private String type;
        private long year;
        private long price;
        private long grossArea;
        private long landArea;

        public static Houses fromCsv(String line) {
            String[] c = line.split(",");
            String hood = c[HOOD_COLUMN];
            String type = c[TYPE_COLUMN];
            long landArea = Long.valueOf(c[LAND_AREA_COLUMN].replaceAll(NON_DIGIT_REGEX, EMPTY_STRING_REGEX));
            long grossArea = Long.valueOf(c[GROSS_AREA_COLUMN].replaceAll(NON_DIGIT_REGEX, EMPTY_STRING_REGEX));
            long year = Long.valueOf(c[YEAR_COLUMN].replaceAll(NON_DIGIT_REGEX, EMPTY_STRING_REGEX));
            long price = Long.valueOf(c[PRICE_COLUMN].replaceAll(NON_DIGIT_REGEX, EMPTY_STRING_REGEX));

            return new Houses(hood, type, year, price, grossArea, landArea);
        }

        public Houses(String hood, String type, long year, long price, long grossArea, long landArea) {
            this.hood = hood;
            this.type = type;
            this.year = year;
            this.price = price;
            this.grossArea = grossArea;
            this.landArea = landArea;
        }

        public String getHood() {
            return hood;
        }

        public void setHood(String hood) {
            this.hood = hood;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public long getYear() {
            return year;
        }

        public void setYear(long year) {
            this.year = year;
        }

        public long getPrice() {
            return price;
        }

        public void setPrice(long price) {
            this.price = price;
        }

        public long getGrossArea() {
            return grossArea;
        }

        public void setGrossArea(long grossArea) {
            this.grossArea = grossArea;
        }

        public long getLandArea() {
            return landArea;
        }

        public void setLandArea(long landArea) {
            this.landArea = landArea;
        }
    }

    public static void main(String[] args) {
        SparkConf conf = SparkConfBuilder.buildLocal("house-stats-df");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        JavaRDD<Houses> houses = sc.textFile("data/rollingsales_bronx.csv")
                .map(line -> Houses.fromCsv(line));

        DataFrame housesDf = sqlContext.createDataFrame(houses, Houses.class);
        housesDf.registerTempTable("houses");
        housesDf.show();
//        housesDf.toJSON().saveAsTextFile("data/rollingsales_bronx.json");

        // The SQL way
//        String query = "SELECT " +
//                "hood, " +
//                "type, " +
//                "count(*) as houseCount, " +
//                "AVG(grossArea) AS avgGrossArea, " +
//                "AVG(landArea) AS avgLandArea, " +
//                "AVG(year) AS avgYear, " +
//                "AVG(price) AS avgPrice " +
//                "FROM houses " +
//                "GROUP BY hood, type " +
//                "ORDER BY hood, type";
//        DataFrame stats = sqlContext.sql(query);
//        stats.show();

        // The Data Frame way
        GroupedData groups = housesDf.groupBy("hood", "type");
          // Variant 1 - not recommended in this case, many operations
//        DataFrame avgs = groups.avg();
//        avgs.show();
//        DataFrame counts = groups.count();
//        counts.show();
//        DataFrame stats = counts.join(avgs);
//        //DataFrame stats = counts.join(avgs, counts.col("hood").equalTo(avgs.col("hood")).and(counts.col("type").equalTo(avgs.col("type"))));
//        stats.show();
        // Variant 2 - a better way
        DataFrame stats = groups.agg(
                count("*").as("houseCount"),
                avg("grossArea").as("avgGrossArea"),
                avg("landArea").as("avgLandArea"),
                avg("price").as("avgPrice"),
                avg("year").as("avgYear"))
                .orderBy("hood", "type");
        stats.show();

    }
}

package pl.com.sages.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import pl.com.sages.spark.conf.SparkConfBuilder;
import scala.Tuple2;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Word Count Java 8
 */

public class HouseStats {
    public static final int HOOD_COLUMN = 1;
    public static final int TYPE_COLUMN = 2;
    public static final int LAND_AREA_COLUMN = 14;
    public static final int GROSS_AREA_COLUMN = 15;
    public static final int YEAR_COLUMN = 16;
    public static final int PRICE_COLUMN = 19;

    public static class HouseKey implements Serializable, Comparable {
        private String hood;
        private String type;

        public String getHood() {
            return hood;
        }

        public String getType() {
            return type;
        }

        public HouseKey(String hood, String type) {
            this.hood = hood;
            this.type = type;
        }

        @Override
        public int compareTo(Object o) {
            HouseKey that = (HouseKey) o;
            int res = this.hood.compareTo(that.getHood());
            if (res == 0) {
                return this.type.compareTo(that.getType());
            }
            return res;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null) return false;
            if (getClass() != o.getClass()) return false;
            HouseKey that = (HouseKey) o;
            return (this.hood.equals(that.getHood())) && (this.type.equals(that.getType()));
        }

        @Override
        public int hashCode() {
            return hood.hashCode() ^ type.hashCode();
        }

        @Override
        public String toString() {
            return String.format("%s, %s", hood, type);

        }
    }

    public static class HouseValue implements Serializable{
        public static final String NON_DIGIT_REGEX = "[^\\d]";
        public static final String EMPTY_STRING_REGEX = "";

        private long count;
        private long year;
        private long price;
        private long grossArea;
        private long landArea;

        public HouseValue(long count, String landArea, String grossArea, String year, String price){
            this.count = count;
            this.landArea = Long.valueOf(landArea.replaceAll(NON_DIGIT_REGEX, EMPTY_STRING_REGEX));
            this.grossArea = Long.valueOf(grossArea.replaceAll(NON_DIGIT_REGEX, EMPTY_STRING_REGEX));
            this.year = Long.valueOf(year.replaceAll(NON_DIGIT_REGEX, EMPTY_STRING_REGEX));
            this.price = Long.valueOf(price.replaceAll(NON_DIGIT_REGEX, EMPTY_STRING_REGEX));
        }
        public HouseValue(long count, long landArea, long grossArea, long year, long price){
            this.count = count;
            this.landArea = landArea;
            this.grossArea = grossArea;
            this.year = year;
            this.price = price;
        }

        public long getCount() {
            return count;
        }

        public long getYear() {
            return year;
        }

        public long getPrice() {
            return price;
        }

        public long getGrossArea() {
            return grossArea;
        }

        public long getLandArea() {
            return landArea;
        }

        public HouseValue add(HouseValue value) {
            count += value.getCount();
            landArea += value.getLandArea();
            grossArea += value.getGrossArea();
            year += value.getYear();
            price += value.getPrice();

            return this;
        }

        public HouseValue average() {
            landArea /= count;
            grossArea /= count;
            year /= count;
            price /= count;

            return this;
        }

        @Override
        public String toString() {
            return String.format("%d, %d, %d, %d, %d", count, landArea, grossArea, year, price);
        }
    }

    public static void main(String[] args) {
        SparkConf conf = SparkConfBuilder.buildLocal("house-stats");

        JavaSparkContext sc = new JavaSparkContext(conf);

        sc.textFile(args[0], 1);
        JavaRDD<String> file = sc.textFile(args[0], 1);
        JavaRDD<String[]> cols = file.map(line -> line.split(",")).cache();
        JavaPairRDD<HouseKey, HouseValue> houses = cols.mapToPair(c -> {
            HouseKey key = new HouseKey(c[HOOD_COLUMN], c[TYPE_COLUMN]);
            HouseValue value = new HouseValue(1, c[LAND_AREA_COLUMN], c[GROSS_AREA_COLUMN],
                    c[YEAR_COLUMN], c[PRICE_COLUMN]);
            return new Tuple2<>(key, value);
        });

        JavaPairRDD<HouseKey, HouseValue> stats =
                houses.reduceByKey((a, b) -> a.add(b))
                        .mapValues(v -> v.average())
                        .sortByKey();
        stats.saveAsTextFile(args[1]);
        //counts.coalesce(1).saveAsTextFile(args[1]);
    }
}

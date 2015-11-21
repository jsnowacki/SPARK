package pl.com.sages.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import pl.com.sages.spark.conf.SparkConfBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Simple Spark SQL example
 */
public class MovieLensDf {
    public static boolean toBool(String s) {
        return !Objects.equals(s, "0");
    }

    public static void main(String[] args) {
        SparkConf conf = SparkConfBuilder.buildLocal("movie-lense-df");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        // user id | item id | rating | timestam
        JavaRDD<Row> dataRdd = sc.textFile("data/ml-100k/u.data")
                .map(line -> {
                    String[] col = line.split("\t");
                    return RowFactory.create(
                            Integer.valueOf(col[0]),
                            Integer.valueOf(col[1]),
                            Integer.valueOf(col[2]),
                            Long.valueOf(col[3])
                    );
                });

        List<StructField> dataFields = new ArrayList<>();
        dataFields.add(DataTypes.createStructField("userId", DataTypes.IntegerType, false));
        dataFields.add(DataTypes.createStructField("itemId", DataTypes.IntegerType, true));
        dataFields.add(DataTypes.createStructField("rating", DataTypes.IntegerType, true));
        dataFields.add(DataTypes.createStructField("timestamp", DataTypes.LongType, true));
        StructType dataSchema = DataTypes.createStructType(dataFields);

        DataFrame data = sqlContext.createDataFrame(dataRdd, dataSchema);

        data.show();

        //movie id | movie title | release date | video release date |
        //        IMDb URL | unknown | Action | Adventure | Animation |
        //        Children's | Comedy | Crime | Documentary | Drama | Fantasy |
        //Film-Noir | Horror | Musical | Mystery | Romance | Sci-Fi |
        //        Thriller | War | Western |
        JavaRDD<Row> itemRdd = sc.textFile("data/ml-100k/u.item")
                .map(line -> {
                        String[] col = line.split("\\|");
                        return RowFactory.create(
                            Integer.valueOf(col[0]),
                            col[1],
                            col[2],
                            col[3],
                            col[4],
                            toBool(col[5]),
                            toBool(col[6]),
                            toBool(col[7]),
                            toBool(col[8]),
                            toBool(col[9]),
                            toBool(col[10]),
                            toBool(col[11]),
                            toBool(col[12]),
                            toBool(col[13]),
                            toBool(col[14]),
                            toBool(col[15]),
                            toBool(col[16]),
                            toBool(col[17]),
                            toBool(col[18]),
                            toBool(col[19]),
                            toBool(col[20]),
                            toBool(col[21]),
                            toBool(col[22]),
                            toBool(col[23])
                    );
                });

        List<StructField> itemFields = new ArrayList<>();
        itemFields.add(DataTypes.createStructField("movieId", DataTypes.IntegerType, false));
        itemFields.add(DataTypes.createStructField("movieTitle", DataTypes.StringType, true));
        itemFields.add(DataTypes.createStructField("releaseDate", DataTypes.StringType, true));
        itemFields.add(DataTypes.createStructField("videoReleaseDate", DataTypes.StringType, true));
        itemFields.add(DataTypes.createStructField("imdbUrl", DataTypes.StringType, true));
        itemFields.add(DataTypes.createStructField("unknown", DataTypes.BooleanType, true));
        itemFields.add(DataTypes.createStructField("action", DataTypes.BooleanType, true));
        itemFields.add(DataTypes.createStructField("adventure", DataTypes.BooleanType, true));
        itemFields.add(DataTypes.createStructField("animation", DataTypes.BooleanType, true));
        itemFields.add(DataTypes.createStructField("childrens", DataTypes.BooleanType, true));
        itemFields.add(DataTypes.createStructField("comedy", DataTypes.BooleanType, true));
        itemFields.add(DataTypes.createStructField("crime", DataTypes.BooleanType, true));
        itemFields.add(DataTypes.createStructField("documentary", DataTypes.BooleanType, true));
        itemFields.add(DataTypes.createStructField("drama", DataTypes.BooleanType, true));
        itemFields.add(DataTypes.createStructField("fantasy", DataTypes.BooleanType, true));
        itemFields.add(DataTypes.createStructField("filmNoir", DataTypes.BooleanType, true));
        itemFields.add(DataTypes.createStructField("horror", DataTypes.BooleanType, true));
        itemFields.add(DataTypes.createStructField("musical", DataTypes.BooleanType, true));
        itemFields.add(DataTypes.createStructField("mystery", DataTypes.BooleanType, true));
        itemFields.add(DataTypes.createStructField("romance", DataTypes.BooleanType, true));
        itemFields.add(DataTypes.createStructField("sciFi", DataTypes.BooleanType, true));
        itemFields.add(DataTypes.createStructField("thriller", DataTypes.BooleanType, true));
        itemFields.add(DataTypes.createStructField("war", DataTypes.BooleanType, true));
        itemFields.add(DataTypes.createStructField("western", DataTypes.BooleanType, true));
        StructType itemSchema = DataTypes.createStructType(itemFields);

        DataFrame item = sqlContext.createDataFrame(itemRdd, itemSchema);

        item.show();
        // user id | age | gender | occupation | zip code
        JavaRDD<Row> userRdd = sc.textFile("data/ml-100k/u.user")
                .map(line -> {
                    String[] col = line.split("\\|");
                    return RowFactory.create(
                        Integer.valueOf(col[0]),
                        Integer.valueOf(col[1]),
                        col[2],
                        col[3],
                        col[4]
                    );
                });

        List<StructField> userFields = new ArrayList<>();
        userFields.add(DataTypes.createStructField("userId", DataTypes.IntegerType, false));
        userFields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
        userFields.add(DataTypes.createStructField("gender", DataTypes.StringType, true));
        userFields.add(DataTypes.createStructField("occupation", DataTypes.StringType, true));
        userFields.add(DataTypes.createStructField("zipCode", DataTypes.StringType, true));
        StructType userSchema = DataTypes.createStructType(userFields);

        DataFrame user = sqlContext.createDataFrame(userRdd, userSchema);

        user.show();
    }
}

package pl.com.sages.spark.conf;

import org.apache.spark.SparkConf;

/**
 * SparkConf builder
 */
public class SparkConfBuilder {
    public static SparkConf buildLocal(String appName) {
        return new SparkConf()
                .setMaster("local[2]")
                .setAppName(appName);
    }
}

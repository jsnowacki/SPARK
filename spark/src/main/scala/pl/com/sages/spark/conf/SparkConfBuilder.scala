package pl.com.sages.spark.conf

import org.apache.spark.SparkConf

/**
 * SparkConf builder
 */
object SparkConfBuilder {
  /*
  Local conf builder
   */
  def buildLocal(appName:String):SparkConf = {
    val sparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName(appName)
    sparkConf
  }
}

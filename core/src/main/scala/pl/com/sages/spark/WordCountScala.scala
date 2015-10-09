package pl.com.sages.spark

import org.apache.spark.SparkContext
import pl.com.sages.spark.conf.SparkConfBuilder

/**
 * Word Count
 */
object WordCountScala {

  def main(args : Array[String]): Unit = {
    val conf = SparkConfBuilder.buildLocal("word-count")

    val sc = new SparkContext(conf)

    val file = sc.textFile(args(0))

    val counts = file.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
    //counts.saveAsTextFile(args(1))
    counts.coalesce(1).saveAsTextFile(args(1))
  }

}

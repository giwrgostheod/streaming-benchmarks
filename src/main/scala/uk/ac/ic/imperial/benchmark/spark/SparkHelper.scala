package uk.ac.ic.imperial.benchmark.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object SparkHelper {
  def getAndConfigureSparkSession() = {
    val conf = new SparkConf()
      .setAppName("Structured Streaming benchmark")
      .setMaster("local[1]")
//      .setMaster("spark://doc-cdt12.lib.ic.ac.uk:7077")
//      .set("spark.cassandra.connection.host", "127.0.0.1")
//      .set("spark.sql.streaming.checkpointLocation", "checkpoint")

    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    SparkSession
      .builder()
      .getOrCreate()
  }

  def getSparkSession() = {
    SparkSession
      .builder()
      .getOrCreate()
  }
}
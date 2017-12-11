package uk.ac.ic.imperial

import uk.ac.ic.imperial.benchmark.spark.{LocalKafka, SparkHelper, SparkYahooRunner}
import uk.ac.ic.imperial.benchmark.yahoo.YahooBenchmark

/**
  * Streaming benchmark Main entry point
  *
  * Goals of the benchmark
  *   * Explore different streaming systems
  *   * Discover bottlenecks and potential solutions
  *   * Explore system scalability
  *
  */
object SparkMainBench {
  // Benchmark Configurations - Ideal for Community Edititon
  val stopSparkOnKafkaNodes = false
  val stopSparkOnFlinkNodes = false
  val numKafkaNodes = 1
  val numFlinkNodes = 1
  val flinkTaskSlots = 1 // Number of CPUs available for a taskmanager.
  val runDurationMillis = 100000 // How long to keep each stream running
  val numTrials = 3
  val benchmarkResultsBase = "./streaming/benchmarks" // Where to store the results of the benchmark

  //////////////////////////////////
  // Event Generation
  //////////////////////////////////
  val recordsPerSecond = 5000000
  val rampUpTimeSeconds = 10 // Ramps up event generation to the specified rate for the given duration to allow the JVM to warm up
  val recordGenerationParallelism = 1 // Parallelism within Spark to generate data for the Kafka Streams benchmark

  val numCampaigns = 100 // The number of campaigns to generate events for. Configures the cardinality of the state that needs to be updated

  val kafkaEventsTopicPartitions = 1 // Number of partitions within Kafka for the `events` stream
  val kafkaOutputTopicPartitions = 1 // Number of partitions within Kafka for the `outout` stream. We write data out to Kafka instead of Redis

  //////////////////////////////////
  // Kafka Streams
  //////////////////////////////////
  // Total number of Kafka Streams applications that will be running.
  val kafkaStreamsNumExecutors = 1
  // Number of threads to use within each Kafka Streams application.
  val kafkaStreamsNumThreadsPerExecutor = 1

  //////////////////////////////////
  // Flink
  //////////////////////////////////
  // Parallelism to use in Flink. Needs to be <= numFlinkNodes * flinkTaskSlots
  val flinkParallelism = 1
  // We can have Flink emit updates for windows more frequently to reduce latency by sacrificing throughput. Setting this to 0 means that
  // we emit updates for windows according to the watermarks, i.e. we will emit the result of a window, once the watermark passes the end
  // of the window.
  val flinkTriggerIntervalMillis = 0
  // How often to log the throughput of Flink in #records. Setting this lower will give us finer grained results, but will sacrifice throughput
  val flinkThroughputLoggingFreq = 100000

  //////////////////////////////////
  // Spark
  //////////////////////////////////
  val sparkParallelism = 8

  /////////////////////////////////////////////////////////////////////////////////////////////////
  /////////////////////////////////////////////////////////////////////////////////////////////////
  /////////////////////////////////////////////////////////////////////////////////////////////////

  def main(args: Array[String]): Unit = {

    val spark = SparkHelper.getAndConfigureSparkSession()
    val kafkaCluster = LocalKafka.setup(spark, stopSparkOnKafkaNodes = stopSparkOnKafkaNodes, numKafkaNodes = numKafkaNodes)

    val benchmark = new YahooBenchmark(
      spark,
      kafkaCluster,
      tuplesPerSecond = recordsPerSecond,
      recordGenParallelism = recordGenerationParallelism,
      rampUpTimeSeconds = rampUpTimeSeconds,
      kafkaEventsTopicPartitions = kafkaEventsTopicPartitions,
      kafkaOutputTopicPartitions = kafkaOutputTopicPartitions,
      numCampaigns = numCampaigns,
      readerWaitTimeMs = runDurationMillis)


    val sparkRunner = new SparkYahooRunner(
      spark,
      kafkaCluster = kafkaCluster,
      parallelism = sparkParallelism)

    benchmark.run(sparkRunner, s"$benchmarkResultsBase/spark", numRuns = numTrials)

    kafkaCluster.stopAll()
  }
}
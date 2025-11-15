/*
 * Kafka Share Groups in Spark Streaming - Examples and Usage Guide
 * ==================================================================
 *
 * This file demonstrates how to use Kafka Share Groups (KIP-932) with Spark Structured Streaming.
 *
 * Share groups enable multiple Spark executors to read from the same Kafka partitions concurrently,
 * with record-level locking and explicit acknowledgment.
 *
 * Benefits:
 * - Parallelism independent of partition count
 * - Dynamic load balancing across executors
 * - Record-level retry with ACCEPT/RELEASE/REJECT
 * - Built-in failure handling
 */

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object ShareGroupKafkaExamples {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("KafkaShareGroupExample")
      .master("local[4]")
      .getOrCreate()

    // Example 1: Basic Share Group Usage
    basicShareGroupExample(spark)

    // Example 2: Share Group with Custom Configuration
    customConfigExample(spark)

    // Example 3: Share Group with Multiple Queries
    multipleQueriesExample(spark)

    // Example 4: Comparison with Traditional Consumer Groups
    comparisonExample(spark)
  }

  /**
   * Example 1: Basic Share Group Usage
   *
   * Simply enable share groups with a flag - executors will automatically
   * use KafkaShareConsumer with explicit acknowledgment.
   */
  def basicShareGroupExample(spark: SparkSession): Unit = {
    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "my-topic")
      // **KEY**: Enable share groups
      .option("kafka.share.group.enable", "true")
      .load()

    val query = df.selectExpr("CAST(value AS STRING)")
      .writeStream
      .format("console")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    query.awaitTermination()
  }

  /**
   * Example 2: Custom Configuration
   *
   * Configure share group behavior, lock duration, and failure handling.
   */
  def customConfigExample(spark: SparkSession): Unit = {
    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "high-volume-topic")

      // Enable share groups
      .option("kafka.share.group.enable", "true")

      // Share group ID (optional - auto-generated if not provided)
      .option("kafka.share.group.id", "my-application-share-group")

      // Record lock duration (time executors have to process before timeout)
      // Recommendation: 2-3x average processing time per record
      .option("group.share.record.lock.duration.ms", "60000") // 60 seconds

      // Max delivery attempts before archiving/DLQ
      .option("group.share.delivery.count.limit", "5")

      // Max records locked per partition (backpressure)
      .option("group.share.partition.max.record.locks", "5000")

      // Failure strategy: RELEASE (retry), REJECT (DLQ), ACCEPT_ON_TASK_FAILURE
      .option("kafka.share.failure.strategy", "RELEASE")

      // Acknowledgment timeout
      .option("kafka.share.acknowledgement.commit.timeout.ms", "30000")

      // Max acknowledgment retry attempts
      .option("kafka.share.max.acknowledgement.retries", "3")

      // Number of parallel executors (not tied to partition count)
      .option("numPartitions", "10")  // 10 executors reading in parallel

      .load()

    val query = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .writeStream
      .format("console")
      .outputMode("append")
      .start()

    query.awaitTermination()
  }

  /**
   * Example 3: Multiple Queries with Same Share Group
   *
   * Multiple streaming queries can use the SAME share group ID to
   * cooperatively consume records. This is useful for scale-out scenarios.
   */
  def multipleQueriesExample(spark: SparkSession): Unit = {
    val shareGroupId = "cooperative-processing-group"

    // Query 1: Process and store to database
    val query1 = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "events-topic")
      .option("kafka.share.group.enable", "true")
      .option("kafka.share.group.id", shareGroupId)
      .load()
      .selectExpr("CAST(value AS STRING) as json")
      .writeStream
      .format("parquet")
      .option("path", "/data/events")
      .option("checkpointLocation", "/checkpoints/query1")
      .start()

    // Query 2: Process and send to analytics (SAME share group)
    val query2 = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "events-topic")
      .option("kafka.share.group.enable", "true")
      .option("kafka.share.group.id", shareGroupId)  // Same group!
      .load()
      .selectExpr("CAST(value AS STRING) as json")
      .writeStream
      .format("console")
      .option("checkpointLocation", "/checkpoints/query2")
      .start()

    // Both queries cooperatively consume - no duplicates across queries
    query1.awaitTermination()
    query2.awaitTermination()
  }

  /**
   * Example 4: Comparison with Traditional Consumer Groups
   */
  def comparisonExample(spark: SparkSession): Unit = {
    println("""
      |==========================================
      | Traditional Consumer Groups vs Share Groups
      |==========================================
      |
      | Traditional (kafka.share.group.enable = false):
      | - Max parallelism = number of partitions
      | - If topic has 5 partitions, max 5 executors can read
      | - Each partition → one executor mapping
      | - Offset-based tracking
      |
      | Share Groups (kafka.share.group.enable = true):
      | - Parallelism independent of partition count
      | - 100 executors can read from 5 partitions
      | - Multiple executors read same partition concurrently
      | - Record-level locking and acknowledgment
      | - Better load balancing with skewed partitions
      |
      |==========================================
      """.stripMargin)

    // Traditional: Limited by partition count
    val traditionalDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "my-topic")  // Has 5 partitions
      // kafka.share.group.enable = false (default)
      .load()
    // Max effective parallelism: 5 (one per partition)

    // Share Group: Parallelism configurable
    val shareGroupDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "my-topic")  // Same topic with 5 partitions
      .option("kafka.share.group.enable", "true")
      .option("numPartitions", "20")  // 20 executors!
      .load()
    // Effective parallelism: 20 (all reading concurrently)
  }

  /**
   * Example 5: Production Configuration with Error Handling
   */
  def productionExample(spark: SparkSession): Unit = {
    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "broker1:9092,broker2:9092,broker3:9092")
      .option("subscribe", "production-events")

      // Share group configuration
      .option("kafka.share.group.enable", "true")
      .option("kafka.share.group.id", "production-app-v1")

      // Lock duration: 2 minutes (adjust based on processing time)
      .option("group.share.record.lock.duration.ms", "120000")

      // Delivery limit before DLQ
      .option("group.share.delivery.count.limit", "3")

      // Backpressure
      .option("group.share.partition.max.record.locks", "10000")

      // On executor failure, RELEASE records for retry
      .option("kafka.share.failure.strategy", "RELEASE")

      // Parallelism
      .option("numPartitions", "50")

      // Kafka consumer settings
      .option("kafka.session.timeout.ms", "30000")
      .option("kafka.heartbeat.interval.ms", "10000")
      .option("kafka.max.poll.interval.ms", "300000")  // 5 minutes
      .option("kafka.max.poll.records", "500")

      .load()

    import spark.implicits._

    val processedDF = df
      .selectExpr("CAST(value AS STRING) as json")
      .as[String]
      .map { json =>
        // Your processing logic here
        // Records are automatically acknowledged on success
        // On exception, records are released based on failure strategy
        processEvent(json)
      }

    val query = processedDF.writeStream
      .format("parquet")
      .option("path", "/data/processed-events")
      .option("checkpointLocation", "/checkpoints/production-app")
      .trigger(Trigger.ProcessingTime("30 seconds"))
      .start()

    query.awaitTermination()
  }

  def processEvent(json: String): String = {
    // Placeholder for actual processing
    json.toUpperCase
  }
}

/**
 * PySpark Example
 * ================
 */
object PySparkExample {
  val pysparkCode =
    """
    |from pyspark.sql import SparkSession
    |
    |spark = SparkSession.builder \\
    |    .appName("KafkaShareGroupPySpark") \\
    |    .getOrCreate()
    |
    |# Enable share groups with simple flag
    |df = spark.readStream \\
    |    .format("kafka") \\
    |    .option("kafka.bootstrap.servers", "localhost:9092") \\
    |    .option("subscribe", "my-topic") \\
    |    .option("kafka.share.group.enable", "true") \\
    |    .option("numPartitions", "10") \\
    |    .load()
    |
    |query = df.selectExpr("CAST(value AS STRING)") \\
    |    .writeStream \\
    |    .format("console") \\
    |    .outputMode("append") \\
    |    .start()
    |
    |query.awaitTermination()
    """.stripMargin
}

/**
 * Configuration Reference
 * =======================
 */
object ConfigurationReference {
  val reference =
    """
    |==============================================================================
    | Kafka Share Group Configuration Reference
    |==============================================================================
    |
    | Spark-Level Configuration:
    | --------------------------
    | kafka.share.group.enable (boolean, default: false)
    |   - Enable share group mode
    |
    | kafka.share.group.id (string, optional)
    |   - Share group ID (auto-generated if not provided)
    |
    | kafka.share.failure.strategy (string, default: RELEASE)
    |   - RELEASE: Retry on failure (transient errors)
    |   - REJECT: Send to DLQ (permanent errors)
    |   - ACCEPT_ON_TASK_FAILURE: Accept and skip (at-most-once)
    |
    | kafka.share.acknowledgement.commit.timeout.ms (long, default: 30000)
    |   - Timeout for acknowledgment commits
    |
    | kafka.share.max.acknowledgement.retries (int, default: 3)
    |   - Max retry attempts for acknowledgments
    |
    | numPartitions (int, default: number of cores)
    |   - Number of parallel executors (independent of Kafka partitions)
    |
    |
    | Kafka Share Group Configuration:
    | ---------------------------------
    | group.share.record.lock.duration.ms (int, default: 30000)
    |   - Record lock duration (milliseconds)
    |   - Recommendation: 2-3x average record processing time
    |
    | group.share.delivery.count.limit (int, default: 5)
    |   - Max delivery attempts before archiving
    |
    | group.share.partition.max.record.locks (int, default: 2000)
    |   - Max concurrent locks per partition (backpressure)
    |
    | group.share.isolation.level (string, default: read_committed)
    |   - read_committed or read_uncommitted
    |
    |==============================================================================
    """.stripMargin
}

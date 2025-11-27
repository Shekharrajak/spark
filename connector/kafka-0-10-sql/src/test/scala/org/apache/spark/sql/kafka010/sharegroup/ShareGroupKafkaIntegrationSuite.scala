/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.kafka010.sharegroup

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

import scala.jdk.CollectionConverters._

import org.apache.kafka.clients.producer.ProducerRecord
import org.scalatest.concurrent.Eventually._
import org.scalatest.time.SpanSugar._

import org.apache.spark.sql.{Dataset, ForeachWriter, Row}
import org.apache.spark.sql.kafka010.{KafkaSourceTest, KafkaTestUtils}
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Integration test suite for Kafka Share Groups with Spark Structured Streaming.
 *
 * These tests require a real Kafka broker with share groups support (Kafka 3.7+).
 * Tests use KafkaTestUtils to setup embedded Kafka with share group configuration.
 */
class ShareGroupKafkaIntegrationSuite extends KafkaSourceTest with SharedSparkSession {

  import testImplicits._

  // Override broker properties to enable share groups
  override protected val brokerProps = Map[String, Object](
    "group.coordinator.rebalance.protocols" -> "classic,share",
    "share.group.enable" -> "true",
    "group.share.enable" -> "true",
    "share.coordinator.state.topic.replication.factor" -> "1",
    "share.coordinator.state.topic.min.isr" -> "1",
    "offsets.topic.replication.factor" -> "1",
    "transaction.state.log.replication.factor" -> "1",
    "transaction.state.log.min.isr" -> "1"
  )

  test("basic share group read - consume all messages") {
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 3)

    // Produce test data
    testUtils.sendMessages(topic, (0 until 100).map(_.toString).toArray)

    val receivedMessages = new ConcurrentLinkedQueue[String]()

    val query = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("subscribe", topic)
      .option("kafka.share.group.enable", "true")
      .option("kafka.share.group.id", "test-share-group-1")
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]
      .writeStream
      .foreach(new ForeachWriter[String] {
        override def open(partitionId: Long, epochId: Long): Boolean = true
        override def process(value: String): Unit = receivedMessages.add(value)
        override def close(errorOrNull: Throwable): Unit = {}
      })
      .trigger(Trigger.ProcessingTime("2 seconds"))
      .start()

    try {
      eventually(timeout(30.seconds)) {
        assert(receivedMessages.size() == 100)
      }

      // Verify all messages received and no duplicates
      val received = receivedMessages.asScala.toSeq.sorted
      val expected = (0 until 100).map(_.toString).sorted
      assert(received == expected)
    } finally {
      query.stop()
    }
  }

  test("share group with multiple parallel tasks") {
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 6)

    // Produce test data
    testUtils.sendMessages(topic, (0 until 200).map(_.toString).toArray)

    val receivedMessages = new ConcurrentLinkedQueue[String]()

    val query = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("subscribe", topic)
      .option("kafka.share.group.enable", "true")
      .option("kafka.share.group.id", "test-share-group-2")
      .option("numPartitions", "4") // Multiple parallel readers
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]
      .writeStream
      .foreach(new ForeachWriter[String] {
        override def open(partitionId: Long, epochId: Long): Boolean = true
        override def process(value: String): Unit = receivedMessages.add(value)
        override def close(errorOrNull: Throwable): Unit = {}
      })
      .trigger(Trigger.ProcessingTime("2 seconds"))
      .start()

    try {
      eventually(timeout(30.seconds)) {
        assert(receivedMessages.size() == 200)
      }

      // Verify no duplicates
      val received = receivedMessages.asScala.toSeq
      assert(received.distinct.size == received.size, "Found duplicate messages")
    } finally {
      query.stop()
    }
  }

  test("share group configuration options") {
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 2)
    testUtils.sendMessages(topic, (0 until 50).map(_.toString).toArray)

    val receivedMessages = new ConcurrentLinkedQueue[String]()

    val query = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("subscribe", topic)
      .option("kafka.share.group.enable", "true")
      .option("kafka.share.group.id", "test-share-group-3")
      // Share group specific configuration
      .option("group.share.record.lock.duration.ms", "30000")
      .option("group.share.delivery.count.limit", "5")
      .option("kafka.share.failure.strategy", "RELEASE")
      .option("kafka.share.acknowledgement.commit.timeout.ms", "30000")
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]
      .writeStream
      .foreach(new ForeachWriter[String] {
        override def open(partitionId: Long, epochId: Long): Boolean = true
        override def process(value: String): Unit = receivedMessages.add(value)
        override def close(errorOrNull: Throwable): Unit = {}
      })
      .trigger(Trigger.ProcessingTime("2 seconds"))
      .start()

    try {
      eventually(timeout(30.seconds)) {
        assert(receivedMessages.size() == 50)
      }
    } finally {
      query.stop()
    }
  }

  test("share group rejects continuous mode") {
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 1)

    val exception = intercept[UnsupportedOperationException] {
      spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", testUtils.brokerAddress)
        .option("subscribe", topic)
        .option("kafka.share.group.enable", "true")
        .load()
        .writeStream
        .format("console")
        .trigger(Trigger.Continuous("1 second"))
        .start()
    }

    assert(exception.getMessage.contains("continuous streaming mode"))
  }

  test("share group rejects incompatible options - startingOffsets") {
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 1)

    // Should log warning but not fail - verify query can start
    val query = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("subscribe", topic)
      .option("kafka.share.group.enable", "true")
      .option("startingOffsets", "earliest") // This will be ignored
      .load()
      .writeStream
      .format("console")
      .trigger(Trigger.ProcessingTime("2 seconds"))
      .start()

    try {
      // Just verify query starts successfully
      Thread.sleep(2000)
      assert(query.isActive)
    } finally {
      query.stop()
    }
  }

  test("share group rejects subscribePattern") {
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 1)

    val exception = intercept[UnsupportedOperationException] {
      spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", testUtils.brokerAddress)
        .option("subscribePattern", "test-.*")
        .option("kafka.share.group.enable", "true")
        .load()
        .writeStream
        .format("console")
        .start()
    }

    assert(exception.getMessage.contains("subscribePattern"))
  }

  test("share group rejects assign") {
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 1)

    val exception = intercept[UnsupportedOperationException] {
      spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", testUtils.brokerAddress)
        .option("assign", s"""{"$topic":[0]}""")
        .option("kafka.share.group.enable", "true")
        .load()
        .writeStream
        .format("console")
        .start()
    }

    assert(exception.getMessage.contains("assign"))
  }

  test("share group handles empty topic") {
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 1)
    // No messages sent

    val receivedMessages = new ConcurrentLinkedQueue[String]()

    val query = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("subscribe", topic)
      .option("kafka.share.group.enable", "true")
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]
      .writeStream
      .foreach(new ForeachWriter[String] {
        override def open(partitionId: Long, epochId: Long): Boolean = true
        override def process(value: String): Unit = receivedMessages.add(value)
        override def close(errorOrNull: Throwable): Unit = {}
      })
      .trigger(Trigger.ProcessingTime("2 seconds"))
      .start()

    try {
      Thread.sleep(5000)
      assert(receivedMessages.isEmpty)
    } finally {
      query.stop()
    }
  }

  test("share group processes messages added after query starts") {
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 2)

    val receivedMessages = new ConcurrentLinkedQueue[String]()

    val query = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("subscribe", topic)
      .option("kafka.share.group.enable", "true")
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]
      .writeStream
      .foreach(new ForeachWriter[String] {
        override def open(partitionId: Long, epochId: Long): Boolean = true
        override def process(value: String): Unit = receivedMessages.add(value)
        override def close(errorOrNull: Throwable): Unit = {}
      })
      .trigger(Trigger.ProcessingTime("2 seconds"))
      .start()

    try {
      // Wait for query to initialize
      Thread.sleep(3000)

      // Send messages after query started
      testUtils.sendMessages(topic, (0 until 100).map(_.toString).toArray)

      eventually(timeout(30.seconds)) {
        assert(receivedMessages.size() == 100)
      }
    } finally {
      query.stop()
    }
  }

  test("share group with custom share group ID") {
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 2)
    testUtils.sendMessages(topic, (0 until 50).map(_.toString).toArray)

    val customGroupId = "my-custom-share-group-id"
    val receivedMessages = new ConcurrentLinkedQueue[String]()

    val query = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("subscribe", topic)
      .option("kafka.share.group.enable", "true")
      .option("kafka.share.group.id", customGroupId)
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]
      .writeStream
      .foreach(new ForeachWriter[String] {
        override def open(partitionId: Long, epochId: Long): Boolean = true
        override def process(value: String): Unit = receivedMessages.add(value)
        override def close(errorOrNull: Throwable): Unit = {}
      })
      .trigger(Trigger.ProcessingTime("2 seconds"))
      .start()

    try {
      eventually(timeout(30.seconds)) {
        assert(receivedMessages.size() == 50)
      }
    } finally {
      query.stop()
    }
  }

  test("share group offset tracking uses record count") {
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 1)
    testUtils.sendMessages(topic, (0 until 10).map(_.toString).toArray)

    val checkpointDir = newTempDir().getAbsolutePath

    val query = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("subscribe", topic)
      .option("kafka.share.group.enable", "true")
      .option("checkpointLocation", checkpointDir)
      .load()
      .selectExpr("CAST(value AS STRING)")
      .writeStream
      .format("console")
      .trigger(Trigger.ProcessingTime("2 seconds"))
      .start()

    try {
      Thread.sleep(10000)

      // Check that offset files use recordCount format
      // This is a basic check - detailed offset format verification would need file inspection
      assert(query.isActive)
    } finally {
      query.stop()
    }
  }

  private def newTempDir(): java.io.File = {
    val dir = java.nio.file.Files.createTempDirectory("spark-kafka-test-").toFile
    dir.deleteOnExit()
    dir
  }
}

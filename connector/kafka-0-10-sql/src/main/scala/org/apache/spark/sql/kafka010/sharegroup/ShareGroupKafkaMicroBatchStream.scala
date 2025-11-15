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

import java.{util => ju}

import scala.jdk.CollectionConverters._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset, ReadLimit}
import org.apache.spark.sql.kafka010.sharegroup.ShareGroupKafkaConfig.FailureStrategy
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * A [[MicroBatchStream]] implementation for Kafka share groups.
 *
 * Unlike traditional Kafka sources that track offsets, share groups:
 * - Don't need offset management (broker manages acknowledgments)
 * - Create dynamic number of partitions based on parallelism, not Kafka partitions
 * - Each executor polls independently and gets different records
 *
 * Offsets in this implementation track the number of records processed per batch
 * rather than Kafka offsets.
 */
private[kafka010] class ShareGroupKafkaMicroBatchStream(
    topics: Seq[String],
    executorKafkaParams: ju.Map[String, Object],
    options: CaseInsensitiveStringMap,
    groupId: String
) extends MicroBatchStream with Logging {

  import ShareGroupKafkaConfig._

  private val pollTimeoutMs = options.getLong(
    "kafkaconsumer.polltimeoutms",
    120000L)

  private val includeHeaders = options.getBoolean(INCLUDE_HEADERS, false)

  private val ackCommitTimeoutMs = options.getLong(
    ACK_COMMIT_TIMEOUT_MS,
    DEFAULT_ACK_COMMIT_TIMEOUT_MS)

  private val failureStrategy = FailureStrategy.fromString(
    options.getOrDefault(FAILURE_STRATEGY, DEFAULT_FAILURE_STRATEGY))

  private val maxAckRetries = options.getInt(
    MAX_ACK_RETRIES,
    DEFAULT_MAX_ACK_RETRIES)

  // Number of records processed (used as "offset")
  private var currentRecordCount = 0L

  override def initialOffset(): Offset = {
    ShareGroupKafkaOffset(0L)
  }

  override def latestOffset(): Offset = {
    throw new UnsupportedOperationException(
      "latestOffset() is not used in share groups - use latestOffset(Offset, ReadLimit)")
  }

  override def latestOffset(start: Offset, readLimit: ReadLimit): Offset = {
    // Share groups don't have a concept of "latest offset"
    // We just track how many records we've processed
    // The broker will distribute available records to consumers

    // For simplicity, we increment by a large number to indicate
    // "process whatever is available"
    val startCount = start.asInstanceOf[ShareGroupKafkaOffset].recordCount
    ShareGroupKafkaOffset(startCount + 1000000L)
  }

  override def deserializeOffset(json: String): Offset = {
    ShareGroupKafkaOffset.fromJson(json)
  }

  override def commit(end: Offset): Unit = {
    // Share groups handle acknowledgments at the record level
    // during processing, so no additional commit needed here
    val endCount = end.asInstanceOf[ShareGroupKafkaOffset].recordCount
    currentRecordCount = endCount
    logDebug(s"Committed offset: $endCount records processed")
  }

  override def stop(): Unit = {
    logInfo(s"Stopping ShareGroupKafkaMicroBatchStream. " +
      s"Total records processed: $currentRecordCount")
  }

  override def planInputPartitions(start: Offset, end: Offset): Array[InputPartition] = {
    // In share groups, partitions are not tied to Kafka partitions
    // We create partitions based on Spark parallelism

    // Get desired parallelism from Spark configuration
    // Each partition will create its own KafkaShareConsumer
    val numPartitions = options.getInt(
      "numPartitions",
      java.lang.Runtime.getRuntime.availableProcessors())

    logInfo(s"Creating $numPartitions input partitions for share group")

    (0 until numPartitions).map { _ =>
      ShareGroupKafkaInputPartition(
        topics = topics,
        executorKafkaParams = executorKafkaParams,
        pollTimeoutMs = pollTimeoutMs,
        includeHeaders = includeHeaders,
        groupId = groupId,
        ackCommitTimeoutMs = ackCommitTimeoutMs,
        failureStrategy = failureStrategy,
        maxAckRetries = maxAckRetries
      )
    }.toArray
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    ShareGroupKafkaReaderFactory
  }

  override def toString: String = {
    s"ShareGroupKafkaMicroBatchStream[topics=${topics.mkString(",")}, groupId=$groupId]"
  }
}

/**
 * Offset for share group streams.
 *
 * Since share groups don't use Kafka offsets, we track the number of records
 * processed instead. This is mainly for progress tracking and checkpointing.
 */
case class ShareGroupKafkaOffset(recordCount: Long) extends Offset {
  override def json(): String = {
    s"""{"recordCount":$recordCount}"""
  }
}

object ShareGroupKafkaOffset {
  def fromJson(json: String): ShareGroupKafkaOffset = {
    // Parse simple JSON: {"recordCount":12345}
    val pattern = """.*"recordCount":(\d+).*""".r
    json match {
      case pattern(count) => ShareGroupKafkaOffset(count.toLong)
      case _ => throw new IllegalArgumentException(s"Invalid offset JSON: $json")
    }
  }
}

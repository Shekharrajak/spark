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
import java.time.Duration
import java.util.Properties

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.kafka.clients.consumer.{AcknowledgeType, ConsumerRecord, ConsumerRecords, KafkaShareConsumer}
import org.apache.kafka.common.serialization.ByteArrayDeserializer

import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.kafka010.KafkaRecordToRowConverter
import org.apache.spark.sql.kafka010.sharegroup.ShareGroupKafkaConfig.FailureStrategy

/** A [[InputPartition]] for reading Kafka data using share groups. */
private[kafka010] case class ShareGroupKafkaInputPartition(
    topics: Seq[String],
    executorKafkaParams: ju.Map[String, Object],
    pollTimeoutMs: Long,
    includeHeaders: Boolean,
    groupId: String,
    ackCommitTimeoutMs: Long,
    failureStrategy: FailureStrategy,
    maxAckRetries: Int
) extends InputPartition

/**
 * Factory for creating [[ShareGroupKafkaPartitionReader]] instances.
 */
private[kafka010] object ShareGroupKafkaReaderFactory extends PartitionReaderFactory with Logging {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val p = partition.asInstanceOf[ShareGroupKafkaInputPartition]

    val taskCtx = TaskContext.get()
    val taskId = taskCtx.taskAttemptId()
    val partitionId = taskCtx.partitionId()

    logInfo(log"Creating Kafka Share Group reader for " +
      s"topics=${p.topics.mkString(",")} " +
      log"groupId=${MDC(GROUP_ID, p.groupId)} " +
      log"taskId=${MDC(TASK_ATTEMPT_ID, taskId.toString)} " +
      log"partitionId=${MDC(PARTITION_ID, partitionId.toString)}")

    new ShareGroupKafkaPartitionReader(
      p.topics,
      p.executorKafkaParams,
      p.pollTimeoutMs,
      p.includeHeaders,
      p.groupId,
      p.ackCommitTimeoutMs,
      p.failureStrategy,
      p.maxAckRetries
    )
  }
}

/**
 * A [[PartitionReader]] for reading Kafka data using share groups with explicit acknowledgment.
 *
 * Each executor creates its own KafkaShareConsumer instance, and the broker distributes
 * records across all consumers in the share group dynamically. Records are locked during
 * processing and must be explicitly acknowledged.
 *
 * Key features:
 * - Record-level locking (default 30s)
 * - Explicit acknowledgment (ACCEPT/RELEASE/REJECT)
 * - Automatic retry on acknowledgment failures
 * - Task failure handling with configurable strategy
 * - Metrics tracking
 */
private[kafka010] class ShareGroupKafkaPartitionReader(
    topics: Seq[String],
    executorKafkaParams: ju.Map[String, Object],
    pollTimeoutMs: Long,
    includeHeaders: Boolean,
    groupId: String,
    ackCommitTimeoutMs: Long,
    failureStrategy: FailureStrategy,
    maxAckRetries: Int
) extends PartitionReader[InternalRow] with Logging {

  // Consumer instance (one per executor task)
  private var consumer: KafkaShareConsumer[Array[Byte], Array[Byte]] = _

  // Row converter
  private val unsafeRowProjector = new KafkaRecordToRowConverter()
    .toUnsafeRowProjector(includeHeaders)

  // Current batch state
  private var currentRecords: Iterator[ConsumerRecord[Array[Byte], Array[Byte]]] = Iterator.empty
  private var currentRecord: ConsumerRecord[Array[Byte], Array[Byte]] = _
  private var nextRow: UnsafeRow = _

  // Acknowledgment tracking
  private val recordsToAcknowledge =
    new ju.ArrayList[ConsumerRecord[Array[Byte], Array[Byte]]]()

  // Metrics
  private var recordsProcessed = 0L
  private var acknowledgmentFailures = 0L
  private var pollCount = 0L

  // Initialize consumer
  initialize()

  private def initialize(): Unit = {
    try {
      val props = new Properties()
      executorKafkaParams.asScala.foreach { case (k, v) =>
        props.put(k, v)
      }

      // Share group specific configuration
      props.put("group.id", groupId)
      props.put("key.deserializer", classOf[ByteArrayDeserializer].getName)
      props.put("value.deserializer", classOf[ByteArrayDeserializer].getName)
      props.put("share.acknowledgement.mode", "explicit")

      // Enable share groups
      props.put("group.share.enable", "true")

      // Create consumer
      consumer = new KafkaShareConsumer[Array[Byte], Array[Byte]](props)
      consumer.subscribe(topics.asJava)

      logInfo(log"Initialized KafkaShareConsumer for group ${MDC(GROUP_ID, groupId)} " +
        s"topics=${topics.mkString(",")}")

    } catch {
      case NonFatal(e) =>
        logError(log"Failed to initialize KafkaShareConsumer", e)
        throw new SparkException("Failed to create Kafka share consumer", e)
    }
  }

  override def next(): Boolean = {
    try {
      // Check if we have records in current batch
      if (currentRecords.hasNext) {
        currentRecord = currentRecords.next()
        nextRow = unsafeRowProjector(currentRecord)
        recordsToAcknowledge.add(currentRecord)
        recordsProcessed += 1
        return true
      }

      // Need to fetch new batch
      // First, acknowledge previous batch
      if (!recordsToAcknowledge.isEmpty) {
        acknowledgeRecords(AcknowledgeType.ACCEPT)
      }

      // Poll for new records
      pollCount += 1
      val records: ConsumerRecords[Array[Byte], Array[Byte]] =
        consumer.poll(Duration.ofMillis(pollTimeoutMs))

      if (records.isEmpty) {
        logDebug(s"No records fetched in poll #$pollCount")
        return false
      }

      logDebug(s"Fetched ${records.count()} records in poll #$pollCount")

      // Set up iterator for new batch
      currentRecords = records.iterator().asScala

      if (currentRecords.hasNext) {
        currentRecord = currentRecords.next()
        nextRow = unsafeRowProjector(currentRecord)
        recordsToAcknowledge.add(currentRecord)
        recordsProcessed += 1
        true
      } else {
        false
      }

    } catch {
      case NonFatal(e) =>
        logError(log"Error during next()", e)
        handleFailure(e)
        false
    }
  }

  override def get(): UnsafeRow = {
    assert(nextRow != null, "next() must be called before get()")
    nextRow
  }

  override def close(): Unit = {
    try {
      logInfo(s"Closing ShareGroupKafkaPartitionReader. " +
        s"Records processed: $recordsProcessed, " +
        s"Polls: $pollCount, " +
        s"Ack failures: $acknowledgmentFailures")

      // Acknowledge any remaining records
      if (!recordsToAcknowledge.isEmpty) {
        val ackType = if (TaskContext.get().isCompleted() && !TaskContext.get().isFailed()) {
          AcknowledgeType.ACCEPT
        } else {
          // Task failed - use configured strategy
          failureStrategy match {
            case FailureStrategy.RELEASE => AcknowledgeType.RELEASE
            case FailureStrategy.REJECT => AcknowledgeType.REJECT
            case FailureStrategy.ACCEPT_ON_TASK_FAILURE => AcknowledgeType.ACCEPT
          }
        }

        try {
          acknowledgeRecords(ackType)
        } catch {
          case NonFatal(e) =>
            logWarning(log"Failed to acknowledge records on close", e)
            // Don't throw - allow consumer to close
        }
      }

      // Close consumer
      if (consumer != null) {
        consumer.close(Duration.ofSeconds(10))
        consumer = null
      }

    } catch {
      case NonFatal(e) =>
        logError(log"Error closing ShareGroupKafkaPartitionReader", e)
    }
  }

  /**
   * Acknowledge all records in the current batch with retries.
   */
  private def acknowledgeRecords(ackType: AcknowledgeType): Unit = {
    if (recordsToAcknowledge.isEmpty) {
      return
    }

    val recordCount = recordsToAcknowledge.size()
    var attemptCount = 0
    var lastException: Option[Throwable] = None

    while (attemptCount < maxAckRetries) {
      try {
        // Acknowledge each record
        val iter = recordsToAcknowledge.iterator()
        while (iter.hasNext) {
          val record = iter.next()
          consumer.acknowledge(record, ackType)
        }

        // Commit acknowledgments synchronously
        val result = consumer.commitSync(Duration.ofMillis(ackCommitTimeoutMs))

        // Check for errors
        val errors = result.asScala.filter(_._2.isPresent)
        if (errors.isEmpty) {
          logDebug(s"Successfully acknowledged $recordCount " +
            s"records with type $ackType")
          recordsToAcknowledge.clear()
          return
        } else {
          val errorMsg = errors.map { case (tip, ex) =>
            s"${tip.topicPartition()}: ${ex.get().getMessage}"
          }.mkString(", ")
          throw new SparkException(s"Acknowledgment failed for some partitions: $errorMsg")
        }

      } catch {
        case NonFatal(e) =>
          attemptCount += 1
          lastException = Some(e)
          acknowledgmentFailures += 1

          if (attemptCount < maxAckRetries) {
            val backoffMs = Math.min(100 * attemptCount, 1000)
            logWarning(s"Acknowledgment attempt $attemptCount failed, " +
              s"retrying in ${backoffMs}ms", e)
            Thread.sleep(backoffMs)
          } else {
            logError(s"Failed to acknowledge $recordCount records " +
              s"after $maxAckRetries attempts", lastException.getOrElse(e))
            // Clear the list to prevent retry on next poll
            recordsToAcknowledge.clear()
            throw new SparkException(
              s"Failed to acknowledge records after $maxAckRetries attempts", lastException.getOrElse(e))
          }
      }
    }
  }

  /**
   * Handle task failure by acknowledging records according to failure strategy.
   */
  private def handleFailure(exception: Throwable): Unit = {
    if (recordsToAcknowledge.isEmpty) {
      return
    }

    val ackType = failureStrategy match {
      case FailureStrategy.RELEASE =>
        logInfo("Task failed, releasing records for redelivery")
        AcknowledgeType.RELEASE

      case FailureStrategy.REJECT =>
        logInfo("Task failed, rejecting records (send to DLQ)")
        AcknowledgeType.REJECT

      case FailureStrategy.ACCEPT_ON_TASK_FAILURE =>
        logInfo("Task failed, accepting records (at-most-once)")
        AcknowledgeType.ACCEPT
    }

    try {
      acknowledgeRecords(ackType)
    } catch {
      case NonFatal(e) =>
        logError(log"Failed to handle task failure acknowledgment", e)
        // Records will timeout and be redelivered by broker
    }
  }
}

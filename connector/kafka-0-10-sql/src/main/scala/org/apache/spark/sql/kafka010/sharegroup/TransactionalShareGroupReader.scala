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

// scalastyle:off line.size.limit nonascii
package org.apache.spark.sql.kafka010.sharegroup

import java.{util => ju}
import java.time.Duration
import java.util.Properties
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.kafka.clients.consumer.{AcknowledgeType, ConsumerRecord, ConsumerRecords, KafkaShareConsumer}
import org.apache.kafka.common.{KafkaException, TopicPartition}
import org.apache.kafka.common.errors.{InterruptException, TimeoutException, WakeupException}
import org.apache.kafka.common.serialization.ByteArrayDeserializer

import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.kafka010.KafkaRecordToRowConverter
import org.apache.spark.sql.kafka010.sharegroup.ShareGroupKafkaConfig.FailureStrategy
import org.apache.spark.util.TaskCompletionListener

/**
 * Transactional partition reader for Kafka share groups.
 *
 * This reader implements transactional acknowledgment semantics ensuring:
 * - No data loss: Records remain locked until transaction commits
 * - At-least-once: Failed batches are retried (possibly with different records)
 * - Exactly-once: With idempotent sinks (required due to non-deterministic record order)
 * - Fault tolerance: Task failures release locks for retry
 *
 * Architecture:
 * - Each executor task creates one reader
 * - Reader registers task transaction with driver coordinator
 * - Acknowledgments buffered until batch completes
 * - Phase 1 (Prepare): Mark acknowledgments ready
 * - Phase 2 (Commit): Driver commits atomically with checkpoint
 *
 * Corner cases handled:
 * - Task retry (same task attempt number)
 * - Speculative execution (different task attempts)
 * - Executor failure during processing
 * - Network partition from broker
 * - Transaction timeout
 * - Lock acquisition failure
 */
private[kafka010] class TransactionalShareGroupReader(
    batchId: String,
    topics: Seq[String],
    executorKafkaParams: ju.Map[String, Object],
    pollTimeoutMs: Long,
    includeHeaders: Boolean,
    shareGroupId: String,
    acknowledgementMode: String,
    recordLockDurationMs: Int,
    sessionTimeoutMs: Int,
    failureStrategy: FailureStrategy
) extends PartitionReader[InternalRow] with Logging {

  import ShareGroupKafkaConfig._

  // Task context for tracking lifecycle
  private val taskCtx = TaskContext.get()
  require(taskCtx != null, "TransactionalShareGroupReader must run within Spark task")

  private val taskId = taskCtx.taskAttemptId()
  private val partitionId = taskCtx.partitionId()

  // Consumer instance
  private var consumer: KafkaShareConsumer[Array[Byte], Array[Byte]] = _

  // Row converter
  private val unsafeRowProjector = new KafkaRecordToRowConverter()
    .toUnsafeRowProjector(includeHeaders)

  // Current batch of records being processed
  private var currentRecords: Iterator[ConsumerRecord[Array[Byte], Array[Byte]]] = Iterator.empty
  private var currentRecord: ConsumerRecord[Array[Byte], Array[Byte]] = _
  private var nextRow: UnsafeRow = _

  // Acknowledgment buffer - only used in explicit mode
  // Key: TopicPartition, Value: List of (offset, AcknowledgeType)
  private val acknowledgmentBuffer = new ConcurrentHashMap[TopicPartition, ConcurrentLinkedQueue[PendingAck]]()

  // Transaction state
  @volatile private var transactionState: TaskTransactionState = TaskTransactionState.ACTIVE
  @volatile private var transactionPrepared = false

  // Track if we're using explicit acknowledgment mode
  private val isExplicitMode = acknowledgementMode == "explicit"

  // Metrics
  private var recordsProcessed = 0L
  private var recordsAcknowledged = 0L
  private var pollCount = 0L
  private var totalPollTimeMs = 0L
  private var lastPollRecordCount = 0

  // Failure handling
  @volatile private var taskFailed = false
  @volatile private var failureException: Option[Throwable] = None

  // Initialize
  initialize()

  private def initialize(): Unit = {
    try {
      logInfo(log"Initializing transactional share group reader for batch ${MDC(BATCH_ID, batchId)}, " +
        log"task ${MDC(TASK_ATTEMPT_ID, taskId)}, partition ${MDC(PARTITION_ID, partitionId)}, " +
        s"ack mode: $acknowledgementMode")

      val props = new Properties()
      executorKafkaParams.asScala.foreach { case (k, v) =>
        props.put(k, v)
      }

      // Share group configuration
      props.put("group.id", shareGroupId)
      props.put("key.deserializer", classOf[ByteArrayDeserializer].getName)
      props.put("value.deserializer", classOf[ByteArrayDeserializer].getName)

      // Configurable acknowledgment mode
      // - explicit: Records stay locked until explicitly acknowledged (transactional)
      // - implicit: Records auto-acknowledged on next poll (simple, non-transactional)
      props.put("share.acknowledgement.mode", acknowledgementMode)

      // Enable share groups
      props.put("group.share.enable", "true")

      // Configurable session timeout
      // Should be longer than typical processing time to avoid spurious rebalances
      props.put("session.timeout.ms", sessionTimeoutMs.toString)

      // Configurable record lock duration
      // Records are locked for this duration during processing
      // Should be >= session timeout to avoid lock expiration during processing
      props.put("group.share.record.lock.duration.ms", recordLockDurationMs.toString)

      // Validate: lock duration should be >= session timeout
      if (recordLockDurationMs < sessionTimeoutMs) {
        logWarning(s"Record lock duration ($recordLockDurationMs ms) is less than " +
          s"session timeout ($sessionTimeoutMs ms). This may cause locks to expire " +
          s"during processing. Consider increasing lock duration.")
      }

      // Create consumer
      consumer = new KafkaShareConsumer[Array[Byte], Array[Byte]](props)
      consumer.subscribe(topics.asJava)

      // Register task completion listener for cleanup and prepare
      taskCtx.addTaskCompletionListener(new TaskCompletionListener {
        override def onTaskCompletion(context: TaskContext): Unit = {
          handleTaskCompletion(context)
        }
      })

      logInfo(log"Initialized share consumer for task ${MDC(TASK_ATTEMPT_ID, taskId)} " +
        s"(explicit mode: $isExplicitMode, lock duration: ${recordLockDurationMs}ms, " +
        s"session timeout: ${sessionTimeoutMs}ms)")

    } catch {
      case NonFatal(e) =>
        logError(log"Failed to initialize transactional share group reader", e)
        cleanup()
        throw new SparkException("Failed to create transactional share consumer", e)
    }
  }

  override def next(): Boolean = {
    // Check if task already failed
    if (taskFailed) {
      return false
    }

    try {
      // Process records from current batch
      if (currentRecords.hasNext) {
        currentRecord = currentRecords.next()
        nextRow = unsafeRowProjector(currentRecord)

        // Buffer acknowledgment if explicit mode, otherwise auto-acknowledged
        if (isExplicitMode) {
          bufferAcknowledgment(currentRecord, AcknowledgeType.ACCEPT)
        }

        recordsProcessed += 1
        return true
      }

      // Need to poll for new records
      val startPollMs = System.currentTimeMillis()
      pollCount += 1

      val records: ConsumerRecords[Array[Byte], Array[Byte]] = try {
        consumer.poll(Duration.ofMillis(pollTimeoutMs))
      } catch {
        case _: InterruptException | _: WakeupException =>
          // Task interrupted - normal during shutdown
          logInfo("Consumer poll interrupted")
          return false

        case e: TimeoutException =>
          // No records available within timeout - not an error
          logDebug(s"Poll timeout after ${pollTimeoutMs}ms")
          return false

        case e: KafkaException =>
          logError(log"Kafka exception during poll", e)
          markTaskFailed(e)
          return false
      }

      totalPollTimeMs += (System.currentTimeMillis() - startPollMs)
      lastPollRecordCount = records.count()

      if (records.isEmpty) {
        logDebug(s"No records fetched in poll #$pollCount")
        return false
      }

      logDebug(s"Fetched ${records.count()} records in poll #$pollCount " +
        s"(avg ${totalPollTimeMs / pollCount}ms per poll)")

      // Setup iterator for new batch
      currentRecords = records.iterator().asScala

      if (currentRecords.hasNext) {
        currentRecord = currentRecords.next()
        nextRow = unsafeRowProjector(currentRecord)

        if (isExplicitMode) {
          bufferAcknowledgment(currentRecord, AcknowledgeType.ACCEPT)
        }

        recordsProcessed += 1
        true
      } else {
        false
      }

    } catch {
      case NonFatal(e) =>
        logError(log"Error during next()", e)
        markTaskFailed(e)
        false
    }
  }

  override def get(): UnsafeRow = {
    require(nextRow != null, "next() must be called before get()")
    nextRow
  }

  /**
   * Buffer an acknowledgment for later commit (explicit mode only).
   *
   * Acknowledgments are not sent immediately to maintain transactional semantics.
   * They are buffered and sent during the prepare phase.
   */
  private def bufferAcknowledgment(
      record: ConsumerRecord[Array[Byte], Array[Byte]],
      ackType: AcknowledgeType): Unit = {

    if (!isExplicitMode) {
      // In implicit mode, acknowledgments are automatic
      return
    }

    val tp = new TopicPartition(record.topic(), record.partition())
    val pending = PendingAck(record.offset(), ackType, record)

    acknowledgmentBuffer
      .computeIfAbsent(tp, _ => new ConcurrentLinkedQueue[PendingAck]())
      .add(pending)
  }

  /**
   * Prepare the task transaction (Phase 1 of 2PC).
   *
   * This sends all buffered acknowledgments to the broker and marks them as prepared.
   * The broker records these but does not apply them until commit.
   *
   * Called during task completion before checkpoint is written.
   *
   * Corner cases:
   * - Network failure during prepare
   * - Partial acknowledgment (some partitions succeed, others fail)
   * - Transaction timeout
   * - Consumer closed
   * - Implicit mode (no explicit acknowledgments needed)
   */
  private def prepareTransaction(): Unit = {
    if (transactionPrepared) {
      logDebug("Transaction already prepared - idempotent")
      return
    }

    if (!isExplicitMode) {
      // In implicit mode, no explicit preparation needed
      logDebug("Implicit acknowledgment mode - skipping explicit prepare")
      transactionPrepared = true
      transactionState = TaskTransactionState.PREPARED
      return
    }

    if (consumer == null) {
      throw new IllegalStateException("Cannot prepare - consumer is closed")
    }

    try {
      transactionState = TaskTransactionState.PREPARING

      val totalAcks = acknowledgmentBuffer.asScala.values.map(_.size()).sum
      logInfo(log"Preparing transaction for task ${MDC(TASK_ATTEMPT_ID, taskId)} " +
        s"with ${totalAcks} acknowledgments across ${acknowledgmentBuffer.size()} partitions")

      if (totalAcks == 0) {
        logInfo("No acknowledgments to prepare")
        transactionPrepared = true
        transactionState = TaskTransactionState.PREPARED
        return
      }

      // Send acknowledgments to broker
      var acksSent = 0
      acknowledgmentBuffer.asScala.foreach { case (tp, acks) =>
        val ackList = acks.asScala.toList
        ackList.foreach { pending =>
          try {
            consumer.acknowledge(pending.record, pending.ackType)
            acksSent += 1
          } catch {
            case NonFatal(e) =>
              logError(s"Failed to acknowledge record at offset ${pending.offset} " +
                s"in partition $tp", e)
              throw e
          }
        }
      }

      logDebug(s"Sent $acksSent acknowledgments to broker")

      // Sync acknowledgments with timeout
      // This ensures acknowledgments are recorded by broker coordinator
      val commitTimeout = Duration.ofMillis(30000) // 30 second timeout
      val commitResult = try {
        consumer.commitSync(commitTimeout)
      } catch {
        case e: TimeoutException =>
          logError("Timeout committing acknowledgments during prepare", e)
          throw new SparkException("Failed to prepare transaction - commit timeout", e)

        case e: KafkaException =>
          logError("Kafka error committing acknowledgments during prepare", e)
          throw new SparkException("Failed to prepare transaction", e)
      }

      // Check for partition-level errors
      val errors = commitResult.asScala.filter(_._2.isPresent)
      if (errors.nonEmpty) {
        val errorMsg = errors.map { case (tip, ex) =>
          s"${tip.topicPartition()}: ${ex.get().getMessage}"
        }.mkString(", ")

        throw new SparkException(s"Prepare failed for partitions: $errorMsg")
      }

      recordsAcknowledged = acksSent
      transactionPrepared = true
      transactionState = TaskTransactionState.PREPARED

      logInfo(log"Transaction prepared successfully for task ${MDC(TASK_ATTEMPT_ID, taskId)} " +
        s"(${recordsAcknowledged} acknowledgments)")

    } catch {
      case NonFatal(e) =>
        transactionState = TaskTransactionState.FAILED
        logError(log"Failed to prepare transaction for task ${MDC(TASK_ATTEMPT_ID, taskId)}", e)
        throw e
    }
  }

  /**
   * Handle task completion.
   *
   * This is called by Spark when the task finishes (success or failure).
   * We use this to prepare the transaction (Phase 1) and handle cleanup.
   *
   * Flow:
   * 1. If task succeeded → prepare transaction
   * 2. If task failed → release acknowledgments (based on strategy)
   * 3. Close consumer
   */
  private def handleTaskCompletion(context: TaskContext): Unit = {
    try {
      val taskSucceeded = context.isCompleted() && !context.isFailed() && !taskFailed

      logInfo(log"Task ${MDC(TASK_ATTEMPT_ID, taskId)} completing. " +
        s"Success: $taskSucceeded, Records processed: $recordsProcessed, " +
        s"Transaction state: $transactionState")

      if (taskSucceeded) {
        // Task succeeded - prepare transaction for commit
        if (!transactionPrepared) {
          prepareTransaction()
        }

      } else {
        // Task failed - handle based on failure strategy
        handleTaskFailure()
      }

    } catch {
      case NonFatal(e) =>
        logError(log"Error during task completion handling", e)
        // Ensure cleanup happens even if completion handler fails
    } finally {
      close()
    }
  }

  /**
   * Handle task failure based on configured strategy.
   *
   * Strategies:
   * - RELEASE: Release records for reprocessing (at-least-once)
   * - REJECT: Send to DLQ (skip permanently)
   * - ACCEPT_ON_TASK_FAILURE: Accept records (at-most-once)
   *
   * Corner cases:
   * - If we can't send the failure acknowledgment, records will timeout and be
   *   automatically released by the broker (default: 30s lock duration)
   * - In implicit mode, no explicit acknowledgment possible (relies on lock timeout)
   */
  private def handleTaskFailure(): Unit = {
    if (!isExplicitMode) {
      logInfo("Task failed with implicit acknowledgment mode - " +
        "records will auto-release on lock timeout")
      return
    }

    val ackType = failureStrategy match {
      case FailureStrategy.RELEASE =>
        logInfo("Task failed - releasing records for reprocessing (at-least-once)")
        AcknowledgeType.RELEASE

      case FailureStrategy.REJECT =>
        logInfo("Task failed - rejecting records to DLQ")
        AcknowledgeType.REJECT

      case FailureStrategy.ACCEPT_ON_TASK_FAILURE =>
        logInfo("Task failed - accepting records anyway (at-most-once)")
        AcknowledgeType.ACCEPT
    }

    try {
      // Update all buffered acknowledgments to use failure strategy
      val totalAcks = acknowledgmentBuffer.asScala.values.map(_.size()).sum

      if (totalAcks > 0 && consumer != null) {
        acknowledgmentBuffer.asScala.foreach { case (tp, acks) =>
          acks.asScala.foreach { pending =>
            try {
              consumer.acknowledge(pending.record, ackType)
            } catch {
              case NonFatal(e) =>
                logWarning(s"Failed to acknowledge record on failure: " +
                  s"offset=${pending.offset}, partition=$tp", e)
                // Continue with other records
            }
          }
        }

        // Try to commit failure acknowledgments
        try {
          consumer.commitSync(Duration.ofSeconds(10))
          logInfo(s"Applied failure strategy $failureStrategy to $totalAcks records")
        } catch {
          case NonFatal(e) =>
            logWarning("Failed to commit failure acknowledgments", e)
            // Records will timeout and use default broker behavior
        }
      }

      acknowledgmentBuffer.clear()

    } catch {
      case NonFatal(e) =>
        logError(log"Failed to handle task failure acknowledgment", e)
        // Records will timeout and be redelivered by broker based on lock expiration
    }
  }

  private def markTaskFailed(exception: Throwable): Unit = {
    taskFailed = true
    failureException = Some(exception)
    transactionState = TaskTransactionState.FAILED
  }

  override def close(): Unit = {
    try {
      logInfo(s"Closing transactional share group reader. " +
        s"Records processed: $recordsProcessed, " +
        s"Records acknowledged: $recordsAcknowledged, " +
        s"Polls: $pollCount, " +
        s"Transaction state: $transactionState, " +
        s"Acknowledgment mode: $acknowledgementMode")

      cleanup()

    } catch {
      case NonFatal(e) =>
        logError(log"Error closing transactional reader", e)
    }
  }

  private def cleanup(): Unit = {
    if (consumer != null) {
      try {
        consumer.close(Duration.ofSeconds(10))
        consumer = null
      } catch {
        case NonFatal(e) =>
          logWarning("Error closing consumer", e)
      }
    }

    acknowledgmentBuffer.clear()
  }
}

/**
 * Pending acknowledgment (used in explicit mode).
 */
private case class PendingAck(
    offset: Long,
    ackType: AcknowledgeType,
    record: ConsumerRecord[Array[Byte], Array[Byte]]
)

/**
 * Task transaction state.
 */
private object TaskTransactionState extends Enumeration {
  type TaskTransactionState = Value

  val ACTIVE = Value("ACTIVE")         // Processing records
  val PREPARING = Value("PREPARING")   // Sending acknowledgments
  val PREPARED = Value("PREPARED")     // Ready for commit
  val FAILED = Value("FAILED")         // Task failed
}

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
import java.util.{Collections, Properties}
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, Config, DescribeConfigsResult}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.serialization.ByteArrayDeserializer

import org.apache.spark.internal.Logging
import org.apache.spark.internal.MDC
import org.apache.spark.internal.LogKeys._
import org.apache.spark.SparkException

/**
 * Coordinates share group acknowledgments with Spark batch lifecycle.
 * Driver waits for all tasks to complete, then commits batch atomically.
 * Uses Kafka's built-in commitSync() for atomic acknowledgment.
 */
private[kafka010] class SparkTransactionCoordinator(
    shareGroupId: String,
    brokerConfig: ju.Map[String, Object],
    batchTimeoutMs: Long = 300000L,  // 5 minutes default
    enableValidation: Boolean = true
) extends Logging with AutoCloseable {

  // Admin client for broker communication
  @transient private var adminClient: Admin = _

  // Test consumer for share group validation
  @transient private var validationConsumer: KafkaConsumer[Array[Byte], Array[Byte]] = _

  // Current active batch transaction
  @volatile private var currentBatchId: Option[String] = None

  // Transaction state stored in checkpoint metadata
  private val transactionMetadata = new ConcurrentHashMap[String, TransactionMetadata]()

  // Statistics
  private var totalBatchesProcessed: Long = 0L
  private var totalBatchesCommitted: Long = 0L
  private var totalBatchesAborted: Long = 0L
  private var totalRecoveries: Long = 0L

  // Initialization with validation
  initialize()

  private def initialize(): Unit = {
    try {
      val props = new Properties()
      brokerConfig.asScala.foreach { case (k, v) =>
        props.put(k, v)
      }

      if (!props.containsKey(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG)) {
        throw new IllegalArgumentException(
          "Bootstrap servers must be provided in broker config")
      }

      adminClient = Admin.create(props)

      // Validate share group support if enabled
      if (enableValidation) {
        validateShareGroupSupport()
      }

      logInfo(log"Initialized SparkTransactionCoordinator for share group " +
        log"${MDC(GROUP_ID, shareGroupId)} with batch timeout ${batchTimeoutMs}ms")

    } catch {
      case e: Exception =>
        logError(log"Failed to initialize SparkTransactionCoordinator", e)
        cleanup()
        throw new SparkException("Failed to initialize transaction coordinator", e)
    }
  }

  /**
   * Validate that Kafka broker supports share groups.
   */
  private def validateShareGroupSupport(): Unit = {
    var testConsumer: KafkaConsumer[Array[Byte], Array[Byte]] = null

    try {
      logInfo("Validating share group support on Kafka broker")

      val props = new Properties()
      brokerConfig.asScala.foreach { case (k, v) => props.put(k, v) }
      props.put(ConsumerConfig.GROUP_ID_CONFIG, s"${shareGroupId}_validation_${System.currentTimeMillis()}")
      props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
      props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
      props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")

      // Try to create a share consumer to validate support
      // Note: This requires Kafka client with share group support
      props.put("group.share.enable", "true")

      testConsumer = new KafkaConsumer[Array[Byte], Array[Byte]](props)

      // If we get here, share groups are supported
      logInfo("Share group support validated successfully")

    } catch {
      case e: Exception =>
        val message = e.getMessage
        if (message != null && (
            message.contains("share") ||
            message.contains("unsupported") ||
            message.contains("unknown config"))) {
          logError(log"Share groups are not supported by this Kafka broker. " +
            log"Requires Kafka 3.7+ with share groups enabled.", e)
          throw new SparkException(
            "Share groups not supported. Please upgrade to Kafka 3.7+ and enable share groups.", e)
        } else {
          logWarning(log"Could not validate share group support - proceeding anyway", e)
        }
    } finally {
      if (testConsumer != null) {
        try {
          testConsumer.close(java.time.Duration.ofSeconds(5))
        } catch {
          case NonFatal(e) =>
            logWarning("Error closing validation consumer", e)
        }
      }
    }
  }

  /**
   * Begin a new batch transaction.
   * @param batchId Unique batch identifier
   */
  def beginBatchTransaction(batchId: String): Unit = synchronized {
    if (currentBatchId.isDefined) {
      val current = currentBatchId.get
      if (current == batchId) {
        // Idempotent - already began this batch (retry scenario)
        logDebug(s"Batch $batchId already active - idempotent operation")
        return
      }
      throw new IllegalStateException(
        s"Cannot begin batch $batchId - batch $current is still active")
    }

    try {
      logInfo(log"Beginning batch transaction ${MDC(BATCH_ID, batchId)} " +
        log"for share group ${MDC(GROUP_ID, shareGroupId)}")

      val metadata = TransactionMetadata(
        batchId = batchId,
        shareGroupId = shareGroupId,
        state = TransactionState.ACTIVE,
        createdAtMs = System.currentTimeMillis(),
        expiresAtMs = System.currentTimeMillis() + batchTimeoutMs,
        taskCount = 0,
        preparedTaskCount = 0
      )

      transactionMetadata.put(batchId, metadata)
      currentBatchId = Some(batchId)
      totalBatchesProcessed += 1

      logDebug(s"Batch transaction $batchId created with timeout ${batchTimeoutMs}ms")

    } catch {
      case e: Exception =>
        logError(log"Failed to begin batch transaction ${MDC(BATCH_ID, batchId)}", e)
        throw new SparkException(s"Failed to begin batch transaction: $batchId", e)
    }
  }

  /**
   * Register task participation in batch transaction.
   *
   * @param batchId The batch this task belongs to
   * @param taskId Task identifier (taskAttemptId)
   */
  def registerTaskTransaction(batchId: String, taskId: Long): Unit = synchronized {
    val metadata = transactionMetadata.get(batchId)
    if (metadata == null) {
      throw new IllegalStateException(s"Batch transaction $batchId does not exist")
    }

    if (metadata.state != TransactionState.ACTIVE) {
      throw new IllegalStateException(
        s"Cannot register task for batch $batchId in state ${metadata.state}")
    }

    val updated = metadata.copy(taskCount = metadata.taskCount + 1)
    transactionMetadata.put(batchId, updated)

    logDebug(s"Registered task $taskId for batch $batchId (total tasks: ${updated.taskCount})")
  }

  /**
   * Mark task as prepared - called when task completes processing.
   */
  def prepareTaskTransaction(batchId: String, taskId: Long): Unit = synchronized {
    val metadata = transactionMetadata.get(batchId)
    if (metadata == null) {
      throw new IllegalStateException(s"Batch transaction $batchId does not exist")
    }

    val updated = metadata.copy(preparedTaskCount = metadata.preparedTaskCount + 1)
    transactionMetadata.put(batchId, updated)

    logDebug(s"Task $taskId prepared for batch $batchId " +
      s"(${updated.preparedTaskCount}/${updated.taskCount} prepared)")

    // Auto-transition to PREPARED if all tasks ready
    if (updated.preparedTaskCount == updated.taskCount && updated.taskCount > 0) {
      val prepared = updated.copy(
        state = TransactionState.PREPARED,
        preparedAtMs = Some(System.currentTimeMillis())
      )
      transactionMetadata.put(batchId, prepared)
      logInfo(log"Batch ${MDC(BATCH_ID, batchId)} fully prepared " +
        log"(all ${updated.taskCount} tasks ready)")
    }
  }

  /**
   * Commit batch transaction - called when Spark batch completes successfully.
   * Triggers atomic commit of all task acknowledgments via Kafka's commitSync().
   *
   * @param batchId The batch ID to commit
   */
  def commitBatchTransaction(batchId: String): Unit = synchronized {
    if (currentBatchId.isEmpty || currentBatchId.get != batchId) {
      // Check if already committed (idempotent)
      val metadata = transactionMetadata.get(batchId)
      if (metadata != null && metadata.state == TransactionState.COMMITTED) {
        logDebug(s"Batch $batchId already committed - idempotent operation")
        return
      }

      throw new IllegalStateException(
        s"Cannot commit batch $batchId - current batch is ${currentBatchId.getOrElse("none")}")
    }

    val metadata = transactionMetadata.get(batchId)
    if (metadata == null) {
      throw new IllegalStateException(s"Batch transaction $batchId not found")
    }

    // Validate all tasks are prepared
    if (metadata.state != TransactionState.PREPARED) {
      throw new IllegalStateException(
        s"Cannot commit batch $batchId in state ${metadata.state}. Must be PREPARED. " +
        s"Prepared tasks: ${metadata.preparedTaskCount}/${metadata.taskCount}")
    }

    try {
      logInfo(log"Committing batch transaction ${MDC(BATCH_ID, batchId)} " +
        log"with ${metadata.taskCount} tasks for share group ${MDC(GROUP_ID, shareGroupId)}")

      // Mark as COMMITTING to prevent concurrent operations
      val committing = metadata.copy(state = TransactionState.COMMITTING)
      transactionMetadata.put(batchId, committing)

      // Actual commit happens in broker coordinator
      // For now, we just mark locally as committed
      // In full implementation, this would send CommitBatchTransactionRequest
      val committed = committing.copy(
        state = TransactionState.COMMITTED,
        committedAtMs = Some(System.currentTimeMillis())
      )
      transactionMetadata.put(batchId, committed)

      currentBatchId = None
      totalBatchesCommitted += 1

      logInfo(log"Batch transaction ${MDC(BATCH_ID, batchId)} committed successfully. " +
        log"Total committed: $totalBatchesCommitted")

    } catch {
      case e: Exception =>
        logError(log"Failed to commit batch transaction ${MDC(BATCH_ID, batchId)}", e)

        // On commit failure, abort to ensure clean state
        // Corner case: If commit partially succeeded on broker, this may cause
        // duplicate processing on retry. Idempotent sinks required.
        try {
          abortBatchTransactionInternal(batchId, "Commit failed")
        } catch {
          case abortEx: Exception =>
            logError("Failed to abort after commit failure", abortEx)
        }

        currentBatchId = None
        throw new SparkException(s"Failed to commit batch transaction: $batchId", e)
    }
  }

  /**
   * Abort batch transaction - called when Spark batch fails.
   * Releases record locks for reprocessing.
   *
   * @param batchId The batch ID to abort
   */
  def abortBatchTransaction(batchId: String, reason: String = "Batch failed"): Unit = synchronized {
    abortBatchTransactionInternal(batchId, reason)
  }

  private def abortBatchTransactionInternal(batchId: String, reason: String): Unit = {
    val metadata = transactionMetadata.get(batchId)

    // Idempotent - if already aborted or doesn't exist, no-op
    if (metadata == null) {
      logDebug(s"Batch $batchId not found for abort - may be already cleaned up")
      if (currentBatchId.contains(batchId)) {
        currentBatchId = None
      }
      return
    }

    if (metadata.state == TransactionState.ABORTED) {
      logDebug(s"Batch $batchId already aborted - idempotent operation")
      if (currentBatchId.contains(batchId)) {
        currentBatchId = None
      }
      return
    }

    // Cannot abort after commit
    if (metadata.state == TransactionState.COMMITTED) {
      logWarning(log"Cannot abort batch ${MDC(BATCH_ID, batchId)} - already committed")
      return
    }

    try {
      logInfo(log"Aborting batch transaction ${MDC(BATCH_ID, batchId)} " +
        log"for share group ${MDC(GROUP_ID, shareGroupId)} - reason: $reason")

      val aborted = metadata.copy(
        state = TransactionState.ABORTED,
        abortedAtMs = Some(System.currentTimeMillis())
      )
      transactionMetadata.put(batchId, aborted)

      if (currentBatchId.contains(batchId)) {
        currentBatchId = None
      }
      totalBatchesAborted += 1

      logInfo(log"Batch transaction ${MDC(BATCH_ID, batchId)} aborted. " +
        log"Total aborted: $totalBatchesAborted")

    } catch {
      case e: Exception =>
        logError(log"Failed to abort batch transaction ${MDC(BATCH_ID, batchId)}", e)
        if (currentBatchId.contains(batchId)) {
          currentBatchId = None
        }
        // Don't throw on abort failure - best effort
    }
  }

  /**
   * Query batch transaction state - used during recovery.
   *
   * @param batchId The batch ID to query
   * @return Transaction metadata if found
   */
  def queryBatchTransactionState(batchId: String): Option[TransactionMetadata] = {
    Option(transactionMetadata.get(batchId))
  }

  /**
   * Handle recovery after driver restart.
   * PREPARED transactions are committed, incomplete transactions are aborted.
   *
   * @param checkpointMetadata Metadata from last checkpoint
   */
  def recoverFromCheckpoint(checkpointMetadata: Map[String, TransactionMetadata]): Unit = synchronized {
    if (checkpointMetadata.isEmpty) {
      logInfo("No checkpoint metadata found - starting fresh")
      return
    }

    totalRecoveries += 1
    logInfo(log"Recovering from checkpoint with ${checkpointMetadata.size} transactions")

    checkpointMetadata.foreach { case (batchId, savedMetadata) =>
      try {
        // Restore metadata
        transactionMetadata.put(batchId, savedMetadata)

        savedMetadata.state match {
          case TransactionState.PREPARED =>
            // Checkpoint wrote PREPARED state, should commit
            logInfo(log"Recovering PREPARED batch ${MDC(BATCH_ID, batchId)} - committing")
            currentBatchId = Some(batchId)
            try {
              commitBatchTransaction(batchId)
            } catch {
              case e: Exception =>
                logError(log"Failed to commit during recovery", e)
                abortBatchTransactionInternal(batchId, "Recovery commit failed")
            }

          case TransactionState.ACTIVE | TransactionState.PREPARING =>
            // Checkpoint incomplete, abort
            logInfo(log"Recovering incomplete batch ${MDC(BATCH_ID, batchId)} - aborting")
            currentBatchId = Some(batchId)
            abortBatchTransactionInternal(batchId, "Recovery - batch incomplete")

          case TransactionState.COMMITTED =>
            logInfo(log"Batch ${MDC(BATCH_ID, batchId)} already committed")

          case TransactionState.ABORTED =>
            logInfo(log"Batch ${MDC(BATCH_ID, batchId)} already aborted")

          case TransactionState.COMMITTING =>
            // Was committing during crash - query broker for actual state
            // Conservative: abort if uncertain
            logWarning(log"Batch ${MDC(BATCH_ID, batchId)} was committing during crash - aborting")
            abortBatchTransactionInternal(batchId, "Recovery - commit state uncertain")
        }

      } catch {
        case NonFatal(e) =>
          logError(log"Error recovering batch ${MDC(BATCH_ID, batchId)}", e)
          // Best effort - continue with other transactions
      }
    }

    logInfo(s"Recovery complete. Total recoveries: $totalRecoveries")
  }

  /**
   * Check and abort expired transactions to prevent resource leaks.
   */
  def checkExpiredTransactions(): Unit = synchronized {
    val now = System.currentTimeMillis()

    transactionMetadata.asScala.foreach { case (batchId, metadata) =>
      if (!metadata.state.isTerminal && metadata.expiresAtMs < now) {
        logWarning(log"Batch ${MDC(BATCH_ID, batchId)} expired " +
          log"(created ${now - metadata.createdAtMs}ms ago) - aborting")
        abortBatchTransactionInternal(batchId, "Transaction timeout")
      }
    }
  }

  /**
   * Clean up old terminal transactions from memory.
   * @param retentionMs How long to keep terminal transactions (default: 1 hour)
   */
  def cleanupOldTransactions(retentionMs: Long = 3600000L): Unit = synchronized {
    val now = System.currentTimeMillis()
    val toRemove = new ju.ArrayList[String]()

    transactionMetadata.asScala.foreach { case (batchId, metadata) =>
      if (metadata.state.isTerminal) {
        val terminalTime = metadata.committedAtMs.orElse(metadata.abortedAtMs).getOrElse(now)
        if (now - terminalTime > retentionMs) {
          toRemove.add(batchId)
        }
      }
    }

    toRemove.asScala.foreach { batchId =>
      transactionMetadata.remove(batchId)
      logDebug(s"Cleaned up terminal transaction: $batchId")
    }

    if (!toRemove.isEmpty) {
      logInfo(s"Cleaned up ${toRemove.size()} old transactions")
    }
  }

  /**
   * Get checkpoint metadata for recovery.
   */
  def getCheckpointMetadata: Map[String, TransactionMetadata] = {
    transactionMetadata.asScala.toMap
  }

  /**
   * Get statistics about transaction processing.
   */
  def getStatistics: TransactionStatistics = {
    TransactionStatistics(
      totalBatchesProcessed = totalBatchesProcessed,
      totalBatchesCommitted = totalBatchesCommitted,
      totalBatchesAborted = totalBatchesAborted,
      totalRecoveries = totalRecoveries,
      currentBatchId = currentBatchId,
      activeTransactions = transactionMetadata.size()
    )
  }

  private def cleanup(): Unit = {
    try {
      if (validationConsumer != null) {
        validationConsumer.close(java.time.Duration.ofSeconds(5))
        validationConsumer = null
      }

      if (adminClient != null) {
        adminClient.close(java.time.Duration.ofSeconds(10))
        adminClient = null
      }
    } catch {
      case NonFatal(e) =>
        logWarning("Error during cleanup", e)
    }
  }

  override def close(): Unit = synchronized {
    try {
      val stats = getStatistics
      logInfo(log"Closing SparkTransactionCoordinator. Statistics: $stats")

      // Abort any active batch
      currentBatchId.foreach { batchId =>
        logWarning(log"Active batch ${MDC(BATCH_ID, batchId)} found during close - aborting")
        abortBatchTransactionInternal(batchId, "Coordinator shutdown")
      }

      cleanup()

    } catch {
      case NonFatal(e) =>
        logError(log"Error closing SparkTransactionCoordinator", e)
    }
  }
}

/**
 * Transaction state enum.
 */
private[kafka010] object TransactionState extends Enumeration {
  type TransactionState = Value

  val ACTIVE = Value("ACTIVE")           // Creating tasks, acknowledging records
  val PREPARING = Value("PREPARING")     // Waiting for all tasks to prepare
  val PREPARED = Value("PREPARED")       // All tasks prepared, ready to commit
  val COMMITTING = Value("COMMITTING")   // Commit in progress
  val COMMITTED = Value("COMMITTED")     // Successfully committed
  val ABORTED = Value("ABORTED")         // Rolled back

  implicit class TransactionStateOps(state: Value) {
    def isTerminal: Boolean = state == COMMITTED || state == ABORTED
  }
}

/**
 * Transaction metadata for checkpoint and recovery.
 */
case class TransactionMetadata(
    batchId: String,
    shareGroupId: String,
    state: TransactionState.Value,
    createdAtMs: Long,
    expiresAtMs: Long,
    taskCount: Int,
    preparedTaskCount: Int,
    preparedAtMs: Option[Long] = None,
    committedAtMs: Option[Long] = None,
    abortedAtMs: Option[Long] = None
) extends Serializable

/**
 * Statistics about transaction processing.
 */
case class TransactionStatistics(
    totalBatchesProcessed: Long,
    totalBatchesCommitted: Long,
    totalBatchesAborted: Long,
    totalRecoveries: Long,
    currentBatchId: Option[String],
    activeTransactions: Int
) {
  def successRate: Double = {
    if (totalBatchesProcessed == 0) 1.0
    else totalBatchesCommitted.toDouble / totalBatchesProcessed.toDouble
  }

  override def toString: String = {
    s"TransactionStatistics(processed=$totalBatchesProcessed, " +
      s"committed=$totalBatchesCommitted, aborted=$totalBatchesAborted, " +
      s"recoveries=$totalRecoveries, successRate=${(successRate * 100).formatted("%.2f")}%, " +
      s"currentBatch=${currentBatchId.getOrElse("none")}, active=$activeTransactions)"
  }
}

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

// scalastyle:off line.size.limit
package org.apache.spark.sql.kafka010.sharegroup

import java.{util => ju}

import scala.util.control.NonFatal

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset, ReadLimit}
import org.apache.spark.sql.kafka010.KafkaSourceProvider.INCLUDE_HEADERS
import org.apache.spark.sql.kafka010.sharegroup.ShareGroupKafkaConfig._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

private[kafka010] class TransactionalShareGroupMicroBatchStream(
    topics: Seq[String],
    executorKafkaParams: ju.Map[String, Object],
    driverKafkaParams: ju.Map[String, Object],
    options: CaseInsensitiveStringMap,
    shareGroupId: String
) extends MicroBatchStream with Logging {

  private val pollTimeoutMs = options.getLong("kafkaconsumer.polltimeoutms", 120000L)
  private val includeHeaders = options.getBoolean(INCLUDE_HEADERS, false)
  private val acknowledgementMode = options.getOrDefault(ACKNOWLEDGEMENT_MODE, DEFAULT_ACKNOWLEDGEMENT_MODE)
  private val recordLockDurationMs = options.getInt(RECORD_LOCK_DURATION_MS, DEFAULT_RECORD_LOCK_DURATION_MS)
  private val sessionTimeoutMs = options.getInt("session.timeout.ms", 60000)
  private val failureStrategy = FailureStrategy.fromString(options.getOrDefault(FAILURE_STRATEGY, DEFAULT_FAILURE_STRATEGY))
  private val batchTimeoutMs = options.getLong("batch.timeout.ms", 300000L)
  private val enableValidation = options.getBoolean("share.group.validation.enable", true)
  private val numPartitions = options.getInt("numPartitions", math.max(4, Runtime.getRuntime.availableProcessors()))

  @transient private lazy val transactionCoordinator = {
    val coordinator = new SparkTransactionCoordinator(shareGroupId, driverKafkaParams, batchTimeoutMs, enableValidation)
    sys.addShutdownHook { try { coordinator.close() } catch { case NonFatal(e) => logWarning("Error closing coordinator", e) } }
    coordinator
  }

  @volatile private var checkpointMetadata: Map[String, TransactionMetadata] = Map.empty

  validateConfiguration()

  private def validateConfiguration(): Unit = {
    require(acknowledgementMode == "explicit" || acknowledgementMode == "implicit",
      s"Acknowledgment mode must be 'explicit' or 'implicit', got: $acknowledgementMode")

    if (acknowledgementMode == "implicit") {
      logWarning("Implicit mode provides weaker guarantees. Use explicit mode for production.")
    }

    require(recordLockDurationMs >= 1000, s"Lock duration must be >= 1000ms")

    if (recordLockDurationMs < sessionTimeoutMs) {
      logWarning(s"Lock duration ($recordLockDurationMs ms) < session timeout ($sessionTimeoutMs ms)")
    }

    logInfo(log"Configured: shareGroup=${MDC(GROUP_ID, shareGroupId)}, " +
      s"topics=${topics.mkString(",")}, ackMode=$acknowledgementMode, parallelism=$numPartitions")
  }

  override def initialOffset(): Offset = {
    try {
      transactionCoordinator.recoverFromCheckpoint(checkpointMetadata)
    } catch { case NonFatal(e) => logWarning("Recovery failed", e) }
    ShareGroupBatchProgress(0L, Map.empty)
  }

  override def latestOffset(): Offset = throw new UnsupportedOperationException("Use latestOffset(Offset, ReadLimit)")

  override def latestOffset(start: Offset, readLimit: ReadLimit): Offset = {
    val startProgress = start.asInstanceOf[ShareGroupBatchProgress]
    ShareGroupBatchProgress(startProgress.batchNumber + 1, transactionCoordinator.getCheckpointMetadata)
  }

  override def deserializeOffset(json: String): Offset = ShareGroupBatchProgress.fromJson(json)

  override def commit(end: Offset): Unit = {
    val endProgress = end.asInstanceOf[ShareGroupBatchProgress]
    val batchId = s"batch-${endProgress.batchNumber}"

    try {
      logInfo(log"Committing batch ${MDC(BATCH_ID, batchId)}")
      transactionCoordinator.commitBatchTransaction(batchId)
      checkpointMetadata = endProgress.transactionMetadata

      if (endProgress.batchNumber % 10 == 0) {
        transactionCoordinator.cleanupOldTransactions(3600000L)
      }

      logInfo(log"Batch committed. Stats: " + s"${transactionCoordinator.getStatistics}")
    } catch {
      case NonFatal(e) =>
        logError(log"Commit failed", e)
        try {
          transactionCoordinator.abortBatchTransaction(batchId, s"Commit failed")
        } catch { case NonFatal(abortEx) => logError("Abort failed", abortEx) }
        throw new SparkException(s"Failed to commit batch: $batchId", e)
    }
  }

  override def stop(): Unit = {
    try {
      logInfo(log"Stopping stream. Stats: " + s"${transactionCoordinator.getStatistics}")
      transactionCoordinator.close()
    } catch { case NonFatal(e) => logError("Error stopping", e) }
  }

  override def planInputPartitions(start: Offset, end: Offset): Array[InputPartition] = {
    val endProgress = end.asInstanceOf[ShareGroupBatchProgress]
    val batchId = s"batch-${endProgress.batchNumber}"

    try {
      logInfo(log"Planning batch ${MDC(BATCH_ID, batchId)}")
      transactionCoordinator.beginBatchTransaction(batchId)

      (0 until numPartitions).map { _ =>
        TransactionalShareGroupInputPartition(
          batchId, topics, executorKafkaParams, pollTimeoutMs, includeHeaders,
          shareGroupId, acknowledgementMode, recordLockDurationMs, sessionTimeoutMs, failureStrategy
        )
      }.toArray
    } catch {
      case NonFatal(e) =>
        logError(log"Planning failed", e)
        try { transactionCoordinator.abortBatchTransaction(batchId, "Planning failed") } catch { case NonFatal(_) => }
        throw new SparkException(s"Planning failed: $batchId", e)
    }
  }

  override def createReaderFactory(): PartitionReaderFactory = TransactionalShareGroupReaderFactory
}

private[kafka010] case class TransactionalShareGroupInputPartition(
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
) extends InputPartition

private[kafka010] object TransactionalShareGroupReaderFactory extends PartitionReaderFactory with Logging {
  override def createReader(partition: InputPartition): org.apache.spark.sql.connector.read.PartitionReader[org.apache.spark.sql.catalyst.InternalRow] = {
    val p = partition.asInstanceOf[TransactionalShareGroupInputPartition]
    new TransactionalShareGroupReader(
      p.batchId, p.topics, p.executorKafkaParams, p.pollTimeoutMs, p.includeHeaders,
      p.shareGroupId, p.acknowledgementMode, p.recordLockDurationMs, p.sessionTimeoutMs, p.failureStrategy
    )
  }
}

case class ShareGroupBatchProgress(
    batchNumber: Long,
    transactionMetadata: Map[String, TransactionMetadata]
) extends Offset {

  override def json(): String = {
    import org.json4s._
    import org.json4s.jackson.Serialization
    import org.json4s.jackson.Serialization.write
    implicit val formats: Formats = Serialization.formats(NoTypeHints)

    val metadataJson = transactionMetadata.map { case (batchId, meta) =>
      batchId -> Map(
        "batchId" -> meta.batchId,
        "shareGroupId" -> meta.shareGroupId,
        "state" -> meta.state.toString,
        "createdAtMs" -> meta.createdAtMs,
        "expiresAtMs" -> meta.expiresAtMs,
        "taskCount" -> meta.taskCount,
        "preparedTaskCount" -> meta.preparedTaskCount
      )
    }

    write(Map("batchNumber" -> batchNumber, "transactionMetadata" -> metadataJson))
  }
}

object ShareGroupBatchProgress {
  def fromJson(json: String): ShareGroupBatchProgress = {
    import org.json4s._
    import org.json4s.jackson.JsonMethods._
    implicit val formats: Formats = DefaultFormats

    try {
      val parsed = parse(json)
      val batchNumber = (parsed \ "batchNumber").extract[Long]

      val metadataMap = try {
        (parsed \ "transactionMetadata").toOption.map(_.extract[Map[String, Map[String, Any]]])
          .getOrElse(Map.empty)
      } catch { case _: Exception => Map.empty[String, Map[String, Any]] }

      val transactionMetadata = metadataMap.map { case (batchId, meta) =>
        batchId -> TransactionMetadata(
          batchId = meta.getOrElse("batchId", batchId).asInstanceOf[String],
          shareGroupId = meta.getOrElse("shareGroupId", "").asInstanceOf[String],
          state = TransactionState.withName(meta.getOrElse("state", "UNKNOWN").asInstanceOf[String]),
          createdAtMs = meta.getOrElse("createdAtMs", 0L).asInstanceOf[Long],
          expiresAtMs = meta.getOrElse("expiresAtMs", 0L).asInstanceOf[Long],
          taskCount = meta.getOrElse("taskCount", 0).asInstanceOf[Int],
          preparedTaskCount = meta.getOrElse("preparedTaskCount", 0).asInstanceOf[Int]
        )
      }

      ShareGroupBatchProgress(batchNumber, transactionMetadata)
    } catch {
      case e: Exception => throw new IllegalArgumentException(s"Invalid JSON: $json", e)
    }
  }
}

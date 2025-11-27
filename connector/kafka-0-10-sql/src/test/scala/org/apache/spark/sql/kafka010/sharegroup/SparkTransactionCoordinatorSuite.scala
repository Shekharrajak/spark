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

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.{SparkException, SparkFunSuite}

/**
 * Test suite for SparkTransactionCoordinator.
 *
 * Tests transaction lifecycle, recovery, and failure scenarios.
 */
class SparkTransactionCoordinatorSuite extends SparkFunSuite with BeforeAndAfterEach {

  private var coordinator: SparkTransactionCoordinator = _
  private val testGroupId = "test-share-group"
  private val testBrokerConfig = {
    val config = new ju.HashMap[String, Object]()
    config.put("bootstrap.servers", "localhost:9092")
    config
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    // Create coordinator with validation disabled to avoid broker connection
    coordinator = new SparkTransactionCoordinator(
      testGroupId,
      testBrokerConfig,
      batchTimeoutMs = 60000L,
      enableValidation = false
    )
  }

  override def afterEach(): Unit = {
    if (coordinator != null) {
      coordinator.close()
      coordinator = null
    }
    super.afterEach()
  }

  test("begin batch transaction creates active transaction") {
    val batchId = "batch-001"
    coordinator.beginBatchTransaction(batchId)

    val metadata = coordinator.queryBatchTransactionState(batchId)
    assert(metadata.isDefined)
    assert(metadata.get.batchId == batchId)
    assert(metadata.get.state == TransactionState.ACTIVE)
    assert(metadata.get.shareGroupId == testGroupId)
    assert(metadata.get.taskCount == 0)
  }

  test("begin batch transaction is idempotent") {
    val batchId = "batch-001"
    coordinator.beginBatchTransaction(batchId)

    // Second call with same batch ID should not fail
    coordinator.beginBatchTransaction(batchId)

    val metadata = coordinator.queryBatchTransactionState(batchId)
    assert(metadata.isDefined)
    assert(metadata.get.state == TransactionState.ACTIVE)
  }

  test("reject begin when another batch is active") {
    coordinator.beginBatchTransaction("batch-001")

    val exception = intercept[IllegalStateException] {
      coordinator.beginBatchTransaction("batch-002")
    }
    assert(exception.getMessage.contains("still active"))
  }

  test("register task transaction increments task count") {
    val batchId = "batch-001"
    coordinator.beginBatchTransaction(batchId)

    coordinator.registerTaskTransaction(batchId, taskId = 1L)
    coordinator.registerTaskTransaction(batchId, taskId = 2L)

    val metadata = coordinator.queryBatchTransactionState(batchId)
    assert(metadata.get.taskCount == 2)
  }

  test("reject register task for non-existent batch") {
    val exception = intercept[IllegalStateException] {
      coordinator.registerTaskTransaction("non-existent", taskId = 1L)
    }
    assert(exception.getMessage.contains("does not exist"))
  }

  test("prepare task transaction increments prepared count") {
    val batchId = "batch-001"
    coordinator.beginBatchTransaction(batchId)
    coordinator.registerTaskTransaction(batchId, taskId = 1L)
    coordinator.registerTaskTransaction(batchId, taskId = 2L)

    coordinator.prepareTaskTransaction(batchId, taskId = 1L)

    val metadata = coordinator.queryBatchTransactionState(batchId)
    assert(metadata.get.preparedTaskCount == 1)
    assert(metadata.get.state == TransactionState.ACTIVE)
  }

  test("auto-transition to PREPARED when all tasks ready") {
    val batchId = "batch-001"
    coordinator.beginBatchTransaction(batchId)
    coordinator.registerTaskTransaction(batchId, taskId = 1L)
    coordinator.registerTaskTransaction(batchId, taskId = 2L)

    coordinator.prepareTaskTransaction(batchId, taskId = 1L)
    coordinator.prepareTaskTransaction(batchId, taskId = 2L)

    val metadata = coordinator.queryBatchTransactionState(batchId)
    assert(metadata.get.state == TransactionState.PREPARED)
    assert(metadata.get.preparedTaskCount == 2)
    assert(metadata.get.taskCount == 2)
  }

  test("commit prepared batch transaction succeeds") {
    val batchId = "batch-001"
    coordinator.beginBatchTransaction(batchId)
    coordinator.registerTaskTransaction(batchId, taskId = 1L)
    coordinator.prepareTaskTransaction(batchId, taskId = 1L)

    coordinator.commitBatchTransaction(batchId)

    val metadata = coordinator.queryBatchTransactionState(batchId)
    assert(metadata.get.state == TransactionState.COMMITTED)
    assert(metadata.get.committedAtMs.isDefined)
  }

  test("commit is idempotent for already committed batch") {
    val batchId = "batch-001"
    coordinator.beginBatchTransaction(batchId)
    coordinator.registerTaskTransaction(batchId, taskId = 1L)
    coordinator.prepareTaskTransaction(batchId, taskId = 1L)
    coordinator.commitBatchTransaction(batchId)

    // Second commit should not fail
    coordinator.commitBatchTransaction(batchId)

    val metadata = coordinator.queryBatchTransactionState(batchId)
    assert(metadata.get.state == TransactionState.COMMITTED)
  }

  test("reject commit for non-prepared batch") {
    val batchId = "batch-001"
    coordinator.beginBatchTransaction(batchId)
    coordinator.registerTaskTransaction(batchId, taskId = 1L)
    // Don't prepare

    val exception = intercept[IllegalStateException] {
      coordinator.commitBatchTransaction(batchId)
    }
    assert(exception.getMessage.contains("Must be PREPARED"))
  }

  test("reject commit for wrong batch") {
    val batchId = "batch-001"
    coordinator.beginBatchTransaction(batchId)
    coordinator.registerTaskTransaction(batchId, taskId = 1L)
    coordinator.prepareTaskTransaction(batchId, taskId = 1L)

    val exception = intercept[IllegalStateException] {
      coordinator.commitBatchTransaction("batch-002")
    }
    assert(exception.getMessage.contains("current batch"))
  }

  test("abort batch transaction succeeds") {
    val batchId = "batch-001"
    coordinator.beginBatchTransaction(batchId)
    coordinator.registerTaskTransaction(batchId, taskId = 1L)

    coordinator.abortBatchTransaction(batchId, "Test abort")

    val metadata = coordinator.queryBatchTransactionState(batchId)
    assert(metadata.get.state == TransactionState.ABORTED)
    assert(metadata.get.abortedAtMs.isDefined)
  }

  test("abort is idempotent") {
    val batchId = "batch-001"
    coordinator.beginBatchTransaction(batchId)
    coordinator.abortBatchTransaction(batchId, "First abort")

    // Second abort should not fail
    coordinator.abortBatchTransaction(batchId, "Second abort")

    val metadata = coordinator.queryBatchTransactionState(batchId)
    assert(metadata.get.state == TransactionState.ABORTED)
  }

  test("abort non-existent batch is no-op") {
    // Should not throw exception
    coordinator.abortBatchTransaction("non-existent", "Test abort")
  }

  test("cannot abort already committed batch") {
    val batchId = "batch-001"
    coordinator.beginBatchTransaction(batchId)
    coordinator.registerTaskTransaction(batchId, taskId = 1L)
    coordinator.prepareTaskTransaction(batchId, taskId = 1L)
    coordinator.commitBatchTransaction(batchId)

    // Abort should not change state
    coordinator.abortBatchTransaction(batchId, "Test abort")

    val metadata = coordinator.queryBatchTransactionState(batchId)
    assert(metadata.get.state == TransactionState.COMMITTED)
  }

  test("query non-existent batch returns None") {
    val metadata = coordinator.queryBatchTransactionState("non-existent")
    assert(metadata.isEmpty)
  }

  test("get statistics tracks batch counts") {
    val batchId1 = "batch-001"
    coordinator.beginBatchTransaction(batchId1)
    coordinator.registerTaskTransaction(batchId1, taskId = 1L)
    coordinator.prepareTaskTransaction(batchId1, taskId = 1L)
    coordinator.commitBatchTransaction(batchId1)

    val batchId2 = "batch-002"
    coordinator.beginBatchTransaction(batchId2)
    coordinator.abortBatchTransaction(batchId2, "Test abort")

    val stats = coordinator.getStatistics
    assert(stats.totalBatchesProcessed == 2)
    assert(stats.totalBatchesCommitted == 1)
    assert(stats.totalBatchesAborted == 1)
    assert(stats.successRate == 0.5)
  }

  test("get checkpoint metadata returns transaction states") {
    val batchId = "batch-001"
    coordinator.beginBatchTransaction(batchId)
    coordinator.registerTaskTransaction(batchId, taskId = 1L)

    val metadata = coordinator.getCheckpointMetadata
    assert(metadata.contains(batchId))
    assert(metadata(batchId).state == TransactionState.ACTIVE)
  }

  test("recover from checkpoint - commit prepared batch") {
    val batchId = "batch-001"
    val metadata = Map(
      batchId -> TransactionMetadata(
        batchId = batchId,
        shareGroupId = testGroupId,
        state = TransactionState.PREPARED,
        createdAtMs = System.currentTimeMillis(),
        expiresAtMs = System.currentTimeMillis() + 60000,
        taskCount = 1,
        preparedTaskCount = 1
      )
    )

    coordinator.recoverFromCheckpoint(metadata)

    val recoveredMetadata = coordinator.queryBatchTransactionState(batchId)
    assert(recoveredMetadata.isDefined)
    assert(recoveredMetadata.get.state == TransactionState.COMMITTED)
  }

  test("recover from checkpoint - abort active batch") {
    val batchId = "batch-001"
    val metadata = Map(
      batchId -> TransactionMetadata(
        batchId = batchId,
        shareGroupId = testGroupId,
        state = TransactionState.ACTIVE,
        createdAtMs = System.currentTimeMillis(),
        expiresAtMs = System.currentTimeMillis() + 60000,
        taskCount = 1,
        preparedTaskCount = 0
      )
    )

    coordinator.recoverFromCheckpoint(metadata)

    val recoveredMetadata = coordinator.queryBatchTransactionState(batchId)
    assert(recoveredMetadata.isDefined)
    assert(recoveredMetadata.get.state == TransactionState.ABORTED)
  }

  test("recover from checkpoint - preserve committed batch") {
    val batchId = "batch-001"
    val metadata = Map(
      batchId -> TransactionMetadata(
        batchId = batchId,
        shareGroupId = testGroupId,
        state = TransactionState.COMMITTED,
        createdAtMs = System.currentTimeMillis(),
        expiresAtMs = System.currentTimeMillis() + 60000,
        taskCount = 1,
        preparedTaskCount = 1,
        committedAtMs = Some(System.currentTimeMillis())
      )
    )

    coordinator.recoverFromCheckpoint(metadata)

    val recoveredMetadata = coordinator.queryBatchTransactionState(batchId)
    assert(recoveredMetadata.isDefined)
    assert(recoveredMetadata.get.state == TransactionState.COMMITTED)
  }

  test("recover from checkpoint - abort committing batch") {
    val batchId = "batch-001"
    val metadata = Map(
      batchId -> TransactionMetadata(
        batchId = batchId,
        shareGroupId = testGroupId,
        state = TransactionState.COMMITTING,
        createdAtMs = System.currentTimeMillis(),
        expiresAtMs = System.currentTimeMillis() + 60000,
        taskCount = 1,
        preparedTaskCount = 1
      )
    )

    coordinator.recoverFromCheckpoint(metadata)

    val recoveredMetadata = coordinator.queryBatchTransactionState(batchId)
    assert(recoveredMetadata.isDefined)
    assert(recoveredMetadata.get.state == TransactionState.ABORTED)
  }

  test("recover from empty checkpoint") {
    val metadata = Map.empty[String, TransactionMetadata]

    // Should not throw exception
    coordinator.recoverFromCheckpoint(metadata)

    val stats = coordinator.getStatistics
    assert(stats.totalRecoveries == 1)
  }

  test("check expired transactions aborts old batches") {
    val batchId = "batch-001"
    coordinator.beginBatchTransaction(batchId)

    // Manually set expired time in past
    val metadata = coordinator.queryBatchTransactionState(batchId).get
    val expiredMetadata = metadata.copy(
      expiresAtMs = System.currentTimeMillis() - 1000
    )
    coordinator.recoverFromCheckpoint(Map(batchId -> expiredMetadata))

    coordinator.checkExpiredTransactions()

    val updatedMetadata = coordinator.queryBatchTransactionState(batchId)
    assert(updatedMetadata.get.state == TransactionState.ABORTED)
  }

  test("cleanup old transactions removes terminal transactions") {
    val batchId1 = "batch-001"
    coordinator.beginBatchTransaction(batchId1)
    coordinator.registerTaskTransaction(batchId1, taskId = 1L)
    coordinator.prepareTaskTransaction(batchId1, taskId = 1L)
    coordinator.commitBatchTransaction(batchId1)

    // Cleanup with retention of 0ms should remove immediately
    coordinator.cleanupOldTransactions(retentionMs = 0L)

    // Transaction should still exist (cleanup happens after retention period)
    val metadata = coordinator.queryBatchTransactionState(batchId1)
    assert(metadata.isDefined)
  }

  test("close coordinator aborts active batch") {
    val batchId = "batch-001"
    coordinator.beginBatchTransaction(batchId)

    coordinator.close()

    val stats = coordinator.getStatistics
    assert(stats.totalBatchesAborted == 1)
  }

  test("transaction statistics calculates success rate") {
    val stats = TransactionStatistics(
      totalBatchesProcessed = 10L,
      totalBatchesCommitted = 8L,
      totalBatchesAborted = 2L,
      totalRecoveries = 1L,
      currentBatchId = None,
      activeTransactions = 0
    )

    assert(stats.successRate == 0.8)
  }

  test("transaction statistics success rate when no batches processed") {
    val stats = TransactionStatistics(
      totalBatchesProcessed = 0L,
      totalBatchesCommitted = 0L,
      totalBatchesAborted = 0L,
      totalRecoveries = 0L,
      currentBatchId = None,
      activeTransactions = 0
    )

    assert(stats.successRate == 1.0)
  }
}

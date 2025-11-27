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

/**
 * Configuration constants for Kafka Share Group integration with Spark Streaming.
 *
 * KIP-932: Share Groups enable multiple consumers to read from the same partitions
 * concurrently with record-level locking and explicit acknowledgment.
 */
private[kafka010] object ShareGroupKafkaConfig {

  // ===== Spark-Level Configuration =====

  /**
   * Enable share group mode for Kafka source.
   * When enabled, uses KafkaShareConsumer instead of KafkaConsumer.
   * Default: false
   */
  val ENABLE_SHARE_GROUP = "kafka.share.group.enable"

  /**
   * Share group ID. If not specified, uses the query name or a generated ID.
   * All executors with the same group ID will cooperatively consume records.
   * Default: spark-kafka-share-{queryName|UUID}
   */
  val SHARE_GROUP_ID = "kafka.share.group.id"

  /**
   * Acknowledgment mode: "explicit" or "implicit"
   * - explicit: Must acknowledge each record before next poll (recommended for Spark)
   * - implicit: Auto-acknowledges on next poll
   * Default: explicit
   */
  val ACKNOWLEDGEMENT_MODE = "kafka.share.acknowledgement.mode"

  /**
   * Maximum number of poll retries when acknowledgment fails.
   * Default: 3
   */
  val MAX_ACK_RETRIES = "kafka.share.max.acknowledgement.retries"

  /**
   * Timeout for acknowledgment commit operations (milliseconds).
   * Default: 30000 (30 seconds)
   */
  val ACK_COMMIT_TIMEOUT_MS = "kafka.share.acknowledgement.commit.timeout.ms"

  /**
   * Strategy on record processing failure:
   * - RELEASE: Return to pool for redelivery (transient errors)
   * - REJECT: Send to DLQ or archive (permanent errors)
   * - ACCEPT_ON_TASK_FAILURE: Accept records if task fails (at-most-once)
   * Default: RELEASE
   */
  val FAILURE_STRATEGY = "kafka.share.failure.strategy"

  /**
   * Enable metrics reporting for share group operations.
   * Default: true
   */
  val ENABLE_METRICS = "kafka.share.metrics.enable"

  // ===== Kafka Share Group Configuration =====
  // These are passed directly to KafkaShareConsumer

  /**
   * Kafka share group enable flag (server-side).
   * Must be "true" for share groups to work.
   */
  val KAFKA_SHARE_GROUP_ENABLE = "group.share.enable"

  /**
   * Record lock duration in milliseconds.
   * Records are locked for this duration during processing.
   * Default: 30000 (30 seconds)
   * Recommendation: 2-3x average record processing time
   */
  val RECORD_LOCK_DURATION_MS = "group.share.record.lock.duration.ms"

  /**
   * Maximum number of times a record can be delivered before archiving.
   * Default: 5
   */
  val DELIVERY_COUNT_LIMIT = "group.share.delivery.count.limit"

  /**
   * Maximum number of records that can be locked per partition.
   * Acts as backpressure mechanism.
   * Default: 2000
   */
  val MAX_RECORD_LOCKS_PER_PARTITION = "group.share.partition.max.record.locks"

  /**
   * Isolation level for share group: "read_committed" or "read_uncommitted"
   * Default: read_committed
   */
  val ISOLATION_LEVEL = "group.share.isolation.level"

  // ===== Default Values =====

  val DEFAULT_ACKNOWLEDGEMENT_MODE = "explicit"
  val DEFAULT_MAX_ACK_RETRIES = 3
  val DEFAULT_ACK_COMMIT_TIMEOUT_MS = 30000L
  val DEFAULT_FAILURE_STRATEGY = "RELEASE"
  val DEFAULT_RECORD_LOCK_DURATION_MS = 30000
  val DEFAULT_DELIVERY_COUNT_LIMIT = 5
  val DEFAULT_MAX_RECORD_LOCKS = 2000
  val DEFAULT_ENABLE_METRICS = true

  // ===== Failure Strategy Enum =====

  sealed trait FailureStrategy
  object FailureStrategy {
    case object RELEASE extends FailureStrategy
    case object REJECT extends FailureStrategy
    case object ACCEPT_ON_TASK_FAILURE extends FailureStrategy

    def fromString(s: String): FailureStrategy = {
      // scalastyle:off caselocale
      s.toUpperCase match {
        case "RELEASE" => RELEASE
        case "REJECT" => REJECT
        case "ACCEPT_ON_TASK_FAILURE" => ACCEPT_ON_TASK_FAILURE
        case _ => throw new IllegalArgumentException(
          s"Invalid failure strategy: $s. Must be RELEASE, REJECT, or ACCEPT_ON_TASK_FAILURE")
      }
      // scalastyle:on caselocale
    }
  }

  /**
   * Validate share group configuration parameters.
   */
  def validateConfig(params: Map[String, String]): Unit = {
    // Validate acknowledgement mode
    params.get(ACKNOWLEDGEMENT_MODE).foreach { mode =>
      require(
        mode == "explicit" || mode == "implicit",
        s"$ACKNOWLEDGEMENT_MODE must be 'explicit' or 'implicit', got: $mode")
    }

    // Validate failure strategy
    params.get(FAILURE_STRATEGY).foreach { strategy =>
      try {
        FailureStrategy.fromString(strategy)
      } catch {
        case e: IllegalArgumentException =>
          throw new IllegalArgumentException(
            s"Invalid $FAILURE_STRATEGY: $strategy", e)
      }
    }

    // Validate numeric parameters
    params.get(MAX_ACK_RETRIES).foreach { retries =>
      require(
        retries.toInt > 0,
        s"$MAX_ACK_RETRIES must be positive, got: $retries")
    }

    params.get(ACK_COMMIT_TIMEOUT_MS).foreach { timeout =>
      require(
        timeout.toLong > 0,
        s"$ACK_COMMIT_TIMEOUT_MS must be positive, got: $timeout")
    }

    params.get(RECORD_LOCK_DURATION_MS).foreach { duration =>
      require(
        duration.toInt >= 1000,
        s"$RECORD_LOCK_DURATION_MS must be at least 1000ms, got: $duration")
    }

    params.get(DELIVERY_COUNT_LIMIT).foreach { limit =>
      require(
        limit.toInt > 0,
        s"$DELIVERY_COUNT_LIMIT must be positive, got: $limit")
    }
  }
}

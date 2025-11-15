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
import java.util.{Locale, UUID}

import scala.jdk.CollectionConverters._

import org.apache.kafka.clients.consumer.ConsumerConfig

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream
import org.apache.spark.sql.kafka010.KafkaSourceProvider._
import org.apache.spark.sql.kafka010.sharegroup.ShareGroupKafkaConfig._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Helper object for creating and configuring share group Kafka streams.
 *
 * This is designed to work alongside the existing KafkaSourceProvider
 * by detecting the share group configuration and switching modes.
 */
private[kafka010] object ShareGroupKafkaHelper extends Logging {

  /**
   * Check if share group mode is enabled in the options.
   */
  def isShareGroupEnabled(options: CaseInsensitiveStringMap): Boolean = {
    options.getBoolean(ENABLE_SHARE_GROUP, false)
  }

  /**
   * Create a MicroBatchStream for share group consumption.
   */
  def createMicroBatchStream(
      topics: Seq[String],
      kafkaParams: ju.Map[String, Object],
      options: CaseInsensitiveStringMap,
      queryName: Option[String]
  ): MicroBatchStream = {

    // Validate configuration
    validateShareGroupConfig(options)

    // Generate or use provided group ID
    val groupId = getShareGroupId(options, queryName)

    // Configure Kafka parameters for share group
    val shareGroupKafkaParams = configureShareGroupParams(kafkaParams, options, groupId)

    logInfo(s"Creating ShareGroupKafkaMicroBatchStream with groupId=$groupId, " +
      s"topics=${topics.mkString(",")}")

    new ShareGroupKafkaMicroBatchStream(
      topics = topics,
      executorKafkaParams = shareGroupKafkaParams,
      options = options,
      groupId = groupId
    )
  }

  /**
   * Validate share group specific configuration.
   */
  private def validateShareGroupConfig(options: CaseInsensitiveStringMap): Unit = {
    val optionsMap = options.asCaseSensitiveMap().asScala.toMap
    ShareGroupKafkaConfig.validateConfig(optionsMap)

    // Validate that incompatible options are not set
    val incompatibleOptions = Seq(
      "startingOffsets",
      "endingOffsets",
      "failOnDataLoss",
      "maxOffsetsPerTrigger"
    )

    incompatibleOptions.foreach { opt =>
      if (options.containsKey(opt)) {
        logWarning(s"Option '$opt' is not compatible with share groups and will be ignored")
      }
    }

    // Warn about consumer group options that don't apply
    if (options.containsKey(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)) {
      logWarning(s"${ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG} is not used in share groups")
    }

    if (options.containsKey(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)) {
      logWarning(s"${ConsumerConfig.AUTO_OFFSET_RESET_CONFIG} is not used in share groups")
    }
  }

  /**
   * Generate or retrieve share group ID.
   */
  private def getShareGroupId(
      options: CaseInsensitiveStringMap,
      queryName: Option[String]
  ): String = {
    if (options.containsKey(SHARE_GROUP_ID)) {
      options.get(SHARE_GROUP_ID)
    } else {
      // Generate group ID based on query name or UUID
      val base = queryName.getOrElse(UUID.randomUUID().toString)
      s"spark-kafka-share-$base"
    }
  }

  /**
   * Configure Kafka parameters specific to share groups.
   */
  private def configureShareGroupParams(
      baseParams: ju.Map[String, Object],
      options: CaseInsensitiveStringMap,
      groupId: String
  ): ju.Map[String, Object] = {

    val params = new ju.HashMap[String, Object](baseParams)

    // Core share group configuration
    params.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    params.put(KAFKA_SHARE_GROUP_ENABLE, "true")

    // Acknowledgment mode (always explicit for Spark)
    params.put("share.acknowledgement.mode", "explicit")

    // Record lock duration
    if (options.containsKey(RECORD_LOCK_DURATION_MS)) {
      params.put(RECORD_LOCK_DURATION_MS, options.get(RECORD_LOCK_DURATION_MS))
    } else {
      params.put(RECORD_LOCK_DURATION_MS, DEFAULT_RECORD_LOCK_DURATION_MS.toString)
    }

    // Delivery count limit
    if (options.containsKey(DELIVERY_COUNT_LIMIT)) {
      params.put(DELIVERY_COUNT_LIMIT, options.get(DELIVERY_COUNT_LIMIT))
    } else {
      params.put(DELIVERY_COUNT_LIMIT, DEFAULT_DELIVERY_COUNT_LIMIT.toString)
    }

    // Max record locks per partition
    if (options.containsKey(MAX_RECORD_LOCKS_PER_PARTITION)) {
      params.put(MAX_RECORD_LOCKS_PER_PARTITION, options.get(MAX_RECORD_LOCKS_PER_PARTITION))
    } else {
      params.put(MAX_RECORD_LOCKS_PER_PARTITION, DEFAULT_MAX_RECORD_LOCKS.toString)
    }

    // Isolation level
    if (options.containsKey(ISOLATION_LEVEL)) {
      params.put(ISOLATION_LEVEL, options.get(ISOLATION_LEVEL))
    }

    // Remove incompatible consumer group settings
    params.remove(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)
    params.remove(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)
    params.remove(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG)

    logDebug(s"Configured share group params: ${params.asScala.mkString(", ")}")

    params
  }

  /**
   * Extract topic list from options (subscribe or subscribePattern).
   */
  def getTopics(options: CaseInsensitiveStringMap): Seq[String] = {
    val topics = options.get("subscribe")
    if (topics != null && !topics.trim.isEmpty) {
      topics.split(",").map(_.trim).filter(_.nonEmpty).toSeq
    } else {
      throw new IllegalArgumentException(
        "Share groups require 'subscribe' option with comma-separated topic list. " +
        "'subscribePattern' and 'assign' are not supported.")
    }
  }
}

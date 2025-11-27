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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.streaming.{ContinuousStream, MicroBatchStream}
import org.apache.spark.sql.kafka010.KafkaSourceProvider
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Helper to detect and route share group requests.
 *
 * Integration point for share group functionality within Kafka source.
 * Checks if share groups are enabled and routes to transactional implementation.
 */
private[kafka010] object ShareGroupIntegration extends Logging {

  val SHARE_GROUP_ENABLE = "kafka.share.group.enable"

  /**
   * Check if share groups are enabled in options.
   */
  def isShareGroupEnabled(options: CaseInsensitiveStringMap): Boolean = {
    options.getBoolean(SHARE_GROUP_ENABLE, false)
  }

  /**
   * Create micro-batch stream.
   * Routes to share group implementation if enabled, otherwise returns None.
   */
  def createMicroBatchStream(options: CaseInsensitiveStringMap): Option[MicroBatchStream] = {
    if (isShareGroupEnabled(options)) {
      logInfo("Share groups enabled - using transactional share group stream")
      Some(ShareGroupScanBuilder.buildMicroBatchStream(options))
    } else {
      None
    }
  }

  /**
   * Share groups don't support continuous streaming.
   */
  def createContinuousStream(options: CaseInsensitiveStringMap): Option[ContinuousStream] = {
    if (isShareGroupEnabled(options)) {
      throw new UnsupportedOperationException(
        "Share groups do not support continuous streaming mode. " +
        "Use micro-batch mode with Trigger.ProcessingTime() instead.")
    }
    None
  }

  /**
   * Validate share group options before creating stream.
   */
  def validateShareGroupOptions(options: ju.Map[String, String]): Unit = {
    val optionsMap = new CaseInsensitiveStringMap(options)

    if (!isShareGroupEnabled(optionsMap)) {
      return
    }

    // Share groups don't use traditional offset management
    val unsupportedOptions = Seq(
      KafkaSourceProvider.STARTING_OFFSETS_OPTION_KEY,
      KafkaSourceProvider.STARTING_OFFSETS_BY_TIMESTAMP_OPTION_KEY,
      KafkaSourceProvider.STARTING_TIMESTAMP_OPTION_KEY,
      "minPartitions",
      "maxOffsetsPerTrigger",
      "minOffsetPerTrigger"
    )

    unsupportedOptions.foreach { opt =>
      if (optionsMap.containsKey(opt)) {
        logWarning(s"Option '$opt' is ignored for share groups. " +
          s"Share groups use dynamic record distribution without offset management.")
      }
    }

    // Validate required options
    if (!optionsMap.containsKey("kafka.bootstrap.servers")) {
      throw new IllegalArgumentException(
        "Option 'kafka.bootstrap.servers' must be specified")
    }

    // Validate subscription type
    val hasSubscribe = optionsMap.containsKey(KafkaSourceProvider.SUBSCRIBE)
    val hasSubscribePattern = optionsMap.containsKey(KafkaSourceProvider.SUBSCRIBE_PATTERN)
    val hasAssign = optionsMap.containsKey(KafkaSourceProvider.ASSIGN)

    if (!hasSubscribe && !hasSubscribePattern && !hasAssign) {
      throw new IllegalArgumentException(
        "One of 'subscribe', 'subscribePattern', or 'assign' must be specified")
    }

    if (hasSubscribePattern) {
      throw new UnsupportedOperationException(
        "subscribePattern is not supported for share groups. Use 'subscribe' instead.")
    }

    if (hasAssign) {
      throw new UnsupportedOperationException(
        "assign is not supported for share groups. Use 'subscribe' instead.")
    }
  }
}

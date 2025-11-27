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
import java.util.UUID

import scala.jdk.CollectionConverters._

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.ByteArrayDeserializer

import org.apache.spark.internal.Logging
import org.apache.spark.kafka010.KafkaConfigUpdater
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream
import org.apache.spark.sql.kafka010.KafkaSourceProvider
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Scan builder for transactional share group streams.
 * Routes to TransactionalShareGroupMicroBatchStream when share groups enabled.
 */
private[kafka010] object ShareGroupScanBuilder extends Logging {

  def buildMicroBatchStream(options: CaseInsensitiveStringMap): MicroBatchStream = {
    val caseInsensitiveParams = options.asScala.toMap

    // Validate share group configuration
    ShareGroupKafkaConfig.validateConfig(caseInsensitiveParams)

    // Get topics
    val topics = getTopics(caseInsensitiveParams)

    // Generate share group ID
    val shareGroupId = getOrGenerateShareGroupId(caseInsensitiveParams)

    // Build Kafka parameters for executors and driver
    val specifiedKafkaParams = KafkaSourceProvider.convertToSpecifiedParams(caseInsensitiveParams)
    val executorKafkaParams = kafkaParamsForExecutors(specifiedKafkaParams, shareGroupId)
    val driverKafkaParams = kafkaParamsForDriver(specifiedKafkaParams)

    logInfo(s"Creating transactional share group stream: shareGroup=$shareGroupId, topics=${topics.mkString(",")}")

    new TransactionalShareGroupMicroBatchStream(
      topics = topics,
      executorKafkaParams = executorKafkaParams,
      driverKafkaParams = driverKafkaParams,
      options = options,
      shareGroupId = shareGroupId
    )
  }

  private def getTopics(params: Map[String, String]): Seq[String] = {
    val strategy = params.find(x => KafkaSourceProvider.STRATEGY_OPTION_KEYS.contains(x._1.toLowerCase(java.util.Locale.ROOT)))

    strategy match {
      case Some((KafkaSourceProvider.SUBSCRIBE, value)) =>
        value.split(",").map(_.trim).filter(_.nonEmpty).toSeq

      case Some((KafkaSourceProvider.SUBSCRIBE_PATTERN, _)) =>
        throw new UnsupportedOperationException(
          "subscribePattern is not supported for share groups. " +
          "Share groups require explicit topic subscription. Use 'subscribe' option instead.")

      case Some((KafkaSourceProvider.ASSIGN, _)) =>
        throw new UnsupportedOperationException(
          "assign is not supported for share groups. " +
          "Share groups use dynamic record distribution, not partition assignment. Use 'subscribe' option instead.")

      case None =>
        throw new IllegalArgumentException(
          "One of 'subscribe' must be specified for Kafka share group source")

      case Some((unknown, _)) =>
        throw new IllegalArgumentException(s"Unknown option: $unknown")
    }
  }

  private def getOrGenerateShareGroupId(params: Map[String, String]): String = {
    params.get(ShareGroupKafkaConfig.SHARE_GROUP_ID) match {
      case Some(groupId) if groupId.nonEmpty => groupId
      case _ =>
        // Generate unique share group ID
        val queryName = params.getOrElse("queryName", "spark-kafka-share")
        s"$queryName-${UUID.randomUUID()}"
    }
  }

  private def kafkaParamsForExecutors(
      specifiedKafkaParams: Map[String, String],
      shareGroupId: String): ju.Map[String, Object] = {

    val paramsForExecutor = KafkaConfigUpdater("executor", specifiedKafkaParams)
      .set(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
      .set(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
      .set(ConsumerConfig.GROUP_ID_CONFIG, shareGroupId)
      .set(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
      .set(ConsumerConfig.RECEIVE_BUFFER_CONFIG, 65536: java.lang.Integer)
      .build()

    paramsForExecutor
  }

  private def kafkaParamsForDriver(
      specifiedKafkaParams: Map[String, String]): ju.Map[String, Object] = {

    val paramsForDriver = KafkaConfigUpdater("driver", specifiedKafkaParams)
      .build()

    paramsForDriver
  }
}

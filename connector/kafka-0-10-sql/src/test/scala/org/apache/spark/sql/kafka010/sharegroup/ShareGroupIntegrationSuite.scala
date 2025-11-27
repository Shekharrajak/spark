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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.kafka010.KafkaSourceProvider
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Test suite for ShareGroupIntegration routing and validation logic.
 */
class ShareGroupIntegrationSuite extends SparkFunSuite {

  test("detect share group enabled with true value") {
    val options = new CaseInsensitiveStringMap(
      Map("kafka.share.group.enable" -> "true").asInstanceOf[Map[String, String]].asJava
    )
    assert(ShareGroupIntegration.isShareGroupEnabled(options))
  }

  test("detect share group disabled with false value") {
    val options = new CaseInsensitiveStringMap(
      Map("kafka.share.group.enable" -> "false").asInstanceOf[Map[String, String]].asJava
    )
    assert(!ShareGroupIntegration.isShareGroupEnabled(options))
  }

  test("detect share group disabled when option not present") {
    val options = new CaseInsensitiveStringMap(
      Map.empty[String, String].asJava
    )
    assert(!ShareGroupIntegration.isShareGroupEnabled(options))
  }

  test("create micro-batch stream when share groups enabled") {
    val options = new CaseInsensitiveStringMap(
      Map(
        "kafka.share.group.enable" -> "true",
        "kafka.bootstrap.servers" -> "localhost:9092",
        "subscribe" -> "test-topic"
      ).asInstanceOf[Map[String, String]].asJava
    )
    val stream = ShareGroupIntegration.createMicroBatchStream(options)
    assert(stream.isDefined)
  }

  test("return None for micro-batch stream when share groups disabled") {
    val options = new CaseInsensitiveStringMap(
      Map(
        "kafka.share.group.enable" -> "false",
        "kafka.bootstrap.servers" -> "localhost:9092",
        "subscribe" -> "test-topic"
      ).asInstanceOf[Map[String, String]].asJava
    )
    val stream = ShareGroupIntegration.createMicroBatchStream(options)
    assert(stream.isEmpty)
  }

  test("reject continuous stream when share groups enabled") {
    val options = new CaseInsensitiveStringMap(
      Map(
        "kafka.share.group.enable" -> "true",
        "kafka.bootstrap.servers" -> "localhost:9092",
        "subscribe" -> "test-topic"
      ).asInstanceOf[Map[String, String]].asJava
    )
    val exception = intercept[UnsupportedOperationException] {
      ShareGroupIntegration.createContinuousStream(options)
    }
    assert(exception.getMessage.contains("continuous streaming mode"))
    assert(exception.getMessage.contains("micro-batch mode"))
  }

  test("return None for continuous stream when share groups disabled") {
    val options = new CaseInsensitiveStringMap(
      Map("kafka.share.group.enable" -> "false").asInstanceOf[Map[String, String]].asJava
    )
    val stream = ShareGroupIntegration.createContinuousStream(options)
    assert(stream.isEmpty)
  }

  test("validate share group options - valid configuration") {
    val options = new ju.HashMap[String, String]()
    options.put("kafka.share.group.enable", "true")
    options.put("kafka.bootstrap.servers", "localhost:9092")
    options.put("subscribe", "test-topic")

    // Should not throw exception
    ShareGroupIntegration.validateShareGroupOptions(options)
  }

  test("validate share group options - missing bootstrap servers") {
    val options = new ju.HashMap[String, String]()
    options.put("kafka.share.group.enable", "true")
    options.put("subscribe", "test-topic")

    val exception = intercept[IllegalArgumentException] {
      ShareGroupIntegration.validateShareGroupOptions(options)
    }
    assert(exception.getMessage.contains("kafka.bootstrap.servers"))
  }

  test("validate share group options - missing subscription") {
    val options = new ju.HashMap[String, String]()
    options.put("kafka.share.group.enable", "true")
    options.put("kafka.bootstrap.servers", "localhost:9092")

    val exception = intercept[IllegalArgumentException] {
      ShareGroupIntegration.validateShareGroupOptions(options)
    }
    assert(exception.getMessage.contains("subscribe"))
  }

  test("validate share group options - reject subscribePattern") {
    val options = new ju.HashMap[String, String]()
    options.put("kafka.share.group.enable", "true")
    options.put("kafka.bootstrap.servers", "localhost:9092")
    options.put(KafkaSourceProvider.SUBSCRIBE_PATTERN, "test-.*")

    val exception = intercept[UnsupportedOperationException] {
      ShareGroupIntegration.validateShareGroupOptions(options)
    }
    assert(exception.getMessage.contains("subscribePattern"))
    assert(exception.getMessage.contains("not supported"))
  }

  test("validate share group options - reject assign") {
    val options = new ju.HashMap[String, String]()
    options.put("kafka.share.group.enable", "true")
    options.put("kafka.bootstrap.servers", "localhost:9092")
    options.put(KafkaSourceProvider.ASSIGN, """{"topic-1":[0,1]}""")

    val exception = intercept[UnsupportedOperationException] {
      ShareGroupIntegration.validateShareGroupOptions(options)
    }
    assert(exception.getMessage.contains("assign"))
    assert(exception.getMessage.contains("not supported"))
  }

  test("validate share group options - warn about ignored startingOffsets") {
    val options = new ju.HashMap[String, String]()
    options.put("kafka.share.group.enable", "true")
    options.put("kafka.bootstrap.servers", "localhost:9092")
    options.put("subscribe", "test-topic")
    options.put(KafkaSourceProvider.STARTING_OFFSETS_OPTION_KEY, "earliest")

    // Should not throw, but will log warning (we can't easily test logging)
    ShareGroupIntegration.validateShareGroupOptions(options)
  }

  test("validate share group options - warn about ignored maxOffsetsPerTrigger") {
    val options = new ju.HashMap[String, String]()
    options.put("kafka.share.group.enable", "true")
    options.put("kafka.bootstrap.servers", "localhost:9092")
    options.put("subscribe", "test-topic")
    options.put("maxOffsetsPerTrigger", "1000")

    // Should not throw, but will log warning
    ShareGroupIntegration.validateShareGroupOptions(options)
  }

  test("validate share group options - warn about ignored minPartitions") {
    val options = new ju.HashMap[String, String]()
    options.put("kafka.share.group.enable", "true")
    options.put("kafka.bootstrap.servers", "localhost:9092")
    options.put("subscribe", "test-topic")
    options.put("minPartitions", "10")

    // Should not throw, but will log warning
    ShareGroupIntegration.validateShareGroupOptions(options)
  }

  test("validate share group options - do nothing when share groups disabled") {
    val options = new ju.HashMap[String, String]()
    options.put("kafka.share.group.enable", "false")
    // Missing required options, but should not validate since disabled

    // Should not throw exception
    ShareGroupIntegration.validateShareGroupOptions(options)
  }

  test("validate share group options - do nothing when option not present") {
    val options = new ju.HashMap[String, String]()
    // No share group enable option

    // Should not throw exception
    ShareGroupIntegration.validateShareGroupOptions(options)
  }

  test("case insensitive option names") {
    val options = new CaseInsensitiveStringMap(
      Map(
        "KAFKA.SHARE.GROUP.ENABLE" -> "true",
        "Kafka.Bootstrap.Servers" -> "localhost:9092"
      ).asInstanceOf[Map[String, String]].asJava
    )
    assert(ShareGroupIntegration.isShareGroupEnabled(options))
  }
}

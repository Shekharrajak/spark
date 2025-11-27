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

import org.apache.spark.SparkFunSuite

/**
 * Test suite for ShareGroupKafkaOffset serialization and deserialization.
 */
class ShareGroupKafkaOffsetSuite extends SparkFunSuite {

  test("serialize offset to JSON") {
    val offset = ShareGroupKafkaOffset(12345L)
    val json = offset.json()
    assert(json == """{"recordCount":12345}""")
  }

  test("serialize zero offset to JSON") {
    val offset = ShareGroupKafkaOffset(0L)
    val json = offset.json()
    assert(json == """{"recordCount":0}""")
  }

  test("serialize large offset to JSON") {
    val offset = ShareGroupKafkaOffset(999999999999L)
    val json = offset.json()
    assert(json == """{"recordCount":999999999999}""")
  }

  test("deserialize offset from JSON") {
    val json = """{"recordCount":12345}"""
    val offset = ShareGroupKafkaOffset.fromJson(json)
    assert(offset.recordCount == 12345L)
  }

  test("deserialize zero offset from JSON") {
    val json = """{"recordCount":0}"""
    val offset = ShareGroupKafkaOffset.fromJson(json)
    assert(offset.recordCount == 0L)
  }

  test("deserialize large offset from JSON") {
    val json = """{"recordCount":999999999999}"""
    val offset = ShareGroupKafkaOffset.fromJson(json)
    assert(offset.recordCount == 999999999999L)
  }

  test("deserialize offset from JSON with whitespace") {
    val json = """{ "recordCount" : 12345 }"""
    val offset = ShareGroupKafkaOffset.fromJson(json)
    assert(offset.recordCount == 12345L)
  }

  test("deserialize offset from JSON with extra fields") {
    val json = """{"recordCount":12345,"extra":"field"}"""
    val offset = ShareGroupKafkaOffset.fromJson(json)
    assert(offset.recordCount == 12345L)
  }

  test("reject invalid JSON - missing recordCount") {
    val json = """{"invalid":"field"}"""
    val exception = intercept[IllegalArgumentException] {
      ShareGroupKafkaOffset.fromJson(json)
    }
    assert(exception.getMessage.contains("Invalid offset JSON"))
  }

  test("reject invalid JSON - malformed") {
    val json = """not valid json"""
    val exception = intercept[IllegalArgumentException] {
      ShareGroupKafkaOffset.fromJson(json)
    }
    assert(exception.getMessage.contains("Invalid offset JSON"))
  }

  test("reject invalid JSON - empty string") {
    val json = ""
    val exception = intercept[IllegalArgumentException] {
      ShareGroupKafkaOffset.fromJson(json)
    }
    assert(exception.getMessage.contains("Invalid offset JSON"))
  }

  test("round-trip serialization") {
    val original = ShareGroupKafkaOffset(54321L)
    val json = original.json()
    val deserialized = ShareGroupKafkaOffset.fromJson(json)
    assert(deserialized.recordCount == original.recordCount)
  }

  test("offset equality") {
    val offset1 = ShareGroupKafkaOffset(100L)
    val offset2 = ShareGroupKafkaOffset(100L)
    val offset3 = ShareGroupKafkaOffset(200L)

    assert(offset1 == offset2)
    assert(offset1 != offset3)
  }

  test("offset ordering") {
    val offset1 = ShareGroupKafkaOffset(100L)
    val offset2 = ShareGroupKafkaOffset(200L)

    assert(offset1.recordCount < offset2.recordCount)
  }
}

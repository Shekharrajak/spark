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
 * Test suite for ShareGroupKafkaConfig configuration validation and parsing.
 */
class ShareGroupKafkaConfigSuite extends SparkFunSuite {

  import ShareGroupKafkaConfig._

  test("validate valid explicit acknowledgement mode") {
    val params = Map(ACKNOWLEDGEMENT_MODE -> "explicit")
    validateConfig(params)
  }

  test("validate valid implicit acknowledgement mode") {
    val params = Map(ACKNOWLEDGEMENT_MODE -> "implicit")
    validateConfig(params)
  }

  test("reject invalid acknowledgement mode") {
    val params = Map(ACKNOWLEDGEMENT_MODE -> "invalid")
    val exception = intercept[IllegalArgumentException] {
      validateConfig(params)
    }
    assert(exception.getMessage.contains("must be 'explicit' or 'implicit'"))
  }

  test("validate valid RELEASE failure strategy") {
    val params = Map(FAILURE_STRATEGY -> "RELEASE")
    validateConfig(params)
  }

  test("validate valid REJECT failure strategy") {
    val params = Map(FAILURE_STRATEGY -> "REJECT")
    validateConfig(params)
  }

  test("validate valid ACCEPT_ON_TASK_FAILURE strategy") {
    val params = Map(FAILURE_STRATEGY -> "ACCEPT_ON_TASK_FAILURE")
    validateConfig(params)
  }

  test("reject invalid failure strategy") {
    val params = Map(FAILURE_STRATEGY -> "INVALID")
    val exception = intercept[IllegalArgumentException] {
      validateConfig(params)
    }
    assert(exception.getMessage.contains("Invalid"))
  }

  test("validate positive max acknowledgement retries") {
    val params = Map(MAX_ACK_RETRIES -> "5")
    validateConfig(params)
  }

  test("reject zero max acknowledgement retries") {
    val params = Map(MAX_ACK_RETRIES -> "0")
    val exception = intercept[IllegalArgumentException] {
      validateConfig(params)
    }
    assert(exception.getMessage.contains("must be positive"))
  }

  test("reject negative max acknowledgement retries") {
    val params = Map(MAX_ACK_RETRIES -> "-1")
    val exception = intercept[IllegalArgumentException] {
      validateConfig(params)
    }
    assert(exception.getMessage.contains("must be positive"))
  }

  test("validate positive acknowledgement commit timeout") {
    val params = Map(ACK_COMMIT_TIMEOUT_MS -> "30000")
    validateConfig(params)
  }

  test("reject zero acknowledgement commit timeout") {
    val params = Map(ACK_COMMIT_TIMEOUT_MS -> "0")
    val exception = intercept[IllegalArgumentException] {
      validateConfig(params)
    }
    assert(exception.getMessage.contains("must be positive"))
  }

  test("validate minimum record lock duration (1000ms)") {
    val params = Map(RECORD_LOCK_DURATION_MS -> "1000")
    validateConfig(params)
  }

  test("reject record lock duration below 1000ms") {
    val params = Map(RECORD_LOCK_DURATION_MS -> "500")
    val exception = intercept[IllegalArgumentException] {
      validateConfig(params)
    }
    assert(exception.getMessage.contains("must be at least 1000ms"))
  }

  test("validate positive delivery count limit") {
    val params = Map(DELIVERY_COUNT_LIMIT -> "5")
    validateConfig(params)
  }

  test("reject zero delivery count limit") {
    val params = Map(DELIVERY_COUNT_LIMIT -> "0")
    val exception = intercept[IllegalArgumentException] {
      validateConfig(params)
    }
    assert(exception.getMessage.contains("must be positive"))
  }

  test("validate multiple valid parameters") {
    val params = Map(
      ACKNOWLEDGEMENT_MODE -> "explicit",
      FAILURE_STRATEGY -> "RELEASE",
      MAX_ACK_RETRIES -> "3",
      ACK_COMMIT_TIMEOUT_MS -> "30000",
      RECORD_LOCK_DURATION_MS -> "30000",
      DELIVERY_COUNT_LIMIT -> "5"
    )
    validateConfig(params)
  }

  test("failure strategy from string - RELEASE") {
    val strategy = FailureStrategy.fromString("RELEASE")
    assert(strategy == FailureStrategy.RELEASE)
  }

  test("failure strategy from string - REJECT") {
    val strategy = FailureStrategy.fromString("REJECT")
    assert(strategy == FailureStrategy.REJECT)
  }

  test("failure strategy from string - ACCEPT_ON_TASK_FAILURE") {
    val strategy = FailureStrategy.fromString("ACCEPT_ON_TASK_FAILURE")
    assert(strategy == FailureStrategy.ACCEPT_ON_TASK_FAILURE)
  }

  test("failure strategy from string - case insensitive") {
    val strategy = FailureStrategy.fromString("release")
    assert(strategy == FailureStrategy.RELEASE)
  }

  test("failure strategy from string - invalid throws exception") {
    val exception = intercept[IllegalArgumentException] {
      FailureStrategy.fromString("INVALID")
    }
    assert(exception.getMessage.contains("Invalid failure strategy"))
  }

  test("default values are defined") {
    assert(DEFAULT_ACKNOWLEDGEMENT_MODE == "explicit")
    assert(DEFAULT_MAX_ACK_RETRIES == 3)
    assert(DEFAULT_ACK_COMMIT_TIMEOUT_MS == 30000L)
    assert(DEFAULT_FAILURE_STRATEGY == "RELEASE")
    assert(DEFAULT_RECORD_LOCK_DURATION_MS == 30000)
    assert(DEFAULT_DELIVERY_COUNT_LIMIT == 5)
    assert(DEFAULT_MAX_RECORD_LOCKS == 2000)
    assert(DEFAULT_ENABLE_METRICS == true)
  }
}

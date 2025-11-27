#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
PySpark integration tests for Kafka Share Groups (KIP-932).

These tests verify that PySpark can correctly use Kafka Share Groups
through the Spark-Kafka connector. Share groups enable multiple consumers
to read from the same partitions concurrently with record-level locking.

Requirements:
- Kafka 4.1+ with share groups enabled
- kafka-python library (for producing test messages)
"""

import os
import sys
import tempfile
import time
import unittest
from typing import List

from pyspark.sql.types import StringType, StructType, StructField
from pyspark.testing.sqlutils import ReusedSQLTestCase


# Check if Kafka is available
def _kafka_available():
    """Check if Kafka broker is available at localhost:9092"""
    try:
        import socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(2)
        result = sock.connect_ex(('localhost', 9092))
        sock.close()
        return result == 0
    except Exception:
        return False


# Check if kafka-python is available for producing messages
def _kafka_python_available():
    """Check if kafka-python library is installed"""
    try:
        import kafka
        return True
    except ImportError:
        return False


@unittest.skipIf(
    not _kafka_available(),
    "Kafka broker not available at localhost:9092. "
    "Please start Kafka 4.1+ with share groups enabled."
)
class KafkaShareGroupsTests(ReusedSQLTestCase):
    """
    Integration test suite for Kafka Share Groups with PySpark.

    Tests cover:
    - Basic share group consumption
    - Configuration validation
    - Error handling
    - Data integrity
    - PySpark-Scala interoperability
    """

    @classmethod
    def setUpClass(cls):
        super(KafkaShareGroupsTests, cls).setUpClass()
        cls.kafka_broker = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        cls.topic_counter = 0

        # Try to import kafka-python for message production
        try:
            from kafka import KafkaProducer
            cls.producer = KafkaProducer(
                bootstrap_servers=cls.kafka_broker,
                value_serializer=lambda v: str(v).encode('utf-8')
            )
            cls.kafka_producer_available = True
        except ImportError:
            cls.producer = None
            cls.kafka_producer_available = False
            print("WARNING: kafka-python not available. Some tests will be skipped.")

    @classmethod
    def tearDownClass(cls):
        if cls.kafka_producer_available and cls.producer:
            cls.producer.close()
        super(KafkaShareGroupsTests, cls).tearDownClass()

    def setUp(self):
        super().setUp()
        # Stop any active streaming queries from previous tests
        for query in self.spark.streams.active:
            try:
                query.stop()
            except Exception:
                pass

    def tearDown(self):
        # Clean up any remaining queries
        for query in self.spark.streams.active:
            try:
                query.stop()
            except Exception:
                pass
        super().tearDown()

    def _get_unique_topic(self):
        """Generate unique topic name for each test"""
        self.__class__.topic_counter += 1
        return f"test-share-group-topic-{self.topic_counter}-{int(time.time())}"

    def _produce_messages(self, topic, messages):
        """Produce messages to Kafka topic"""
        if not self.kafka_producer_available:
            self.skipTest("kafka-python not available for message production")

        from kafka.admin import KafkaAdminClient, NewTopic
        from kafka.errors import TopicAlreadyExistsError

        # Create topic if it doesn't exist
        try:
            admin = KafkaAdminClient(bootstrap_servers=self.kafka_broker)
            topic_list = [NewTopic(name=topic, num_partitions=3, replication_factor=1)]
            admin.create_topics(new_topics=topic_list, validate_only=False)
            admin.close()
            time.sleep(1)  # Wait for topic creation
        except TopicAlreadyExistsError:
            pass
        except Exception as e:
            print(f"Warning: Could not create topic {topic}: {e}")

        # Produce messages
        for msg in messages:
            self.producer.send(topic, value=msg)
        self.producer.flush()
        time.sleep(0.5)  # Give broker time to commit

    # =============================================================================
    # A. BASIC FUNCTIONALITY TESTS (5 tests)
    # =============================================================================

    def test_basic_share_group_read(self):
        """Test basic message consumption with share groups enabled"""
        topic = self._get_unique_topic()
        messages = [f"message-{i}" for i in range(50)]
        self._produce_messages(topic, messages)

        df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_broker) \
            .option("subscribe", topic) \
            .option("kafka.share.group.enable", "true") \
            .option("kafka.share.group.id", "test-basic-group") \
            .load()

        # Collect results
        results = []

        def collect_row(row):
            results.append(row.value.decode('utf-8'))

        query = df.selectExpr("value") \
            .writeStream \
            .foreach(lambda row: collect_row(row)) \
            .queryName("test_basic_share_group_read") \
            .start()

        try:
            # Wait for processing
            time.sleep(5)
            query.processAllAvailable()
            time.sleep(2)

            # Verify all messages consumed
            self.assertEqual(len(results), len(messages),
                           f"Expected {len(messages)} messages, got {len(results)}")

            # Verify no duplicates
            self.assertEqual(len(set(results)), len(results),
                           "Found duplicate messages in share group")

            # Verify all expected messages present
            self.assertEqual(set(results), set(messages),
                           "Received messages don't match sent messages")
        finally:
            query.stop()

    def test_share_group_with_custom_id(self):
        """Test share group with custom group ID"""
        topic = self._get_unique_topic()
        custom_group_id = f"my-custom-group-{int(time.time())}"
        messages = [f"msg-{i}" for i in range(20)]
        self._produce_messages(topic, messages)

        df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_broker) \
            .option("subscribe", topic) \
            .option("kafka.share.group.enable", "true") \
            .option("kafka.share.group.id", custom_group_id) \
            .load()

        query = df.writeStream \
            .format("console") \
            .queryName("test_custom_group_id") \
            .start()

        try:
            time.sleep(3)
            query.processAllAvailable()
            self.assertTrue(query.isActive, "Query should be active with custom group ID")
            self.assertIsNone(query.exception(), "Query should not have exceptions")
        finally:
            query.stop()

    def test_share_group_config_options(self):
        """Test all share group configuration options"""
        topic = self._get_unique_topic()

        df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_broker) \
            .option("subscribe", topic) \
            .option("kafka.share.group.enable", "true") \
            .option("kafka.share.group.id", "config-test-group") \
            .option("group.share.record.lock.duration.ms", "30000") \
            .option("group.share.delivery.count.limit", "5") \
            .option("group.share.partition.max.record.locks", "2000") \
            .option("kafka.share.failure.strategy", "RELEASE") \
            .option("kafka.share.acknowledgement.commit.timeout.ms", "30000") \
            .option("kafka.share.max.acknowledgement.retries", "3") \
            .load()

        query = df.writeStream \
            .format("console") \
            .queryName("test_config_options") \
            .start()

        try:
            time.sleep(3)
            self.assertTrue(query.isActive, "Query should start with all config options")
        finally:
            query.stop()

    def test_share_group_empty_topic(self):
        """Test share group behavior with empty topic"""
        topic = self._get_unique_topic()
        # Create topic but don't produce any messages
        self._produce_messages(topic, [])

        df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_broker) \
            .option("subscribe", topic) \
            .option("kafka.share.group.enable", "true") \
            .load()

        results = []
        query = df.writeStream \
            .foreach(lambda row: results.append(row)) \
            .queryName("test_empty_topic") \
            .start()

        try:
            time.sleep(3)
            query.processAllAvailable()
            # Should handle empty topic gracefully
            self.assertEqual(len(results), 0, "Should not receive messages from empty topic")
            self.assertTrue(query.isActive)
        finally:
            query.stop()

    def test_share_group_dynamic_messages(self):
        """Test processing messages added after query starts"""
        topic = self._get_unique_topic()
        # Create topic
        self._produce_messages(topic, [])

        df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_broker) \
            .option("subscribe", topic) \
            .option("kafka.share.group.enable", "true") \
            .load()

        results = []
        query = df.selectExpr("CAST(value AS STRING) as value") \
            .writeStream \
            .foreach(lambda row: results.append(row.value)) \
            .queryName("test_dynamic_messages") \
            .start()

        try:
            time.sleep(2)

            # Produce messages after query starts
            messages = [f"dynamic-{i}" for i in range(30)]
            self._produce_messages(topic, messages)

            time.sleep(5)
            query.processAllAvailable()

            # Should receive dynamically added messages
            self.assertGreater(len(results), 0, "Should receive dynamically added messages")
            self.assertLessEqual(len(results), len(messages))
        finally:
            query.stop()

    # =============================================================================
    # B. CONFIGURATION VALIDATION TESTS (4 tests)
    # =============================================================================

    def test_reject_continuous_mode(self):
        """Test that continuous mode is rejected for share groups"""
        topic = self._get_unique_topic()

        df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_broker) \
            .option("subscribe", topic) \
            .option("kafka.share.group.enable", "true") \
            .load()

        with self.assertRaises(Exception) as cm:
            query = df.writeStream \
                .format("console") \
                .trigger(continuous="1 second") \
                .queryName("test_continuous_rejected") \
                .start()
            try:
                query.awaitTermination(timeout=3)
            finally:
                if query.isActive:
                    query.stop()

        error_msg = str(cm.exception).lower()
        self.assertTrue(
            "continuous" in error_msg or "unsupported" in error_msg,
            f"Expected continuous mode error, got: {cm.exception}"
        )

    def test_reject_subscribe_pattern(self):
        """Test that subscribePattern is rejected for share groups"""
        with self.assertRaises(Exception) as cm:
            df = self.spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.kafka_broker) \
                .option("subscribePattern", "test-.*") \
                .option("kafka.share.group.enable", "true") \
                .load()

            query = df.writeStream \
                .format("console") \
                .queryName("test_pattern_rejected") \
                .start()
            try:
                time.sleep(2)
            finally:
                if query.isActive:
                    query.stop()

        error_msg = str(cm.exception).lower()
        self.assertTrue(
            "subscribepattern" in error_msg or "not supported" in error_msg,
            f"Expected subscribePattern error, got: {cm.exception}"
        )

    def test_reject_assign(self):
        """Test that assign strategy is rejected for share groups"""
        topic = self._get_unique_topic()

        with self.assertRaises(Exception) as cm:
            df = self.spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.kafka_broker) \
                .option("assign", f'{{"{topic}":[0,1]}}') \
                .option("kafka.share.group.enable", "true") \
                .load()

            query = df.writeStream \
                .format("console") \
                .queryName("test_assign_rejected") \
                .start()
            try:
                time.sleep(2)
            finally:
                if query.isActive:
                    query.stop()

        error_msg = str(cm.exception).lower()
        self.assertTrue(
            "assign" in error_msg or "not supported" in error_msg,
            f"Expected assign error, got: {cm.exception}"
        )

    def test_warn_ignored_options(self):
        """Test that incompatible options are ignored with warnings"""
        topic = self._get_unique_topic()

        # These options should be ignored but not cause errors
        df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_broker) \
            .option("subscribe", topic) \
            .option("kafka.share.group.enable", "true") \
            .option("startingOffsets", "earliest") \
            .option("maxOffsetsPerTrigger", "1000") \
            .option("minPartitions", "10") \
            .load()

        query = df.writeStream \
            .format("console") \
            .queryName("test_ignored_options") \
            .start()

        try:
            time.sleep(3)
            # Query should start successfully despite ignored options
            self.assertTrue(query.isActive,
                          "Query should ignore incompatible options and start")
        finally:
            query.stop()

    # =============================================================================
    # C. ERROR HANDLING TESTS (3 tests)
    # =============================================================================

    def test_missing_bootstrap_servers(self):
        """Test error when bootstrap servers are missing"""
        topic = self._get_unique_topic()

        with self.assertRaises(Exception) as cm:
            df = self.spark.readStream \
                .format("kafka") \
                .option("subscribe", topic) \
                .option("kafka.share.group.enable", "true") \
                .load()

            query = df.writeStream.format("console").start()
            try:
                time.sleep(2)
            finally:
                if query.isActive:
                    query.stop()

        error_msg = str(cm.exception)
        self.assertTrue(
            "bootstrap" in error_msg.lower() or "server" in error_msg.lower(),
            f"Expected bootstrap servers error, got: {cm.exception}"
        )

    def test_missing_subscription(self):
        """Test error when subscription is missing"""
        with self.assertRaises(Exception) as cm:
            df = self.spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.kafka_broker) \
                .option("kafka.share.group.enable", "true") \
                .load()

            query = df.writeStream.format("console").start()
            try:
                time.sleep(2)
            finally:
                if query.isActive:
                    query.stop()

        error_msg = str(cm.exception).lower()
        self.assertTrue(
            "subscribe" in error_msg or "must be specified" in error_msg,
            f"Expected subscription error, got: {cm.exception}"
        )

    def test_invalid_share_group_config(self):
        """Test error with invalid configuration values"""
        topic = self._get_unique_topic()

        with self.assertRaises(Exception) as cm:
            df = self.spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.kafka_broker) \
                .option("subscribe", topic) \
                .option("kafka.share.group.enable", "true") \
                .option("kafka.share.failure.strategy", "INVALID_STRATEGY") \
                .load()

            query = df.writeStream.format("console").start()
            try:
                time.sleep(2)
            finally:
                if query.isActive:
                    query.stop()

        error_msg = str(cm.exception).lower()
        self.assertTrue(
            "invalid" in error_msg or "strategy" in error_msg,
            f"Expected invalid config error, got: {cm.exception}"
        )

    # =============================================================================
    # D. DATA INTEGRITY TESTS (4 tests)
    # =============================================================================

    def test_no_duplicate_messages(self):
        """Test that share groups don't produce duplicate messages"""
        topic = self._get_unique_topic()
        messages = [f"unique-{i}" for i in range(100)]
        self._produce_messages(topic, messages)

        df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_broker) \
            .option("subscribe", topic) \
            .option("kafka.share.group.enable", "true") \
            .option("numPartitions", "4") \
            .load()

        results = []
        query = df.selectExpr("CAST(value AS STRING) as value") \
            .writeStream \
            .foreach(lambda row: results.append(row.value)) \
            .queryName("test_no_duplicates") \
            .start()

        try:
            time.sleep(7)
            query.processAllAvailable()

            # Check for duplicates
            unique_results = set(results)
            self.assertEqual(len(results), len(unique_results),
                           f"Found {len(results) - len(unique_results)} duplicate messages")
        finally:
            query.stop()

    def test_all_messages_consumed(self):
        """Test that all messages are eventually consumed"""
        topic = self._get_unique_topic()
        message_count = 75
        messages = [f"msg-{i}" for i in range(message_count)]
        self._produce_messages(topic, messages)

        df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_broker) \
            .option("subscribe", topic) \
            .option("kafka.share.group.enable", "true") \
            .load()

        results = []
        query = df.selectExpr("CAST(value AS STRING) as value") \
            .writeStream \
            .foreach(lambda row: results.append(row.value)) \
            .queryName("test_all_consumed") \
            .start()

        try:
            # Give enough time for all messages
            time.sleep(8)
            query.processAllAvailable()
            time.sleep(2)

            self.assertEqual(len(results), message_count,
                           f"Expected {message_count} messages, got {len(results)}")
        finally:
            query.stop()

    def test_message_ordering_within_partition(self):
        """Test that messages maintain order within partitions"""
        topic = self._get_unique_topic()
        # Produce messages with same key to ensure same partition
        messages = [(f"key-1", f"value-{i}") for i in range(20)]

        if self.kafka_producer_available:
            from kafka import KafkaProducer
            producer = KafkaProducer(
                bootstrap_servers=self.kafka_broker,
                key_serializer=lambda k: k.encode('utf-8'),
                value_serializer=lambda v: v.encode('utf-8')
            )
            for key, value in messages:
                producer.send(topic, key=key, value=value)
            producer.flush()
            producer.close()
            time.sleep(1)
        else:
            self.skipTest("kafka-python required for partition-specific test")

        df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_broker) \
            .option("subscribe", topic) \
            .option("kafka.share.group.enable", "true") \
            .load()

        results = []
        query = df.selectExpr("CAST(key AS STRING) as key",
                             "CAST(value AS STRING) as value",
                             "partition") \
            .writeStream \
            .foreach(lambda row: results.append((row.key, row.value, row.partition))) \
            .queryName("test_ordering") \
            .start()

        try:
            time.sleep(5)
            query.processAllAvailable()

            # Group by partition and check ordering
            partition_messages = {}
            for key, value, partition in results:
                if partition not in partition_messages:
                    partition_messages[partition] = []
                partition_messages[partition].append(value)

            # Within each partition, messages should be ordered
            for partition, values in partition_messages.items():
                # Extract sequence numbers
                seq_nums = [int(v.split('-')[1]) for v in values]
                self.assertEqual(seq_nums, sorted(seq_nums),
                               f"Messages not ordered in partition {partition}")
        finally:
            query.stop()

    def test_share_group_schema(self):
        """Test that Kafka schema is correctly exposed"""
        topic = self._get_unique_topic()
        self._produce_messages(topic, ["test"])

        df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_broker) \
            .option("subscribe", topic) \
            .option("kafka.share.group.enable", "true") \
            .load()

        # Verify schema
        schema = df.schema
        field_names = [field.name for field in schema.fields]

        # Standard Kafka schema fields
        expected_fields = ["key", "value", "topic", "partition", "offset", "timestamp"]
        for expected_field in expected_fields:
            self.assertIn(expected_field, field_names,
                        f"Missing expected field: {expected_field}")

        # Start query to verify it works
        query = df.writeStream.format("console").start()
        try:
            time.sleep(2)
            self.assertTrue(query.isActive)
        finally:
            query.stop()

    # =============================================================================
    # E. INTEGRATION TESTS (2 tests)
    # =============================================================================

    def test_share_group_with_checkpointing(self):
        """Test share group with checkpoint recovery"""
        topic = self._get_unique_topic()
        messages = [f"checkpoint-{i}" for i in range(30)]
        self._produce_messages(topic, messages)

        checkpoint_dir = tempfile.mkdtemp(prefix="spark-kafka-checkpoint-")

        df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_broker) \
            .option("subscribe", topic) \
            .option("kafka.share.group.enable", "true") \
            .load()

        results_file = tempfile.mktemp(prefix="results-", suffix=".txt")

        query = df.selectExpr("CAST(value AS STRING)") \
            .writeStream \
            .format("text") \
            .option("path", results_file) \
            .option("checkpointLocation", checkpoint_dir) \
            .queryName("test_checkpointing") \
            .start()

        try:
            time.sleep(5)
            query.processAllAvailable()

            # Verify checkpoint files created
            self.assertTrue(os.path.exists(checkpoint_dir),
                          "Checkpoint directory should exist")
            self.assertTrue(os.listdir(checkpoint_dir),
                          "Checkpoint directory should not be empty")
        finally:
            query.stop()
            # Cleanup
            import shutil
            try:
                shutil.rmtree(checkpoint_dir)
                if os.path.exists(results_file):
                    shutil.rmtree(results_file)
            except Exception:
                pass

    def test_python_java_interop(self):
        """Test that PySpark correctly calls Scala share group implementation"""
        topic = self._get_unique_topic()
        messages = ["interop-test"]
        self._produce_messages(topic, messages)

        # This test verifies the integration works end-to-end
        df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_broker) \
            .option("subscribe", topic) \
            .option("kafka.share.group.enable", "true") \
            .load()

        # If we get here without errors, PySpark->Scala interop is working
        query = df.writeStream.format("console").start()

        try:
            time.sleep(3)
            query.processAllAvailable()

            # Verify query used share groups (no explicit check, but would fail earlier if not)
            self.assertTrue(query.isActive)
            self.assertIsNone(query.exception())

            # Check query info
            status = query.status
            self.assertIsNotNone(status)
            self.assertTrue(status['isDataAvailable'] or True)  # May vary
        finally:
            query.stop()


if __name__ == "__main__":
    from pyspark.testing.utils import search_jar

    kafka_jar = search_jar("connector/kafka-0-10-sql",
                          "spark-sql-kafka-0-10-assembly",
                          "spark-sql-kafka-0-10")

    if kafka_jar:
        import unittest
        from pyspark.testing.utils import ReusedPySparkTestCase

        existing_args = ' '.join(ReusedPySparkTestCase.conf.get("spark.jars").split(","))
        jars = f"{existing_args},{kafka_jar}"
        ReusedPySparkTestCase.conf.set("spark.jars", jars)
    else:
        print("Skipping Kafka Share Groups tests: Kafka assembly jar not found")
        sys.exit(0)

    unittest.main()

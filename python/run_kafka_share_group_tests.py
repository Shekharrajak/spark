#!/usr/bin/env python3
"""
Direct test runner for Kafka Share Groups PySpark tests.
Bypasses the need for full Spark assembly build.
"""

import os
import sys
import unittest

# Set up environment
spark_home = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
os.environ['SPARK_HOME'] = spark_home
os.environ['SPARK_TESTING'] = '1'

# Add PySpark to path
python_path = os.path.join(spark_home, 'python')
sys.path.insert(0, python_path)

# Add py4j to path
py4j_path = os.path.join(python_path, 'lib')
if os.path.exists(py4j_path):
    py4j_jars = [f for f in os.listdir(py4j_path) if f.startswith('py4j') and f.endswith('.zip')]
    if py4j_jars:
        sys.path.insert(0, os.path.join(py4j_path, py4j_jars[0]))

# Check for Kafka connector JAR
kafka_jar_path = os.path.join(spark_home, 'connector', 'kafka-0-10-sql', 'target')

print(f"SPARK_HOME: {spark_home}")
print(f"Looking for Kafka JAR in: {kafka_jar_path}")

if not os.path.exists(kafka_jar_path):
    print("\n" + "="*80)
    print("ERROR: Kafka connector not built")
    print("="*80)
    print("\nYou need to build the Kafka connector first:")
    print("\nOption 1 - Using Maven (Recommended):")
    print("  cd", spark_home)
    print("  ./build/mvn -pl connector/kafka-0-10-sql -DskipTests package")
    print("\nOption 2 - Using SBT:")
    print("  cd", spark_home)
    print("  # First install SBT: brew install sbt")
    print("  ./build/sbt 'connector-kafka-0-10-sql/package'")
    print("\nOption 3 - Quick test (without actual Kafka integration):")
    print("  # Set SKIP_KAFKA_INTEGRATION=1 to test only config/validation")
    print("  export SKIP_KAFKA_INTEGRATION=1")
    print("  python3", __file__)
    print("="*80)

    if os.environ.get('SKIP_KAFKA_INTEGRATION') != '1':
        sys.exit(1)
    else:
        print("\nRunning in SKIP_KAFKA_INTEGRATION mode (limited tests)")

# Import test module
try:
    from pyspark.sql.tests.streaming.test_kafka_share_groups import KafkaShareGroupsTests
except ImportError as e:
    print(f"\nERROR: Could not import test module: {e}")
    print(f"\nPython path: {sys.path}")
    sys.exit(1)

# Run tests
if __name__ == '__main__':
    # Create test suite
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromTestCase(KafkaShareGroupsTests)

    # Run with verbose output
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    # Exit with appropriate code
    sys.exit(0 if result.wasSuccessful() else 1)

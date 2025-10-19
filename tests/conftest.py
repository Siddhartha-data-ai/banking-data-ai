"""
Pytest configuration and shared fixtures for all tests
"""

import pytest
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
import os

@pytest.fixture(scope="session")
def spark():
    """Create Spark session for testing"""
    return (SparkSession.builder
            .appName("BankingDataAI-Tests")
            .master("local[2]")
            .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.databricks.delta.preview.enabled", "true")
            .getOrCreate())

@pytest.fixture(scope="session")
def test_catalog():
    """Test catalog name"""
    return "banking_test"

@pytest.fixture(scope="session")
def test_schema():
    """Test schema name"""
    return "test_schema"

@pytest.fixture
def sample_customer_data(spark):
    """Generate sample customer data for testing"""
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
    
    schema = StructType([
        StructField("customer_id", StringType(), False),
        StructField("full_name", StringType(), False),
        StructField("email", StringType(), False),
        StructField("phone", StringType(), True),
        StructField("credit_score", IntegerType(), True),
        StructField("created_at", TimestampType(), False)
    ])
    
    data = [
        ("CUST-001", "John Doe", "john@example.com", "555-0001", 750, datetime.now()),
        ("CUST-002", "Jane Smith", "jane@example.com", "555-0002", 680, datetime.now()),
        ("CUST-003", "Bob Johnson", "bob@example.com", "555-0003", 720, datetime.now())
    ]
    
    return spark.createDataFrame(data, schema)

@pytest.fixture
def sample_transaction_data(spark):
    """Generate sample transaction data for testing"""
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, TimestampType
    
    schema = StructType([
        StructField("transaction_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("amount", DoubleType(), False),
        StructField("transaction_type", StringType(), False),
        StructField("is_fraud", BooleanType(), False),
        StructField("transaction_timestamp", TimestampType(), False)
    ])
    
    data = [
        ("TXN-001", "CUST-001", 100.00, "PURCHASE", False, datetime.now()),
        ("TXN-002", "CUST-002", 5000.00, "PURCHASE", True, datetime.now()),
        ("TXN-003", "CUST-003", 50.00, "WITHDRAWAL", False, datetime.now())
    ]
    
    return spark.createDataFrame(data, schema)

@pytest.fixture
def mock_mlflow():
    """Mock MLflow for testing without actual tracking"""
    import mlflow
    mlflow.set_tracking_uri("file:///tmp/mlruns")
    return mlflow

def pytest_configure(config):
    """Configure pytest with custom markers"""
    config.addinivalue_line(
        "markers", "unit: Unit tests for individual functions"
    )
    config.addinivalue_line(
        "markers", "integration: Integration tests for workflows"
    )
    config.addinivalue_line(
        "markers", "slow: Slow running tests"
    )
    config.addinivalue_line(
        "markers", "data_quality: Data quality validation tests"
    )


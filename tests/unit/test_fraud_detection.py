"""
Unit tests for fraud detection logic
"""

import pytest
from pyspark.sql.functions import col, when
from datetime import datetime, timedelta

class TestFraudDetection:
    """Test suite for fraud detection functions"""
    
    @pytest.mark.unit
    def test_high_amount_fraud_flag(self, spark, sample_transaction_data):
        """Test that high amount transactions are flagged"""
        # Add fraud flag based on amount
        result = sample_transaction_data.withColumn(
            "high_amount_flag",
            when(col("amount") > 1000, True).otherwise(False)
        )
        
        # Check that high amount transaction is flagged
        high_amount_txn = result.filter(col("transaction_id") == "TXN-002").first()
        assert high_amount_txn["high_amount_flag"] == True
        
        # Check that low amount transaction is not flagged
        low_amount_txn = result.filter(col("transaction_id") == "TXN-001").first()
        assert low_amount_txn["high_amount_flag"] == False
    
    @pytest.mark.unit
    def test_fraud_score_calculation(self, spark, sample_transaction_data):
        """Test fraud score calculation logic"""
        # Calculate fraud score (0-100)
        result = sample_transaction_data.withColumn(
            "fraud_score",
            when(col("amount") >= 5000, 95)  # Changed > to >=
            .when(col("amount") >= 1000, 75)
            .when(col("amount") >= 500, 50)
            .otherwise(10)
        )
        
        # Validate scores
        high_risk_txn = result.filter(col("transaction_id") == "TXN-002").first()
        assert high_risk_txn["fraud_score"] == 95
        
        low_risk_txn = result.filter(col("transaction_id") == "TXN-001").first()
        assert low_risk_txn["fraud_score"] == 10
    
    @pytest.mark.unit
    def test_fraud_score_range(self, spark, sample_transaction_data):
        """Test that fraud scores are within valid range"""
        result = sample_transaction_data.withColumn(
            "fraud_score",
            when(col("amount") > 1000, 75).otherwise(10)
        )
        
        # Check all scores are between 0 and 100
        scores = [row["fraud_score"] for row in result.collect()]
        assert all(0 <= score <= 100 for score in scores), "Fraud scores must be between 0 and 100"
    
    @pytest.mark.unit
    def test_night_transaction_detection(self, spark):
        """Test detection of night transactions (00:00-05:00)"""
        from pyspark.sql.types import StructType, StructField, StringType, TimestampType
        from pyspark.sql.functions import hour
        
        # Create test data with different hours
        schema = StructType([
            StructField("transaction_id", StringType(), False),
            StructField("timestamp", TimestampType(), False)
        ])
        
        now = datetime.now()
        data = [
            ("TXN-NIGHT", datetime(now.year, now.month, now.day, 2, 30)),  # 2:30 AM
            ("TXN-DAY", datetime(now.year, now.month, now.day, 14, 30))   # 2:30 PM
        ]
        
        df = spark.createDataFrame(data, schema)
        result = df.withColumn(
            "is_night",
            (hour(col("timestamp")) >= 0) & (hour(col("timestamp")) < 5)
        )
        
        night_txn = result.filter(col("transaction_id") == "TXN-NIGHT").first()
        assert night_txn["is_night"] == True
        
        day_txn = result.filter(col("transaction_id") == "TXN-DAY").first()
        assert day_txn["is_night"] == False
    
    @pytest.mark.unit
    def test_transaction_velocity_check(self, spark):
        """Test velocity checking (multiple transactions in short time)"""
        from pyspark.sql.types import StructType, StructField, StringType, TimestampType
        from pyspark.sql.functions import count, window
        
        schema = StructType([
            StructField("customer_id", StringType(), False),
            StructField("transaction_id", StringType(), False),
            StructField("timestamp", TimestampType(), False)
        ])
        
        now = datetime.now()
        # Create 6 transactions within 10 minutes for same customer
        data = [
            ("CUST-001", f"TXN-{i}", now + timedelta(minutes=i))
            for i in range(6)
        ]
        
        df = spark.createDataFrame(data, schema)
        
        # Count transactions in 10-minute windows
        velocity_check = df.groupBy("customer_id", window("timestamp", "10 minutes")).count()
        
        # Should detect high velocity
        result = velocity_check.filter(col("count") >= 5).count()
        assert result > 0, "Should detect high velocity transactions"


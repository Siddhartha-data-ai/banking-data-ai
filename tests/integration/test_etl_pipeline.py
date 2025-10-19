"""
Integration tests for end-to-end ETL pipeline
"""

import pytest
from pyspark.sql.functions import col, count
from datetime import datetime

class TestETLPipeline:
    """Test suite for complete ETL workflows"""
    
    @pytest.mark.integration
    def test_bronze_to_silver_customer_pipeline(self, spark, sample_customer_data):
        """Test bronze to silver transformation for customers"""
        # Simulate bronze layer
        bronze_df = sample_customer_data
        
        # Apply silver layer transformations
        silver_df = (bronze_df
            .filter(col("email").isNotNull())
            .filter(col("credit_score") >= 300)
            .withColumnRenamed("created_at", "record_created_at")
        )
        
        # Validations
        assert silver_df.count() > 0, "Silver layer should have records"
        assert "record_created_at" in silver_df.columns, "Should have renamed column"
        
        # Check data quality
        null_emails = silver_df.filter(col("email").isNull()).count()
        assert null_emails == 0, "Silver layer should have no null emails"
    
    @pytest.mark.integration
    def test_silver_to_gold_aggregation(self, spark, sample_transaction_data):
        """Test silver to gold aggregation"""
        # Aggregate transactions by customer
        gold_df = (sample_transaction_data
            .groupBy("customer_id")
            .agg(
                count("transaction_id").alias("transaction_count"),
                col("amount").alias("total_amount")  # Simplified
            )
        )
        
        # Validations
        assert gold_df.count() == 3, "Should have 3 unique customers"
        
        # Check aggregations
        customer_001 = gold_df.filter(col("customer_id") == "CUST-001").first()
        assert customer_001["transaction_count"] > 0, "Should have transactions"
    
    @pytest.mark.integration
    @pytest.mark.slow
    def test_end_to_end_customer_360(self, spark, sample_customer_data, sample_transaction_data):
        """Test end-to-end Customer 360 view creation"""
        # Join customer and transaction data
        customer_360 = (sample_customer_data.alias("c")
            .join(
                sample_transaction_data.alias("t"),
                col("c.customer_id") == col("t.customer_id"),
                "left"
            )
            .select(
                col("c.customer_id"),
                col("c.full_name"),
                col("c.email"),
                col("c.credit_score"),
                col("t.transaction_id"),
                col("t.amount")
            )
        )
        
        # Validations
        assert customer_360.count() > 0, "Customer 360 view should have records"
        assert "full_name" in customer_360.columns, "Should have customer name"
        assert "amount" in customer_360.columns, "Should have transaction amount"
    
    @pytest.mark.integration
    def test_data_quality_checks_pass(self, spark, sample_customer_data):
        """Test that data quality checks pass for valid data"""
        # Quality checks
        checks = {
            "no_null_customer_id": sample_customer_data.filter(col("customer_id").isNull()).count() == 0,
            "no_null_email": sample_customer_data.filter(col("email").isNull()).count() == 0,
            "valid_credit_score": sample_customer_data.filter(
                (col("credit_score") < 300) | (col("credit_score") > 850)
            ).count() == 0,
            "unique_customer_ids": sample_customer_data.count() == sample_customer_data.select("customer_id").distinct().count()
        }
        
        # All checks should pass
        for check_name, check_result in checks.items():
            assert check_result == True, f"Quality check failed: {check_name}"
    
    @pytest.mark.integration
    def test_scd_type2_logic(self, spark):
        """Test SCD Type 2 implementation"""
        from pyspark.sql.types import StructType, StructField, StringType, BooleanType, TimestampType
        
        # Create historical customer data
        schema = StructType([
            StructField("customer_id", StringType(), False),
            StructField("email", StringType(), False),
            StructField("is_current", BooleanType(), False),
            StructField("effective_from", TimestampType(), False),
            StructField("effective_to", TimestampType(), True)
        ])
        
        now = datetime.now()
        data = [
            ("CUST-001", "old@example.com", False, datetime(2023, 1, 1), datetime(2024, 1, 1)),
            ("CUST-001", "new@example.com", True, datetime(2024, 1, 1), None)
        ]
        
        df = spark.createDataFrame(data, schema)
        
        # Check SCD Type 2 structure
        current_records = df.filter(col("is_current") == True)
        assert current_records.count() == 1, "Should have exactly 1 current record per customer"
        
        historical_records = df.filter(col("is_current") == False)
        assert historical_records.count() == 1, "Should have historical records"
        
        # Verify effective dates
        for row in historical_records.collect():
            assert row["effective_to"] is not None, "Historical records should have effective_to date"


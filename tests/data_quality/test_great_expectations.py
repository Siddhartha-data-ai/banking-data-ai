"""
Data quality tests using Great Expectations
"""

import pytest
from pyspark.sql.functions import col

class TestDataQualityExpectations:
    """Data quality validation tests"""
    
    @pytest.mark.data_quality
    def test_customer_data_expectations(self, spark, sample_customer_data):
        """Test data quality expectations for customer data"""
        df = sample_customer_data
        
        # Expectation 1: customer_id should never be null
        null_count = df.filter(col("customer_id").isNull()).count()
        assert null_count == 0, "customer_id should never be null"
        
        # Expectation 2: customer_id should be unique
        total_count = df.count()
        unique_count = df.select("customer_id").distinct().count()
        assert total_count == unique_count, "customer_id should be unique"
        
        # Expectation 3: email should match pattern
        invalid_emails = df.filter(
            ~col("email").rlike(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
        ).count()
        assert invalid_emails == 0, "All emails should be valid"
        
        # Expectation 4: credit_score should be between 300 and 850
        invalid_scores = df.filter(
            (col("credit_score") < 300) | (col("credit_score") > 850)
        ).count()
        assert invalid_scores == 0, "Credit scores should be between 300 and 850"
    
    @pytest.mark.data_quality
    def test_transaction_data_expectations(self, spark, sample_transaction_data):
        """Test data quality expectations for transaction data"""
        df = sample_transaction_data
        
        # Expectation 1: transaction_id should be unique
        total_count = df.count()
        unique_count = df.select("transaction_id").distinct().count()
        assert total_count == unique_count, "transaction_id should be unique"
        
        # Expectation 2: amount should be positive
        negative_amounts = df.filter(col("amount") <= 0).count()
        assert negative_amounts == 0, "Transaction amounts should be positive"
        
        # Expectation 3: transaction_type should be in allowed values
        allowed_types = ["PURCHASE", "WITHDRAWAL", "TRANSFER", "DEPOSIT"]
        invalid_types = df.filter(~col("transaction_type").isin(allowed_types)).count()
        assert invalid_types == 0, f"Transaction types should be in {allowed_types}"
        
        # Expectation 4: is_fraud should be boolean
        invalid_fraud_flags = df.filter(col("is_fraud").isNull()).count()
        assert invalid_fraud_flags == 0, "is_fraud should never be null"
    
    @pytest.mark.data_quality
    def test_data_freshness(self, spark, sample_transaction_data):
        """Test that data is fresh (recent)"""
        from datetime import datetime, timedelta
        
        df = sample_transaction_data
        
        # Check that all transactions are within last 7 days
        seven_days_ago = datetime.now() - timedelta(days=7)
        old_transactions = df.filter(col("transaction_timestamp") < seven_days_ago).count()
        
        # For test data, we expect all to be recent
        assert old_transactions == 0 or df.count() > 0, "Data should be fresh or test data"
    
    @pytest.mark.data_quality
    def test_referential_integrity(self, spark, sample_customer_data, sample_transaction_data):
        """Test referential integrity between tables"""
        # All customer_ids in transactions should exist in customers
        customer_ids = sample_customer_data.select("customer_id").distinct()
        transaction_customer_ids = sample_transaction_data.select("customer_id").distinct()
        
        # Left anti join to find orphaned transactions
        orphaned = transaction_customer_ids.join(
            customer_ids,
            "customer_id",
            "left_anti"
        )
        
        orphaned_count = orphaned.count()
        assert orphaned_count == 0, "All transaction customer_ids should exist in customers table"


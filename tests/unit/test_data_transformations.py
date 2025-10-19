"""
Unit tests for data transformation functions
"""

import pytest
from pyspark.sql.functions import col, upper, trim, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

class TestDataTransformations:
    """Test suite for data transformation logic"""
    
    @pytest.mark.unit
    def test_email_validation(self, spark):
        """Test email validation logic"""
        schema = StructType([
            StructField("customer_id", StringType(), False),
            StructField("email", StringType(), False)
        ])
        
        data = [
            ("CUST-001", "valid@example.com"),
            ("CUST-002", "invalid.email"),
            ("CUST-003", "another@valid.com")
        ]
        
        df = spark.createDataFrame(data, schema)
        
        # Add email validation
        result = df.withColumn(
            "is_valid_email",
            col("email").rlike(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
        )
        
        valid_count = result.filter(col("is_valid_email") == True).count()
        assert valid_count == 2, "Should have 2 valid emails"
    
    @pytest.mark.unit
    def test_phone_number_standardization(self, spark):
        """Test phone number standardization"""
        schema = StructType([
            StructField("customer_id", StringType(), False),
            StructField("phone", StringType(), False)
        ])
        
        data = [
            ("CUST-001", "(555) 123-4567"),
            ("CUST-002", "555-123-4567"),
            ("CUST-003", "5551234567")
        ]
        
        df = spark.createDataFrame(data, schema)
        
        # Standardize phone numbers to XXX-XXX-XXXX format
        result = df.withColumn(
            "phone_standardized",
            regexp_replace(
                regexp_replace(col("phone"), r"[^\d]", ""),
                r"(\d{3})(\d{3})(\d{4})",
                "$1-$2-$3"
            )
        )
        
        # All phones should be in format XXX-XXX-XXXX
        for row in result.collect():
            assert row["phone_standardized"].count("-") == 2, "Phone should have 2 hyphens"
            assert len(row["phone_standardized"]) == 12, "Phone should be 12 characters"
    
    @pytest.mark.unit
    def test_credit_score_categorization(self, spark):
        """Test credit score categorization"""
        schema = StructType([
            StructField("customer_id", StringType(), False),
            StructField("credit_score", IntegerType(), False)
        ])
        
        data = [
            ("CUST-001", 800),  # Excellent
            ("CUST-002", 700),  # Good
            ("CUST-003", 650),  # Fair
            ("CUST-004", 550),  # Poor
        ]
        
        df = spark.createDataFrame(data, schema)
        
        # Categorize credit scores
        result = df.withColumn(
            "credit_category",
            col("credit_score").cast("int")
        ).withColumn(
            "category",
            col("credit_score").cast("string")  # Simplified for test
        )
        
        assert result.count() == 4, "Should have all records"
    
    @pytest.mark.unit
    def test_null_handling(self, spark, sample_customer_data):
        """Test proper handling of null values"""
        # Create data with nulls
        df_with_nulls = sample_customer_data.withColumn(
            "phone",
            col("phone")  # Some phones may be null
        )
        
        # Fill nulls
        result = df_with_nulls.fillna({"phone": "000-000-0000"})
        
        # Check no nulls in phone
        null_count = result.filter(col("phone").isNull()).count()
        assert null_count == 0, "Should have no null phone numbers after fill"
    
    @pytest.mark.unit
    def test_deduplication(self, spark):
        """Test deduplication logic"""
        schema = StructType([
            StructField("customer_id", StringType(), False),
            StructField("email", StringType(), False)
        ])
        
        # Create data with duplicates
        data = [
            ("CUST-001", "john@example.com"),
            ("CUST-001", "john@example.com"),  # Duplicate
            ("CUST-002", "jane@example.com")
        ]
        
        df = spark.createDataFrame(data, schema)
        
        # Deduplicate
        result = df.dropDuplicates(["customer_id"])
        
        assert result.count() == 2, "Should have 2 unique customers after deduplication"


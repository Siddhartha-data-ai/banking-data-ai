# Databricks notebook source
# MAGIC %md
# MAGIC # Data Quality Monitoring
# MAGIC 
# MAGIC Monitor data quality across all banking tables:
# MAGIC - Completeness checks
# MAGIC - Freshness validation
# MAGIC - Consistency rules
# MAGIC - Anomaly detection

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime, timedelta
import json

# COMMAND ----------

# Configuration
CATALOG = "banking_catalog"
BRONZE_SCHEMA = "banking_bronze"
SILVER_SCHEMA = "banking_silver"
GOLD_SCHEMA = "banking_gold"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Quality Rules

# COMMAND ----------

quality_rules = {
    f"{CATALOG}.{BRONZE_SCHEMA}.customers": {
        "min_row_count": 100,
        "max_null_percentage": {"email": 5, "phone": 5, "credit_score": 0},
        "freshness_hours": 24,
        "unique_keys": ["customer_id"]
    },
    f"{CATALOG}.{BRONZE_SCHEMA}.accounts": {
        "min_row_count": 100,
        "max_null_percentage": {"account_id": 0, "customer_id": 0, "balance": 0},
        "freshness_hours": 24,
        "unique_keys": ["account_id"]
    },
    f"{CATALOG}.{BRONZE_SCHEMA}.transactions": {
        "min_row_count": 1000,
        "max_null_percentage": {"transaction_id": 0, "amount": 0},
        "freshness_hours": 6,
        "unique_keys": ["transaction_id"]
    },
    f"{CATALOG}.{BRONZE_SCHEMA}.loans": {
        "min_row_count": 50,
        "max_null_percentage": {"loan_id": 0, "customer_id": 0},
        "freshness_hours": 24,
        "unique_keys": ["loan_id"]
    },
    f"{CATALOG}.{BRONZE_SCHEMA}.credit_cards": {
        "min_row_count": 100,
        "max_null_percentage": {"card_id": 0, "customer_id": 0},
        "freshness_hours": 24,
        "unique_keys": ["card_id"]
    }
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check Functions

# COMMAND ----------

def check_row_count(table_name, min_count):
    """Check if table has minimum row count"""
    try:
        df = spark.table(table_name)
        count = df.count()
        status = "PASS" if count >= min_count else "FAIL"
        return {
            "check": "Row Count",
            "table": table_name,
            "status": status,
            "actual": count,
            "expected": f">= {min_count}",
            "message": f"Table has {count} rows" if status == "PASS" else f"Table has only {count} rows, expected >= {min_count}"
        }
    except Exception as e:
        return {
            "check": "Row Count",
            "table": table_name,
            "status": "ERROR",
            "message": str(e)
        }

def check_null_percentage(table_name, null_rules):
    """Check null percentage for specified columns"""
    results = []
    try:
        df = spark.table(table_name)
        total_rows = df.count()
        
        for column, max_null_pct in null_rules.items():
            if column in df.columns:
                null_count = df.filter(col(column).isNull()).count()
                null_pct = (null_count / total_rows * 100) if total_rows > 0 else 0
                status = "PASS" if null_pct <= max_null_pct else "FAIL"
                
                results.append({
                    "check": "Null Percentage",
                    "table": table_name,
                    "column": column,
                    "status": status,
                    "actual": f"{null_pct:.2f}%",
                    "expected": f"<= {max_null_pct}%",
                    "message": f"Null percentage is {null_pct:.2f}%" if status == "PASS" else f"Null percentage {null_pct:.2f}% exceeds threshold {max_null_pct}%"
                })
    except Exception as e:
        results.append({
            "check": "Null Percentage",
            "table": table_name,
            "status": "ERROR",
            "message": str(e)
        })
    
    return results

def check_freshness(table_name, max_hours):
    """Check data freshness"""
    try:
        df = spark.table(table_name)
        
        # Try common timestamp columns
        timestamp_cols = [col for col in df.columns if 'created_at' in col.lower() or 'updated_at' in col.lower()]
        
        if not timestamp_cols:
            return {
                "check": "Freshness",
                "table": table_name,
                "status": "SKIP",
                "message": "No timestamp column found"
            }
        
        timestamp_col = timestamp_cols[0]
        max_timestamp = df.agg(max(col(timestamp_col))).collect()[0][0]
        
        if max_timestamp:
            hours_old = (datetime.now() - max_timestamp).total_seconds() / 3600
            status = "PASS" if hours_old <= max_hours else "FAIL"
            
            return {
                "check": "Freshness",
                "table": table_name,
                "status": status,
                "actual": f"{hours_old:.1f} hours old",
                "expected": f"<= {max_hours} hours",
                "message": f"Data is {hours_old:.1f} hours old" if status == "PASS" else f"Data is stale: {hours_old:.1f} hours old, expected <= {max_hours}"
            }
    except Exception as e:
        return {
            "check": "Freshness",
            "table": table_name,
            "status": "ERROR",
            "message": str(e)
        }

def check_uniqueness(table_name, key_columns):
    """Check uniqueness of key columns"""
    try:
        df = spark.table(table_name)
        total_rows = df.count()
        unique_rows = df.select(key_columns).distinct().count()
        
        status = "PASS" if total_rows == unique_rows else "FAIL"
        duplicates = total_rows - unique_rows
        
        return {
            "check": "Uniqueness",
            "table": table_name,
            "columns": ", ".join(key_columns),
            "status": status,
            "actual": f"{duplicates} duplicates",
            "expected": "0 duplicates",
            "message": "All keys are unique" if status == "PASS" else f"Found {duplicates} duplicate keys"
        }
    except Exception as e:
        return {
            "check": "Uniqueness",
            "table": table_name,
            "status": "ERROR",
            "message": str(e)
        }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Quality Checks

# COMMAND ----------

print("=" * 80)
print("DATA QUALITY MONITORING")
print(f"Timestamp: {datetime.now()}")
print("=" * 80)

all_results = []

for table_name, rules in quality_rules.items():
    print(f"\n{'='*80}")
    print(f"Checking: {table_name}")
    print(f"{'='*80}")
    
    # Row count check
    result = check_row_count(table_name, rules["min_row_count"])
    all_results.append(result)
    print(f"✓ {result['check']}: {result['status']} - {result['message']}")
    
    # Null percentage checks
    null_results = check_null_percentage(table_name, rules["max_null_percentage"])
    for result in null_results:
        all_results.append(result)
        icon = "✓" if result['status'] == "PASS" else "✗"
        print(f"{icon} {result['check']} ({result['column']}): {result['status']} - {result['message']}")
    
    # Freshness check
    result = check_freshness(table_name, rules["freshness_hours"])
    all_results.append(result)
    icon = "✓" if result['status'] in ["PASS", "SKIP"] else "✗"
    print(f"{icon} {result['check']}: {result['status']} - {result['message']}")
    
    # Uniqueness check
    result = check_uniqueness(table_name, rules["unique_keys"])
    all_results.append(result)
    icon = "✓" if result['status'] == "PASS" else "✗"
    print(f"{icon} {result['check']}: {result['status']} - {result['message']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

# Create summary DataFrame
results_df = spark.createDataFrame(all_results)

print("\n" + "=" * 80)
print("QUALITY CHECKS SUMMARY")
print("=" * 80)

# Count by status
status_counts = results_df.groupBy("status").count().orderBy("status")
print("\nStatus Distribution:")
status_counts.show()

# Failed checks
failed_checks = results_df.filter(col("status") == "FAIL")
if failed_checks.count() > 0:
    print(f"\n⚠️  {failed_checks.count()} FAILED CHECKS:")
    failed_checks.select("check", "table", "message").show(truncate=False)
else:
    print("\n✓ All quality checks passed!")

# Error checks
error_checks = results_df.filter(col("status") == "ERROR")
if error_checks.count() > 0:
    print(f"\n✗ {error_checks.count()} ERROR CHECKS:")
    error_checks.select("check", "table", "message").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Results

# COMMAND ----------

# Save results to Delta table
results_df_with_timestamp = results_df.withColumn("check_timestamp", lit(datetime.now()))

output_table = f"{CATALOG}.{SILVER_SCHEMA}.data_quality_results"

results_df_with_timestamp.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(output_table)

print(f"\n✓ Results saved to {output_table}")

# COMMAND ----------

print(f"\n{'='*80}")
print("Data Quality Monitoring Complete")
print(f"{'='*80}")


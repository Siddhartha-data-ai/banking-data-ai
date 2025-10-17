# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline Monitoring Dashboard
# MAGIC 
# MAGIC Real-time monitoring of banking data pipelines:
# MAGIC - Pipeline health status
# MAGIC - Processing metrics
# MAGIC - Error tracking
# MAGIC - Performance trends

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
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
# MAGIC ## Table Metrics

# COMMAND ----------

def get_table_metrics(table_name):
    """Get metrics for a table"""
    try:
        df = spark.table(table_name)
        
        # Basic metrics
        row_count = df.count()
        column_count = len(df.columns)
        
        # Size metrics
        table_info = spark.sql(f"DESCRIBE DETAIL {table_name}").collect()[0]
        size_bytes = table_info['sizeInBytes'] if 'sizeInBytes' in table_info.asDict() else 0
        size_mb = size_bytes / (1024 * 1024)
        
        # Timestamp columns for freshness
        timestamp_cols = [col for col in df.columns if any(x in col.lower() for x in ['created', 'updated', 'date', 'time'])]
        
        latest_timestamp = None
        if timestamp_cols:
            try:
                latest_timestamp = df.agg(max(col(timestamp_cols[0]))).collect()[0][0]
            except:
                pass
        
        return {
            "table_name": table_name,
            "row_count": row_count,
            "column_count": column_count,
            "size_mb": round(size_mb, 2),
            "latest_timestamp": latest_timestamp,
            "status": "Healthy" if row_count > 0 else "Empty"
        }
    except Exception as e:
        return {
            "table_name": table_name,
            "status": "Error",
            "error": str(e)
        }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Layer Status

# COMMAND ----------

print("=" * 80)
print("BANKING DATA PIPELINE MONITORING")
print(f"Timestamp: {datetime.now()}")
print("=" * 80)

# Bronze Layer
print("\n" + "=" * 80)
print("BRONZE LAYER (Raw Data)")
print("=" * 80)

bronze_tables = [
    f"{CATALOG}.{BRONZE_SCHEMA}.customers",
    f"{CATALOG}.{BRONZE_SCHEMA}.accounts",
    f"{CATALOG}.{BRONZE_SCHEMA}.transactions",
    f"{CATALOG}.{BRONZE_SCHEMA}.loans",
    f"{CATALOG}.{BRONZE_SCHEMA}.credit_cards"
]

bronze_metrics = []
for table in bronze_tables:
    metrics = get_table_metrics(table)
    bronze_metrics.append(metrics)
    status_icon = "✓" if metrics['status'] == "Healthy" else "✗"
    print(f"{status_icon} {table}")
    print(f"   Rows: {metrics.get('row_count', 'N/A'):,}")
    print(f"   Size: {metrics.get('size_mb', 'N/A')} MB")
    if 'latest_timestamp' in metrics and metrics['latest_timestamp']:
        print(f"   Latest: {metrics['latest_timestamp']}")

# COMMAND ----------

# Silver Layer
print("\n" + "=" * 80)
print("SILVER LAYER (Cleaned Data)")
print("=" * 80)

silver_tables = [
    f"{CATALOG}.{SILVER_SCHEMA}.customers_clean",
    f"{CATALOG}.{SILVER_SCHEMA}.accounts_clean",
    f"{CATALOG}.{SILVER_SCHEMA}.transactions_clean",
    f"{CATALOG}.{SILVER_SCHEMA}.loans_clean",
    f"{CATALOG}.{SILVER_SCHEMA}.credit_cards_clean"
]

silver_metrics = []
for table in silver_tables:
    metrics = get_table_metrics(table)
    silver_metrics.append(metrics)
    status_icon = "✓" if metrics['status'] == "Healthy" else "✗"
    print(f"{status_icon} {table}")
    print(f"   Rows: {metrics.get('row_count', 'N/A'):,}")
    print(f"   Size: {metrics.get('size_mb', 'N/A')} MB")

# COMMAND ----------

# Gold Layer
print("\n" + "=" * 80)
print("GOLD LAYER (Business Analytics)")
print("=" * 80)

gold_tables = [
    f"{CATALOG}.{GOLD_SCHEMA}.customer_360",
    f"{CATALOG}.{GOLD_SCHEMA}.fraud_alerts",
    f"{CATALOG}.{GOLD_SCHEMA}.customer_fraud_profiles",
    f"{CATALOG}.{GOLD_SCHEMA}.fraud_analytics"
]

gold_metrics = []
for table in gold_tables:
    metrics = get_table_metrics(table)
    gold_metrics.append(metrics)
    status_icon = "✓" if metrics['status'] == "Healthy" else "✗"
    print(f"{status_icon} {table}")
    print(f"   Rows: {metrics.get('row_count', 'N/A'):,}")
    print(f"   Size: {metrics.get('size_mb', 'N/A')} MB")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Flow Verification

# COMMAND ----------

print("\n" + "=" * 80)
print("DATA FLOW VERIFICATION")
print("=" * 80)

# Check that silver has data from bronze
bronze_customers = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.customers").count()
silver_customers = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.customers_clean").count()
coverage_pct = (silver_customers / bronze_customers * 100) if bronze_customers > 0 else 0

print(f"\nCustomers:")
print(f"  Bronze: {bronze_customers:,}")
print(f"  Silver: {silver_customers:,}")
print(f"  Coverage: {coverage_pct:.1f}%")
status = "✓" if coverage_pct >= 95 else "⚠️"
print(f"  Status: {status}")

# Check transactions
bronze_trans = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.transactions").count()
silver_trans = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.transactions_clean").count()
trans_coverage = (silver_trans / bronze_trans * 100) if bronze_trans > 0 else 0

print(f"\nTransactions:")
print(f"  Bronze: {bronze_trans:,}")
print(f"  Silver: {silver_trans:,}")
print(f"  Coverage: {trans_coverage:.1f}%")
status = "✓" if trans_coverage >= 95 else "⚠️"
print(f"  Status: {status}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Business Metrics

# COMMAND ----------

print("\n" + "=" * 80)
print("KEY BUSINESS METRICS")
print("=" * 80)

# Total accounts and balances
accounts_df = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.accounts_clean")
total_accounts = accounts_df.count()
total_balance = accounts_df.agg(sum("balance")).collect()[0][0]
active_accounts = accounts_df.filter(col("account_status") == "Active").count()

print(f"\nAccounts:")
print(f"  Total: {total_accounts:,}")
print(f"  Active: {active_accounts:,}")
print(f"  Total Balance: ${total_balance:,.2f}")

# Transactions (last 24h)
trans_df = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.transactions_clean")
recent_trans = trans_df.filter(
    col("transaction_date") >= current_timestamp() - expr("INTERVAL 24 HOURS")
)
trans_24h = recent_trans.count()
volume_24h = recent_trans.agg(sum("amount")).collect()[0][0] or 0

print(f"\nTransactions (24h):")
print(f"  Count: {trans_24h:,}")
print(f"  Volume: ${volume_24h:,.2f}")

# Fraud alerts
fraud_df = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.fraud_alerts")
total_alerts = fraud_df.count()
critical_alerts = fraud_df.filter(col("alert_severity") == "Critical").count()

print(f"\nFraud Alerts:")
print(f"  Total: {total_alerts:,}")
print(f"  Critical: {critical_alerts:,}")

# Loans
loans_df = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.loans_clean")
active_loans = loans_df.filter(col("loan_status_clean") == "Active").count()
total_loan_balance = loans_df.agg(sum("remaining_balance")).collect()[0][0] or 0
delinquent_loans = loans_df.filter(col("is_delinquent") == True).count()

print(f"\nLoans:")
print(f"  Active: {active_loans:,}")
print(f"  Outstanding: ${total_loan_balance:,.2f}")
print(f"  Delinquent: {delinquent_loans:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Health Summary

# COMMAND ----------

print("\n" + "=" * 80)
print("PIPELINE HEALTH SUMMARY")
print("=" * 80)

# Count healthy vs unhealthy tables
all_metrics = bronze_metrics + silver_metrics + gold_metrics
total_tables = len(all_metrics)
healthy_tables = sum(1 for m in all_metrics if m['status'] == 'Healthy')
empty_tables = sum(1 for m in all_metrics if m['status'] == 'Empty')
error_tables = sum(1 for m in all_metrics if m['status'] == 'Error')

print(f"\nTable Status:")
print(f"  Total Tables: {total_tables}")
print(f"  Healthy: {healthy_tables} ({healthy_tables/total_tables*100:.1f}%)")
print(f"  Empty: {empty_tables}")
print(f"  Errors: {error_tables}")

overall_health = "✓ HEALTHY" if error_tables == 0 and empty_tables == 0 else "⚠️ ATTENTION NEEDED"
print(f"\nOverall Status: {overall_health}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Monitoring Results

# COMMAND ----------

# Create monitoring record
monitoring_record = {
    "timestamp": datetime.now(),
    "bronze_row_count": sum(m.get('row_count', 0) for m in bronze_metrics),
    "silver_row_count": sum(m.get('row_count', 0) for m in silver_metrics),
    "gold_row_count": sum(m.get('row_count', 0) for m in gold_metrics),
    "total_tables": total_tables,
    "healthy_tables": healthy_tables,
    "error_tables": error_tables,
    "overall_status": overall_health,
    "total_account_balance": float(total_balance) if total_balance else 0,
    "transactions_24h": trans_24h,
    "fraud_alerts": total_alerts
}

monitoring_df = spark.createDataFrame([monitoring_record])

output_table = f"{CATALOG}.{SILVER_SCHEMA}.pipeline_monitoring"

monitoring_df.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(output_table)

print(f"\n✓ Monitoring results saved to {output_table}")

# COMMAND ----------

print(f"\n{'='*80}")
print("Pipeline Monitoring Complete")
print(f"{'='*80}")


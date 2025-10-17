# Databricks notebook source
# MAGIC %md
# MAGIC # Run All ML Predictions
# MAGIC 
# MAGIC Execute all machine learning models for comprehensive banking analytics:
# MAGIC - Fraud detection on recent transactions
# MAGIC - Credit risk assessment for all customers
# MAGIC - Customer churn prediction
# MAGIC - Loan default prediction

# COMMAND ----------

import sys
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

CATALOG = "banking_catalog"
SILVER_SCHEMA = "banking_silver"
GOLD_SCHEMA = "banking_gold"

print(f"Starting ML predictions at {datetime.now()}")
print(f"Catalog: {CATALOG}")
print(f"Silver Schema: {SILVER_SCHEMA}")
print(f"Gold Schema: {GOLD_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Fraud Detection

# COMMAND ----------

print("\n" + "="*60)
print("1. FRAUD DETECTION")
print("="*60)

try:
    # Run fraud detection model
    dbutils.notebook.run(
        "./predict_fraud_enhanced",
        timeout_seconds=0,
        arguments={}
    )
    print("✓ Fraud detection completed successfully")
except Exception as e:
    print(f"✗ Fraud detection failed: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Credit Risk Assessment

# COMMAND ----------

print("\n" + "="*60)
print("2. CREDIT RISK ASSESSMENT")
print("="*60)

try:
    # Run credit risk model
    dbutils.notebook.run(
        "./predict_credit_risk",
        timeout_seconds=0,
        arguments={}
    )
    print("✓ Credit risk assessment completed successfully")
except Exception as e:
    print(f"✗ Credit risk assessment failed: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Customer Churn Prediction

# COMMAND ----------

print("\n" + "="*60)
print("3. CUSTOMER CHURN PREDICTION")
print("="*60)

try:
    # Run churn prediction model
    dbutils.notebook.run(
        "./predict_customer_churn",
        timeout_seconds=0,
        arguments={}
    )
    print("✓ Churn prediction completed successfully")
except Exception as e:
    print(f"✗ Churn prediction failed: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Loan Default Prediction

# COMMAND ----------

print("\n" + "="*60)
print("4. LOAN DEFAULT PREDICTION")
print("="*60)

try:
    # Run loan default model
    dbutils.notebook.run(
        "./predict_loan_default",
        timeout_seconds=0,
        arguments={}
    )
    print("✓ Loan default prediction completed successfully")
except Exception as e:
    print(f"✗ Loan default prediction failed: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("\n" + "="*60)
print("ML PREDICTIONS SUMMARY")
print("="*60)
print(f"Completed at: {datetime.now()}")
print("\nAll ML models executed. Check individual model outputs for details.")
print("\nGenerated Tables:")
print(f"- {GOLD_SCHEMA}.fraud_alerts")
print(f"- {GOLD_SCHEMA}.customer_credit_risk_scores")
print(f"- {GOLD_SCHEMA}.customer_churn_predictions")
print(f"- {GOLD_SCHEMA}.loan_default_predictions")
print("\nMLflow Experiments:")
print("- /banking/fraud_detection")
print("- /banking/credit_risk_assessment")
print("- /banking/churn_prediction")
print("- /banking/loan_default_prediction")
print("\n✓ All predictions complete!")


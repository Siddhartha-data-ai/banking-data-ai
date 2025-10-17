# Databricks notebook source
# MAGIC %md
# MAGIC # Credit Risk Assessment ML Model
# MAGIC 
# MAGIC Build and deploy a model to assess customer credit risk:
# MAGIC - Feature engineering from customer, account, and transaction data
# MAGIC - Gradient Boosting classifier for risk prediction
# MAGIC - Credit score adjustment recommendations
# MAGIC - MLflow tracking and deployment

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import BinaryClassificationEvaluator, RegressionEvaluator
import mlflow
import mlflow.spark
from datetime import datetime

# COMMAND ----------

# Configuration
CATALOG = "banking_catalog"
SILVER_SCHEMA = "banking_silver"
GOLD_SCHEMA = "banking_gold"
MODEL_NAME = "banking_credit_risk"

mlflow.set_experiment("/banking/credit_risk_assessment")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Customer 360 Data

# COMMAND ----------

# Load comprehensive customer data
customer_360 = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.customer_360")

print(f"Total customers: {customer_360.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature Engineering

# COMMAND ----------

# Create target variable: High risk customers
# High risk = credit score < 650 OR delinquent loans/cards OR default
features_df = customer_360.select(
    col("customer_id"),
    # Target: 1 = High Risk, 0 = Low/Medium Risk
    when(
        (col("credit_score") < 650) |
        (coalesce(col("delinquent_loans"), lit(0)) > 0) |
        (coalesce(col("delinquent_credit_cards"), lit(0)) > 0) |
        (coalesce(col("defaulted_loans"), lit(0)) > 0) |
        (col("debt_to_income_ratio") > 0.43),
        1
    ).otherwise(0).alias("label"),
    # Features
    col("credit_score"),
    col("annual_income"),
    col("age"),
    col("customer_tenure_years"),
    coalesce(col("total_accounts"), lit(0)).alias("total_accounts"),
    coalesce(col("total_account_balance"), lit(0)).alias("total_account_balance"),
    coalesce(col("total_assets"), lit(0)).alias("total_assets"),
    coalesce(col("total_liabilities"), lit(0)).alias("total_liabilities"),
    coalesce(col("debt_to_income_ratio"), lit(0)).alias("debt_to_income_ratio"),
    coalesce(col("transactions_90d"), lit(0)).alias("transactions_90d"),
    coalesce(col("transaction_volume_90d"), lit(0)).alias("transaction_volume_90d"),
    coalesce(col("total_loans"), lit(0)).alias("total_loans"),
    coalesce(col("total_loan_balance"), lit(0)).alias("total_loan_balance"),
    coalesce(col("total_credit_cards"), lit(0)).alias("total_credit_cards"),
    coalesce(col("total_credit_card_balance"), lit(0)).alias("total_credit_card_balance"),
    coalesce(col("avg_credit_utilization"), lit(0)).alias("avg_credit_utilization"),
    coalesce(col("delinquent_loans"), lit(0)).alias("delinquent_loans"),
    coalesce(col("delinquent_credit_cards"), lit(0)).alias("delinquent_credit_cards"),
    coalesce(col("fraud_incidents"), lit(0)).alias("fraud_incidents"),
    col("product_count"),
    col("customer_health_score"),
    col("churn_risk_score")
).fillna(0)

# Show class distribution
print("Risk distribution:")
features_df.groupBy("label").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Feature Vector

# COMMAND ----------

feature_cols = [
    "credit_score", "annual_income", "age", "customer_tenure_years",
    "total_accounts", "total_account_balance", "total_assets", "total_liabilities",
    "debt_to_income_ratio", "transactions_90d", "transaction_volume_90d",
    "total_loans", "total_loan_balance", "total_credit_cards",
    "total_credit_card_balance", "avg_credit_utilization",
    "delinquent_loans", "delinquent_credit_cards", "fraud_incidents",
    "product_count", "customer_health_score", "churn_risk_score"
]

assembler = VectorAssembler(
    inputCols=feature_cols,
    outputCol="features_raw",
    handleInvalid="skip"
)

scaler = StandardScaler(
    inputCol="features_raw",
    outputCol="features",
    withStd=True,
    withMean=False
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Split Data

# COMMAND ----------

train_df, test_df = features_df.randomSplit([0.8, 0.2], seed=42)

print(f"Training set: {train_df.count()}")
print(f"Test set: {test_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train Model

# COMMAND ----------

with mlflow.start_run(run_name="credit_risk_gbt") as run:
    
    mlflow.log_param("model_type", "GradientBoostedTrees")
    mlflow.log_param("max_iter", 100)
    mlflow.log_param("max_depth", 5)
    
    # GBT Classifier
    gbt = GBTClassifier(
        featuresCol="features",
        labelCol="label",
        predictionCol="prediction",
        maxIter=100,
        maxDepth=5,
        seed=42
    )
    
    # Pipeline
    pipeline = Pipeline(stages=[assembler, scaler, gbt])
    
    # Train
    print("Training model...")
    model = pipeline.fit(train_df)
    print("✓ Model trained")
    
    # Predictions
    predictions = model.transform(test_df)
    
    # Evaluate
    evaluator_auc = BinaryClassificationEvaluator(
        labelCol="label",
        rawPredictionCol="rawPrediction",
        metricName="areaUnderROC"
    )
    
    evaluator_pr = BinaryClassificationEvaluator(
        labelCol="label",
        rawPredictionCol="rawPrediction",
        metricName="areaUnderPR"
    )
    
    auc = evaluator_auc.evaluate(predictions)
    pr_auc = evaluator_pr.evaluate(predictions)
    
    mlflow.log_metric("auc_roc", auc)
    mlflow.log_metric("auc_pr", pr_auc)
    
    print(f"\n=== Model Performance ===")
    print(f"AUC-ROC: {auc:.4f}")
    print(f"AUC-PR: {pr_auc:.4f}")
    
    # Confusion Matrix
    print("\nConfusion Matrix:")
    predictions.groupBy("label", "prediction").count().orderBy("label", "prediction").show()
    
    # Log and register model
    mlflow.spark.log_model(model, "model")
    model_details = mlflow.register_model(f"runs:/{run.info.run_id}/model", MODEL_NAME)
    
    print(f"\n✓ Model registered: {MODEL_NAME}")
    print(f"✓ Version: {model_details.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature Importance

# COMMAND ----------

gbt_model = model.stages[-1]
importances = gbt_model.featureImportances.toArray()

importance_df = spark.createDataFrame(
    [(feature_cols[i], float(importances[i])) for i in range(len(feature_cols))],
    ["feature", "importance"]
).orderBy(desc("importance"))

print("Feature Importance:")
importance_df.show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Score Customers

# COMMAND ----------

def assess_credit_risk(customer_table, output_table):
    """
    Assess credit risk for all customers
    """
    model_uri = f"models:/{MODEL_NAME}/latest"
    loaded_model = mlflow.spark.load_model(model_uri)
    
    customers = spark.table(customer_table)
    
    # Prepare features
    features_df = customers.select(
        col("customer_id"),
        col("credit_score"),
        col("annual_income"),
        col("age"),
        col("customer_tenure_years"),
        coalesce(col("total_accounts"), lit(0)).alias("total_accounts"),
        coalesce(col("total_account_balance"), lit(0)).alias("total_account_balance"),
        coalesce(col("total_assets"), lit(0)).alias("total_assets"),
        coalesce(col("total_liabilities"), lit(0)).alias("total_liabilities"),
        coalesce(col("debt_to_income_ratio"), lit(0)).alias("debt_to_income_ratio"),
        coalesce(col("transactions_90d"), lit(0)).alias("transactions_90d"),
        coalesce(col("transaction_volume_90d"), lit(0)).alias("transaction_volume_90d"),
        coalesce(col("total_loans"), lit(0)).alias("total_loans"),
        coalesce(col("total_loan_balance"), lit(0)).alias("total_loan_balance"),
        coalesce(col("total_credit_cards"), lit(0)).alias("total_credit_cards"),
        coalesce(col("total_credit_card_balance"), lit(0)).alias("total_credit_card_balance"),
        coalesce(col("avg_credit_utilization"), lit(0)).alias("avg_credit_utilization"),
        coalesce(col("delinquent_loans"), lit(0)).alias("delinquent_loans"),
        coalesce(col("delinquent_credit_cards"), lit(0)).alias("delinquent_credit_cards"),
        coalesce(col("fraud_incidents"), lit(0)).alias("fraud_incidents"),
        col("product_count"),
        col("customer_health_score"),
        col("churn_risk_score")
    ).fillna(0)
    
    # Score
    predictions = loaded_model.transform(features_df)
    
    results = predictions.select(
        "customer_id",
        col("prediction").cast("int").alias("high_risk_flag"),
        when(col("prediction") == 1, "High Risk")
        .otherwise("Low Risk").alias("risk_category")
    )
    
    results.write.format("delta").mode("overwrite").saveAsTable(output_table)
    
    print(f"✓ Assessed {results.count()} customers")
    print(f"✓ Results saved to {output_table}")
    
    return results

# Example usage
# risk_scores = assess_credit_risk(
#     f"{CATALOG}.{GOLD_SCHEMA}.customer_360",
#     f"{CATALOG}.{GOLD_SCHEMA}.customer_credit_risk_scores"
# )

# COMMAND ----------

print(f"""
✓ Credit Risk Model Complete

Model: {MODEL_NAME}
Type: Gradient Boosted Trees
Features: {len(feature_cols)}

Use the model to:
- Assess new loan applications
- Monitor existing customer risk
- Set credit limits
- Identify customers for risk intervention
""")


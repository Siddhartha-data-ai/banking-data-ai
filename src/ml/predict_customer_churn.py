# Databricks notebook source
# MAGIC %md
# MAGIC # Customer Churn Prediction ML Model
# MAGIC 
# MAGIC Build and deploy a model to predict customer churn:
# MAGIC - Identify at-risk customers before they leave
# MAGIC - Feature engineering from engagement metrics
# MAGIC - Proactive retention strategies
# MAGIC - MLflow tracking and deployment

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
import mlflow
import mlflow.spark

# COMMAND ----------

# Configuration
CATALOG = "banking_catalog"
GOLD_SCHEMA = "banking_gold"
MODEL_NAME = "banking_churn_prediction"

mlflow.set_experiment("/banking/churn_prediction")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Customer Data

# COMMAND ----------

customer_360 = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.customer_360")

print(f"Total customers: {customer_360.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Churn

# COMMAND ----------

# Define churn indicators:
# - No transactions in 90 days
# - All accounts dormant
# - Account status not active
# - High churn risk score

features_df = customer_360.select(
    col("customer_id"),
    # Target: Likely to churn (1) or not (0)
    when(
        (col("transactions_90d") == 0) |
        (col("dormant_accounts") > 0) |
        (col("account_status") != "Active") |
        (col("churn_risk_score") >= 70),
        1
    ).otherwise(0).alias("label"),
    # Features
    col("customer_tenure_years"),
    col("age"),
    col("credit_score"),
    col("annual_income"),
    coalesce(col("product_count"), lit(0)).alias("product_count"),
    coalesce(col("total_accounts"), lit(0)).alias("total_accounts"),
    coalesce(col("active_accounts"), lit(0)).alias("active_accounts"),
    coalesce(col("dormant_accounts"), lit(0)).alias("dormant_accounts"),
    coalesce(col("total_account_balance"), lit(0)).alias("total_account_balance"),
    coalesce(col("transactions_90d"), lit(0)).alias("transactions_90d"),
    coalesce(col("transaction_volume_90d"), lit(0)).alias("transaction_volume_90d"),
    coalesce(col("avg_transaction_amount"), lit(0)).alias("avg_transaction_amount"),
    coalesce(col("net_flow_90d"), lit(0)).alias("net_flow_90d"),
    coalesce(col("total_loans"), lit(0)).alias("total_loans"),
    coalesce(col("delinquent_loans"), lit(0)).alias("delinquent_loans"),
    coalesce(col("total_credit_cards"), lit(0)).alias("total_credit_cards"),
    coalesce(col("avg_credit_utilization"), lit(0)).alias("avg_credit_utilization"),
    coalesce(col("fraud_incidents"), lit(0)).alias("fraud_incidents"),
    col("customer_health_score"),
    col("churn_risk_score"),
    col("customer_segment"),
    col("engagement_level")
).fillna(0)

# Show distribution
print("Churn distribution:")
features_df.groupBy("label").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature Engineering

# COMMAND ----------

# Index categorical variables
segment_indexer = StringIndexer(
    inputCol="customer_segment",
    outputCol="segment_index",
    handleInvalid="keep"
)

engagement_indexer = StringIndexer(
    inputCol="engagement_level",
    outputCol="engagement_index",
    handleInvalid="keep"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Feature Vector

# COMMAND ----------

feature_cols = [
    "customer_tenure_years", "age", "credit_score", "annual_income",
    "product_count", "total_accounts", "active_accounts", "dormant_accounts",
    "total_account_balance", "transactions_90d", "transaction_volume_90d",
    "avg_transaction_amount", "net_flow_90d", "total_loans", "delinquent_loans",
    "total_credit_cards", "avg_credit_utilization", "fraud_incidents",
    "customer_health_score", "churn_risk_score",
    "segment_index", "engagement_index"
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
# MAGIC ## Train Model

# COMMAND ----------

train_df, test_df = features_df.randomSplit([0.8, 0.2], seed=42)

print(f"Training set: {train_df.count()}")
print(f"Test set: {test_df.count()}")

# COMMAND ----------

with mlflow.start_run(run_name="churn_prediction_rf") as run:
    
    mlflow.log_param("model_type", "RandomForest")
    mlflow.log_param("num_trees", 100)
    mlflow.log_param("max_depth", 10)
    
    # Random Forest
    rf = RandomForestClassifier(
        featuresCol="features",
        labelCol="label",
        predictionCol="prediction",
        probabilityCol="probability",
        numTrees=100,
        maxDepth=10,
        seed=42
    )
    
    # Pipeline
    pipeline = Pipeline(stages=[
        segment_indexer,
        engagement_indexer,
        assembler,
        scaler,
        rf
    ])
    
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
    
    multi_evaluator = MulticlassClassificationEvaluator(
        labelCol="label",
        predictionCol="prediction"
    )
    
    auc = evaluator_auc.evaluate(predictions)
    pr_auc = evaluator_pr.evaluate(predictions)
    accuracy = multi_evaluator.evaluate(predictions, {multi_evaluator.metricName: "accuracy"})
    precision = multi_evaluator.evaluate(predictions, {multi_evaluator.metricName: "weightedPrecision"})
    recall = multi_evaluator.evaluate(predictions, {multi_evaluator.metricName: "weightedRecall"})
    f1 = multi_evaluator.evaluate(predictions, {multi_evaluator.metricName: "f1"})
    
    mlflow.log_metric("auc_roc", auc)
    mlflow.log_metric("auc_pr", pr_auc)
    mlflow.log_metric("accuracy", accuracy)
    mlflow.log_metric("precision", precision)
    mlflow.log_metric("recall", recall)
    mlflow.log_metric("f1_score", f1)
    
    print(f"\n=== Model Performance ===")
    print(f"AUC-ROC: {auc:.4f}")
    print(f"AUC-PR: {pr_auc:.4f}")
    print(f"Accuracy: {accuracy:.4f}")
    print(f"Precision: {precision:.4f}")
    print(f"Recall: {recall:.4f}")
    print(f"F1 Score: {f1:.4f}")
    
    print("\nConfusion Matrix:")
    predictions.groupBy("label", "prediction").count().orderBy("label", "prediction").show()
    
    # Log and register
    mlflow.spark.log_model(model, "model")
    model_details = mlflow.register_model(f"runs:/{run.info.run_id}/model", MODEL_NAME)
    
    print(f"\n✓ Model registered: {MODEL_NAME}")
    print(f"✓ Version: {model_details.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature Importance

# COMMAND ----------

rf_model = model.stages[-1]
importances = rf_model.featureImportances.toArray()

importance_df = spark.createDataFrame(
    [(feature_cols[i], float(importances[i])) for i in range(len(feature_cols))],
    ["feature", "importance"]
).orderBy(desc("importance"))

print("Top Features for Churn Prediction:")
importance_df.show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Identify At-Risk Customers

# COMMAND ----------

def identify_churn_risk(customer_table, output_table):
    """
    Identify customers at risk of churning
    """
    model_uri = f"models:/{MODEL_NAME}/latest"
    loaded_model = mlflow.spark.load_model(model_uri)
    
    customers = spark.table(customer_table)
    
    # Prepare features (same as training)
    features_df = customers.select(
        col("customer_id"),
        col("full_name"),
        col("email"),
        col("customer_tenure_years"),
        col("age"),
        col("credit_score"),
        col("annual_income"),
        coalesce(col("product_count"), lit(0)).alias("product_count"),
        coalesce(col("total_accounts"), lit(0)).alias("total_accounts"),
        coalesce(col("active_accounts"), lit(0)).alias("active_accounts"),
        coalesce(col("dormant_accounts"), lit(0)).alias("dormant_accounts"),
        coalesce(col("total_account_balance"), lit(0)).alias("total_account_balance"),
        coalesce(col("transactions_90d"), lit(0)).alias("transactions_90d"),
        coalesce(col("transaction_volume_90d"), lit(0)).alias("transaction_volume_90d"),
        coalesce(col("avg_transaction_amount"), lit(0)).alias("avg_transaction_amount"),
        coalesce(col("net_flow_90d"), lit(0)).alias("net_flow_90d"),
        coalesce(col("total_loans"), lit(0)).alias("total_loans"),
        coalesce(col("delinquent_loans"), lit(0)).alias("delinquent_loans"),
        coalesce(col("total_credit_cards"), lit(0)).alias("total_credit_cards"),
        coalesce(col("avg_credit_utilization"), lit(0)).alias("avg_credit_utilization"),
        coalesce(col("fraud_incidents"), lit(0)).alias("fraud_incidents"),
        col("customer_health_score"),
        col("churn_risk_score"),
        col("customer_segment"),
        col("engagement_level")
    ).fillna(0)
    
    # Score
    predictions = loaded_model.transform(features_df)
    
    # Extract churn probability
    churn_probability_udf = udf(lambda v: float(v[1]), DoubleType())
    
    results = predictions.select(
        "customer_id",
        "full_name",
        "email",
        "customer_segment",
        "product_count",
        "transactions_90d",
        col("prediction").cast("int").alias("churn_prediction"),
        churn_probability_udf("probability").alias("churn_probability")
    ).withColumn("retention_priority",
        case(
            when(col("churn_probability") > 0.8, "Critical"),
            when(col("churn_probability") > 0.6, "High"),
            when(col("churn_probability") > 0.4, "Medium")
        ).otherwise("Low")
    )
    
    # Save results
    results.write.format("delta").mode("overwrite").saveAsTable(output_table)
    
    print(f"✓ Scored {results.count()} customers")
    print(f"✓ Results saved to {output_table}")
    
    # Show high-risk customers
    print("\nHigh-Risk Customers (Sample):")
    results.filter(col("retention_priority").isin(["Critical", "High"])) \
        .orderBy(desc("churn_probability")) \
        .show(10, truncate=False)
    
    return results

# Example usage
# at_risk = identify_churn_risk(
#     f"{CATALOG}.{GOLD_SCHEMA}.customer_360",
#     f"{CATALOG}.{GOLD_SCHEMA}.customer_churn_predictions"
# )

# COMMAND ----------

print(f"""
✓ Churn Prediction Model Complete

Model: {MODEL_NAME}
Type: Random Forest
Features: {len(feature_cols)}

Use for:
- Proactive customer retention
- Targeted marketing campaigns
- Service improvement initiatives
- Customer success outreach
""")


# Databricks notebook source
# MAGIC %md
# MAGIC # Fraud Detection ML Model
# MAGIC 
# MAGIC Build and deploy a machine learning model to detect fraudulent transactions:
# MAGIC - Feature engineering from transaction patterns
# MAGIC - Random Forest classifier with MLflow tracking
# MAGIC - Model deployment to registry
# MAGIC - Batch and real-time scoring

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
from pyspark.ml.classification import RandomForestClassifier, GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
import mlflow
import mlflow.spark
from datetime import datetime

# COMMAND ----------

# Configuration
CATALOG = "banking_catalog"
SILVER_SCHEMA = "banking_silver"
MODEL_NAME = "banking_fraud_detection"

# MLflow setup
mlflow.set_experiment(f"/banking/fraud_detection")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load and Prepare Data

# COMMAND ----------

# Load transactions
transactions_df = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.transactions_clean")

# Filter completed transactions only
df = transactions_df.filter(col("status") == "Completed")

print(f"Total transactions: {df.count()}")
print(f"Fraudulent transactions: {df.filter(col('is_fraud') == True).count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature Engineering

# COMMAND ----------

# Create features
features_df = df.select(
    col("transaction_id"),
    col("is_fraud").cast("int").alias("label"),
    col("amount"),
    col("transaction_hour"),
    col("transaction_day_of_week"),
    col("is_international").cast("int").alias("is_international_flag"),
    col("is_night_transaction").cast("int").alias("is_night_flag"),
    col("is_weekend").cast("int").alias("is_weekend_flag"),
    col("transactions_24h"),
    col("amount_24h"),
    col("channel"),
    col("transaction_type"),
    when(col("merchant_category").isNotNull(), col("merchant_category")).otherwise("NONE").alias("merchant_category")
)

# Show class distribution
print("Class distribution:")
features_df.groupBy("label").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Encode Categorical Variables

# COMMAND ----------

# String indexers for categorical features
channel_indexer = StringIndexer(inputCol="channel", outputCol="channel_index", handleInvalid="keep")
type_indexer = StringIndexer(inputCol="transaction_type", outputCol="type_index", handleInvalid="keep")
merchant_indexer = StringIndexer(inputCol="merchant_category", outputCol="merchant_index", handleInvalid="keep")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Feature Vector

# COMMAND ----------

# Assemble features
feature_cols = [
    "amount", "transaction_hour", "transaction_day_of_week",
    "is_international_flag", "is_night_flag", "is_weekend_flag",
    "transactions_24h", "amount_24h",
    "channel_index", "type_index", "merchant_index"
]

assembler = VectorAssembler(
    inputCols=feature_cols,
    outputCol="features_raw",
    handleInvalid="skip"
)

# Scale features
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

# Split into train and test (80/20)
train_df, test_df = features_df.randomSplit([0.8, 0.2], seed=42)

print(f"Training set: {train_df.count()}")
print(f"Test set: {test_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train Model with MLflow

# COMMAND ----------

with mlflow.start_run(run_name="fraud_detection_rf") as run:
    
    # Log parameters
    mlflow.log_param("model_type", "RandomForest")
    mlflow.log_param("num_trees", 100)
    mlflow.log_param("max_depth", 10)
    mlflow.log_param("train_size", train_df.count())
    mlflow.log_param("test_size", test_df.count())
    
    # Create Random Forest classifier
    rf = RandomForestClassifier(
        featuresCol="features",
        labelCol="label",
        predictionCol="prediction",
        probabilityCol="probability",
        numTrees=100,
        maxDepth=10,
        seed=42
    )
    
    # Create pipeline
    pipeline = Pipeline(stages=[
        channel_indexer,
        type_indexer,
        merchant_indexer,
        assembler,
        scaler,
        rf
    ])
    
    # Train model
    print("Training model...")
    model = pipeline.fit(train_df)
    print("✓ Model trained successfully")
    
    # Make predictions on test set
    predictions = model.transform(test_df)
    
    # Evaluate model
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
    
    # Calculate metrics
    auc = evaluator_auc.evaluate(predictions)
    pr_auc = evaluator_pr.evaluate(predictions)
    accuracy = multi_evaluator.evaluate(predictions, {multi_evaluator.metricName: "accuracy"})
    precision = multi_evaluator.evaluate(predictions, {multi_evaluator.metricName: "weightedPrecision"})
    recall = multi_evaluator.evaluate(predictions, {multi_evaluator.metricName: "weightedRecall"})
    f1 = multi_evaluator.evaluate(predictions, {multi_evaluator.metricName: "f1"})
    
    # Log metrics
    mlflow.log_metric("auc_roc", auc)
    mlflow.log_metric("auc_pr", pr_auc)
    mlflow.log_metric("accuracy", accuracy)
    mlflow.log_metric("precision", precision)
    mlflow.log_metric("recall", recall)
    mlflow.log_metric("f1_score", f1)
    
    # Log model
    mlflow.spark.log_model(model, "model")
    
    print(f"\n=== Model Performance ===")
    print(f"AUC-ROC: {auc:.4f}")
    print(f"AUC-PR: {pr_auc:.4f}")
    print(f"Accuracy: {accuracy:.4f}")
    print(f"Precision: {precision:.4f}")
    print(f"Recall: {recall:.4f}")
    print(f"F1 Score: {f1:.4f}")
    
    # Confusion matrix
    print("\nConfusion Matrix:")
    predictions.groupBy("label", "prediction").count().orderBy("label", "prediction").show()
    
    # Register model
    model_uri = f"runs:/{run.info.run_id}/model"
    model_details = mlflow.register_model(model_uri, MODEL_NAME)
    
    print(f"\n✓ Model registered: {MODEL_NAME}")
    print(f"✓ Version: {model_details.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature Importance

# COMMAND ----------

# Get feature importance from Random Forest
rf_model = model.stages[-1]
importances = rf_model.featureImportances.toArray()

# Create importance DataFrame
importance_df = spark.createDataFrame(
    [(feature_cols[i], float(importances[i])) for i in range(len(feature_cols))],
    ["feature", "importance"]
).orderBy(desc("importance"))

print("Feature Importance:")
importance_df.show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Score New Transactions

# COMMAND ----------

def score_transactions(input_table, output_table):
    """
    Score transactions for fraud probability
    """
    # Load model from registry
    model_uri = f"models:/{MODEL_NAME}/latest"
    loaded_model = mlflow.spark.load_model(model_uri)
    
    # Load transactions to score
    transactions = spark.table(input_table)
    
    # Prepare features
    features_df = transactions.select(
        col("transaction_id"),
        col("amount"),
        col("transaction_hour"),
        col("transaction_day_of_week"),
        col("is_international").cast("int").alias("is_international_flag"),
        col("is_night_transaction").cast("int").alias("is_night_flag"),
        col("is_weekend").cast("int").alias("is_weekend_flag"),
        col("transactions_24h"),
        col("amount_24h"),
        col("channel"),
        col("transaction_type"),
        when(col("merchant_category").isNotNull(), col("merchant_category")).otherwise("NONE").alias("merchant_category")
    )
    
    # Score
    predictions = loaded_model.transform(features_df)
    
    # Extract fraud probability
    fraud_probability_udf = udf(lambda v: float(v[1]), DoubleType())
    
    results = predictions.select(
        "transaction_id",
        "prediction",
        fraud_probability_udf("probability").alias("fraud_probability")
    ).withColumn("fraud_prediction", col("prediction").cast("boolean"))
    
    # Save results
    results.write.format("delta").mode("overwrite").saveAsTable(output_table)
    
    print(f"✓ Scored {results.count()} transactions")
    print(f"✓ Results saved to {output_table}")
    
    return results

# Example usage
# scored = score_transactions(
#     f"{CATALOG}.{SILVER_SCHEMA}.transactions_clean",
#     f"{CATALOG}.{SILVER_SCHEMA}.transaction_fraud_scores"
# )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Model Metadata

# COMMAND ----------

print(f"""
Model Details:
- Name: {MODEL_NAME}
- Type: Random Forest Classifier
- Features: {len(feature_cols)}
- Training Date: {datetime.now()}
- MLflow Experiment: /banking/fraud_detection

Usage:
```python
model = mlflow.spark.load_model(f"models:/{MODEL_NAME}/latest")
predictions = model.transform(df)
```
""")


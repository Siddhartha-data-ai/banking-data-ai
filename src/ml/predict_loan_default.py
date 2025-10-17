# Databricks notebook source
# MAGIC %md
# MAGIC # Loan Default Prediction ML Model
# MAGIC 
# MAGIC Build and deploy a model to predict loan defaults:
# MAGIC - Early warning system for loan defaults
# MAGIC - Feature engineering from loan and customer data
# MAGIC - Risk-based pricing and intervention strategies
# MAGIC - MLflow tracking and deployment

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
import mlflow
import mlflow.spark

# COMMAND ----------

# Configuration
CATALOG = "banking_catalog"
SILVER_SCHEMA = "banking_silver"
MODEL_NAME = "banking_loan_default"

mlflow.set_experiment("/banking/loan_default_prediction")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Data

# COMMAND ----------

# Load loans and customer data
loans_df = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.loans_clean")
customers_df = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.customers_clean")

# Join loans with customer information
df = loans_df.join(
    customers_df.select("customer_id", "credit_score", "annual_income", "age", 
                        "customer_tenure_years", "risk_rating"),
    "customer_id",
    "left"
)

# Filter active or delinquent loans only
df = df.filter(col("loan_status_clean").isin(["Active", "Delinquent", "Serious Delinquency", "Default"]))

print(f"Total loans: {df.count()}")
print(f"Defaulted loans: {df.filter(col('is_default') == True).count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature Engineering

# COMMAND ----------

# Create target: Will default (1) or not (0)
features_df = df.select(
    col("loan_id"),
    # Target: Default or severe delinquency
    when(
        (col("is_default") == True) |
        (col("days_past_due") > 90) |
        (col("missed_payments") >= 3),
        1
    ).otherwise(0).alias("label"),
    # Loan features
    col("loan_amount"),
    col("interest_rate"),
    col("term_months"),
    col("monthly_payment"),
    col("remaining_balance"),
    col("loan_age_months"),
    col("percent_paid"),
    col("payments_made"),
    col("missed_payments"),
    col("days_past_due"),
    col("debt_to_income_ratio"),
    col("loan_to_value_ratio"),
    col("risk_score"),
    # Customer features
    col("credit_score"),
    col("annual_income"),
    col("age"),
    col("customer_tenure_years"),
    # Categorical
    col("loan_type"),
    col("loan_purpose"),
    col("collateral_type"),
    col("payment_frequency")
).fillna(0)

# Show distribution
print("Default distribution:")
features_df.groupBy("label").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Encode Categorical Features

# COMMAND ----------

loan_type_indexer = StringIndexer(
    inputCol="loan_type",
    outputCol="loan_type_index",
    handleInvalid="keep"
)

purpose_indexer = StringIndexer(
    inputCol="loan_purpose",
    outputCol="purpose_index",
    handleInvalid="keep"
)

collateral_indexer = StringIndexer(
    inputCol="collateral_type",
    outputCol="collateral_index",
    handleInvalid="keep"
)

frequency_indexer = StringIndexer(
    inputCol="payment_frequency",
    outputCol="frequency_index",
    handleInvalid="keep"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Feature Vector

# COMMAND ----------

feature_cols = [
    "loan_amount", "interest_rate", "term_months", "monthly_payment",
    "remaining_balance", "loan_age_months", "percent_paid",
    "payments_made", "missed_payments", "days_past_due",
    "debt_to_income_ratio", "loan_to_value_ratio", "risk_score",
    "credit_score", "annual_income", "age", "customer_tenure_years",
    "loan_type_index", "purpose_index", "collateral_index", "frequency_index"
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

with mlflow.start_run(run_name="loan_default_gbt") as run:
    
    mlflow.log_param("model_type", "GradientBoostedTrees")
    mlflow.log_param("max_iter", 100)
    mlflow.log_param("max_depth", 6)
    
    # GBT Classifier
    gbt = GBTClassifier(
        featuresCol="features",
        labelCol="label",
        predictionCol="prediction",
        maxIter=100,
        maxDepth=6,
        seed=42
    )
    
    # Pipeline
    pipeline = Pipeline(stages=[
        loan_type_indexer,
        purpose_indexer,
        collateral_indexer,
        frequency_indexer,
        assembler,
        scaler,
        gbt
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

gbt_model = model.stages[-1]
importances = gbt_model.featureImportances.toArray()

importance_df = spark.createDataFrame(
    [(feature_cols[i], float(importances[i])) for i in range(len(feature_cols))],
    ["feature", "importance"]
).orderBy(desc("importance"))

print("Top Features for Default Prediction:")
importance_df.show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Score Loans

# COMMAND ----------

def predict_loan_default(loan_table, output_table):
    """
    Predict default risk for all active loans
    """
    model_uri = f"models:/{MODEL_NAME}/latest"
    loaded_model = mlflow.spark.load_model(model_uri)
    
    # Load and join data
    loans = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.loans_clean")
    customers = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.customers_clean")
    
    df = loans.join(
        customers.select("customer_id", "credit_score", "annual_income", 
                        "age", "customer_tenure_years", "risk_rating"),
        "customer_id",
        "left"
    ).filter(col("loan_status_clean").isin(["Active", "Delinquent"]))
    
    # Prepare features
    features_df = df.select(
        col("loan_id"),
        col("customer_id"),
        col("loan_type"),
        col("loan_amount"),
        col("remaining_balance"),
        col("monthly_payment"),
        col("loan_amount"),
        col("interest_rate"),
        col("term_months"),
        col("monthly_payment"),
        col("remaining_balance"),
        col("loan_age_months"),
        col("percent_paid"),
        col("payments_made"),
        col("missed_payments"),
        col("days_past_due"),
        col("debt_to_income_ratio"),
        col("loan_to_value_ratio"),
        col("risk_score"),
        col("credit_score"),
        col("annual_income"),
        col("age"),
        col("customer_tenure_years"),
        col("loan_type"),
        col("loan_purpose"),
        col("collateral_type"),
        col("payment_frequency")
    ).fillna(0)
    
    # Score
    predictions = loaded_model.transform(features_df)
    
    results = predictions.select(
        "loan_id",
        "customer_id",
        "loan_type",
        "loan_amount",
        "remaining_balance",
        "monthly_payment",
        "missed_payments",
        "days_past_due",
        col("prediction").cast("int").alias("default_prediction"),
        when(col("prediction") == 1, "High Risk").otherwise("Low Risk").alias("risk_level")
    )
    
    # Save
    results.write.format("delta").mode("overwrite").saveAsTable(output_table)
    
    print(f"✓ Scored {results.count()} loans")
    print(f"✓ Results saved to {output_table}")
    
    # Show high-risk loans
    print("\nHigh-Risk Loans (Sample):")
    results.filter(col("risk_level") == "High Risk") \
        .orderBy(desc("loan_amount")) \
        .show(10, truncate=False)
    
    return results

# Example usage
# default_predictions = predict_loan_default(
#     f"{CATALOG}.{SILVER_SCHEMA}.loans_clean",
#     f"{CATALOG}.{GOLD_SCHEMA}.loan_default_predictions"
# )

# COMMAND ----------

print(f"""
✓ Loan Default Prediction Model Complete

Model: {MODEL_NAME}
Type: Gradient Boosted Trees
Features: {len(feature_cols)}

Use for:
- Loan approval decisions
- Risk-based pricing
- Portfolio risk assessment
- Early intervention strategies
- Loss provisioning
""")


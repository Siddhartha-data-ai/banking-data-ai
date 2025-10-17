# ML Predictions - Quick Start Guide

Train and deploy machine learning models for banking analytics.

## Overview

The Banking Data & AI Platform includes four production-ready ML models:

1. **Fraud Detection** - Identify suspicious transactions
2. **Credit Risk Assessment** - Evaluate customer creditworthiness
3. **Customer Churn Prediction** - Identify at-risk customers
4. **Loan Default Prediction** - Assess loan repayment risk

## Prerequisites

- Banking data loaded (bronze, silver, gold layers)
- Databricks cluster with ML runtime (14.3 LTS ML recommended)
- MLflow enabled in workspace
- Customer 360 table built

## Quick Start - Run All Models

### Option 1: Run All at Once

```bash
# Upload and run this notebook
databricks workspace import src/ml/run_all_predictions.py
databricks runs submit --notebook-path /Workspace/src/ml/run_all_predictions.py
```

This executes all four models in sequence.

### Option 2: Run Individual Models

Upload and run each model notebook separately:

```bash
databricks runs submit --notebook-path /Workspace/src/ml/predict_fraud_enhanced.py
databricks runs submit --notebook-path /Workspace/src/ml/predict_credit_risk.py
databricks runs submit --notebook-path /Workspace/src/ml/predict_customer_churn.py
databricks runs submit --notebook-path /Workspace/src/ml/predict_loan_default.py
```

## Model Details

### 1. Fraud Detection

**Purpose**: Detect fraudulent transactions in real-time

**Features**:
- Transaction amount and patterns
- Time of day / day of week
- International transactions
- Velocity (transactions in 24h)
- Merchant category
- Channel (ATM, online, card, etc.)

**Algorithm**: Random Forest Classifier

**Performance** (typical):
- AUC-ROC: ~0.95
- Precision: ~0.90
- Recall: ~0.85

**Usage**:
```python
# Score new transactions
from mlflow import pyfunc

model = pyfunc.load_model("models:/banking_fraud_detection/latest")
predictions = model.predict(transactions_df)
```

**Output Tables**:
- `banking_silver.transaction_fraud_scores`
- `banking_gold.fraud_alerts`

### 2. Credit Risk Assessment

**Purpose**: Assess customer credit risk for lending decisions

**Features**:
- Credit score
- Income and DTI ratio
- Account balances
- Transaction patterns
- Existing loans and credit cards
- Payment history
- Delinquency indicators

**Algorithm**: Gradient Boosted Trees

**Performance**:
- AUC-ROC: ~0.88
- Accuracy: ~0.85

**Usage**:
```python
model = pyfunc.load_model("models:/banking_credit_risk/latest")
risk_scores = model.predict(customer_df)
```

**Output Tables**:
- `banking_gold.customer_credit_risk_scores`

### 3. Customer Churn Prediction

**Purpose**: Identify customers likely to close accounts

**Features**:
- Transaction frequency
- Account activity
- Tenure and age
- Product usage
- Balance trends
- Service interactions
- Customer health score

**Algorithm**: Random Forest Classifier

**Performance**:
- AUC-ROC: ~0.82
- F1 Score: ~0.78

**Usage**:
```python
model = pyfunc.load_model("models:/banking_churn_prediction/latest")
churn_predictions = model.predict(customer_df)
```

**Output Tables**:
- `banking_gold.customer_churn_predictions`

**Retention Priorities**:
- Critical: >80% churn probability
- High: 60-80%
- Medium: 40-60%
- Low: <40%

### 4. Loan Default Prediction

**Purpose**: Predict likelihood of loan default

**Features**:
- Loan characteristics (amount, term, rate)
- Payment history
- Missed payments
- Days past due
- DTI ratio
- Collateral and LTV
- Customer credit score
- Income and employment

**Algorithm**: Gradient Boosted Trees

**Performance**:
- AUC-ROC: ~0.90
- Precision: ~0.85

**Usage**:
```python
model = pyfunc.load_model("models:/banking_loan_default/latest")
default_predictions = model.predict(loans_df)
```

**Output Tables**:
- `banking_gold.loan_default_predictions`

## Model Training

### Automatic Retraining

Set up a job to retrain models periodically:

```yaml
# Add to resources/jobs/ml_training.yml
resources:
  jobs:
    banking_ml_training:
      name: banking_ml_training
      schedule:
        quartz_cron_expression: "0 0 3 * * ?" # Daily at 3 AM
      tasks:
        - task_key: train_fraud_model
          notebook_task:
            notebook_path: ../../src/ml/predict_fraud_enhanced.py
```

### Manual Retraining

Run model notebooks when:
- New data is available
- Model performance degrades
- Business logic changes
- Feature engineering improves

## Model Monitoring

### Check Model Performance

```sql
-- View MLflow experiments
SELECT * FROM system.mlflow.experiments 
WHERE name LIKE '%banking%';

-- Check model versions
SELECT * FROM system.mlflow.models
WHERE name = 'banking_fraud_detection';
```

### View Predictions

```sql
-- Recent fraud alerts
SELECT * FROM banking_gold.fraud_alerts
WHERE alert_severity = 'Critical'
ORDER BY transaction_date DESC
LIMIT 20;

-- High-risk customers
SELECT customer_id, full_name, churn_probability, retention_priority
FROM banking_gold.customer_churn_predictions
WHERE retention_priority IN ('Critical', 'High')
ORDER BY churn_probability DESC;

-- Loan default risks
SELECT loan_id, customer_id, loan_amount, remaining_balance, risk_level
FROM banking_gold.loan_default_predictions
WHERE risk_level = 'High Risk'
ORDER BY loan_amount DESC;
```

## Model Registry

### Promote Models

```python
import mlflow
from mlflow.tracking import MlflowClient

client = MlflowClient()

# Promote to staging
client.transition_model_version_stage(
    name="banking_fraud_detection",
    version=1,
    stage="Staging"
)

# Promote to production
client.transition_model_version_stage(
    name="banking_fraud_detection",
    version=1,
    stage="Production"
)
```

### Model Aliases

```python
# Set alias for easy reference
client.set_registered_model_alias(
    name="banking_fraud_detection",
    alias="champion",
    version=1
)

# Load by alias
model = mlflow.pyfunc.load_model("models:/banking_fraud_detection@champion")
```

## Batch Scoring

### Schedule Regular Scoring

```python
# Add to jobs configuration
tasks:
  - task_key: score_fraud
    schedule:
      quartz_cron_expression: "0 */6 * * *" # Every 6 hours
    notebook_task:
      notebook_path: ../../src/ml/score_fraud_batch.py
```

### Score New Data

```python
# In your scoring notebook
from mlflow import pyfunc

# Load latest model
model = pyfunc.load_model("models:/banking_fraud_detection/latest")

# Load new transactions
new_transactions = spark.table("banking_silver.transactions_clean") \
    .filter(col("transaction_date") >= current_date())

# Prepare features (same as training)
features_df = prepare_features(new_transactions)

# Score
predictions = model.predict(features_df)

# Save results
predictions.write.format("delta").mode("append") \
    .saveAsTable("banking_gold.fraud_scores_daily")
```

## Real-Time Scoring

### Deploy Model as API

```python
# Create model serving endpoint
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

endpoint = w.serving_endpoints.create(
    name="banking-fraud-api",
    config={
        "served_models": [{
            "model_name": "banking_fraud_detection",
            "model_version": "1",
            "workload_size": "Small",
            "scale_to_zero_enabled": True
        }]
    }
)
```

### Call Model API

```python
import requests

url = f"{DATABRICKS_HOST}/serving-endpoints/banking-fraud-api/invocations"
headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}

data = {
    "dataframe_records": [
        {
            "amount": 1500.00,
            "transaction_hour": 23,
            "is_international": 1,
            "transactions_24h": 15
            # ... other features
        }
    ]
}

response = requests.post(url, json=data, headers=headers)
prediction = response.json()
```

## Feature Engineering

### Add Custom Features

```python
# In your model notebook
features_df = features_df \
    .withColumn("amount_to_avg_ratio", 
                col("amount") / col("avg_transaction_amount")) \
    .withColumn("weekend_large_transaction",
                when((col("is_weekend") == 1) & (col("amount") > 1000), 1)
                .otherwise(0))
```

### Feature Store (Advanced)

```python
from databricks.feature_store import FeatureStoreClient

fs = FeatureStoreClient()

# Create feature table
fs.create_table(
    name="banking_catalog.features.customer_features",
    primary_keys=["customer_id"],
    df=customer_features_df,
    description="Customer banking features"
)

# Use in training
training_set = fs.create_training_set(
    df=labels_df,
    feature_lookups=[
        FeatureLookup(
            table_name="banking_catalog.features.customer_features",
            feature_names=["credit_score", "total_balance", "transaction_count"],
            lookup_key="customer_id"
        )
    ],
    label="churn_label"
)
```

## Hyperparameter Tuning

### Grid Search Example

```python
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

# Build parameter grid
paramGrid = ParamGridBuilder() \
    .addGrid(rf.numTrees, [50, 100, 200]) \
    .addGrid(rf.maxDepth, [5, 10, 15]) \
    .addGrid(rf.minInstancesPerNode, [1, 5, 10]) \
    .build()

# Cross-validation
crossval = CrossValidator(
    estimator=pipeline,
    estimatorParamMaps=paramGrid,
    evaluator=evaluator,
    numFolds=3
)

# Train
cvModel = crossval.fit(train_df)
best_model = cvModel.bestModel
```

## Performance Optimization

### Tips for Better Models

1. **Feature Engineering**: Add domain-specific features
2. **Data Balance**: Handle imbalanced classes with SMOTE or class weights
3. **Feature Selection**: Remove low-importance features
4. **Ensemble Methods**: Combine multiple models
5. **Regular Retraining**: Update models with new data

### Monitor Model Drift

```sql
-- Compare prediction distributions over time
SELECT 
    DATE_TRUNC('week', prediction_date) as week,
    AVG(fraud_probability) as avg_fraud_score,
    PERCENTILE(fraud_probability, 0.95) as p95_fraud_score
FROM banking_gold.fraud_scores_daily
GROUP BY week
ORDER BY week DESC;
```

## Troubleshooting

### Common Issues

**Issue**: Model training fails with OOM
```python
# Solution: Reduce data size or increase cluster
sample_df = df.sample(fraction=0.5, seed=42)
```

**Issue**: Poor model performance
```python
# Solution: Check class balance
df.groupBy("label").count().show()

# Adjust sample weights
rf = RandomForestClassifier(
    weightCol="classWeightCol"  # Add weights for imbalanced classes
)
```

**Issue**: Scoring is slow
```python
# Solution: Batch predictions
predictions = model.transform(df.repartition(20))
```

## Next Steps

1. **Customize Models**: Adjust features for your use case
2. **Add Models**: Create models for other predictions
3. **Monitor Performance**: Set up model monitoring dashboards
4. **A/B Testing**: Compare model versions
5. **AutoML**: Try Databricks AutoML for quick models

## Resources

- [Databricks ML Guide](https://docs.databricks.com/machine-learning/index.html)
- [MLflow Documentation](https://mlflow.org/docs/latest/index.html)
- [Feature Store](https://docs.databricks.com/machine-learning/feature-store/index.html)
- [Model Serving](https://docs.databricks.com/machine-learning/model-serving/index.html)

Happy Modeling! 🤖📊


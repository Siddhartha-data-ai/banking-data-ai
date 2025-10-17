# Quick Start Guide - Banking Data & AI Platform

Get up and running with the Banking Data & AI Platform in under 10 minutes!

## 🎯 Prerequisites

- Databricks workspace (any tier)
- Unity Catalog enabled
- Cluster running DBR 13.3+ or 14.3+ ML Runtime

## 🚀 5-Minute Setup

### Step 1: Deploy Resources (2 minutes)

```bash
cd /Users/kanikamondal/Databricks/banking-data-ai

# Validate configuration
databricks bundle validate

# Deploy to workspace
databricks bundle deploy
```

### Step 2: Generate Sample Data (2 minutes)

```bash
# Make script executable
chmod +x start_pipeline.sh

# Run data generation
./start_pipeline.sh
```

This creates:
- 10,000 customer accounts
- 100,000+ transactions
- 5,000 loan applications
- 15,000 credit card accounts
- Associated customer profiles

### Step 3: Run DLT Pipeline (1 minute)

In Databricks UI:
1. Navigate to **Workflows** → **Delta Live Tables**
2. Find `banking_bronze_to_silver_dlt`
3. Click **Start**

Or via CLI:
```bash
databricks pipelines start-update --pipeline-name banking_bronze_to_silver_dlt
```

## 🎨 What You Get

### Data Tables (Bronze → Silver → Gold)

**Bronze Layer** (Raw data):
- `banking_bronze.accounts`
- `banking_bronze.transactions`
- `banking_bronze.customers`
- `banking_bronze.loans`
- `banking_bronze.credit_cards`

**Silver Layer** (Cleaned):
- `banking_silver.accounts_clean`
- `banking_silver.transactions_clean`
- `banking_silver.customers_clean`
- `banking_silver.loans_clean`
- `banking_silver.credit_cards_clean`

**Gold Layer** (Analytics-ready):
- `banking_gold.customer_360`
- `banking_gold.fraud_analytics`
- `banking_gold.credit_risk_summary`

### Machine Learning Models

Run predictions:
```bash
cd src/ml
python run_all_predictions.py
```

Available models:
- **Fraud Detection**: Detect suspicious transactions
- **Credit Risk**: Assess loan default probability
- **Customer Churn**: Predict account closures
- **Loan Default**: Evaluate borrower risk

### Banking Chatbot

Launch the chatbot:
```bash
cd src/chatbot
pip install -r requirements.txt
python launch_chatbot.py
```

Ask questions like:
- "What's my account balance?"
- "Show me recent transactions"
- "Are there any fraud alerts?"
- "What's my credit score?"
- "Loan application status"

## 📊 View Your Data

### SQL Queries

```sql
-- Customer 360 view
SELECT * FROM banking_gold.customer_360 LIMIT 10;

-- Recent transactions
SELECT * FROM banking_silver.transactions_clean 
WHERE transaction_date >= CURRENT_DATE - INTERVAL 7 DAYS
ORDER BY transaction_date DESC;

-- High-risk loans
SELECT * FROM banking_silver.loans_clean 
WHERE risk_score > 700
ORDER BY loan_amount DESC;
```

### Data Quality Dashboard

```bash
./launch_dq_dashboard.sh
```

## 🔍 Next Steps

1. **Customize Data Generation**: Edit files in `src/bronze/` to match your data schema
2. **Add Business Rules**: Modify transformation logic in `src/pipelines/`
3. **Tune ML Models**: Adjust hyperparameters in `src/ml/`
4. **Extend Chatbot**: Add custom queries in `src/chatbot/`
5. **Create Dashboards**: Build visualizations in Databricks SQL

## 🛠️ Common Commands

```bash
# Check pipeline status
databricks pipelines get --pipeline-name banking_bronze_to_silver_dlt

# Run specific ML model
python src/ml/predict_fraud_enhanced.py

# Generate more data
python src/bronze/generate_transactions_data.py

# View data quality metrics
python src/analytics/data_quality_monitoring.py
```

## ⚡ Quick Wins

### 1. Real-Time Fraud Detection
```python
from databricks import sql
# Query recent high-risk transactions
# Integrate with alerting system
```

### 2. Customer Segmentation
```sql
SELECT customer_segment, COUNT(*), AVG(total_balance)
FROM banking_gold.customer_360
GROUP BY customer_segment;
```

### 3. Credit Risk Monitoring
```bash
python src/ml/predict_loan_default.py
```

## 🆘 Troubleshooting

**Issue**: Pipeline fails with catalog not found
**Solution**: Run `src/setup/00_create_catalog.sql` first

**Issue**: Chatbot connection error
**Solution**: Update `DATABRICKS_HOST` and `DATABRICKS_TOKEN` environment variables

**Issue**: ML model not found
**Solution**: Run training first: `python src/ml/predict_fraud_enhanced.py --train`

## 📚 Learn More

- `DEPLOYMENT.md` - Full deployment guide
- `CHATBOT_QUICKSTART.md` - Chatbot documentation
- `ML_PREDICTIONS_QUICKSTART.md` - ML model details
- `DQ_MONITORING_GUIDE.md` - Data quality setup

## 🎓 Sample Use Cases

1. **Fraud Prevention**: Real-time transaction monitoring
2. **Credit Assessment**: Automated loan approvals
3. **Customer Retention**: Churn prediction and intervention
4. **Regulatory Compliance**: AML detection and reporting
5. **Personalization**: Targeted product recommendations

---

**Ready to go?** Start with `./start_pipeline.sh` and explore your banking data platform!


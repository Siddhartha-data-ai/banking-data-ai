# Banking Data & AI Platform - Deployment Guide

Complete deployment instructions for the Banking Data & AI Platform on Databricks.

## Prerequisites

- Databricks workspace (Premium or Enterprise)
- Unity Catalog enabled
- Cluster with DBR 14.3+ and ML runtime
- Admin or appropriate permissions to create catalogs/schemas
- Databricks CLI installed (optional, for automation)

## Deployment Steps

### 1. Prepare Your Environment

```bash
# Clone or navigate to project directory
cd /Users/kanikamondal/Databricks/banking-data-ai

# Set environment variables (if using Databricks CLI)
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="your-token"
```

### 2. Create Catalog Structure

Run the setup SQL scripts in order:

#### Option A: Using Databricks SQL Editor

1. Open Databricks SQL Editor
2. Run `src/setup/00_create_catalog.sql`
3. Run `src/setup/01_create_bronze_tables.sql`
4. Run `src/setup/02_create_silver_tables.sql`
5. Run `src/setup/03_create_gold_tables.sql`

#### Option B: Using Databricks CLI

```bash
databricks sql execute --file src/setup/00_create_catalog.sql
databricks sql execute --file src/setup/01_create_bronze_tables.sql
databricks sql execute --file src/setup/02_create_silver_tables.sql
databricks sql execute --file src/setup/03_create_gold_tables.sql
```

### 3. Deploy Databricks Assets

```bash
# Validate bundle configuration
databricks bundle validate

# Deploy to development
databricks bundle deploy --target dev

# Or deploy to production
databricks bundle deploy --target prod
```

This will create:
- DLT Pipeline: `banking_bronze_to_silver_dlt`
- Job: `banking_etl_orchestration`
- All schemas and tables

### 4. Generate Sample Data

#### Option A: Run Individual Notebooks

Upload and run these notebooks in Databricks:

1. `src/bronze/generate_customers_data.py`
2. `src/bronze/generate_accounts_data.py`
3. `src/bronze/generate_transactions_data.py`
4. `src/bronze/generate_loans_data.py`
5. `src/bronze/generate_credit_cards_data.py`

#### Option B: Run ETL Job

```bash
# Start the ETL orchestration job
databricks jobs run-now --job-name banking_etl_orchestration
```

### 5. Start DLT Pipeline

```bash
# Find pipeline ID
databricks pipelines list | grep banking_bronze_to_silver_dlt

# Start the pipeline
databricks pipelines start --pipeline-id <pipeline-id>
```

Or use the UI:
1. Navigate to **Workflows** → **Delta Live Tables**
2. Find `banking_bronze_to_silver_dlt`
3. Click **Start**

### 6. Build Gold Layer

Run these notebooks:

1. `src/gold/build_customer_360.py`
2. `src/gold/build_fraud_detection.py`

### 7. Train ML Models (Optional)

```bash
# Run all ML model training and predictions
databricks runs submit --notebook-path src/ml/run_all_predictions.py
```

Or run individual models:
- `src/ml/predict_fraud_enhanced.py`
- `src/ml/predict_credit_risk.py`
- `src/ml/predict_customer_churn.py`
- `src/ml/predict_loan_default.py`

### 8. Launch Chatbot (Optional)

```bash
cd src/chatbot

# Install dependencies
pip install -r requirements.txt

# Set environment variables
export DATABRICKS_HOST="your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="your-token"
export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/your-warehouse-id"

# Launch chatbot
python launch_chatbot.py
```

The chatbot will open at `http://localhost:8501`

## Verification

### Check Data Tables

```sql
-- Verify bronze layer
SELECT COUNT(*) FROM banking_catalog.banking_bronze.customers;
SELECT COUNT(*) FROM banking_catalog.banking_bronze.accounts;
SELECT COUNT(*) FROM banking_catalog.banking_bronze.transactions;

-- Verify silver layer
SELECT COUNT(*) FROM banking_catalog.banking_silver.customers_clean;
SELECT COUNT(*) FROM banking_catalog.banking_silver.accounts_clean;

-- Verify gold layer
SELECT COUNT(*) FROM banking_catalog.banking_gold.customer_360;
SELECT COUNT(*) FROM banking_catalog.banking_gold.fraud_alerts;
```

### Run Data Quality Checks

```bash
databricks runs submit --notebook-path src/analytics/data_quality_monitoring.py
```

### Check Pipeline Monitoring

```bash
databricks runs submit --notebook-path src/analytics/pipeline_monitoring_dashboard.py
```

## Configuration

### Environment-Specific Settings

Edit `databricks.yml` for environment-specific configurations:

```yaml
targets:
  dev:
    variables:
      catalog_name: ${workspace.current_user.userName}_banking_dev
  
  staging:
    variables:
      catalog_name: banking_staging
  
  prod:
    variables:
      catalog_name: banking_prod
```

### Adjust Data Volume

Edit configuration in bronze layer generators:

```python
# In src/bronze/generate_customers_data.py
NUM_CUSTOMERS = 10000  # Adjust as needed

# In src/bronze/generate_transactions_data.py
TRANSACTIONS_PER_ACCOUNT = 50  # Adjust as needed
```

## Scheduling

### Set Up Automated Jobs

The ETL job includes a schedule:
- **Frequency**: Daily at 2 AM
- **Timezone**: America/Los_Angeles

To modify:

```yaml
# In resources/jobs/etl_orchestration.yml
schedule:
  quartz_cron_expression: "0 0 2 * * ?"
  timezone_id: "America/Los_Angeles"
```

### Enable DLT Continuous Mode

For real-time processing:

```yaml
# In resources/pipelines/bronze_to_silver_dlt.yml
continuous: true
```

## Monitoring

### Set Up Alerts

Configure email notifications in job definitions:

```yaml
email_notifications:
  on_failure:
    - data-team@example.com
  on_success:
    - data-team@example.com
```

### View Pipeline Metrics

- Navigate to **Workflows** → **Jobs**
- Select `banking_etl_orchestration`
- View run history and metrics

## Troubleshooting

### Common Issues

**Issue**: Catalog not found
```sql
-- Solution: Verify catalog exists
SHOW CATALOGS;
CREATE CATALOG IF NOT EXISTS banking_catalog;
```

**Issue**: Permission denied
```sql
-- Solution: Grant necessary permissions
GRANT USAGE ON CATALOG banking_catalog TO `user@example.com`;
GRANT CREATE ON CATALOG banking_catalog TO `user@example.com`;
```

**Issue**: DLT pipeline fails
```bash
# Solution: Check pipeline logs
databricks pipelines get --pipeline-id <id>
```

**Issue**: Chatbot connection error
```bash
# Solution: Verify environment variables
echo $DATABRICKS_HOST
echo $DATABRICKS_TOKEN
echo $DATABRICKS_HTTP_PATH
```

## Performance Tuning

### Optimize Table Performance

```sql
-- Optimize Delta tables
OPTIMIZE banking_catalog.banking_silver.transactions_clean ZORDER BY (transaction_date);
OPTIMIZE banking_catalog.banking_gold.customer_360 ZORDER BY (customer_id);

-- Vacuum old files (after 7 days)
VACUUM banking_catalog.banking_silver.transactions_clean RETAIN 168 HOURS;
```

### Cluster Configuration

Recommended cluster specs:
- **Driver**: 16 GB Memory, 4 Cores
- **Workers**: 2-10 (autoscaling)
- **Worker Type**: 32 GB Memory, 8 Cores
- **Databricks Runtime**: 14.3 LTS ML

## Security

### Row-Level Security (RLS)

```sql
-- Example: Create row filter for customer data
CREATE OR REPLACE FUNCTION banking_catalog.banking_silver.customer_filter()
RETURN CASE 
  WHEN IS_ACCOUNT_GROUP_MEMBER('admins') THEN TRUE
  WHEN current_user() = email THEN TRUE
  ELSE FALSE
END;

ALTER TABLE banking_catalog.banking_silver.customers_clean
SET ROW FILTER banking_catalog.banking_silver.customer_filter ON (email);
```

### Column-Level Security (CLS)

```sql
-- Example: Mask SSN for non-admins
ALTER TABLE banking_catalog.banking_silver.customers_clean
ALTER COLUMN ssn_last_4 
SET MASK 
  CASE 
    WHEN IS_ACCOUNT_GROUP_MEMBER('admins') THEN ssn_last_4
    ELSE 'XXX-XX-XXXX'
  END;
```

## Next Steps

1. Customize data generators for your specific needs
2. Add custom ML models for your use cases
3. Create Databricks SQL dashboards
4. Set up monitoring and alerting
5. Implement CI/CD pipelines
6. Scale to production data volumes

## Support

For issues or questions:
- Check documentation in `/docs`
- Review error logs in Databricks UI
- Consult Databricks documentation

## Cleanup

To remove all resources:

```sql
-- WARNING: This will delete all data!
DROP CATALOG banking_catalog CASCADE;
```

```bash
# Remove bundle deployment
databricks bundle destroy --target dev
```


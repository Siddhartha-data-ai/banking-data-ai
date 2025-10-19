# 📘 Runbook - Banking Data AI Platform

## Operations & Troubleshooting Guide

This runbook provides operational procedures and troubleshooting steps for the Banking Data AI Platform.

---

## 🚨 **Incident Response**

### Severity Levels

| Level | Description | Response Time | Examples |
|-------|-------------|---------------|----------|
| **P0** | Critical - Production down | < 15 minutes | API down, data pipeline failure, security breach |
| **P1** | High - Major feature broken | < 1 hour | ML model failing, fraud detection offline |
| **P2** | Medium - Minor feature issue | < 4 hours | Dashboard loading slowly, partial data loss |
| **P3** | Low - Cosmetic or enhancement | < 24 hours | UI issue, documentation error |

### Incident Response Steps

1. **Acknowledge**: Confirm incident in monitoring system
2. **Assess**: Determine severity and impact
3. **Investigate**: Check logs, metrics, traces
4. **Mitigate**: Apply immediate fix or rollback
5. **Resolve**: Apply permanent fix
6. **Document**: Create post-mortem

---

## 🔍 **Troubleshooting Guide**

### 1. Data Pipeline Failures

#### Symptoms
- Job failed in Databricks
- Data not appearing in tables
- DLT pipeline error

#### Investigation Steps
```bash
# Check job run history
databricks jobs list-runs --job-id <JOB_ID>

# View logs for failed run
databricks runs get-output --run-id <RUN_ID>

# Check table freshness
SELECT MAX(timestamp) FROM banking_catalog.banking_bronze.transactions
```

#### Common Causes & Fixes

**Cause**: Schema mismatch
```sql
-- Solution: Check schema evolution
DESCRIBE HISTORY banking_catalog.banking_bronze.transactions

-- Fix: Enable schema evolution
ALTER TABLE banking_catalog.banking_bronze.transactions 
SET TBLPROPERTIES ('delta.autoMerge.enabled' = 'true')
```

**Cause**: Insufficient permissions
```sql
-- Solution: Grant required permissions
GRANT SELECT, MODIFY ON TABLE banking_catalog.banking_bronze.transactions 
TO `data_engineers`;
```

**Cause**: Resource exhaustion
```bash
# Solution: Scale up cluster or optimize query
# Check cluster metrics in Databricks UI
# Consider increasing workers or using larger node types
```

---

### 2. ML Model Prediction Failures

#### Symptoms
- Model API returning errors
- Predictions not updating
- High error rates in logs

#### Investigation Steps
```python
# Check model status in MLflow
import mlflow
client = mlflow.tracking.MlflowClient()
model_versions = client.search_model_versions("name='banking_fraud_detection'")

for mv in model_versions:
    print(f"Version: {mv.version}, Stage: {mv.current_stage}")
```

#### Common Causes & Fixes

**Cause**: Model not found
```python
# Solution: Register model in MLflow
mlflow.register_model(
    model_uri="runs:/<run-id>/model",
    name="banking_fraud_detection"
)
```

**Cause**: Feature schema mismatch
```python
# Solution: Validate input features
expected_features = ['amount', 'is_international', 'hour_of_day', ...]
actual_features = df.columns

missing = set(expected_features) - set(actual_features)
if missing:
    print(f"Missing features: {missing}")
```

**Cause**: Model drift detected
```python
# Solution: Retrain model
# 1. Run training notebook
# 2. Evaluate new model
# 3. Promote to production if better
```

---

### 3. API Performance Issues

#### Symptoms
- Slow response times (> 2 seconds)
- Timeout errors
- High CPU/memory usage

#### Investigation Steps
```bash
# Check API health
curl http://localhost:8000/health

# View API logs
docker logs banking-api --tail=100

# Check metrics
curl http://localhost:8000/metrics
```

#### Common Causes & Fixes

**Cause**: Database query slowness
```sql
-- Solution: Add indexes or optimize query
OPTIMIZE banking_catalog.banking_gold.customer_360 
ZORDER BY (customer_id);

-- Analyze query plan
EXPLAIN SELECT * FROM banking_catalog.banking_gold.customer_360 
WHERE customer_id = 'CUST-123';
```

**Cause**: Too many concurrent requests
```python
# Solution: Add rate limiting
from fastapi_limiter import FastAPILimiter

@app.on_event("startup")
async def startup():
    await FastAPILimiter.init(redis)

@app.get("/api/v1/customers/{id}")
@limiter.limit("100/minute")
async def get_customer(id: str):
    ...
```

**Cause**: Large response payloads
```python
# Solution: Implement pagination
@app.get("/api/v1/transactions")
async def get_transactions(limit: int = 10, offset: int = 0):
    return transactions[offset:offset+limit]
```

---

### 4. Security Issues

#### Symptoms
- Unauthorized access attempts
- Audit log anomalies
- PII exposure

#### Investigation Steps
```sql
-- Check audit logs
SELECT * FROM banking_catalog.audit_logging.audit_events
WHERE event_time >= CURRENT_TIMESTAMP - INTERVAL 1 HOUR
AND event_type = 'UNAUTHORIZED_ACCESS'
ORDER BY event_time DESC;

-- Check PII access
SELECT * FROM banking_catalog.audit_logging.pii_access_log
WHERE access_time >= CURRENT_TIMESTAMP - INTERVAL 1 HOUR
ORDER BY access_time DESC;
```

#### Response Actions

**Unauthorized Access**
```sql
-- 1. Revoke user access immediately
REVOKE ALL PRIVILEGES ON CATALOG banking_catalog FROM USER <user_email>;

-- 2. Review recent activity
SELECT * FROM banking_catalog.audit_logging.audit_events
WHERE user_email = '<user_email>'
AND event_time >= CURRENT_TIMESTAMP - INTERVAL 24 HOURS;

-- 3. Alert security team
-- 4. Change credentials if compromised
```

**PII Exposure**
```sql
-- 1. Verify masking is applied
SHOW CREATE TABLE banking_catalog.banking_gold.customer_360_cls_protected;

-- 2. Check if user should have access
SELECT * FROM banking_catalog.banking_gold.user_roles
WHERE user_email = '<user_email>';

-- 3. Apply stricter masking if needed
ALTER TABLE banking_catalog.banking_gold.customer_360
ALTER COLUMN ssn_last_4 SET MASK banking_gold.mask_ssn_strict;
```

---

### 5. Cost Overruns

#### Symptoms
- Unexpectedly high cloud bills
- Long-running clusters
- High storage costs

#### Investigation Steps
```sql
-- Check cluster usage
SELECT 
    user_email,
    SUM(total_cost) as cost,
    SUM(duration_hours) as hours
FROM banking_catalog.cost_monitoring.cluster_usage
WHERE usage_date >= CURRENT_DATE - INTERVAL 7 DAYS
GROUP BY user_email
ORDER BY cost DESC;

-- Check storage costs
SELECT 
    table_name,
    size_gb,
    monthly_cost
FROM banking_catalog.cost_monitoring.storage_costs
WHERE measurement_date = CURRENT_DATE
ORDER BY monthly_cost DESC
LIMIT 20;
```

#### Common Causes & Fixes

**Cause**: Idle clusters running
```bash
# Solution: Configure auto-termination
# In cluster settings:
Auto Termination: 30 minutes

# Or via API:
databricks clusters edit --json '{
  "cluster_id": "<CLUSTER_ID>",
  "autotermination_minutes": 30
}'
```

**Cause**: Unoptimized tables
```sql
-- Solution: Run OPTIMIZE and VACUUM
OPTIMIZE banking_catalog.banking_bronze.transactions;
VACUUM banking_catalog.banking_bronze.transactions RETAIN 168 HOURS;

-- Set automatic optimization
ALTER TABLE banking_catalog.banking_bronze.transactions
SET TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);
```

**Cause**: Expensive queries
```sql
-- Solution: Identify and optimize
-- Check query history in Databricks SQL
-- Add caching for frequent queries
CACHE TABLE frequently_used_data AS
SELECT * FROM banking_catalog.banking_gold.customer_360;
```

---

## 📋 **Standard Operating Procedures (SOPs)**

### SOP: Deploy to Production

1. **Pre-deployment checks**
   ```bash
   # Run all tests
   pytest tests/ -v
   
   # Validate bundle
   databricks bundle validate -t prod
   
   # Check for linting errors
   flake8 src/
   black --check src/
   ```

2. **Create deployment ticket**
   - Document changes
   - Get approvals
   - Schedule maintenance window

3. **Deploy**
   ```bash
   # Deploy to production
   databricks bundle deploy -t prod
   
   # Verify deployment
   databricks bundle validate -t prod
   ```

4. **Post-deployment verification**
   ```bash
   # Run smoke tests
   curl https://api.banking.com/health
   
   # Check pipelines
   databricks pipelines list
   
   # Monitor for 30 minutes
   # Check logs, metrics, errors
   ```

5. **Rollback (if needed)**
   ```bash
   # Rollback to previous version
   git revert <commit-hash>
   databricks bundle deploy -t prod
   ```

---

### SOP: Disaster Recovery

1. **Assess situation**
   - Determine data loss extent
   - Check backup availability
   - Estimate recovery time

2. **Initiate recovery**
   ```sql
   -- Restore from Delta Lake time travel
   RESTORE TABLE banking_catalog.banking_bronze.transactions
   TO TIMESTAMP AS OF '2025-10-19 10:00:00';
   
   -- Or restore to version
   RESTORE TABLE banking_catalog.banking_bronze.transactions
   TO VERSION AS OF 42;
   ```

3. **Verify data integrity**
   ```sql
   -- Run data quality checks
   SELECT COUNT(*) FROM banking_catalog.banking_bronze.transactions;
   SELECT MAX(timestamp) FROM banking_catalog.banking_bronze.transactions;
   ```

4. **Resume operations**
   - Restart pipelines
   - Verify all systems operational
   - Notify stakeholders

---

### SOP: User Access Management

1. **Grant access**
   ```sql
   -- Add user to group
   GRANT `fraud_analysts` TO USER <user_email>;
   
   -- Or grant direct permissions
   GRANT SELECT ON TABLE banking_catalog.banking_gold.customer_360 
   TO USER <user_email>;
   ```

2. **Revoke access**
   ```sql
   -- Remove from group
   REVOKE `fraud_analysts` FROM USER <user_email>;
   
   -- Or revoke direct permissions
   REVOKE SELECT ON TABLE banking_catalog.banking_gold.customer_360 
   FROM USER <user_email>;
   ```

3. **Audit access**
   ```sql
   -- Check user permissions
   SHOW GRANTS ON TABLE banking_catalog.banking_gold.customer_360;
   
   -- Check user activity
   SELECT * FROM banking_catalog.audit_logging.audit_events
   WHERE user_email = '<user_email>'
   ORDER BY event_time DESC LIMIT 100;
   ```

---

## 📊 **Monitoring & Alerts**

### Key Metrics to Monitor

| Metric | Threshold | Action |
|--------|-----------|--------|
| API Response Time | > 2 seconds | Investigate slow queries |
| Job Failure Rate | > 5% | Check logs, increase resources |
| Fraud Detection Latency | > 1 second | Scale up, optimize model |
| Storage Growth | > 20% per week | Run OPTIMIZE/VACUUM |
| Cost | > Budget + 10% | Review usage, optimize |

### Alert Configuration

```yaml
# Example alert configuration
alerts:
  - name: high_api_latency
    condition: avg(response_time_ms) > 2000
    window: 5m
    severity: P1
    notify:
      - oncall@bank.com
  
  - name: job_failure
    condition: job_status == 'FAILED'
    severity: P0
    notify:
      - data-engineering@bank.com
```

---

## 🔧 **Maintenance Tasks**

### Daily
- [ ] Check pipeline status
- [ ] Review error logs
- [ ] Monitor cost dashboard

### Weekly
- [ ] Run OPTIMIZE on large tables
- [ ] Review slow queries
- [ ] Check model performance metrics

### Monthly
- [ ] Run VACUUM on tables (7-day retention)
- [ ] Review and rotate secrets
- [ ] Update dependencies
- [ ] Review access permissions

---

## 📞 **Contacts**

| Role | Contact | Escalation |
|------|---------|------------|
| On-Call Engineer | oncall@bank.com | Slack: #incidents |
| Data Engineering Lead | de-lead@bank.com | Phone: XXX-XXXX |
| Security Team | security@bank.com | Phone: XXX-XXXX |
| Cloud Operations | cloudops@bank.com | Ticket: https://... |

---

**Last Updated**: October 19, 2025  
**Version**: 1.0.0  
**Owner**: Data Engineering Team


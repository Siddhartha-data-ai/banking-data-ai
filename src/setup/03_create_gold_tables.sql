-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Create Gold Layer Tables
-- MAGIC 
-- MAGIC Creates table definitions for gold layer (business-ready analytics)
-- MAGIC Note: Most gold tables are created by the build scripts, but we can create placeholders here

-- COMMAND ----------

USE CATALOG banking_catalog;
USE SCHEMA banking_gold;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Note: The main gold tables are created by notebooks:
-- MAGIC - customer_360 (created by build_customer_360.py)
-- MAGIC - fraud_alerts (created by build_fraud_detection.py)
-- MAGIC - customer_fraud_profiles (created by build_fraud_detection.py)
-- MAGIC - fraud_analytics (created by build_fraud_detection.py)
-- MAGIC - merchant_fraud_analysis (created by build_fraud_detection.py)
-- MAGIC 
-- MAGIC These tables are created with the proper schema when the build scripts run.

-- COMMAND ----------

-- Show all tables
SHOW TABLES IN banking_catalog.banking_gold;

-- COMMAND ----------

SELECT 'Gold schema ready!' as status;


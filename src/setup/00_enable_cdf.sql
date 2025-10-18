-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Enable Change Data Feed (CDF) on All Banking Tables
-- MAGIC 
-- MAGIC This script enables CDF on all Delta tables to track:
-- MAGIC - INSERT operations
-- MAGIC - UPDATE operations (with before/after values)
-- MAGIC - DELETE operations
-- MAGIC 
-- MAGIC Benefits:
-- MAGIC - Efficient incremental processing
-- MAGIC - Change tracking for auditing
-- MAGIC - Support for CDC consumers
-- MAGIC - Enable streaming from Delta tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Enable CDF on Bronze Layer Tables

-- COMMAND ----------

USE CATALOG banking_catalog;
USE SCHEMA banking_bronze;

-- COMMAND ----------

-- Enable CDF on Customers table
ALTER TABLE customers 
SET TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true'
);

-- Enable CDF on Accounts table
ALTER TABLE accounts 
SET TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true'
);

-- Enable CDF on Transactions table
ALTER TABLE transactions 
SET TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true'
);

-- Enable CDF on Loans table
ALTER TABLE loans 
SET TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true'
);

-- Enable CDF on Credit Cards table
ALTER TABLE credit_cards 
SET TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true'
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Enable CDF on Silver Layer Tables

-- COMMAND ----------

USE SCHEMA banking_silver;

-- COMMAND ----------

-- Note: Silver tables are created by DLT pipelines
-- CDF will be enabled through DLT table properties
-- We'll update the pipeline code to include CDF

SELECT 'CDF enabled on all bronze tables. Update DLT pipelines for silver layer.' as status;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Verify CDF is Enabled

-- COMMAND ----------

-- Check Bronze tables
SELECT 
  table_catalog,
  table_schema,
  table_name,
  table_properties
FROM system.information_schema.tables
WHERE table_catalog = 'banking_catalog'
  AND table_schema = 'banking_bronze'
  AND table_properties LIKE '%enableChangeDataFeed%';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## How to Read CDF
-- MAGIC 
-- MAGIC ```python
-- MAGIC # Read changes since version 10
-- MAGIC df_changes = spark.read.format("delta") \
-- MAGIC     .option("readChangeFeed", "true") \
-- MAGIC     .option("startingVersion", 10) \
-- MAGIC     .table("banking_catalog.banking_bronze.customers")
-- MAGIC 
-- MAGIC # Columns available:
-- MAGIC # - All original columns
-- MAGIC # - _change_type: 'insert', 'update_preimage', 'update_postimage', 'delete'
-- MAGIC # - _commit_version: Delta table version
-- MAGIC # - _commit_timestamp: When change occurred
-- MAGIC ```

-- COMMAND ----------

SELECT '✓ Change Data Feed enabled successfully!' as status;


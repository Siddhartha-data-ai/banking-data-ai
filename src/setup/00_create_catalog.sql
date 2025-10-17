-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Create Banking Catalog and Schemas
-- MAGIC 
-- MAGIC This script creates the Unity Catalog structure for the banking platform:
-- MAGIC - Catalog creation
-- MAGIC - Bronze, Silver, and Gold schemas
-- MAGIC - Permissions and grants

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create Catalog

-- COMMAND ----------

-- Create the main banking catalog
CREATE CATALOG IF NOT EXISTS banking_catalog
COMMENT 'Banking Data & AI Platform Catalog';

USE CATALOG banking_catalog;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create Schemas

-- COMMAND ----------

-- Bronze Schema (Raw Data)
CREATE SCHEMA IF NOT EXISTS banking_bronze
COMMENT 'Bronze layer - Raw data from source systems';

-- Silver Schema (Cleaned Data)
CREATE SCHEMA IF NOT EXISTS banking_silver
COMMENT 'Silver layer - Cleaned and validated data';

-- Gold Schema (Business Analytics)
CREATE SCHEMA IF NOT EXISTS banking_gold
COMMENT 'Gold layer - Business-ready aggregations and analytics';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Verify Structure

-- COMMAND ----------

-- Show all schemas
SHOW SCHEMAS IN banking_catalog;

-- COMMAND ----------

DESCRIBE CATALOG banking_catalog;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Set Default Permissions

-- COMMAND ----------

-- Grant usage on catalog to all users (adjust as needed)
-- GRANT USAGE ON CATALOG banking_catalog TO `account users`;
-- GRANT SELECT ON CATALOG banking_catalog TO `account users`;

-- Grant full access to data engineers (adjust as needed)
-- GRANT ALL PRIVILEGES ON CATALOG banking_catalog TO `data_engineers`;

-- COMMAND ----------

SELECT 'Banking catalog structure created successfully!' as status;


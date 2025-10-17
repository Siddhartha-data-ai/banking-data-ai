#!/bin/bash

# Banking Data Pipeline Startup Script
# This script initializes the banking data platform by generating sample data

echo "======================================================================"
echo "Banking Data & AI Platform - Pipeline Startup"
echo "======================================================================"

# Configuration
CATALOG="banking_catalog"
BRONZE_SCHEMA="banking_bronze"

echo ""
echo "Configuration:"
echo "  Catalog: $CATALOG"
echo "  Bronze Schema: $BRONZE_SCHEMA"
echo ""

# Check if running in Databricks
if [ -z "$DATABRICKS_RUNTIME_VERSION" ]; then
    echo "⚠️  Warning: Not running in Databricks environment"
    echo "This script should be run in a Databricks notebook or job"
    echo ""
    read -p "Continue anyway? (y/n): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

echo "======================================================================"
echo "Step 1: Creating Catalog and Schemas"
echo "======================================================================"

# Run setup scripts (would need to be converted to databricks CLI commands)
echo "✓ Run src/setup/00_create_catalog.sql in Databricks SQL"
echo "✓ Run src/setup/01_create_bronze_tables.sql in Databricks SQL"
echo ""

echo "======================================================================"
echo "Step 2: Generating Bronze Layer Data"
echo "======================================================================"

echo "Generating synthetic banking data..."
echo ""

echo "→ Generating customers data..."
echo "  Run: src/bronze/generate_customers_data.py"
echo ""

echo "→ Generating accounts data..."
echo "  Run: src/bronze/generate_accounts_data.py"
echo ""

echo "→ Generating transactions data..."
echo "  Run: src/bronze/generate_transactions_data.py"
echo ""

echo "→ Generating loans data..."
echo "  Run: src/bronze/generate_loans_data.py"
echo ""

echo "→ Generating credit cards data..."
echo "  Run: src/bronze/generate_credit_cards_data.py"
echo ""

echo "======================================================================"
echo "Step 3: Running DLT Pipeline (Silver Layer)"
echo "======================================================================"

echo "Start the DLT pipeline: banking_bronze_to_silver_dlt"
echo "This will transform bronze data to silver layer"
echo ""

echo "======================================================================"
echo "Step 4: Building Gold Layer Analytics"
echo "======================================================================"

echo "→ Building Customer 360..."
echo "  Run: src/gold/build_customer_360.py"
echo ""

echo "→ Building Fraud Analytics..."
echo "  Run: src/gold/build_fraud_detection.py"
echo ""

echo "======================================================================"
echo "Step 5: Running ML Models (Optional)"
echo "======================================================================"

echo "→ Training and running ML predictions..."
echo "  Run: src/ml/run_all_predictions.py"
echo ""

echo "======================================================================"
echo "Pipeline Startup Complete!"
echo "======================================================================"
echo ""
echo "Next Steps:"
echo "  1. Verify data in Databricks SQL Editor"
echo "  2. Launch chatbot: cd src/chatbot && python launch_chatbot.py"
echo "  3. View dashboards in Databricks SQL"
echo "  4. Monitor data quality: src/analytics/data_quality_monitoring.py"
echo ""
echo "Quick Queries to Try:"
echo "  SELECT * FROM $CATALOG.$BRONZE_SCHEMA.customers LIMIT 10;"
echo "  SELECT * FROM $CATALOG.banking_silver.customers_clean LIMIT 10;"
echo "  SELECT * FROM $CATALOG.banking_gold.customer_360 LIMIT 10;"
echo ""
echo "✓ All steps outlined. Execute notebooks in Databricks to complete setup."


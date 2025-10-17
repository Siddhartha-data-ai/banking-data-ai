# Banking Data & AI Platform

A comprehensive data and AI platform for banking operations built on Databricks, featuring automated data pipelines, machine learning models, real-time analytics, and an intelligent chatbot.

## 🏦 Features

### Data Pipeline
- **Bronze Layer**: Raw data ingestion for accounts, transactions, customers, loans, and credit cards
- **Silver Layer**: Cleaned and validated banking data with quality checks
- **Gold Layer**: Business-ready aggregations including Customer 360 and fraud analytics

### Machine Learning Models
- **Fraud Detection**: Real-time transaction fraud detection using ML
- **Credit Risk Assessment**: Predict loan default probability
- **Customer Churn Prediction**: Identify at-risk customers
- **Loan Default Prediction**: Assess borrower creditworthiness
- **AML Detection**: Anti-money laundering pattern detection

### Analytics & Monitoring
- Data quality monitoring dashboards
- Transaction monitoring and alerts
- Customer behavior analytics
- Risk assessment dashboards
- Cost optimization analysis

### Banking Chatbot
- Natural language query interface
- Account balance and transaction inquiries
- Fraud alert notifications
- Credit score information
- Loan application status

## 📁 Project Structure

```
banking-data-ai/
├── config/                      # Configuration templates
├── resources/                   # Databricks resource definitions
│   ├── grants/                 # Security and access grants
│   ├── jobs/                   # Job orchestration configs
│   ├── pipelines/              # DLT pipeline definitions
│   └── schemas/                # Schema definitions
├── src/
│   ├── analytics/              # Analytics and monitoring scripts
│   ├── bronze/                 # Data generators for bronze layer
│   ├── chatbot/                # Banking chatbot implementation
│   ├── gold/                   # Gold layer transformations
│   ├── ml/                     # Machine learning models
│   ├── pipelines/              # Silver layer pipelines
│   ├── setup/                  # SQL setup scripts
│   └── transformations/        # Data transformation logic
└── docs/                       # Documentation
```

## 🚀 Quick Start

### Prerequisites
- Databricks workspace (Premium or Enterprise)
- Unity Catalog enabled
- Cluster with DBR 13.3+ and ML runtime

### Setup Steps

1. **Clone and Navigate**
   ```bash
   cd /Users/kanikamondal/Databricks/banking-data-ai
   ```

2. **Configure Databricks**
   ```bash
   databricks bundle validate
   databricks bundle deploy
   ```

3. **Generate Sample Data**
   ```bash
   ./start_pipeline.sh
   ```

4. **Launch Chatbot**
   ```bash
   cd src/chatbot
   python launch_chatbot.py
   ```

## 📊 Data Schemas

### Bronze Tables
- `accounts`: Customer account information
- `transactions`: Transaction history
- `customers`: Customer demographics and profiles
- `loans`: Loan applications and status
- `credit_cards`: Credit card accounts and usage

### Silver Tables
- Cleaned and validated versions of bronze tables
- Added business rules and data quality checks
- Standardized formats and deduplication

### Gold Tables
- `customer_360`: Comprehensive customer view
- `fraud_analytics`: Fraud detection metrics
- `credit_risk_summary`: Credit risk aggregations
- `transaction_analytics`: Transaction patterns and insights

## 🤖 Machine Learning Models

All ML models are production-ready with:
- Feature engineering pipelines
- Model training and validation
- MLflow experiment tracking
- Model registry integration
- Batch and real-time inference

## 🔒 Security Features

- Row-level security (RLS) for customer data
- Column-level security (CLS) for PII
- Audit logging
- Data encryption at rest and in transit
- Role-based access control (RBAC)

## 📈 Monitoring & Observability

- Data quality metrics and alerts
- Pipeline health monitoring
- Model performance tracking
- Cost optimization recommendations
- Real-time dashboards

## 📚 Documentation

See individual guide files:
- `QUICK_START.md` - Get started in 5 minutes
- `DEPLOYMENT.md` - Detailed deployment instructions
- `CHATBOT_QUICKSTART.md` - Chatbot setup guide
- `ML_PREDICTIONS_QUICKSTART.md` - ML model usage
- `DQ_MONITORING_GUIDE.md` - Data quality monitoring

## 🤝 Contributing

This is a template project. Customize for your banking use cases.

## 📝 License

Apache 2.0

## 🆘 Support

For issues or questions, refer to the documentation guides or Databricks support.


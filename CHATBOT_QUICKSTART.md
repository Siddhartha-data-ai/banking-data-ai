# Banking Chatbot - Quick Start Guide

Get your Banking AI Assistant up and running in minutes!

## Prerequisites

- Python 3.8 or higher
- Access to Databricks SQL Warehouse
- Banking data already loaded (see QUICK_START.md)

## Installation

### 1. Navigate to Chatbot Directory

```bash
cd /Users/kanikamondal/Databricks/banking-data-ai/src/chatbot
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

This installs:
- `streamlit`: Web application framework
- `databricks-sql-connector`: Connection to Databricks
- `pandas`: Data manipulation
- `python-dotenv`: Environment variable management

### 3. Configure Environment Variables

Create a `.env` file in the chatbot directory:

```bash
# .env file
DATABRICKS_HOST=your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=your-access-token
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/your-warehouse-id
```

Or export them directly:

```bash
export DATABRICKS_HOST="your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="dapi1234567890abcdef"
export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/abc123def456"
```

#### How to Get These Values:

**DATABRICKS_HOST:**
- Your workspace URL without `https://`
- Example: `dbc-12345678-9abc.cloud.databricks.com`

**DATABRICKS_TOKEN:**
1. In Databricks, click your user icon → **User Settings**
2. Go to **Access Tokens** tab
3. Click **Generate New Token**
4. Copy the token (save it securely!)

**DATABRICKS_HTTP_PATH:**
1. Go to **SQL Warehouses** in Databricks
2. Select your warehouse
3. Click **Connection Details**
4. Copy the **HTTP Path**
5. Format: `/sql/1.0/warehouses/abc123def456`

## Launch Chatbot

### Option 1: Using Launch Script

```bash
python launch_chatbot.py
```

### Option 2: Direct Streamlit Launch

```bash
streamlit run banking_chatbot.py
```

The chatbot will open in your browser at `http://localhost:8501`

## Using the Chatbot

### 1. Login

In the sidebar:
1. Enter your email address (must exist in customer database)
2. Click **Login**
3. You'll see your name and customer segment

Example test emails (if you used default data generation):
- `james.smith123@email.com`
- `mary.johnson456@email.com`
- Any email from your generated customer data

### 2. Ask Questions

Type questions in natural language:

#### Account Queries
- "What's my account balance?"
- "Show me all my accounts"
- "How much money do I have?"

#### Transaction Queries
- "Show my recent transactions"
- "What did I spend on?"
- "Transaction history"

#### Credit Card Queries
- "Credit card balance"
- "When is my payment due?"
- "What's my credit limit?"
- "Show rewards balance"

#### Loan Queries
- "My loans"
- "Loan payment amount"
- "Mortgage balance"

#### Fraud & Security
- "Any fraud alerts?"
- "Is my account secure?"
- "Suspicious activity"

#### Financial Summary
- "Financial summary"
- "Net worth"
- "My financial overview"

### 3. Quick Actions

Use sidebar buttons for instant queries:
- **Check Balance**: View all account balances
- **Recent Transactions**: Last 30 days of activity
- **Fraud Alerts**: Security notifications

## Features

### Real-Time Data
- Queries live data from Databricks
- No data caching
- Always up-to-date information

### Secure
- Uses Databricks authentication
- No passwords stored in chatbot
- Token-based access

### Conversational
- Natural language understanding
- Context-aware responses
- Helpful suggestions

### Comprehensive
- Account information
- Transaction history
- Credit cards
- Loans
- Fraud alerts
- Financial analytics

## Customization

### Add New Query Types

Edit `banking_chatbot.py`:

```python
def process_user_query(user_input):
    # Add your custom logic
    if "investment" in user_input_lower:
        # Add investment portfolio query
        return get_investments(customer_id)
```

### Modify UI

Change the Streamlit layout:

```python
# Change page title
st.set_page_config(
    page_title="My Bank Assistant",
    page_icon="💼"
)

# Add custom styling
st.markdown("""
<style>
    .stApp {
        background-color: #f0f2f6;
    }
</style>
""", unsafe_allow_html=True)
```

### Add More Features

Ideas for extensions:
- Bill payment functionality
- Money transfer interface
- Investment recommendations
- Budget tracking
- Spending analytics
- Goal setting and tracking

## Troubleshooting

### Connection Issues

**Error**: "Failed to connect to Databricks"

Solutions:
1. Verify environment variables are set correctly
2. Check token hasn't expired
3. Ensure SQL warehouse is running
4. Test connection with Databricks SQL directly

```python
# Test connection
from databricks import sql

connection = sql.connect(
    server_hostname="your-workspace.cloud.databricks.com",
    http_path="/sql/1.0/warehouses/your-id",
    access_token="your-token"
)
print("✓ Connection successful!")
connection.close()
```

### Query Errors

**Error**: "Query error: Table not found"

Solution: Ensure all tables exist:

```sql
-- Check if tables exist
SHOW TABLES IN banking_catalog.banking_silver;
SHOW TABLES IN banking_catalog.banking_gold;
```

### Login Issues

**Error**: "Customer not found"

Solutions:
1. Check email spelling
2. Verify customer data was generated
3. Query customers table directly:

```sql
SELECT customer_id, full_name, email 
FROM banking_catalog.banking_silver.customers_clean 
LIMIT 10;
```

### Performance Issues

**Slow Responses**:
1. Check SQL warehouse size (scale up if needed)
2. Optimize queries with indexes
3. Add caching for frequently accessed data

```python
# Add caching
import streamlit as st

@st.cache_data(ttl=300)  # Cache for 5 minutes
def get_account_balances(customer_id):
    # Your query here
    pass
```

## Advanced Configuration

### Multi-User Deployment

Deploy on a server:

```bash
# Run on specific port
streamlit run banking_chatbot.py --server.port 8080

# Run headless (no browser auto-open)
streamlit run banking_chatbot.py --server.headless true

# Specify host
streamlit run banking_chatbot.py --server.address 0.0.0.0
```

### Docker Deployment

Create `Dockerfile`:

```dockerfile
FROM python:3.10-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY banking_chatbot.py .

EXPOSE 8501

CMD ["streamlit", "run", "banking_chatbot.py", "--server.port=8501", "--server.headless=true"]
```

Build and run:

```bash
docker build -t banking-chatbot .
docker run -p 8501:8501 \
  -e DATABRICKS_HOST="your-host" \
  -e DATABRICKS_TOKEN="your-token" \
  -e DATABRICKS_HTTP_PATH="your-path" \
  banking-chatbot
```

### Add Authentication

Implement user authentication:

```python
import streamlit_authenticator as stauth

# Configure authenticator
authenticator = stauth.Authenticate(
    names=['Admin', 'User'],
    usernames=['admin', 'user'],
    passwords=['hashed_pw1', 'hashed_pw2'],
    cookie_name='banking_chatbot',
    key='random_signature_key',
    cookie_expiry_days=30
)

name, authentication_status, username = authenticator.login('Login', 'main')

if authentication_status:
    # Show chatbot
    st.write(f'Welcome {name}')
```

## Next Steps

1. **Enhance Queries**: Add more banking functions
2. **Add Voice**: Integrate speech recognition
3. **Mobile App**: Create mobile version
4. **Analytics**: Track chatbot usage
5. **AI Enhancement**: Integrate with OpenAI/Azure OpenAI for better NLU

## Resources

- [Streamlit Documentation](https://docs.streamlit.io/)
- [Databricks SQL Connector](https://docs.databricks.com/dev-tools/python-sql-connector.html)
- [Chatbot Best Practices](https://streamlit.io/gallery)

## Support

For help:
1. Check Streamlit logs in terminal
2. Review Databricks connection settings
3. Test SQL queries independently
4. Check chatbot logs for errors

Enjoy your Banking AI Assistant! 🏦🤖


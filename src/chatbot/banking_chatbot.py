"""
Banking AI Chatbot

An intelligent chatbot for banking operations that can:
- Answer account and balance inquiries
- Provide transaction history
- Check credit card information
- Report fraud alerts
- Provide loan status
- Offer financial advice
"""

import os
import streamlit as st
from databricks import sql
from datetime import datetime, timedelta
import pandas as pd

# Page configuration
st.set_page_config(
    page_title="Banking AI Assistant",
    page_icon="🏦",
    layout="wide"
)

# Databricks connection configuration
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST", "your-workspace.cloud.databricks.com")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN", "your-token")
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH", "/sql/1.0/warehouses/your-warehouse-id")

CATALOG = "banking_catalog"
SILVER_SCHEMA = "banking_silver"
GOLD_SCHEMA = "banking_gold"

# Initialize session state
if 'chat_history' not in st.session_state:
    st.session_state.chat_history = []
if 'customer_id' not in st.session_state:
    st.session_state.customer_id = None

def get_databricks_connection():
    """Establish connection to Databricks SQL warehouse"""
    try:
        connection = sql.connect(
            server_hostname=DATABRICKS_HOST,
            http_path=DATABRICKS_HTTP_PATH,
            access_token=DATABRICKS_TOKEN
        )
        return connection
    except Exception as e:
        st.error(f"Failed to connect to Databricks: {e}")
        return None

def execute_query(query):
    """Execute SQL query and return results as DataFrame"""
    conn = get_databricks_connection()
    if conn is None:
        return None
    
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(results, columns=columns)
        cursor.close()
        conn.close()
        return df
    except Exception as e:
        st.error(f"Query error: {e}")
        return None

def get_customer_by_email(email):
    """Get customer information by email"""
    query = f"""
    SELECT customer_id, full_name, email, phone, customer_segment, credit_score
    FROM {CATALOG}.{SILVER_SCHEMA}.customers_clean
    WHERE email = '{email}'
    LIMIT 1
    """
    return execute_query(query)

def get_account_balances(customer_id):
    """Get all account balances for a customer"""
    query = f"""
    SELECT account_type, account_number, balance, available_balance, 
           account_status, last_transaction_date
    FROM {CATALOG}.{SILVER_SCHEMA}.accounts_clean
    WHERE customer_id = '{customer_id}'
    ORDER BY balance DESC
    """
    return execute_query(query)

def get_recent_transactions(customer_id, days=30):
    """Get recent transactions for a customer"""
    query = f"""
    SELECT transaction_date, transaction_type, amount, merchant_name,
           status, description
    FROM {CATALOG}.{SILVER_SCHEMA}.transactions_clean
    WHERE customer_id = '{customer_id}'
    AND transaction_date >= CURRENT_DATE - INTERVAL {days} DAYS
    ORDER BY transaction_date DESC
    LIMIT 20
    """
    return execute_query(query)

def get_credit_cards(customer_id):
    """Get credit card information for a customer"""
    query = f"""
    SELECT card_type, card_last_4, credit_limit, current_balance,
           available_credit, utilization_percent, payment_due_date,
           minimum_payment, rewards_balance
    FROM {CATALOG}.{SILVER_SCHEMA}.credit_cards_clean
    WHERE customer_id = '{customer_id}'
    AND card_status = 'Active'
    """
    return execute_query(query)

def get_loans(customer_id):
    """Get loan information for a customer"""
    query = f"""
    SELECT loan_type, loan_amount, remaining_balance, monthly_payment,
           interest_rate, next_payment_date, loan_status_clean,
           days_past_due
    FROM {CATALOG}.{SILVER_SCHEMA}.loans_clean
    WHERE customer_id = '{customer_id}'
    AND loan_status_clean IN ('Active', 'Delinquent')
    """
    return execute_query(query)

def get_fraud_alerts(customer_id):
    """Check for fraud alerts"""
    query = f"""
    SELECT alert_severity, alert_reason, transaction_date, amount,
           merchant_name, recommended_action
    FROM {CATALOG}.{GOLD_SCHEMA}.fraud_alerts
    WHERE customer_id = '{customer_id}'
    ORDER BY transaction_date DESC
    LIMIT 5
    """
    return execute_query(query)

def get_customer_360(customer_id):
    """Get comprehensive customer view"""
    query = f"""
    SELECT total_assets, total_liabilities, net_worth, product_count,
           customer_value_tier, customer_health_score, churn_risk_score,
           engagement_level, relationship_strength
    FROM {CATALOG}.{GOLD_SCHEMA}.customer_360
    WHERE customer_id = '{customer_id}'
    """
    return execute_query(query)

def process_user_query(user_input):
    """Process user query and generate response"""
    user_input_lower = user_input.lower()
    
    if not st.session_state.customer_id:
        return "Please log in first by entering your email in the sidebar."
    
    customer_id = st.session_state.customer_id
    
    # Account balance queries
    if any(word in user_input_lower for word in ['balance', 'account', 'how much']):
        df = get_account_balances(customer_id)
        if df is not None and not df.empty:
            response = "Here are your account balances:\n\n"
            for _, row in df.iterrows():
                response += f"**{row['account_type']}** (••••{row['account_number'][-4:]})\n"
                response += f"  Balance: ${row['balance']:,.2f}\n"
                response += f"  Available: ${row['available_balance']:,.2f}\n"
                response += f"  Status: {row['account_status']}\n\n"
            return response
        return "No accounts found."
    
    # Transaction queries
    elif any(word in user_input_lower for word in ['transaction', 'purchase', 'spent', 'history']):
        df = get_recent_transactions(customer_id, 30)
        if df is not None and not df.empty:
            response = "Here are your recent transactions:\n\n"
            for _, row in df.iterrows():
                date = row['transaction_date'].strftime('%Y-%m-%d') if hasattr(row['transaction_date'], 'strftime') else row['transaction_date']
                response += f"**{date}** - {row['transaction_type']}\n"
                response += f"  Amount: ${row['amount']:,.2f}\n"
                if row['merchant_name']:
                    response += f"  Merchant: {row['merchant_name']}\n"
                response += f"  Status: {row['status']}\n\n"
            return response
        return "No recent transactions found."
    
    # Credit card queries
    elif any(word in user_input_lower for word in ['credit card', 'card', 'credit limit', 'payment due']):
        df = get_credit_cards(customer_id)
        if df is not None and not df.empty:
            response = "Here are your credit cards:\n\n"
            for _, row in df.iterrows():
                response += f"**{row['card_type']}** (••••{row['card_last_4']})\n"
                response += f"  Credit Limit: ${row['credit_limit']:,.2f}\n"
                response += f"  Current Balance: ${row['current_balance']:,.2f}\n"
                response += f"  Available Credit: ${row['available_credit']:,.2f}\n"
                response += f"  Utilization: {row['utilization_percent']:.1f}%\n"
                response += f"  Minimum Payment: ${row['minimum_payment']:,.2f}\n"
                response += f"  Rewards Balance: {row['rewards_balance']:,.0f} points\n\n"
            return response
        return "No active credit cards found."
    
    # Loan queries
    elif any(word in user_input_lower for word in ['loan', 'mortgage', 'debt']):
        df = get_loans(customer_id)
        if df is not None and not df.empty:
            response = "Here are your loans:\n\n"
            for _, row in df.iterrows():
                response += f"**{row['loan_type']}**\n"
                response += f"  Original Amount: ${row['loan_amount']:,.2f}\n"
                response += f"  Remaining Balance: ${row['remaining_balance']:,.2f}\n"
                response += f"  Monthly Payment: ${row['monthly_payment']:,.2f}\n"
                response += f"  Interest Rate: {row['interest_rate']:.2f}%\n"
                response += f"  Status: {row['loan_status_clean']}\n\n"
            return response
        return "No active loans found."
    
    # Fraud alerts
    elif any(word in user_input_lower for word in ['fraud', 'suspicious', 'alert', 'security']):
        df = get_fraud_alerts(customer_id)
        if df is not None and not df.empty:
            response = "⚠️ **Fraud Alerts**\n\n"
            for _, row in df.iterrows():
                response += f"**{row['alert_severity']} Alert**\n"
                response += f"  Date: {row['transaction_date']}\n"
                response += f"  Amount: ${row['amount']:,.2f}\n"
                if row['merchant_name']:
                    response += f"  Merchant: {row['merchant_name']}\n"
                response += f"  Reason: {row['alert_reason']}\n"
                response += f"  Action: {row['recommended_action']}\n\n"
            return response
        return "✓ No fraud alerts found. Your account is secure."
    
    # Financial summary
    elif any(word in user_input_lower for word in ['summary', 'overview', 'financial', 'wealth']):
        df = get_customer_360(customer_id)
        if df is not None and not df.empty:
            row = df.iloc[0]
            response = "📊 **Your Financial Summary**\n\n"
            response += f"**Assets & Liabilities**\n"
            response += f"  Total Assets: ${row['total_assets']:,.2f}\n"
            response += f"  Total Liabilities: ${row['total_liabilities']:,.2f}\n"
            response += f"  Net Worth: ${row['net_worth']:,.2f}\n\n"
            response += f"**Customer Profile**\n"
            response += f"  Value Tier: {row['customer_value_tier']}\n"
            response += f"  Health Score: {row['customer_health_score']}/100\n"
            response += f"  Products: {row['product_count']}\n"
            response += f"  Engagement: {row['engagement_level']}\n"
            response += f"  Relationship: {row['relationship_strength']}\n"
            return response
        return "Unable to retrieve financial summary."
    
    # Default response
    else:
        return """I can help you with:
- Account balances
- Recent transactions
- Credit card information
- Loan details
- Fraud alerts
- Financial summary

Please ask me a specific question about any of these topics!"""

# Main UI
st.title("🏦 Banking AI Assistant")
st.markdown("Your intelligent banking companion")

# Sidebar for customer login
with st.sidebar:
    st.header("Customer Login")
    email = st.text_input("Enter your email:", placeholder="customer@email.com")
    
    if st.button("Login"):
        if email:
            df = get_customer_by_email(email)
            if df is not None and not df.empty:
                st.session_state.customer_id = df.iloc[0]['customer_id']
                st.session_state.customer_name = df.iloc[0]['full_name']
                st.session_state.customer_segment = df.iloc[0]['customer_segment']
                st.success(f"Welcome, {st.session_state.customer_name}!")
            else:
                st.error("Customer not found")
        else:
            st.warning("Please enter an email")
    
    if st.session_state.customer_id:
        st.info(f"Logged in as: {st.session_state.customer_name}")
        st.info(f"Segment: {st.session_state.customer_segment}")
        
        if st.button("Logout"):
            st.session_state.customer_id = None
            st.session_state.customer_name = None
            st.session_state.chat_history = []
            st.rerun()
    
    st.markdown("---")
    st.markdown("### Quick Actions")
    if st.button("Check Balance"):
        if st.session_state.customer_id:
            st.session_state.chat_history.append({
                "user": "What's my account balance?",
                "bot": process_user_query("balance")
            })
    
    if st.button("Recent Transactions"):
        if st.session_state.customer_id:
            st.session_state.chat_history.append({
                "user": "Show recent transactions",
                "bot": process_user_query("transactions")
            })
    
    if st.button("Fraud Alerts"):
        if st.session_state.customer_id:
            st.session_state.chat_history.append({
                "user": "Any fraud alerts?",
                "bot": process_user_query("fraud")
            })

# Chat interface
st.markdown("### Chat")

# Display chat history
for chat in st.session_state.chat_history:
    with st.chat_message("user"):
        st.write(chat["user"])
    with st.chat_message("assistant"):
        st.write(chat["bot"])

# Chat input
user_input = st.chat_input("Ask me anything about your banking...")

if user_input:
    # Add user message to history
    with st.chat_message("user"):
        st.write(user_input)
    
    # Process and respond
    response = process_user_query(user_input)
    
    with st.chat_message("assistant"):
        st.write(response)
    
    # Save to history
    st.session_state.chat_history.append({
        "user": user_input,
        "bot": response
    })

# Footer
st.markdown("---")
st.markdown("*Banking AI Assistant - Secure, Intelligent, Always Available*")


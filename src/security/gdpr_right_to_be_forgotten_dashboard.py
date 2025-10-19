# Databricks notebook source
# MAGIC %md
# MAGIC # GDPR Right to be Forgotten Dashboard
# MAGIC 
# MAGIC Automated dashboard for managing GDPR Article 17 - Right to Erasure:
# MAGIC - Customer erasure requests
# MAGIC - Automated anonymization workflow
# MAGIC - Legal hold checking
# MAGIC - Verification and audit trail
# MAGIC - Compliance tracking

# COMMAND ----------

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dashboard Configuration

# COMMAND ----------

# Page configuration
st.set_page_config(
    page_title="🗑️ GDPR Right to be Forgotten",
    page_icon="🗑️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .request-card {
        background-color: #f8f9fa;
        padding: 20px;
        border-radius: 10px;
        margin: 10px 0;
        border-left: 5px solid #007bff;
    }
    .request-pending {
        border-left-color: #ffc107;
    }
    .request-completed {
        border-left-color: #28a745;
    }
    .request-rejected {
        border-left-color: #dc3545;
    }
    .legal-hold {
        background-color: #fff3cd;
        padding: 15px;
        border-radius: 5px;
        border: 2px solid #ffc107;
        margin: 10px 0;
    }
    .success-box {
        background-color: #d4edda;
        padding: 15px;
        border-radius: 5px;
        border: 1px solid #c3e6cb;
        margin: 10px 0;
    }
    .info-box {
        background-color: #d1ecf1;
        padding: 15px;
        border-radius: 5px;
        border: 1px solid #bee5eb;
        margin: 10px 0;
    }
</style>
""", unsafe_allow_html=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Functions

# COMMAND ----------

# Load erasure requests
def load_erasure_requests(status_filter=None):
    """Load GDPR erasure requests"""
    query = """
    SELECT 
        request_id,
        request_date,
        customer_id,
        customer_email,
        customer_name,
        request_type,
        request_status,
        gdpr_article,
        deadline_date,
        days_to_deadline,
        is_overdue,
        requested_by,
        processed_by,
        processing_completed_date
    FROM banking_catalog.gdpr.data_subject_requests
    WHERE request_type = 'ERASURE'
    """
    
    if status_filter and status_filter != "All":
        query += f" AND request_status = '{status_filter}'"
    
    query += " ORDER BY request_date DESC"
    
    df = spark.sql(query).toPandas()
    return df

# Load erasure log
def load_erasure_log():
    """Load detailed erasure log"""
    query = """
    SELECT 
        erasure_id,
        request_id,
        erasure_timestamp,
        customer_id,
        customer_email,
        erasure_method,
        erasure_reason,
        tables_anonymized,
        records_anonymized,
        has_legal_hold,
        legal_hold_reason,
        can_be_deleted,
        verification_completed,
        requested_by,
        approved_by,
        executed_by
    FROM banking_catalog.gdpr.erasure_log
    ORDER BY erasure_timestamp DESC
    """
    
    df = spark.sql(query).toPandas()
    return df

# Check customer legal holds
def check_legal_holds(customer_id):
    """Check if customer has any legal holds"""
    
    # Check for active loans
    query_loans = f"""
    SELECT COUNT(*) as active_loans
    FROM banking_catalog.banking_gold.fact_loan_performance
    WHERE customer_sk IN (
        SELECT customer_sk FROM banking_catalog.banking_gold.dim_customer 
        WHERE customer_id = '{customer_id}' AND is_current = TRUE
    ) AND loan_status IN ('Active', 'Delinquent')
    """
    
    loan_result = spark.sql(query_loans).collect()[0]['active_loans']
    
    # Check for fraud investigations
    query_fraud = f"""
    SELECT COUNT(*) as fraud_cases
    FROM banking_catalog.banking_gold.fraud_alerts_realtime
    WHERE customer_id = '{customer_id}'
      AND alert_timestamp >= current_timestamp() - INTERVAL 90 DAYS
    """
    
    fraud_result = spark.sql(query_fraud).collect()[0]['fraud_cases']
    
    has_hold = (loan_result > 0) or (fraud_result > 0)
    reasons = []
    
    if loan_result > 0:
        reasons.append(f"{loan_result} active loan(s)")
    if fraud_result > 0:
        reasons.append(f"{fraud_result} recent fraud alert(s)")
    
    return has_hold, reasons

# Get customer data summary
def get_customer_data_summary(customer_id):
    """Get summary of customer data across all tables"""
    
    summary = {}
    
    # Customer records
    query = f"""
    SELECT COUNT(*) as count FROM banking_catalog.banking_gold.dim_customer 
    WHERE customer_id = '{customer_id}'
    """
    summary['customer_records'] = spark.sql(query).collect()[0]['count']
    
    # Account records
    query = f"""
    SELECT COUNT(*) as count FROM banking_catalog.banking_gold.dim_account 
    WHERE customer_id = '{customer_id}'
    """
    summary['account_records'] = spark.sql(query).collect()[0]['count']
    
    # Transaction records
    query = f"""
    SELECT COUNT(*) as count FROM banking_catalog.banking_gold.fact_transactions 
    WHERE customer_sk IN (
        SELECT customer_sk FROM banking_catalog.banking_gold.dim_customer 
        WHERE customer_id = '{customer_id}'
    )
    """
    summary['transaction_records'] = spark.sql(query).collect()[0]['count']
    
    return summary

# Load GDPR compliance statistics
def load_gdpr_stats():
    """Load GDPR compliance statistics"""
    query = """
    SELECT 
        request_type,
        COUNT(*) as total_requests,
        COUNT(CASE WHEN request_status = 'COMPLETED' THEN 1 END) as completed,
        COUNT(CASE WHEN request_status = 'PENDING' THEN 1 END) as pending,
        COUNT(CASE WHEN is_overdue THEN 1 END) as overdue,
        AVG(DATEDIFF(processing_completed_date, request_date)) as avg_days_to_complete
    FROM banking_catalog.gdpr.data_subject_requests
    GROUP BY request_type
    """
    
    df = spark.sql(query).toPandas()
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dashboard Header

# COMMAND ----------

# Title
st.title("🗑️ GDPR Right to be Forgotten Dashboard")
st.markdown("**Automated Customer Data Erasure Management (GDPR Article 17)**")
st.markdown("---")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sidebar

# COMMAND ----------

# Sidebar
with st.sidebar:
    st.header("⚙️ Controls")
    
    # Status filter
    status_filter = st.selectbox(
        "Request Status",
        ["All", "PENDING", "IN_PROGRESS", "COMPLETED", "REJECTED"]
    )
    
    # Show overdue only
    show_overdue = st.checkbox("Show Overdue Only", value=False)
    
    st.markdown("---")
    
    # New Erasure Request
    st.header("➕ New Erasure Request")
    
    with st.form("new_request_form"):
        customer_id_input = st.text_input("Customer ID")
        customer_email_input = st.text_input("Customer Email")
        reason_input = st.text_area("Reason for Erasure")
        
        submit_button = st.form_submit_button("Submit Request")
        
        if submit_button:
            if customer_id_input and customer_email_input:
                # Check legal holds
                has_hold, hold_reasons = check_legal_holds(customer_id_input)
                
                if has_hold:
                    st.error(f"❌ Cannot process: Customer has legal holds")
                    for reason in hold_reasons:
                        st.warning(f"• {reason}")
                else:
                    # Create request
                    spark.sql(f"""
                        INSERT INTO banking_catalog.gdpr.data_subject_requests (
                            customer_id, customer_email, request_type, request_status,
                            gdpr_article, requested_by, deadline_date
                        ) VALUES (
                            '{customer_id_input}',
                            '{customer_email_input}',
                            'ERASURE',
                            'PENDING',
                            'Article 17',
                            '{st.session_state.get("user_email", "admin@bank.com")}',
                            current_timestamp() + INTERVAL 30 DAYS
                        )
                    """)
                    st.success("✅ Erasure request submitted successfully!")
                    st.experimental_rerun()
            else:
                st.error("Please fill in all required fields")
    
    st.markdown("---")
    st.info("**GDPR Requirement:** Respond within 30 days")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Metrics

# COMMAND ----------

# Load data
erasure_df = load_erasure_requests(status_filter)
erasure_log_df = load_erasure_log()
gdpr_stats_df = load_gdpr_stats()

# Filter overdue if needed
if show_overdue:
    erasure_df = erasure_df[erasure_df['is_overdue'] == True]

# Key Metrics
col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    total_requests = len(erasure_df)
    st.metric("📋 Total Requests", f"{total_requests:,}")

with col2:
    pending = len(erasure_df[erasure_df['request_status'] == 'PENDING'])
    st.metric("⏳ Pending", f"{pending:,}", delta="Action Required" if pending > 0 else None)

with col3:
    in_progress = len(erasure_df[erasure_df['request_status'] == 'IN_PROGRESS'])
    st.metric("🔄 In Progress", f"{in_progress:,}")

with col4:
    completed = len(erasure_df[erasure_df['request_status'] == 'COMPLETED'])
    completion_rate = (completed / total_requests * 100) if total_requests > 0 else 0
    st.metric("✅ Completed", f"{completed:,}", delta=f"{completion_rate:.0f}% rate")

with col5:
    overdue = len(erasure_df[erasure_df['is_overdue'] == True])
    st.metric("⚠️ Overdue", f"{overdue:,}", delta="Alert" if overdue > 0 else None)

st.markdown("---")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Active Requests Section

# COMMAND ----------

# Active Requests
st.header("📋 Active Erasure Requests")

if len(erasure_df[erasure_df['request_status'].isin(['PENDING', 'IN_PROGRESS'])]) > 0:
    
    for idx, row in erasure_df[erasure_df['request_status'].isin(['PENDING', 'IN_PROGRESS'])].iterrows():
        
        # Determine card class
        card_class = "request-card"
        if row['request_status'] == 'PENDING':
            card_class += " request-pending"
        elif row['request_status'] == 'IN_PROGRESS':
            card_class += " request-in-progress"
        
        # Create expandable section
        with st.expander(f"Request #{row['request_id']} - {row['customer_email']} - {row['request_status']} {'⚠️ OVERDUE' if row['is_overdue'] else ''}"):
            
            req_col1, req_col2 = st.columns(2)
            
            with req_col1:
                st.markdown(f"""
                **Customer Information:**
                - Customer ID: `{row['customer_id']}`
                - Email: `{row['customer_email']}`
                - Name: `{row.get('customer_name', 'N/A')}`
                
                **Request Details:**
                - Request Date: {row['request_date']}
                - Deadline: {row['deadline_date']}
                - Days to Deadline: **{row['days_to_deadline']}**
                - Status: **{row['request_status']}**
                """)
            
            with req_col2:
                # Check legal holds
                has_hold, hold_reasons = check_legal_holds(row['customer_id'])
                
                if has_hold:
                    st.markdown(f"""
                    <div class="legal-hold">
                        <strong>⚠️ Legal Holds Detected:</strong><br>
                        {'<br>'.join(['• ' + r for r in hold_reasons])}
                    </div>
                    """, unsafe_allow_html=True)
                else:
                    st.markdown("""
                    <div class="success-box">
                        <strong>✅ No Legal Holds</strong><br>
                        Request can be processed
                    </div>
                    """, unsafe_allow_html=True)
                
                # Get customer data summary
                data_summary = get_customer_data_summary(row['customer_id'])
                
                st.markdown(f"""
                **Data to be Anonymized:**
                - Customer Records: {data_summary['customer_records']}
                - Account Records: {data_summary['account_records']}
                - Transaction Records: {data_summary['transaction_records']:,}
                """)
            
            # Action buttons
            action_col1, action_col2, action_col3 = st.columns(3)
            
            with action_col1:
                if st.button(f"✅ Approve & Execute", key=f"approve_{row['request_id']}"):
                    if not has_hold:
                        try:
                            # Execute anonymization
                            spark.sql(f"""
                                CALL banking_catalog.gdpr.anonymize_customer_data(
                                    '{row['customer_id']}',
                                    'GDPR Article 17 Request #{row['request_id']}'
                                )
                            """)
                            
                            # Update request status
                            spark.sql(f"""
                                UPDATE banking_catalog.gdpr.data_subject_requests
                                SET request_status = 'COMPLETED',
                                    processing_completed_date = current_timestamp(),
                                    processed_by = current_user()
                                WHERE request_id = {row['request_id']}
                            """)
                            
                            st.success(f"✅ Customer {row['customer_id']} anonymized successfully!")
                            st.experimental_rerun()
                        except Exception as e:
                            st.error(f"❌ Error: {str(e)}")
                    else:
                        st.error("Cannot approve: Legal holds present")
            
            with action_col2:
                if st.button(f"❌ Reject", key=f"reject_{row['request_id']}"):
                    rejection_reason = st.text_input("Rejection Reason", key=f"reason_{row['request_id']}")
                    if rejection_reason:
                        spark.sql(f"""
                            UPDATE banking_catalog.gdpr.data_subject_requests
                            SET request_status = 'REJECTED',
                                rejection_reason = '{rejection_reason}'
                            WHERE request_id = {row['request_id']}
                        """)
                        st.success("Request rejected")
                        st.experimental_rerun()
            
            with action_col3:
                if st.button(f"📋 Export Data", key=f"export_{row['request_id']}"):
                    st.info("Exporting customer data...")
                    # This would trigger the Article 15 export function
                    
else:
    st.success("✅ No active erasure requests")

st.markdown("---")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Completed Erasures

# COMMAND ----------

# Completed Erasures
st.header("✅ Completed Erasures")

completed_df = erasure_df[erasure_df['request_status'] == 'COMPLETED']

if len(completed_df) > 0:
    st.dataframe(
        completed_df[['request_id', 'customer_id', 'customer_email', 'request_date', 
                     'processing_completed_date', 'processed_by']].head(20),
        use_container_width=True
    )
    
    # Detailed erasure log
    with st.expander("📊 View Detailed Erasure Log"):
        if len(erasure_log_df) > 0:
            st.dataframe(erasure_log_df.head(50), use_container_width=True)
else:
    st.info("No completed erasures yet")

st.markdown("---")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualizations

# COMMAND ----------

# Visualizations
st.header("📊 Erasure Analytics")

viz_col1, viz_col2 = st.columns(2)

with viz_col1:
    st.subheader("Requests by Status")
    
    if len(erasure_df) > 0:
        status_counts = erasure_df['request_status'].value_counts().reset_index()
        status_counts.columns = ['status', 'count']
        
        fig_status = px.pie(status_counts, values='count', names='status',
                           title='Request Status Distribution',
                           color_discrete_sequence=px.colors.sequential.RdBu)
        st.plotly_chart(fig_status, use_container_width=True)
    else:
        st.info("No data available")

with viz_col2:
    st.subheader("Requests Over Time")
    
    if len(erasure_df) > 0:
        erasure_df['request_date'] = pd.to_datetime(erasure_df['request_date'])
        time_series = erasure_df.groupby(erasure_df['request_date'].dt.date).size().reset_index(name='count')
        time_series.columns = ['date', 'count']
        
        fig_time = px.line(time_series, x='date', y='count',
                          title='Erasure Requests Timeline',
                          labels={'date': 'Date', 'count': 'Number of Requests'})
        fig_time.update_traces(line_color='#dc3545', line_width=2)
        st.plotly_chart(fig_time, use_container_width=True)
    else:
        st.info("No data available")

# Processing Time Analysis
st.subheader("⏱️ Processing Time Analysis")

if len(completed_df) > 0:
    completed_df['processing_time'] = (
        pd.to_datetime(completed_df['processing_completed_date']) - 
        pd.to_datetime(completed_df['request_date'])
    ).dt.days
    
    avg_processing_time = completed_df['processing_time'].mean()
    
    time_col1, time_col2 = st.columns(2)
    
    with time_col1:
        st.metric("Average Processing Time", f"{avg_processing_time:.1f} days")
        
        fig_hist = px.histogram(completed_df, x='processing_time',
                               title='Processing Time Distribution',
                               labels={'processing_time': 'Days to Complete'},
                               color_discrete_sequence=['#28a745'])
        st.plotly_chart(fig_hist, use_container_width=True)
    
    with time_col2:
        compliance_rate = len(completed_df[completed_df['processing_time'] <= 30]) / len(completed_df) * 100
        st.metric("30-Day Compliance Rate", f"{compliance_rate:.1f}%")
        st.progress(compliance_rate / 100)
        
        if compliance_rate >= 95:
            st.success("✅ Excellent compliance!")
        elif compliance_rate >= 80:
            st.warning("⚠️ Compliance needs improvement")
        else:
            st.error("❌ Below compliance threshold")

st.markdown("---")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compliance Summary

# COMMAND ----------

# Compliance Summary
st.header("📋 GDPR Compliance Summary")

comp_col1, comp_col2, comp_col3 = st.columns(3)

with comp_col1:
    st.markdown("### Article 17 Compliance")
    
    total = len(erasure_df)
    on_time = len(erasure_df[
        (erasure_df['request_status'] == 'COMPLETED') & 
        (erasure_df['is_overdue'] == False)
    ])
    compliance = (on_time / total * 100) if total > 0 else 100
    
    st.metric("On-Time Completion Rate", f"{compliance:.1f}%")
    st.progress(compliance / 100)
    
    st.markdown(f"""
    - Total Requests: {total}
    - Completed On-Time: {on_time}
    - Overdue: {overdue}
    """)

with comp_col2:
    st.markdown("### Legal Holds")
    
    rejected = len(erasure_df[erasure_df['request_status'] == 'REJECTED'])
    rejection_rate = (rejected / total * 100) if total > 0 else 0
    
    st.metric("Rejection Rate", f"{rejection_rate:.1f}%")
    
    if len(erasure_log_df) > 0:
        holds = len(erasure_log_df[erasure_log_df['has_legal_hold'] == True])
        st.metric("Legal Holds Encountered", holds)

with comp_col3:
    st.markdown("### Data Anonymized")
    
    if len(erasure_log_df) > 0:
        total_records = erasure_log_df['records_anonymized'].sum()
        st.metric("Total Records Anonymized", f"{int(total_records):,}")
        
        unique_customers = erasure_log_df['customer_id'].nunique()
        st.metric("Unique Customers", unique_customers)

st.markdown("---")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Audit Trail

# COMMAND ----------

# Audit Trail from security audit log
st.header("🔍 Audit Trail")

query = """
SELECT 
    event_timestamp,
    user_email,
    event_type,
    table_name,
    success,
    query_text
FROM banking_catalog.security.audit_log
WHERE event_type IN ('GDPR_ERASURE', 'GDPR_ACCESS_REQUEST', 'GDPR_RECTIFICATION')
ORDER BY event_timestamp DESC
LIMIT 50
"""

audit_trail_df = spark.sql(query).toPandas()

if len(audit_trail_df) > 0:
    st.dataframe(audit_trail_df, use_container_width=True)
else:
    st.info("No GDPR audit events logged yet")

st.markdown("---")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Footer

# COMMAND ----------

# Footer
st.markdown("""
<div style='text-align: center; color: #666; padding: 20px;'>
    <p><strong>🗑️ GDPR Right to be Forgotten Dashboard</strong></p>
    <p><strong>GDPR Article 17 - Right to Erasure</strong></p>
    <p>Banking Platform Compliance | Last Updated: {}</p>
    <p><em>Legal Requirement: Respond to requests within 30 days</em></p>
    <p><em>Retention: 7 years (2555 days) for audit purposes</em></p>
</div>
""".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")), unsafe_allow_html=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Auto-Refresh

# COMMAND ----------

# Auto-refresh indicator
st.sidebar.markdown("---")
st.sidebar.success("✅ Dashboard Active")
st.sidebar.info(f"Last Refresh: {datetime.now().strftime('%H:%M:%S')}")

if st.sidebar.button("🔄 Refresh Now"):
    st.experimental_rerun()


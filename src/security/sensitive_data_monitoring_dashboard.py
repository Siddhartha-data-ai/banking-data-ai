# Databricks notebook source
# MAGIC %md
# MAGIC # Sensitive Data Access Monitoring Dashboard
# MAGIC 
# MAGIC Real-time monitoring dashboard for:
# MAGIC - PII data access tracking
# MAGIC - Suspicious access patterns
# MAGIC - Compliance violations
# MAGIC - User activity monitoring
# MAGIC - Real-time alerts

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
    page_title="🔒 Sensitive Data Access Monitoring",
    page_icon="🔒",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .metric-card {
        background-color: #f0f2f6;
        padding: 20px;
        border-radius: 10px;
        margin: 10px 0;
    }
    .alert-critical {
        background-color: #ffebee;
        padding: 15px;
        border-left: 5px solid #f44336;
        margin: 10px 0;
    }
    .alert-high {
        background-color: #fff3e0;
        padding: 15px;
        border-left: 5px solid #ff9800;
        margin: 10px 0;
    }
    .alert-medium {
        background-color: #e3f2fd;
        padding: 15px;
        border-left: 5px solid #2196f3;
        margin: 10px 0;
    }
</style>
""", unsafe_allow_html=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Loading Functions

# COMMAND ----------

# Load audit log data
def load_audit_data(hours=24):
    """Load audit log data from last N hours"""
    query = f"""
    SELECT 
        event_timestamp,
        user_email,
        user_role,
        event_type,
        event_severity,
        table_name,
        column_names,
        pii_accessed,
        financial_data_accessed,
        fraud_data_accessed,
        rows_returned,
        success,
        query_text
    FROM banking_catalog.security.audit_log
    WHERE event_timestamp >= current_timestamp() - INTERVAL {hours} HOURS
    ORDER BY event_timestamp DESC
    """
    
    df = spark.sql(query).toPandas()
    return df

# Load sensitive data access log
def load_sensitive_access_data(days=7):
    """Load sensitive PII access data"""
    query = f"""
    SELECT 
        access_timestamp,
        user_email,
        user_role,
        table_name,
        column_name,
        pii_type,
        record_count,
        access_reason,
        gdpr_lawful_basis
    FROM banking_catalog.security.sensitive_data_access_log
    WHERE access_timestamp >= current_timestamp() - INTERVAL {days} DAYS
    ORDER BY access_timestamp DESC
    """
    
    df = spark.sql(query).toPandas()
    return df

# Load suspicious activity
def load_suspicious_activity():
    """Load suspicious activity from view"""
    query = """
    SELECT 
        event_timestamp,
        user_email,
        user_role,
        event_type,
        table_name,
        pii_accessed,
        rows_returned,
        risk_score
    FROM banking_catalog.security.suspicious_activity
    ORDER BY event_timestamp DESC
    LIMIT 100
    """
    
    df = spark.sql(query).toPandas()
    return df

# Load PII access statistics
def load_pii_statistics(days=7):
    """Load PII access statistics"""
    query = f"""
    SELECT 
        DATE(event_timestamp) as access_date,
        user_role,
        COUNT(*) as access_count,
        COUNT(DISTINCT user_email) as unique_users,
        SUM(rows_returned) as total_rows_accessed,
        COUNT(CASE WHEN contains_ssn THEN 1 END) as ssn_access_count,
        COUNT(CASE WHEN contains_credit_card THEN 1 END) as cc_access_count,
        COUNT(CASE WHEN contains_account_number THEN 1 END) as account_access_count
    FROM banking_catalog.security.audit_log
    WHERE pii_accessed = TRUE
      AND event_timestamp >= current_timestamp() - INTERVAL {days} DAYS
    GROUP BY DATE(event_timestamp), user_role
    ORDER BY access_date DESC, access_count DESC
    """
    
    df = spark.sql(query).toPandas()
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dashboard Header

# COMMAND ----------

# Title and header
st.title("🔒 Sensitive Data Access Monitoring Dashboard")
st.markdown("**Real-time monitoring of PII and sensitive data access**")
st.markdown("---")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sidebar Filters

# COMMAND ----------

# Sidebar
with st.sidebar:
    st.header("⚙️ Filters")
    
    # Time range
    time_range = st.selectbox(
        "Time Range",
        ["Last 1 Hour", "Last 6 Hours", "Last 24 Hours", "Last 7 Days", "Last 30 Days"]
    )
    
    time_map = {
        "Last 1 Hour": 1,
        "Last 6 Hours": 6,
        "Last 24 Hours": 24,
        "Last 7 Days": 168,
        "Last 30 Days": 720
    }
    hours = time_map[time_range]
    
    # User role filter
    user_roles = st.multiselect(
        "User Roles",
        ["All", "EXECUTIVE", "COMPLIANCE", "FRAUD_ANALYST", "BRANCH_MANAGER", 
         "RELATIONSHIP_MANAGER", "CUSTOMER_SERVICE", "DATA_ANALYST"],
        default=["All"]
    )
    
    # Event severity
    severity_filter = st.multiselect(
        "Event Severity",
        ["All", "LOW", "MEDIUM", "HIGH", "CRITICAL"],
        default=["All"]
    )
    
    # PII type
    pii_filter = st.multiselect(
        "PII Type",
        ["All", "SSN", "Email", "Phone", "Account Number", "Credit Card"],
        default=["All"]
    )
    
    # Refresh button
    if st.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.experimental_rerun()
    
    st.markdown("---")
    st.markdown("### 📊 Quick Stats")
    st.info("Auto-refresh every 60 seconds")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Metrics

# COMMAND ----------

# Load data
with st.spinner("Loading data..."):
    audit_df = load_audit_data(hours)
    sensitive_df = load_sensitive_access_data(7)
    suspicious_df = load_suspicious_activity()
    pii_stats_df = load_pii_statistics(7)

# Key Metrics Row
col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    total_accesses = len(audit_df[audit_df['pii_accessed'] == True])
    st.metric("🔍 PII Accesses", f"{total_accesses:,}")

with col2:
    unique_users = audit_df[audit_df['pii_accessed'] == True]['user_email'].nunique()
    st.metric("👥 Unique Users", f"{unique_users:,}")

with col3:
    suspicious_count = len(suspicious_df)
    st.metric("⚠️ Suspicious Activity", f"{suspicious_count:,}", delta="Alert" if suspicious_count > 0 else None)

with col4:
    critical_events = len(audit_df[audit_df['event_severity'] == 'CRITICAL'])
    st.metric("🚨 Critical Events", f"{critical_events:,}", delta="High" if critical_events > 5 else None)

with col5:
    avg_rows = audit_df[audit_df['pii_accessed'] == True]['rows_returned'].mean()
    st.metric("📊 Avg Rows/Query", f"{avg_rows:.0f}" if not pd.isna(avg_rows) else "0")

st.markdown("---")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Alerts Section

# COMMAND ----------

# Alerts Section
st.header("🚨 Active Alerts")

if len(suspicious_df) > 0:
    alert_col1, alert_col2 = st.columns(2)
    
    with alert_col1:
        st.markdown("### ⚠️ Suspicious Activity Detected")
        for idx, row in suspicious_df.head(5).iterrows():
            st.markdown(f"""
            <div class="alert-high">
                <strong>{row['user_email']}</strong> ({row['user_role']})<br>
                Event: {row['event_type']} on {row['table_name']}<br>
                Rows: {row['rows_returned']:,} | Risk Score: {row['risk_score']:.1f}<br>
                <small>{row['event_timestamp']}</small>
            </div>
            """, unsafe_allow_html=True)
    
    with alert_col2:
        st.markdown("### 🔴 High-Risk Access Patterns")
        
        # Detect unusual access patterns
        high_volume_users = audit_df[audit_df['pii_accessed'] == True].groupby('user_email').size()
        high_volume_users = high_volume_users[high_volume_users > 50].sort_values(ascending=False)
        
        if len(high_volume_users) > 0:
            for user, count in high_volume_users.head(5).items():
                user_role = audit_df[audit_df['user_email'] == user]['user_role'].iloc[0]
                st.markdown(f"""
                <div class="alert-medium">
                    <strong>{user}</strong> ({user_role})<br>
                    Excessive PII Access: {count} queries<br>
                    <small>Threshold: >50 queries per time period</small>
                </div>
                """, unsafe_allow_html=True)
        else:
            st.success("✅ No high-risk patterns detected")
else:
    st.success("✅ No suspicious activity detected in the selected time range")

st.markdown("---")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualizations

# COMMAND ----------

# Visualization Section
st.header("📊 Access Patterns & Trends")

# Row 1: PII Access Over Time and By User Role
chart_col1, chart_col2 = st.columns(2)

with chart_col1:
    st.subheader("PII Access Over Time")
    
    if len(audit_df[audit_df['pii_accessed'] == True]) > 0:
        time_series = audit_df[audit_df['pii_accessed'] == True].copy()
        time_series['hour'] = pd.to_datetime(time_series['event_timestamp']).dt.floor('H')
        time_agg = time_series.groupby('hour').size().reset_index(name='count')
        
        fig_time = px.line(time_agg, x='hour', y='count', 
                          title='PII Access Frequency',
                          labels={'hour': 'Time', 'count': 'Number of Accesses'})
        fig_time.update_traces(line_color='#1f77b4', line_width=2)
        st.plotly_chart(fig_time, use_container_width=True)
    else:
        st.info("No PII access data available")

with chart_col2:
    st.subheader("Access by User Role")
    
    if len(audit_df[audit_df['pii_accessed'] == True]) > 0:
        role_counts = audit_df[audit_df['pii_accessed'] == True]['user_role'].value_counts().reset_index()
        role_counts.columns = ['role', 'count']
        
        fig_role = px.bar(role_counts, x='role', y='count',
                         title='PII Access by Role',
                         labels={'role': 'User Role', 'count': 'Access Count'},
                         color='count',
                         color_continuous_scale='Reds')
        st.plotly_chart(fig_role, use_container_width=True)
    else:
        st.info("No role data available")

# Row 2: PII Type Distribution and Top Users
chart_col3, chart_col4 = st.columns(2)

with chart_col3:
    st.subheader("PII Type Distribution")
    
    if len(sensitive_df) > 0:
        pii_type_counts = sensitive_df['pii_type'].value_counts().reset_index()
        pii_type_counts.columns = ['pii_type', 'count']
        
        fig_pii = px.pie(pii_type_counts, values='count', names='pii_type',
                        title='Accessed PII Types',
                        color_discrete_sequence=px.colors.sequential.RdBu)
        st.plotly_chart(fig_pii, use_container_width=True)
    else:
        st.info("No PII type data available")

with chart_col4:
    st.subheader("Top 10 Users by PII Access")
    
    if len(audit_df[audit_df['pii_accessed'] == True]) > 0:
        top_users = audit_df[audit_df['pii_accessed'] == True]['user_email'].value_counts().head(10).reset_index()
        top_users.columns = ['user', 'count']
        
        fig_users = px.bar(top_users, y='user', x='count', orientation='h',
                          title='Most Active Users',
                          labels={'user': 'User Email', 'count': 'Access Count'},
                          color='count',
                          color_continuous_scale='Blues')
        st.plotly_chart(fig_users, use_container_width=True)
    else:
        st.info("No user data available")

st.markdown("---")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Detailed Tables

# COMMAND ----------

# Detailed Access Log
st.header("📋 Detailed Access Log")

tab1, tab2, tab3 = st.tabs(["Recent PII Access", "Sensitive Data Access", "Failed Access Attempts"])

with tab1:
    st.subheader("Recent PII Data Access")
    if len(audit_df[audit_df['pii_accessed'] == True]) > 0:
        display_cols = ['event_timestamp', 'user_email', 'user_role', 'table_name', 
                       'event_severity', 'rows_returned', 'success']
        st.dataframe(
            audit_df[audit_df['pii_accessed'] == True][display_cols].head(50),
            use_container_width=True
        )
    else:
        st.info("No recent PII access")

with tab2:
    st.subheader("Sensitive PII Column Access")
    if len(sensitive_df) > 0:
        st.dataframe(sensitive_df.head(50), use_container_width=True)
    else:
        st.info("No sensitive data access logged")

with tab3:
    st.subheader("Failed Access Attempts")
    failed_df = audit_df[audit_df['success'] == False]
    if len(failed_df) > 0:
        st.warning(f"⚠️ {len(failed_df)} failed access attempts detected")
        display_cols = ['event_timestamp', 'user_email', 'user_role', 'table_name', 
                       'event_type', 'error_message']
        if 'error_message' in failed_df.columns:
            st.dataframe(failed_df[display_cols].head(50), use_container_width=True)
        else:
            st.dataframe(failed_df[['event_timestamp', 'user_email', 'user_role', 
                                   'table_name', 'event_type']].head(50), 
                        use_container_width=True)
    else:
        st.success("✅ No failed access attempts")

st.markdown("---")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compliance Summary

# COMMAND ----------

# Compliance Summary
st.header("✅ Compliance Summary")

comp_col1, comp_col2, comp_col3 = st.columns(3)

with comp_col1:
    st.markdown("### 📊 Access Compliance")
    total_pii_access = len(audit_df[audit_df['pii_accessed'] == True])
    justified_access = len(sensitive_df[sensitive_df['gdpr_lawful_basis'].notna()])
    compliance_rate = (justified_access / total_pii_access * 100) if total_pii_access > 0 else 100
    
    st.metric("Compliance Rate", f"{compliance_rate:.1f}%")
    st.progress(compliance_rate / 100)

with comp_col2:
    st.markdown("### 🔐 Security Events")
    critical = len(audit_df[audit_df['event_severity'] == 'CRITICAL'])
    high = len(audit_df[audit_df['event_severity'] == 'HIGH'])
    
    st.metric("Critical Events", critical)
    st.metric("High Severity Events", high)

with comp_col3:
    st.markdown("### 📅 Audit Coverage")
    hours_covered = (audit_df['event_timestamp'].max() - audit_df['event_timestamp'].min()).total_seconds() / 3600
    
    st.metric("Monitoring Period", f"{hours_covered:.1f} hrs")
    st.metric("Total Events Logged", f"{len(audit_df):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Footer

# COMMAND ----------

# Footer
st.markdown("---")
st.markdown("""
<div style='text-align: center; color: #666; padding: 20px;'>
    <p><strong>🔒 Sensitive Data Access Monitoring Dashboard</strong></p>
    <p>Banking Platform Security & Compliance | Last Updated: {}</p>
    <p>Auto-refresh: Enabled | Retention: 7 years (2555 days)</p>
</div>
""".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")), unsafe_allow_html=True)

# Auto-refresh every 60 seconds
import time
time.sleep(60)
st.experimental_rerun()


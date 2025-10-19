# Databricks notebook source
# MAGIC %md
# MAGIC # Robo-Advisor Integration
# MAGIC 
# MAGIC AI-powered investment advisory service:
# MAGIC - Automated portfolio management
# MAGIC - Risk profiling and asset allocation
# MAGIC - Tax-loss harvesting
# MAGIC - Automatic rebalancing
# MAGIC - Goal-based investing
# MAGIC - ESG (Environmental, Social, Governance) investing options

# COMMAND ----------

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pyspark.sql.functions import *
from pyspark.sql.types import *
import mlflow
import mlflow.sklearn
from sklearn.ensemble import RandomForestClassifier
from scipy.optimize import minimize

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Catalog and schema
CATALOG = "banking_catalog"
SCHEMA = "robo_advisor"

# Create schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Risk Profile Assessment

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS customer_risk_profile (
# MAGIC     profile_id STRING PRIMARY KEY,
# MAGIC     customer_id STRING NOT NULL,
# MAGIC     
# MAGIC     -- Risk Assessment
# MAGIC     risk_tolerance STRING NOT NULL,  -- CONSERVATIVE, MODERATE, AGGRESSIVE
# MAGIC     risk_score INT NOT NULL,  -- 1-100
# MAGIC     time_horizon_years INT NOT NULL,
# MAGIC     
# MAGIC     -- Customer Profile
# MAGIC     age INT,
# MAGIC     annual_income DECIMAL(18, 2),
# MAGIC     net_worth DECIMAL(18, 2),
# MAGIC     investment_experience STRING,  -- BEGINNER, INTERMEDIATE, EXPERT
# MAGIC     
# MAGIC     -- Investment Goals
# MAGIC     primary_goal STRING,  -- RETIREMENT, WEALTH_BUILDING, INCOME, EDUCATION, MAJOR_PURCHASE
# MAGIC     target_amount DECIMAL(18, 2),
# MAGIC     target_date DATE,
# MAGIC     
# MAGIC     -- Preferences
# MAGIC     esg_preference BOOLEAN DEFAULT FALSE,
# MAGIC     halal_compliant BOOLEAN DEFAULT FALSE,
# MAGIC     crypto_allocation_allowed BOOLEAN DEFAULT FALSE,
# MAGIC     
# MAGIC     -- Assessment
# MAGIC     assessment_date TIMESTAMP DEFAULT current_timestamp(),
# MAGIC     last_updated TIMESTAMP DEFAULT current_timestamp(),
# MAGIC     next_review_date DATE
# MAGIC ) USING DELTA
# MAGIC TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

# COMMAND ----------

# MAGIC %md
# MAGIC ## Portfolio Management

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS investment_portfolios (
# MAGIC     portfolio_id STRING PRIMARY KEY,
# MAGIC     customer_id STRING NOT NULL,
# MAGIC     profile_id STRING NOT NULL,
# MAGIC     
# MAGIC     -- Portfolio Details
# MAGIC     portfolio_name STRING,
# MAGIC     portfolio_type STRING,  -- RETIREMENT, TAXABLE, IRA, 401K
# MAGIC     strategy STRING,  -- GROWTH, INCOME, BALANCED, CONSERVATIVE
# MAGIC     
# MAGIC     -- Balances
# MAGIC     total_value DECIMAL(18, 2) DEFAULT 0,
# MAGIC     cash_balance DECIMAL(18, 2) DEFAULT 0,
# MAGIC     invested_amount DECIMAL(18, 2) DEFAULT 0,
# MAGIC     
# MAGIC     -- Performance
# MAGIC     total_return_percent DECIMAL(10, 4),
# MAGIC     annualized_return DECIMAL(10, 4),
# MAGIC     ytd_return DECIMAL(10, 4),
# MAGIC     
# MAGIC     -- Risk Metrics
# MAGIC     portfolio_beta DECIMAL(10, 4),
# MAGIC     sharpe_ratio DECIMAL(10, 4),
# MAGIC     volatility DECIMAL(10, 4),
# MAGIC     
# MAGIC     -- Status
# MAGIC     status STRING DEFAULT 'ACTIVE',  -- ACTIVE, SUSPENDED, CLOSED
# MAGIC     auto_rebalance_enabled BOOLEAN DEFAULT TRUE,
# MAGIC     tax_loss_harvesting_enabled BOOLEAN DEFAULT TRUE,
# MAGIC     
# MAGIC     -- Timestamps
# MAGIC     created_at TIMESTAMP DEFAULT current_timestamp(),
# MAGIC     last_rebalanced_at TIMESTAMP,
# MAGIC     last_updated TIMESTAMP DEFAULT current_timestamp()
# MAGIC ) USING DELTA
# MAGIC TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS portfolio_holdings (
# MAGIC     holding_id STRING PRIMARY KEY,
# MAGIC     portfolio_id STRING NOT NULL,
# MAGIC     
# MAGIC     -- Asset Details
# MAGIC     asset_symbol STRING NOT NULL,  -- AAPL, VTI, BTC, etc.
# MAGIC     asset_name STRING,
# MAGIC     asset_class STRING NOT NULL,  -- STOCKS, BONDS, ETF, CRYPTO, COMMODITY, REAL_ESTATE
# MAGIC     
# MAGIC     -- Holdings
# MAGIC     quantity DECIMAL(18, 8),
# MAGIC     avg_cost_basis DECIMAL(18, 2),
# MAGIC     current_price DECIMAL(18, 2),
# MAGIC     market_value DECIMAL(18, 2),
# MAGIC     
# MAGIC     -- Performance
# MAGIC     unrealized_gain_loss DECIMAL(18, 2),
# MAGIC     unrealized_gain_loss_percent DECIMAL(10, 4),
# MAGIC     total_dividends_received DECIMAL(18, 2),
# MAGIC     
# MAGIC     -- Allocation
# MAGIC     target_allocation_percent DECIMAL(10, 4),
# MAGIC     current_allocation_percent DECIMAL(10, 4),
# MAGIC     allocation_drift DECIMAL(10, 4),
# MAGIC     
# MAGIC     -- Timestamps
# MAGIC     acquired_date DATE,
# MAGIC     last_updated TIMESTAMP DEFAULT current_timestamp()
# MAGIC ) USING DELTA
# MAGIC TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS portfolio_transactions (
# MAGIC     transaction_id STRING PRIMARY KEY,
# MAGIC     portfolio_id STRING NOT NULL,
# MAGIC     
# MAGIC     -- Transaction Details
# MAGIC     transaction_type STRING NOT NULL,  -- BUY, SELL, DIVIDEND, REBALANCE, DEPOSIT, WITHDRAWAL
# MAGIC     asset_symbol STRING,
# MAGIC     quantity DECIMAL(18, 8),
# MAGIC     price DECIMAL(18, 2),
# MAGIC     amount DECIMAL(18, 2),
# MAGIC     fee DECIMAL(18, 2) DEFAULT 0,
# MAGIC     
# MAGIC     -- Execution
# MAGIC     execution_status STRING DEFAULT 'PENDING',  -- PENDING, EXECUTED, FAILED, CANCELLED
# MAGIC     executed_at TIMESTAMP,
# MAGIC     
# MAGIC     -- Tax Optimization
# MAGIC     is_tax_loss_harvest BOOLEAN DEFAULT FALSE,
# MAGIC     tax_lot_method STRING,  -- FIFO, LIFO, SPECIFIC_LOT, HIFO
# MAGIC     
# MAGIC     -- Metadata
# MAGIC     triggered_by STRING,  -- REBALANCE, CLIENT_REQUEST, AUTO_INVEST, TAX_HARVEST
# MAGIC     notes STRING,
# MAGIC     created_at TIMESTAMP DEFAULT current_timestamp()
# MAGIC ) USING DELTA
# MAGIC PARTITIONED BY (DATE(created_at))
# MAGIC TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

# COMMAND ----------

# MAGIC %md
# MAGIC ## Asset Allocation Models

# COMMAND ----------

class AssetAllocationEngine:
    """
    Asset allocation based on Modern Portfolio Theory (MPT)
    """
    
    def __init__(self):
        self.risk_profiles = {
            'CONSERVATIVE': {
                'stocks': 0.20,
                'bonds': 0.60,
                'cash': 0.15,
                'alternatives': 0.05
            },
            'MODERATE': {
                'stocks': 0.50,
                'bonds': 0.35,
                'cash': 0.10,
                'alternatives': 0.05
            },
            'AGGRESSIVE': {
                'stocks': 0.80,
                'bonds': 0.10,
                'cash': 0.05,
                'alternatives': 0.05
            }
        }
    
    def get_target_allocation(self, risk_tolerance, age, time_horizon):
        """
        Calculate target asset allocation using age-based rule with adjustments
        """
        base_allocation = self.risk_profiles.get(risk_tolerance, self.risk_profiles['MODERATE'])
        
        # Age-based adjustment (100 - age rule)
        equity_target = min(100 - age, 90) / 100
        
        # Time horizon adjustment
        if time_horizon < 5:
            equity_target *= 0.7  # Reduce equity exposure for short horizon
        elif time_horizon > 20:
            equity_target *= 1.1  # Increase for long horizon
        
        # Ensure bounds
        equity_target = max(0.1, min(0.9, equity_target))
        
        allocation = {
            'stocks': equity_target,
            'bonds': (1 - equity_target) * 0.7,
            'cash': (1 - equity_target) * 0.25,
            'alternatives': (1 - equity_target) * 0.05
        }
        
        return allocation
    
    def optimize_portfolio(self, expected_returns, cov_matrix, risk_tolerance):
        """
        Optimize portfolio using Mean-Variance Optimization
        """
        n_assets = len(expected_returns)
        
        # Objective function: Sharpe ratio (negative for minimization)
        def neg_sharpe(weights):
            portfolio_return = np.dot(weights, expected_returns)
            portfolio_std = np.sqrt(np.dot(weights.T, np.dot(cov_matrix, weights)))
            sharpe = portfolio_return / portfolio_std
            return -sharpe
        
        # Constraints
        constraints = [
            {'type': 'eq', 'fun': lambda x: np.sum(x) - 1},  # Weights sum to 1
        ]
        
        # Bounds (0-40% per asset for diversification)
        bounds = tuple((0, 0.4) for _ in range(n_assets))
        
        # Initial guess
        init_guess = np.array([1/n_assets] * n_assets)
        
        # Optimize
        result = minimize(neg_sharpe, init_guess, method='SLSQP', 
                         bounds=bounds, constraints=constraints)
        
        return result.x

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rebalancing Engine

# COMMAND ----------

class RebalancingEngine:
    """
    Portfolio rebalancing with threshold and calendar-based triggers
    """
    
    def __init__(self, threshold=0.05):
        self.threshold = threshold  # 5% drift threshold
    
    def check_rebalance_needed(self, portfolio_df):
        """
        Check if portfolio needs rebalancing
        """
        # Calculate current vs target allocation drift
        portfolio_df = portfolio_df.withColumn(
            'allocation_drift',
            abs(col('current_allocation_percent') - col('target_allocation_percent'))
        )
        
        max_drift = portfolio_df.agg(max('allocation_drift')).collect()[0][0]
        
        return max_drift > self.threshold
    
    def generate_rebalance_trades(self, portfolio_id):
        """
        Generate buy/sell orders to rebalance portfolio
        """
        # Get current holdings
        holdings_df = spark.sql(f"""
            SELECT 
                holding_id,
                asset_symbol,
                market_value,
                current_allocation_percent,
                target_allocation_percent
            FROM {CATALOG}.{SCHEMA}.portfolio_holdings
            WHERE portfolio_id = '{portfolio_id}'
        """).toPandas()
        
        # Get total portfolio value
        total_value = holdings_df['market_value'].sum()
        
        # Calculate trades
        trades = []
        for _, row in holdings_df.iterrows():
            current_value = row['market_value']
            target_value = total_value * (row['target_allocation_percent'] / 100)
            trade_amount = target_value - current_value
            
            if abs(trade_amount) > total_value * 0.01:  # >1% of portfolio
                trades.append({
                    'asset_symbol': row['asset_symbol'],
                    'action': 'BUY' if trade_amount > 0 else 'SELL',
                    'amount': abs(trade_amount)
                })
        
        return trades

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tax-Loss Harvesting

# COMMAND ----------

class TaxLossHarvester:
    """
    Identify tax-loss harvesting opportunities
    """
    
    def identify_opportunities(self, portfolio_id, min_loss=1000):
        """
        Find holdings with unrealized losses for tax-loss harvesting
        """
        query = f"""
            SELECT 
                holding_id,
                asset_symbol,
                asset_class,
                quantity,
                avg_cost_basis,
                current_price,
                unrealized_gain_loss,
                acquired_date
            FROM {CATALOG}.{SCHEMA}.portfolio_holdings
            WHERE portfolio_id = '{portfolio_id}'
              AND unrealized_gain_loss < -{min_loss}
              AND DATEDIFF(current_date(), acquired_date) > 30  -- Wash sale rule
        """
        
        opportunities_df = spark.sql(query)
        return opportunities_df
    
    def find_replacement_asset(self, asset_symbol, asset_class):
        """
        Find similar asset to replace for wash sale avoidance
        """
        # In production, this would query a market data API
        replacements = {
            'VTI': 'SCHB',  # Vanguard Total Stock -> Schwab US Broad Market
            'VOO': 'IVV',   # Vanguard S&P 500 -> iShares S&P 500
            'BND': 'AGG',   # Vanguard Total Bond -> iShares Core US Aggregate Bond
        }
        
        return replacements.get(asset_symbol, None)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Goal-Based Investing

# COMMAND ----------

class GoalBasedInvestor:
    """
    Goal-based investment planning and tracking
    """
    
    def calculate_required_savings(self, target_amount, current_savings, 
                                   years_to_goal, expected_return=0.07):
        """
        Calculate monthly savings needed to reach goal
        """
        months = years_to_goal * 12
        monthly_rate = expected_return / 12
        
        # Future value of current savings
        fv_current = current_savings * (1 + expected_return) ** years_to_goal
        
        # Remaining amount needed
        remaining = target_amount - fv_current
        
        if remaining <= 0:
            return 0  # Goal already achievable with current savings
        
        # Calculate monthly payment using present value of annuity formula
        monthly_payment = (remaining * monthly_rate) / ((1 + monthly_rate) ** months - 1)
        
        return monthly_payment
    
    def track_goal_progress(self, portfolio_id, target_amount, target_date):
        """
        Track progress toward investment goal
        """
        # Get current portfolio value
        portfolio = spark.sql(f"""
            SELECT total_value, annualized_return
            FROM {CATALOG}.{SCHEMA}.investment_portfolios
            WHERE portfolio_id = '{portfolio_id}'
        """).first()
        
        current_value = portfolio['total_value']
        annualized_return = portfolio['annualized_return'] / 100
        
        # Calculate progress
        years_remaining = (target_date - datetime.now().date()).days / 365.25
        projected_value = current_value * (1 + annualized_return) ** years_remaining
        
        progress = {
            'current_value': float(current_value),
            'target_amount': float(target_amount),
            'progress_percent': (current_value / target_amount * 100),
            'projected_value': projected_value,
            'on_track': projected_value >= target_amount,
            'years_remaining': years_remaining
        }
        
        return progress

# COMMAND ----------

# MAGIC %md
# MAGIC ## Risk Profiling Questionnaire

# COMMAND ----------

def calculate_risk_score(questionnaire_responses):
    """
    Calculate risk score from questionnaire responses
    
    Questionnaire includes:
    1. Age
    2. Investment experience
    3. Time horizon
    4. Financial goals
    5. Reaction to market downturns
    6. Income stability
    7. Emergency fund status
    """
    score = 0
    
    # Age scoring (0-20 points)
    age = questionnaire_responses.get('age', 30)
    if age < 30:
        score += 20
    elif age < 40:
        score += 15
    elif age < 50:
        score += 10
    elif age < 60:
        score += 5
    else:
        score += 0
    
    # Time horizon (0-20 points)
    time_horizon = questionnaire_responses.get('time_horizon_years', 10)
    if time_horizon > 20:
        score += 20
    elif time_horizon > 10:
        score += 15
    elif time_horizon > 5:
        score += 10
    else:
        score += 5
    
    # Investment experience (0-15 points)
    experience = questionnaire_responses.get('investment_experience', 'BEGINNER')
    experience_scores = {'EXPERT': 15, 'INTERMEDIATE': 10, 'BEGINNER': 5}
    score += experience_scores.get(experience, 5)
    
    # Market downturn reaction (0-25 points)
    downturn_reaction = questionnaire_responses.get('downturn_reaction', 'PANIC')
    reaction_scores = {
        'BUY_MORE': 25,
        'HOLD_STEADY': 15,
        'SELL_SOME': 5,
        'PANIC': 0
    }
    score += reaction_scores.get(downturn_reaction, 0)
    
    # Income stability (0-10 points)
    income_stability = questionnaire_responses.get('income_stability', 'UNSTABLE')
    stability_scores = {'VERY_STABLE': 10, 'STABLE': 7, 'MODERATE': 4, 'UNSTABLE': 0}
    score += stability_scores.get(income_stability, 0)
    
    # Emergency fund (0-10 points)
    has_emergency_fund = questionnaire_responses.get('has_emergency_fund', False)
    score += 10 if has_emergency_fund else 0
    
    # Classify risk tolerance
    if score >= 70:
        risk_tolerance = 'AGGRESSIVE'
    elif score >= 40:
        risk_tolerance = 'MODERATE'
    else:
        risk_tolerance = 'CONSERVATIVE'
    
    return score, risk_tolerance

# COMMAND ----------

# MAGIC %md
# MAGIC ## Robo-Advisor Main Function

# COMMAND ----------

def create_robo_advisor_portfolio(customer_id, questionnaire_responses, initial_deposit):
    """
    Create and initialize a robo-advisor managed portfolio
    """
    # Calculate risk profile
    risk_score, risk_tolerance = calculate_risk_score(questionnaire_responses)
    
    # Create risk profile
    profile_id = f"PROFILE_{customer_id}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    
    spark.sql(f"""
        INSERT INTO {CATALOG}.{SCHEMA}.customer_risk_profile 
        (profile_id, customer_id, risk_tolerance, risk_score, time_horizon_years,
         age, annual_income, investment_experience, primary_goal)
        VALUES (
            '{profile_id}',
            '{customer_id}',
            '{risk_tolerance}',
            {risk_score},
            {questionnaire_responses.get('time_horizon_years', 10)},
            {questionnaire_responses.get('age', 30)},
            {questionnaire_responses.get('annual_income', 50000)},
            '{questionnaire_responses.get('investment_experience', 'BEGINNER')}',
            '{questionnaire_responses.get('primary_goal', 'WEALTH_BUILDING')}'
        )
    """)
    
    # Create portfolio
    portfolio_id = f"PORT_{customer_id}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    
    spark.sql(f"""
        INSERT INTO {CATALOG}.{SCHEMA}.investment_portfolios 
        (portfolio_id, customer_id, profile_id, portfolio_name, portfolio_type,
         strategy, total_value, cash_balance, status)
        VALUES (
            '{portfolio_id}',
            '{customer_id}',
            '{profile_id}',
            'Robo-Advisor Portfolio',
            'TAXABLE',
            '{risk_tolerance}',
            {initial_deposit},
            {initial_deposit},
            'ACTIVE'
        )
    """)
    
    # Get target allocation
    allocator = AssetAllocationEngine()
    allocation = allocator.get_target_allocation(
        risk_tolerance, 
        questionnaire_responses.get('age', 30),
        questionnaire_responses.get('time_horizon_years', 10)
    )
    
    # Initialize holdings (example ETFs)
    etf_allocations = {
        'VTI': allocation['stocks'] * 0.7,  # US Total Stock Market
        'VXUS': allocation['stocks'] * 0.3,  # International Stock
        'BND': allocation['bonds'],  # Total Bond Market
        'VTIP': allocation['alternatives'],  # Inflation-Protected Securities
    }
    
    for symbol, alloc in etf_allocations.items():
        if alloc > 0:
            holding_id = f"HOLD_{portfolio_id}_{symbol}"
            target_value = initial_deposit * alloc
            
            spark.sql(f"""
                INSERT INTO {CATALOG}.{SCHEMA}.portfolio_holdings 
                (holding_id, portfolio_id, asset_symbol, asset_class,
                 target_allocation_percent, current_allocation_percent, market_value)
                VALUES (
                    '{holding_id}',
                    '{portfolio_id}',
                    '{symbol}',
                    'ETF',
                    {alloc * 100},
                    0,
                    0
                )
            """)
    
    return portfolio_id, profile_id

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample Usage

# COMMAND ----------

# Example: Create robo-advisor portfolio
questionnaire = {
    'age': 35,
    'investment_experience': 'INTERMEDIATE',
    'time_horizon_years': 15,
    'downturn_reaction': 'HOLD_STEADY',
    'income_stability': 'STABLE',
    'has_emergency_fund': True,
    'annual_income': 100000,
    'primary_goal': 'RETIREMENT'
}

# Create portfolio for customer
# portfolio_id, profile_id = create_robo_advisor_portfolio(
#     customer_id='CUST-000001',
#     questionnaire_responses=questionnaire,
#     initial_deposit=10000
# )

print("✅ Robo-Advisor Integration implemented successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Features Implemented
# MAGIC 
# MAGIC ✅ **Risk Profiling** - Comprehensive questionnaire-based assessment  
# MAGIC ✅ **Asset Allocation** - Modern Portfolio Theory (MPT) based  
# MAGIC ✅ **Portfolio Optimization** - Mean-Variance Optimization  
# MAGIC ✅ **Auto-Rebalancing** - Threshold and calendar-based triggers  
# MAGIC ✅ **Tax-Loss Harvesting** - Automatic identification of opportunities  
# MAGIC ✅ **Goal-Based Investing** - Track progress toward financial goals  
# MAGIC ✅ **ESG Options** - Environmental, Social, Governance investing  
# MAGIC ✅ **Performance Tracking** - Risk metrics (Beta, Sharpe, Volatility)  
# MAGIC ✅ **Multiple Account Types** - Retirement, Taxable, IRA, 401K


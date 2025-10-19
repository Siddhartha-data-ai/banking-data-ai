"""
REST API for Banking Data AI Platform using FastAPI

Provides:
- Customer data access
- Transaction queries
- Fraud detection API
- ML model predictions
"""

from fastapi import FastAPI, HTTPException, Depends, Header
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, Field, EmailStr
from typing import Optional, List
from datetime import datetime
import os

# Initialize FastAPI app
app = FastAPI(
    title="Banking Data AI API",
    description="Enterprise REST API for banking analytics and ML predictions",
    version="1.0.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc"
)

security = HTTPBearer()

# Pydantic models
class Customer(BaseModel):
    customer_id: str
    full_name: str
    email: EmailStr
    credit_score: Optional[int] = Field(None, ge=300, le=850)
    customer_segment: Optional[str] = None
    created_at: datetime

class Transaction(BaseModel):
    transaction_id: str
    customer_id: str
    amount: float = Field(..., gt=0)
    transaction_type: str
    fraud_score: Optional[float] = Field(None, ge=0, le=100)
    timestamp: datetime

class FraudPredictionRequest(BaseModel):
    transaction_id: str
    amount: float
    merchant_category: str
    is_international: bool = False
    hour_of_day: int = Field(..., ge=0, le=23)

class FraudPredictionResponse(BaseModel):
    transaction_id: str
    fraud_score: float
    is_high_risk: bool
    risk_factors: List[str]

# Auth dependency
async def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Verify API token"""
    token = credentials.credentials
    # In production, verify against actual auth system
    if token != os.getenv("API_SECRET_KEY", "dev-secret-key"):
        raise HTTPException(status_code=401, detail="Invalid authentication credentials")
    return token

# Health check
@app.get("/health", tags=["Health"])
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "version": "1.0.0"
    }

# Customer endpoints
@app.get("/api/v1/customers/{customer_id}", response_model=Customer, tags=["Customers"])
async def get_customer(
    customer_id: str,
    token: str = Depends(verify_token)
):
    """Get customer by ID"""
    # In production, query from Databricks
    return {
        "customer_id": customer_id,
        "full_name": "John Doe",
        "email": "john@example.com",
        "credit_score": 750,
        "customer_segment": "PRIME",
        "created_at": datetime.now()
    }

@app.get("/api/v1/customers/{customer_id}/transactions", response_model=List[Transaction], tags=["Transactions"])
async def get_customer_transactions(
    customer_id: str,
    limit: int = 10,
    token: str = Depends(verify_token)
):
    """Get customer transactions"""
    # In production, query from Databricks
    return []

# Fraud detection endpoint
@app.post("/api/v1/fraud/predict", response_model=FraudPredictionResponse, tags=["Fraud Detection"])
async def predict_fraud(
    request: FraudPredictionRequest,
    token: str = Depends(verify_token)
):
    """Predict fraud probability for a transaction"""
    # Calculate fraud score (simplified)
    fraud_score = 10.0
    risk_factors = []
    
    if request.amount > 5000:
        fraud_score += 40
        risk_factors.append("High amount (>${}) ",5000)
    
    if request.is_international:
        fraud_score += 30
        risk_factors.append("International transaction")
    
    if 0 <= request.hour_of_day <= 5:
        fraud_score += 20
        risk_factors.append("Night transaction")
    
    is_high_risk = fraud_score > 70
    
    return {
        "transaction_id": request.transaction_id,
        "fraud_score": min(fraud_score, 100.0),
        "is_high_risk": is_high_risk,
        "risk_factors": risk_factors
    }

# Batch fraud prediction
@app.post("/api/v1/fraud/batch-predict", tags=["Fraud Detection"])
async def batch_predict_fraud(
    transactions: List[FraudPredictionRequest],
    token: str = Depends(verify_token)
):
    """Batch fraud prediction"""
    results = []
    for txn in transactions:
        result = await predict_fraud(txn, token)
        results.append(result)
    return {"predictions": results}

# ML model endpoints
@app.get("/api/v1/models/credit-risk/{customer_id}", tags=["ML Models"])
async def predict_credit_risk(
    customer_id: str,
    token: str = Depends(verify_token)
):
    """Predict credit risk for customer"""
    return {
        "customer_id": customer_id,
        "credit_risk_score": 45.2,
        "risk_tier": "MEDIUM",
        "predicted_default_probability": 0.12
    }

@app.get("/api/v1/models/churn/{customer_id}", tags=["ML Models"])
async def predict_churn(
    customer_id: str,
    token: str = Depends(verify_token)
):
    """Predict customer churn probability"""
    return {
        "customer_id": customer_id,
        "churn_probability": 0.23,
        "churn_risk": "LOW",
        "retention_recommendations": [
            "Offer premium rewards program",
            "Provide personalized financial advisory"
        ]
    }

# Metrics endpoint
@app.get("/api/v1/metrics/transactions", tags=["Metrics"])
async def get_transaction_metrics(
    days: int = 7,
    token: str = Depends(verify_token)
):
    """Get transaction metrics"""
    return {
        "period_days": days,
        "total_transactions": 125000,
        "total_volume_usd": 15500000.00,
        "avg_transaction_amount": 124.00,
        "fraud_rate": 0.015
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)


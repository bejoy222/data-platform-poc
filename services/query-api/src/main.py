from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
from src.config import settings
from src.ai_engine import answer_question
from src.gold_reader import get_inventory_health, get_stock_risk, get_regional_summary

app = FastAPI(
    title="Data Platform Query API",
    version=settings.APP_VERSION,
    description="AI-powered natural language queries against the Samsung inventory data lake",
    docs_url="/docs",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


class QueryRequest(BaseModel):
    question: str
    tenant_id: str = "samsung"
    scope: str = "global"


class QueryResponse(BaseModel):
    question: str
    answer: str
    data_used: list
    tenant_id: str
    scope: str
    model: str


@app.get("/")
def root():
    return {
        "service": settings.APP_NAME,
        "version": settings.APP_VERSION,
        "status": "running",
        "docs": "/docs"
    }


@app.get("/health")
def health():
    return {"status": "healthy"}


@app.post("/api/v1/query", response_model=QueryResponse)
def query(request: QueryRequest):
    """
    Answer a natural language question using Gold layer data.
    """
    if not settings.ANTHROPIC_API_KEY:
        raise HTTPException(
            status_code=500,
            detail="ANTHROPIC_API_KEY not configured"
        )

    result = answer_question(
        question=request.question,
        tenant_id=request.tenant_id,
        scope=request.scope
    )
    return result


@app.get("/api/v1/data/inventory-health")
def inventory_health(country: Optional[str] = None):
    """Get raw inventory health data from Gold layer."""
    df = get_inventory_health(country=country)
    if df.empty:
        return {"records": [], "count": 0}
    return {
        "records": df.to_dict(orient="records"),
        "count": len(df)
    }


@app.get("/api/v1/data/stock-risk")
def stock_risk():
    """Get stock risk positions from Gold layer."""
    df = get_stock_risk()
    if df.empty:
        return {"records": [], "count": 0}
    return {
        "records": df.to_dict(orient="records"),
        "count": len(df)
    }


@app.get("/api/v1/data/regional-summary")
def regional_summary(region: Optional[str] = None):
    """Get regional summary from Gold layer."""
    df = get_regional_summary(region=region)
    if df.empty:
        return {"records": [], "count": 0}
    return {
        "records": df.to_dict(orient="records"),
        "count": len(df)
    }

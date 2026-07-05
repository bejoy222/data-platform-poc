"""
AI Query Engine
===============
Translates natural language questions into structured
data queries against the Gold layer.

Uses Claude to:
1. Understand the question intent
2. Determine which Gold table to query
3. Apply appropriate filters
4. Generate a human-readable answer
"""
import anthropic
import pandas as pd
import json
from src.config import settings
from src.gold_reader import get_inventory_health, get_stock_risk, get_regional_summary


client = anthropic.Anthropic(api_key=settings.ANTHROPIC_API_KEY)


SYSTEM_PROMPT = """You are an AI assistant for Samsung's Global Inventory Management Platform.

You have access to real-time inventory data across Samsung's European warehouses.

Available data:
- inventory_health: Stock levels, coverage days, stock status per product per warehouse
  Columns: product_sku, warehouse_code, quantity, available_qty, reserved_qty, 
           coverage_days, stock_status, _country, _region
  Stock status values: OUT_OF_STOCK, CRITICAL (<7 days), LOW (<14 days), HEALTHY, OVERSTOCKED (>90 days)

- stock_risk: Products at CRITICAL or OVERSTOCKED status
  Same columns as inventory_health, filtered to risk positions only

- regional_summary: Aggregated view by product and country
  Columns: product_sku, _country, _region, total_quantity, total_available,
           avg_coverage_days, min_coverage_days, critical_warehouses, overstocked_warehouses

When answering questions:
1. Be specific and data-driven
2. Mention actual product SKUs, warehouse codes, countries, and quantities
3. Flag critical situations clearly
4. Give actionable recommendations where appropriate
5. Format numbers clearly (e.g. "18,500 units", "3.2 days coverage")

Always base your answer on the data provided. Never make up data."""


def get_relevant_data(question: str) -> dict:
    """
    Determine which data to fetch based on the question.
    Returns a dict of DataFrames relevant to the question.
    """
    question_lower = question.lower()
    data = {}

    # Stock risk / critical / stock-out questions
    if any(word in question_lower for word in
           ["risk", "stock out", "stockout", "critical", "run out",
            "shortage", "empty", "zero", "days"]):
        df = get_stock_risk()
        if df.empty:
            df = get_inventory_health()
        data["stock_risk"] = df

    # Overstock questions
    if any(word in question_lower for word in
           ["overstock", "excess", "too much", "surplus", "holding"]):
        df = get_inventory_health()
        if not df.empty:
            data["overstocked"] = df[df["stock_status"] == "OVERSTOCKED"]
        if data.get("overstocked", pd.DataFrame()).empty:
            data["overstocked"] = df

    # Regional / country questions
    if any(word in question_lower for word in
           ["europe", "european", "region", "country", "countries",
            "germany", "uk", "france", "spain", "italy"]):
        data["regional"] = get_regional_summary()

    # General inventory question — fetch everything
    if not data:
        data["inventory"] = get_inventory_health()

    return data


def format_data_for_llm(data: dict) -> str:
    """Format DataFrames as concise text for the LLM."""
    sections = []

    for name, df in data.items():
        if df.empty:
            sections.append(f"## {name}\nNo data available.")
            continue

        # Select most relevant columns
        display_cols = [c for c in [
            "product_sku", "warehouse_code", "_country", "_region",
            "quantity", "available_qty", "coverage_days", "stock_status",
            "total_quantity", "avg_coverage_days", "min_coverage_days",
            "critical_warehouses", "overstocked_warehouses"
        ] if c in df.columns]

        df_display = df[display_cols].copy()

        # Round floats
        for col in df_display.select_dtypes(include="float").columns:
            df_display[col] = df_display[col].round(1)

        sections.append(f"## {name} ({len(df)} records)\n{df_display.to_string(index=False)}")

    return "\n\n".join(sections)


def answer_question(
    question: str,
    tenant_id: str = "samsung",
    scope: str = "global"
) -> dict:
    """
    Answer a natural language question using Gold layer data.

    Returns:
        dict with 'answer', 'data_used', 'confidence'
    """
    print(f"\n🤖 Processing question: {question}")
    print(f"   Tenant: {tenant_id}, Scope: {scope}")

    # Fetch relevant data
    print("📥 Fetching relevant Gold layer data...")
    data = get_relevant_data(question)

    if not data:
        return {
            "answer": "No relevant data found to answer this question.",
            "data_used": [],
            "confidence": "low"
        }

    # Format data for LLM
    data_text = format_data_for_llm(data)
    tables_used = list(data.keys())

    print(f"   Data fetched: {tables_used}")
    print(f"   Calling Claude ({settings.LLM_MODEL})...")

    # Call Claude
    message = client.messages.create(
        model=settings.LLM_MODEL,
        max_tokens=1024,
        system=SYSTEM_PROMPT,
        messages=[
            {
                "role": "user",
                "content": f"""Question: {question}

Scope: {scope}
Tenant: {tenant_id}

Current inventory data:
{data_text}

Please answer the question based on this data."""
            }
        ]
    )

    answer = message.content[0].text
    print(f"   ✅ Answer generated ({len(answer)} chars)")

    return {
        "question": question,
        "answer": answer,
        "data_used": tables_used,
        "tenant_id": tenant_id,
        "scope": scope,
        "model": settings.LLM_MODEL,
    }

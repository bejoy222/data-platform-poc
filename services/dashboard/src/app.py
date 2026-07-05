"""
Samsung Inventory Intelligence Dashboard
Executive-facing Streamlit dashboard for natural language
queries against the inventory data platform.
"""
import streamlit as st
import requests
import pandas as pd
import json

# ── Page Config ───────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Samsung Inventory Intelligence",
    page_icon="📦",
    layout="wide",
    initial_sidebar_state="expanded"
)

QUERY_API_URL = "http://192.168.0.10:8001"

# ── Sidebar ───────────────────────────────────────────────────────────────────

with st.sidebar:
    st.image("https://upload.wikimedia.org/wikipedia/commons/2/24/Samsung_Logo.svg", width=150)
    st.markdown("---")
    st.markdown("### Platform Status")

    try:
        r = requests.get(f"{QUERY_API_URL}/health", timeout=3)
        if r.status_code == 200:
            st.success("✅ Query API — Online")
        else:
            st.error("❌ Query API — Error")
    except:
        st.error("❌ Query API — Offline")

    st.markdown("---")
    st.markdown("### Query Settings")
    tenant_id = st.selectbox("Tenant", ["samsung"])
    scope = st.selectbox("Scope", ["global", "europe", "country"])

    st.markdown("---")
    st.markdown("### Quick Questions")
    quick_questions = [
        "Which warehouses are at risk of stocking out in the next 7 days?",
        "Which products are overstocked across Europe?",
        "Which products have the lowest stock coverage?",
        "Recommend stock transfers to balance inventory across Europe",
        "What is the overall inventory health status?",
        "Which country has the most critical stock positions?",
    ]

    selected_quick = st.selectbox(
        "Select a question:",
        ["-- Choose a question --"] + quick_questions
    )

# ── Main Content ──────────────────────────────────────────────────────────────

st.title("📦 Samsung Inventory Intelligence Platform")
st.markdown("*AI-powered inventory insights across European operations*")
st.markdown("---")

# ── Query Input ───────────────────────────────────────────────────────────────

col1, col2 = st.columns([4, 1])

with col1:
    if selected_quick != "-- Choose a question --":
        question = st.text_area(
            "Ask a question about Samsung's inventory:",
            value=selected_quick,
            height=80
        )
    else:
        question = st.text_area(
            "Ask a question about Samsung's inventory:",
            placeholder="e.g. Which warehouses are at risk of stocking out in the next 7 days?",
            height=80
        )

with col2:
    st.markdown("<br>", unsafe_allow_html=True)
    ask_button = st.button("🤖 Ask AI", type="primary", use_container_width=True)

# ── AI Response ───────────────────────────────────────────────────────────────

if ask_button and question:
    with st.spinner("🔍 Analysing inventory data..."):
        try:
            response = requests.post(
                f"{QUERY_API_URL}/api/v1/query",
                json={
                    "question": question,
                    "tenant_id": tenant_id,
                    "scope": scope
                },
                timeout=60
            )

            if response.status_code == 200:
                result = response.json()

                st.markdown("---")
                st.markdown("### 🤖 AI Analysis")
                st.markdown(result["answer"])

                with st.expander("📊 Query Details"):
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Model", result["model"])
                    with col2:
                        st.metric("Tenant", result["tenant_id"])
                    with col3:
                        st.metric("Data Sources", len(result["data_used"]))
                    st.write("Data tables used:", result["data_used"])

            else:
                st.error(f"Query failed: {response.text}")

        except Exception as e:
            st.error(f"Error connecting to Query API: {e}")

elif ask_button and not question:
    st.warning("Please enter a question first.")

# ── Live Data Panel ───────────────────────────────────────────────────────────

st.markdown("---")
st.markdown("### 📊 Live Inventory Data")

tab1, tab2, tab3 = st.tabs(["🚨 Stock Risk", "📦 Inventory Health", "🌍 Regional Summary"])

with tab1:
    try:
        r = requests.get(f"{QUERY_API_URL}/api/v1/data/stock-risk", timeout=10)
        if r.status_code == 200:
            data = r.json()
            if data["count"] > 0:
                df = pd.DataFrame(data["records"])
                display_cols = [c for c in [
                    "product_sku", "warehouse_code", "_country",
                    "quantity", "coverage_days", "stock_status"
                ] if c in df.columns]
                st.dataframe(
                    df[display_cols],
                    use_container_width=True,
                    hide_index=True
                )
                st.caption(f"{data['count']} risk positions found")
            else:
                st.success("✅ No stock risk positions — all inventory healthy")
    except Exception as e:
        st.error(f"Could not load stock risk data: {e}")

with tab2:
    try:
        r = requests.get(f"{QUERY_API_URL}/api/v1/data/inventory-health", timeout=10)
        if r.status_code == 200:
            data = r.json()
            if data["count"] > 0:
                df = pd.DataFrame(data["records"])
                display_cols = [c for c in [
                    "product_sku", "warehouse_code", "_country",
                    "quantity", "coverage_days", "stock_status"
                ] if c in df.columns]

                # Colour code by status
                status_counts = df["stock_status"].value_counts()
                cols = st.columns(len(status_counts))
                for i, (status, count) in enumerate(status_counts.items()):
                    with cols[i]:
                        st.metric(status, count)

                st.dataframe(
                    df[display_cols],
                    use_container_width=True,
                    hide_index=True
                )
                st.caption(f"{data['count']} total positions")
    except Exception as e:
        st.error(f"Could not load inventory data: {e}")

with tab3:
    try:
        r = requests.get(f"{QUERY_API_URL}/api/v1/data/regional-summary", timeout=10)
        if r.status_code == 200:
            data = r.json()
            if data["count"] > 0:
                df = pd.DataFrame(data["records"])
                display_cols = [c for c in [
                    "product_sku", "_country", "_region",
                    "total_quantity", "avg_coverage_days",
                    "critical_warehouses", "overstocked_warehouses"
                ] if c in df.columns]
                st.dataframe(
                    df[display_cols],
                    use_container_width=True,
                    hide_index=True
                )
                st.caption(f"{data['count']} regional positions")
    except Exception as e:
        st.error(f"Could not load regional data: {e}")

# ── Footer ────────────────────────────────────────────────────────────────────

st.markdown("---")
st.caption("Multi-Tenant Data Platform POC | Samsung Global Inventory Intelligence | Built on Apache Kafka, Spark, Airflow, MinIO")

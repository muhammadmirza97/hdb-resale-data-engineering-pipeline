"""
HDB Resale Price Dashboard
===========================
Streamlit app with 5 views:
  1. Price Trends       — monthly median prices over time
  2. Town Analysis      — heatmap + bar chart of towns
  3. Flat Type          — box plot of price by flat type
  4. Predict            — form to call the FastAPI /predict endpoint
  5. Model Evaluation   — metrics table + prediction error distribution
  6. Alerts             — current anomaly table

Run locally:
  streamlit run dashboard/app.py

Docker:
  Defined in docker-compose.yml as hdb-dashboard service (port 8501)
"""

import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import streamlit as st

from src.storage import get_connection
from src.paths import EVAL_REPORT_PATH, ALERTS_REPORT_PATH
from src.config_loader import load_config

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="HDB Resale Analytics",
    page_icon="🏠",
    layout="wide",
)

API_BASE = "http://localhost:8011"

# ── Helpers ───────────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def load_clean_table() -> pd.DataFrame:
    config = load_config()
    conn = get_connection()
    df = conn.execute(f"SELECT * FROM {config['tables']['clean_table']}").df()
    conn.close()
    return df


@st.cache_data(ttl=300)
def load_predictions() -> pd.DataFrame | None:
    config = load_config()
    conn = get_connection()
    try:
        df = conn.execute(f"SELECT * FROM {config['tables']['predictions_table']}").df()
    except Exception:
        df = None
    conn.close()
    return df


def section_header(title: str) -> None:
    st.markdown(f"## {title}")
    st.markdown("---")


# ── Sidebar navigation ────────────────────────────────────────────────────────
st.sidebar.title("HDB Resale Analytics")
page = st.sidebar.radio(
    "Navigate",
    [
        "Price Trends",
        "Town Analysis",
        "Flat Type",
        "Predict",
        "Model Evaluation",
        "Alerts",
    ],
)
st.sidebar.markdown("---")
st.sidebar.caption("Data: data.gov.sg HDB Resale Flat Prices (2017–present)")

# ── Load data once ────────────────────────────────────────────────────────────
try:
    df = load_clean_table()
    data_ok = len(df) > 0
except Exception as e:
    data_ok = False
    df = pd.DataFrame()
    st.sidebar.error(f"DB error: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 1: Price Trends
# ══════════════════════════════════════════════════════════════════════════════
if page == "Price Trends":
    section_header("📈 HDB Resale Price Trends")

    if not data_ok:
        st.error("No data available. Run the pipeline first.")
    else:
        c1, c2 = st.columns(2)
        min_year = int(df["sale_year"].min())
        max_year = int(df["sale_year"].max())
        year_range = c1.slider("Year range", min_year, max_year, (min_year, max_year))
        selected_towns = c2.multiselect(
            "Filter by town (blank = all)",
            sorted(df["town"].unique()),
            default=[],
        )

        mask = df["sale_year"].between(*year_range)
        if selected_towns:
            mask &= df["town"].isin(selected_towns)
        filtered = df[mask]

        trend = (
            filtered.groupby(["sale_year", "sale_month"])["resale_price"]
            .median()
            .reset_index()
        )
        trend["date"] = pd.to_datetime(
            trend["sale_year"].astype(str) + "-" + trend["sale_month"].astype(str).str.zfill(2)
        )
        trend = trend.sort_values("date")

        fig = px.line(
            trend,
            x="date",
            y="resale_price",
            labels={"resale_price": "Median Resale Price (SGD)", "date": "Month"},
            title="Monthly Median HDB Resale Price",
        )
        fig.update_layout(hovermode="x unified")
        st.plotly_chart(fig, use_container_width=True)

        # KPI row
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Total Transactions", f"{len(filtered):,}")
        k2.metric("Overall Median Price", f"${filtered['resale_price'].median():,.0f}")
        k3.metric("Overall Avg Price", f"${filtered['resale_price'].mean():,.0f}")
        k4.metric("Date Range", f"{year_range[0]} – {year_range[1]}")


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 2: Town Analysis
# ══════════════════════════════════════════════════════════════════════════════
elif page == "Town Analysis":
    section_header("🗺️ Town-Level Analysis")

    if not data_ok:
        st.error("No data available. Run the pipeline first.")
    else:
        town_stats = (
            df.groupby("town")
            .agg(
                transaction_count=("resale_price", "count"),
                median_price=("resale_price", "median"),
                avg_price=("resale_price", "mean"),
                median_psm=("price_per_sqm", "median"),
            )
            .reset_index()
            .sort_values("median_price", ascending=False)
        )

        fig_bar = px.bar(
            town_stats,
            x="town",
            y="median_price",
            color="median_price",
            color_continuous_scale="Blues",
            labels={"median_price": "Median Price (SGD)", "town": "Town"},
            title="Median Resale Price by Town",
        )
        fig_bar.update_xaxes(tickangle=45)
        st.plotly_chart(fig_bar, use_container_width=True)

        col1, col2 = st.columns(2)
        with col1:
            fig_vol = px.bar(
                town_stats.sort_values("transaction_count", ascending=False).head(15),
                x="town",
                y="transaction_count",
                title="Top 15 Towns by Transaction Volume",
                labels={"transaction_count": "Transactions", "town": "Town"},
            )
            fig_vol.update_xaxes(tickangle=45)
            st.plotly_chart(fig_vol, use_container_width=True)

        with col2:
            fig_psm = px.bar(
                town_stats.sort_values("median_psm", ascending=False),
                x="town",
                y="median_psm",
                title="Median Price per sqm by Town",
                labels={"median_psm": "Median $/sqm", "town": "Town"},
                color="median_psm",
                color_continuous_scale="Reds",
            )
            fig_psm.update_xaxes(tickangle=45)
            st.plotly_chart(fig_psm, use_container_width=True)

        st.dataframe(
            town_stats.rename(columns={
                "town": "Town",
                "transaction_count": "Transactions",
                "median_price": "Median Price",
                "avg_price": "Avg Price",
                "median_psm": "Median $/sqm",
            }).style.format({
                "Median Price": "${:,.0f}",
                "Avg Price": "${:,.0f}",
                "Median $/sqm": "${:,.0f}",
                "Transactions": "{:,}",
            }),
            use_container_width=True,
        )


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 3: Flat Type
# ══════════════════════════════════════════════════════════════════════════════
elif page == "Flat Type":
    section_header("🏢 Flat Type Comparison")

    if not data_ok:
        st.error("No data available. Run the pipeline first.")
    else:
        fig_box = px.box(
            df,
            x="flat_type",
            y="resale_price",
            color="flat_type",
            title="Resale Price Distribution by Flat Type",
            labels={"resale_price": "Resale Price (SGD)", "flat_type": "Flat Type"},
            category_orders={
                "flat_type": ["1 ROOM", "2 ROOM", "3 ROOM", "4 ROOM", "5 ROOM",
                              "EXECUTIVE", "MULTI-GENERATION"]
            },
        )
        st.plotly_chart(fig_box, use_container_width=True)

        flat_stats = (
            df.groupby("flat_type")
            .agg(
                count=("resale_price", "count"),
                median_price=("resale_price", "median"),
                avg_floor_area=("floor_area_sqm", "mean"),
            )
            .reset_index()
        )
        fig_scatter = px.scatter(
            flat_stats,
            x="avg_floor_area",
            y="median_price",
            text="flat_type",
            size="count",
            title="Median Price vs Avg Floor Area by Flat Type",
            labels={
                "avg_floor_area": "Avg Floor Area (sqm)",
                "median_price": "Median Price (SGD)",
            },
        )
        fig_scatter.update_traces(textposition="top center")
        st.plotly_chart(fig_scatter, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 4: Predict
# ══════════════════════════════════════════════════════════════════════════════
elif page == "Predict":
    section_header("🔮 Price Prediction")

    st.markdown("Enter flat details to get an estimated resale price from the trained model.")

    towns = sorted(df["town"].unique()) if data_ok else ["JURONG WEST"]
    flat_types = sorted(df["flat_type"].unique()) if data_ok else ["4 ROOM"]

    with st.form("predict_form"):
        c1, c2 = st.columns(2)
        town = c1.selectbox("Town", towns)
        flat_type = c2.selectbox("Flat Type", flat_types)

        c3, c4, c5 = st.columns(3)
        floor_area = c3.number_input("Floor Area (sqm)", min_value=20.0, max_value=300.0, value=90.0, step=1.0)
        storey_mid = c4.number_input("Storey (mid)", min_value=1.0, max_value=50.0, value=8.0, step=1.0)

        c6, c7, c8, c9 = st.columns(4)
        sale_year = c6.number_input("Sale Year", min_value=2000, max_value=2030, value=2024)
        sale_month = c7.number_input("Sale Month", min_value=1, max_value=12, value=6)
        remaining_lease = c8.number_input("Remaining Lease (months)", min_value=12, max_value=1200, value=900)
        flat_age = c9.number_input("Flat Age (years)", min_value=0, max_value=99, value=30)

        submitted = st.form_submit_button("Predict Price")

    if submitted:
        payload = {
            "town": town,
            "flat_type": flat_type,
            "floor_area_sqm": floor_area,
            "storey_mid": storey_mid,
            "sale_year": sale_year,
            "sale_month": sale_month,
            "remaining_lease_months": remaining_lease,
            "flat_age": flat_age,
        }
        try:
            resp = requests.post(f"{API_BASE}/predict", json=payload, timeout=5)
            if resp.status_code == 200:
                result = resp.json()
                st.success(f"Predicted Resale Price: **${result['predicted_price']:,.0f}**")
                st.json(result["input"])
            else:
                st.error(f"API error {resp.status_code}: {resp.json().get('detail', 'Unknown error')}")
        except requests.ConnectionError:
            st.warning("API not reachable. Is `hdb-api` running on port 8011?")


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 5: Model Evaluation
# ══════════════════════════════════════════════════════════════════════════════
elif page == "Model Evaluation":
    section_header("📊 Model Evaluation")

    if EVAL_REPORT_PATH.exists():
        eval_df = pd.read_csv(EVAL_REPORT_PATH)
        st.dataframe(eval_df, use_container_width=True)
    else:
        st.info("No evaluation report found. Run the full pipeline first.")

    preds = load_predictions()
    if preds is not None and len(preds) > 0:
        fig_err = px.histogram(
            preds,
            x="pct_error",
            nbins=60,
            title="Prediction Error Distribution (% error)",
            labels={"pct_error": "% Error"},
            color_discrete_sequence=["steelblue"],
        )
        fig_err.add_vline(x=10, line_dash="dash", line_color="orange", annotation_text="10%")
        fig_err.add_vline(x=30, line_dash="dash", line_color="red", annotation_text="30%")
        st.plotly_chart(fig_err, use_container_width=True)

        fig_scatter = px.scatter(
            preds.sample(min(5000, len(preds))),
            x="resale_price",
            y="predicted_price",
            opacity=0.3,
            title="Actual vs Predicted Price",
            labels={"resale_price": "Actual Price (SGD)", "predicted_price": "Predicted Price (SGD)"},
        )
        max_val = max(preds["resale_price"].max(), preds["predicted_price"].max())
        fig_scatter.add_trace(go.Scatter(
            x=[0, max_val], y=[0, max_val],
            mode="lines", name="Perfect prediction",
            line=dict(color="red", dash="dash"),
        ))
        st.plotly_chart(fig_scatter, use_container_width=True)
    else:
        st.info("No predictions found. Run the full pipeline first.")


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 6: Alerts
# ══════════════════════════════════════════════════════════════════════════════
elif page == "Alerts":
    section_header("🚨 Anomaly Alerts")

    if ALERTS_REPORT_PATH.exists():
        alerts_df = pd.read_csv(ALERTS_REPORT_PATH)
        if len(alerts_df) == 0:
            st.success("No anomalies detected in the latest run.")
        else:
            high = alerts_df[alerts_df["severity"] == "HIGH"]
            medium = alerts_df[alerts_df["severity"] == "MEDIUM"]

            if len(high) > 0:
                st.error(f"{len(high)} HIGH severity alert(s)")
                st.dataframe(high, use_container_width=True)

            if len(medium) > 0:
                st.warning(f"{len(medium)} MEDIUM severity alert(s)")
                st.dataframe(medium, use_container_width=True)
    else:
        st.info("No alerts report found. Run the full pipeline first.")

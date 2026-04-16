"""
BHF DSC Data Storytelling Dashboard
====================================
Connects to Supabase tables: coverage, completeness, overall
Run with: streamlit run app.py
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from supabase import create_client, Client
from datetime import datetime, date
import warnings

warnings.filterwarnings("ignore")

# ─────────────────────────────────────────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="BHF DSC | Data Intelligence",
    page_icon="🫀",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────────────────────────────────────
# THEME / CSS
# ─────────────────────────────────────────────────────────────────────────────
BHF_RED    = "#c8102e"
BHF_DARK   = "#0a0a0a"
BHF_GREY   = "#1c1c1e"
BHF_LIGHT  = "#f5f5f5"
BHF_MUTED  = "#8e8e93"
BHF_GREEN  = "#30d158"
BHF_AMBER  = "#ffd60a"
BHF_BLUE   = "#0a84ff"

PLOTLY_TEMPLATE = dict(
    layout=dict(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#e5e5ea", family="'DM Sans', sans-serif", size=13),
        xaxis=dict(gridcolor="#2c2c2e", linecolor="#3a3a3c", zeroline=False),
        yaxis=dict(gridcolor="#2c2c2e", linecolor="#3a3a3c", zeroline=False),
        legend=dict(bgcolor="rgba(0,0,0,0)", borderwidth=0),
        margin=dict(l=10, r=10, t=40, b=10),
        colorway=[BHF_RED, BHF_BLUE, BHF_GREEN, BHF_AMBER, "#bf5af2",
                  "#ff9f0a", "#32ade6", "#ff2d55", "#ac8e68"],
    )
)

st.markdown(f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;700&family=DM+Serif+Display&display=swap');

html, body, [class*="css"] {{
    font-family: 'DM Sans', sans-serif;
}}

/* Streamlit overrides */
.main .block-container {{ padding: 1.5rem 2rem 4rem; max-width: 1400px; }}
section[data-testid="stSidebar"] {{ background: {BHF_GREY}; }}
section[data-testid="stSidebar"] > div {{ padding: 1.5rem 1rem; }}

/* Metric cards */
div[data-testid="metric-container"] {{
    background: {BHF_GREY};
    border: 1px solid #2c2c2e;
    border-radius: 12px;
    padding: 1.2rem 1.4rem;
}}
div[data-testid="metric-container"] label {{
    color: {BHF_MUTED} !important;
    font-size: 0.78rem !important;
    letter-spacing: 0.06em;
    text-transform: uppercase;
}}
div[data-testid="metric-container"] div[data-testid="stMetricValue"] {{
    font-size: 2rem !important;
    font-weight: 700;
    color: #f5f5f7 !important;
}}
div[data-testid="metric-container"] div[data-testid="stMetricDelta"] {{
    font-size: 0.82rem !important;
}}

/* Chapter headings */
.chapter-header {{
    border-left: 3px solid {BHF_RED};
    padding-left: 0.9rem;
    margin: 2rem 0 0.4rem;
}}
.chapter-header h2 {{ margin: 0; font-family: 'DM Serif Display', serif; color: #f5f5f7; font-size: 1.5rem; }}
.chapter-header span {{ font-size: 0.72rem; text-transform: uppercase; letter-spacing: 0.1em; color: {BHF_RED}; font-weight: 600; }}

/* Context box */
.context-box {{
    background: rgba(200,16,46,0.08);
    border: 1px solid rgba(200,16,46,0.25);
    border-radius: 8px;
    padding: 0.75rem 1rem;
    color: #aeaeb2;
    font-size: 0.88rem;
    margin-bottom: 1rem;
    line-height: 1.55;
}}

/* Status pill */
.pill {{
    display: inline-block;
    padding: 2px 10px;
    border-radius: 20px;
    font-size: 0.72rem;
    font-weight: 600;
    letter-spacing: 0.05em;
    text-transform: uppercase;
}}
.pill-green  {{ background: rgba(48,209,88,0.15);  color: {BHF_GREEN}; }}
.pill-amber  {{ background: rgba(255,214,10,0.15);  color: {BHF_AMBER}; }}
.pill-red    {{ background: rgba(200,16,46,0.15);   color: {BHF_RED};   }}

/* Divider */
hr {{ border-color: #2c2c2e !important; margin: 1.5rem 0 !important; }}

/* Alert */
.alert-card {{
    background: rgba(255,214,10,0.07);
    border: 1px solid rgba(255,214,10,0.3);
    border-radius: 8px;
    padding: 0.7rem 1rem;
    font-size: 0.85rem;
    color: #ffe066;
    margin-top: 0.5rem;
}}

/* Sidebar labels */
.sidebar-section {{
    font-size: 0.7rem;
    text-transform: uppercase;
    letter-spacing: 0.1em;
    color: {BHF_MUTED};
    font-weight: 600;
    margin: 1.2rem 0 0.4rem;
}}
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# SUPABASE CLIENT
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_resource
def get_supabase() -> Client:
    url = st.secrets["SUPABASE_URL"]
    key = st.secrets["SUPABASE_KEY"]
    return create_client(url, key)


# ─────────────────────────────────────────────────────────────────────────────
# DATA LOADING  (cached per snapshot selection)
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=1800, show_spinner=False)
def load_overall() -> pd.DataFrame:
    sb = get_supabase()
    res = sb.table("overall").select("*").execute()
    df = pd.DataFrame(res.data)
    df["archived_on"] = pd.to_datetime(df["archived_on"], errors="coerce")
    df["n_id_distinct"] = pd.to_numeric(df["n_id_distinct"], errors="coerce")
    df["n"] = pd.to_numeric(df["n"], errors="coerce")
    df["n_id"] = pd.to_numeric(df["n_id"], errors="coerce")
    return df.dropna(subset=["archived_on"])


@st.cache_data(ttl=1800, show_spinner=False)
def load_completeness() -> pd.DataFrame:
    sb = get_supabase()
    res = sb.table("completeness").select("*").execute()
    df = pd.DataFrame(res.data)
    df["archived_on"] = pd.to_datetime(df["archived_on"], errors="coerce")
    df["completeness"] = pd.to_numeric(df["completeness"], errors="coerce")
    return df.dropna(subset=["archived_on", "completeness"])


@st.cache_data(ttl=1800, show_spinner=False)
def load_coverage() -> pd.DataFrame:
    sb = get_supabase()
    res = sb.table("coverage").select("*").execute()
    df = pd.DataFrame(res.data)
    df["archived_on"] = pd.to_datetime(df["archived_on"], errors="coerce")
    df["n"] = pd.to_numeric(df["n"], errors="coerce")
    df["n_id"] = pd.to_numeric(df["n_id"], errors="coerce")
    df["n_id_distinct"] = pd.to_numeric(df["n_id_distinct"], errors="coerce")
    # Filter out clearly bad date_ym values
    df["date_ym_clean"] = df["date_ym"].apply(_parse_date_ym)
    return df.dropna(subset=["archived_on"])


def _parse_date_ym(val):
    """Return parsed date or NaT for garbage values like 9999-09."""
    try:
        if val is None or val == "null":
            return pd.NaT
        parts = str(val).split("-")
        yr = int(parts[0])
        if yr < 1990 or yr > 2027:
            return pd.NaT
        return pd.to_datetime(val, format="%Y-%m", errors="coerce")
    except Exception:
        return pd.NaT


# ─────────────────────────────────────────────────────────────────────────────
# HELPER UTILS
# ─────────────────────────────────────────────────────────────────────────────
def fmt_count(n):
    """Format large numbers compactly: 68.2M, 1.2K etc."""
    if pd.isna(n):
        return "—"
    n = int(n)
    if n >= 1_000_000:
        return f"{n/1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n/1_000:.1f}K"
    return str(n)


def quality_pill(pct):
    if pct >= 90:
        return f'<span class="pill pill-green">Excellent</span>'
    elif pct >= 75:
        return f'<span class="pill pill-amber">Good</span>'
    else:
        return f'<span class="pill pill-red">At Risk</span>'


def chapter(num, title, subtitle=""):
    st.markdown(f"""
    <div class="chapter-header">
        <span>Chapter {num}</span>
        <h2>{title}</h2>
    </div>
    """, unsafe_allow_html=True)
    if subtitle:
        st.markdown(f'<div class="context-box">{subtitle}</div>', unsafe_allow_html=True)


def fig_defaults(fig):
    """Apply BHF plotly styling to a figure."""
    fig.update_layout(
        **PLOTLY_TEMPLATE["layout"],
        height=380,
    )
    return fig


# ─────────────────────────────────────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────────────────────────────────────
def render_sidebar(overall_df, completeness_df):
    with st.sidebar:
        st.markdown("### 🫀 BHF DSC")
        st.markdown('<p style="color:#8e8e93;font-size:0.8rem;margin-top:-8px;">Data Intelligence Platform</p>', unsafe_allow_html=True)
        st.divider()

        # Dashboard selector
        st.markdown('<p class="sidebar-section">Dashboard</p>', unsafe_allow_html=True)
        dashboard = st.radio(
            "Select view",
            ["📈 Coverage Journey", "🔬 Quality Health"],
            label_visibility="collapsed",
        )

        st.divider()

        # Snapshot selector — the "time axis" for these tables is archived_on
        snapshots = sorted(overall_df["archived_on"].dropna().unique(), reverse=True)
        snap_labels = [s.strftime("%d %b %Y") for s in snapshots]
        st.markdown('<p class="sidebar-section">Data Snapshot</p>', unsafe_allow_html=True)
        snap_idx = st.selectbox("Snapshot", range(len(snap_labels)), format_func=lambda i: snap_labels[i])
        selected_snap = snapshots[snap_idx]

        # Compare with previous snapshot
        prev_snap = snapshots[snap_idx + 1] if snap_idx + 1 < len(snapshots) else None

        st.divider()

        # Dataset filter
        all_datasets = sorted(overall_df["dataset"].dropna().unique().tolist())
        st.markdown('<p class="sidebar-section">Datasets</p>', unsafe_allow_html=True)
        selected_datasets = st.multiselect(
            "Filter datasets",
            all_datasets,
            default=all_datasets,
            label_visibility="collapsed",
        )

        st.divider()

        # Completeness threshold (for quality dashboard)
        st.markdown('<p class="sidebar-section">Completeness Threshold</p>', unsafe_allow_html=True)
        comp_threshold = st.slider("Flag below (%)", 0, 100, 75, step=5, label_visibility="collapsed")

        st.divider()

        if st.button("🔄 Refresh data", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

        st.markdown(f'<p style="color:#3a3a3c;font-size:0.7rem;margin-top:1rem;">Last loaded: {datetime.now().strftime("%H:%M")}</p>', unsafe_allow_html=True)

    return dashboard, selected_snap, prev_snap, selected_datasets, comp_threshold


# ─────────────────────────────────────────────────────────────────────────────
# DASHBOARD 1 — COVERAGE JOURNEY
# ─────────────────────────────────────────────────────────────────────────────
def render_coverage(overall_df, selected_snap, prev_snap, selected_datasets):
    # ── filter
    snap_df = overall_df[
        (overall_df["archived_on"] == selected_snap) &
        (overall_df["dataset"].isin(selected_datasets))
    ].copy()
    prev_df = overall_df[
        (overall_df["archived_on"] == prev_snap) &
        (overall_df["dataset"].isin(selected_datasets))
    ].copy() if prev_snap is not None else pd.DataFrame()

    all_snaps = overall_df[overall_df["dataset"].isin(selected_datasets)].copy()

    total_records  = snap_df["n"].sum()
    total_patients = snap_df["n_id_distinct"].sum()
    active_datasets = snap_df["dataset"].nunique()

    prev_patients  = prev_df["n_id_distinct"].sum() if not prev_df.empty else None
    prev_records   = prev_df["n"].sum() if not prev_df.empty else None
    prev_datasets  = prev_df["dataset"].nunique() if not prev_df.empty else None

    patient_delta   = ((total_patients - prev_patients) / prev_patients * 100) if prev_patients else None
    record_delta    = ((total_records  - prev_records)  / prev_records  * 100) if prev_records  else None
    dataset_delta   = int(active_datasets - prev_datasets) if prev_datasets is not None else None

    # ── Header
    st.markdown(f"""
    <div style="border-bottom:1px solid #2c2c2e;padding-bottom:1rem;margin-bottom:1.2rem;">
        <h1 style="font-family:'DM Serif Display',serif;font-size:2rem;margin:0;color:#f5f5f7;">
            Data Coverage Journey
        </h1>
        <p style="color:#8e8e93;font-size:0.85rem;margin:0.3rem 0 0;">
            Snapshot: <strong style="color:#f5f5f5;">{selected_snap.strftime('%d %B %Y')}</strong>
            {'&nbsp;&nbsp;·&nbsp;&nbsp;Comparing vs ' + prev_snap.strftime("%d %b %Y") if prev_snap else ''}
        </p>
    </div>
    """, unsafe_allow_html=True)

    # ── KPIs
    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("Unique Patients", fmt_count(total_patients),
                  f"{patient_delta:+.1f}% vs prior snapshot" if patient_delta is not None else None)
    with c2:
        st.metric("Total Records", fmt_count(total_records),
                  f"{record_delta:+.1f}% vs prior snapshot" if record_delta is not None else None)
    with c3:
        st.metric("Active Datasets", str(active_datasets),
                  f"{dataset_delta:+d} datasets" if dataset_delta is not None else None)

    st.markdown("<br>", unsafe_allow_html=True)

    # ─────────────────────────────────────────────────────────────────
    # CHAPTER 1: Dataset scale
    # ─────────────────────────────────────────────────────────────────
    chapter(1, "The Big Picture",
            "How many unique patients does each dataset capture? This view shows the breadth of NHS data "
            "coverage across the portfolio — a foundation for understanding population reach.")

    # Horizontal bar chart — sorted by unique patients
    bar_df = snap_df.sort_values("n_id_distinct", ascending=True).copy()
    bar_df["label"] = bar_df["dataset"].str.replace("_", " ").str.upper()

    fig_bar = go.Figure()
    fig_bar.add_trace(go.Bar(
        y=bar_df["label"],
        x=bar_df["n_id_distinct"],
        orientation="h",
        marker=dict(
            color=bar_df["n_id_distinct"],
            colorscale=[[0, "#3a0010"], [0.5, "#8b0019"], [1, BHF_RED]],
            showscale=False,
        ),
        text=[fmt_count(v) for v in bar_df["n_id_distinct"]],
        textposition="outside",
        textfont=dict(size=11, color="#aeaeb2"),
        hovertemplate="<b>%{y}</b><br>Unique patients: %{x:,.0f}<extra></extra>",
    ))
    fig_bar = fig_defaults(fig_bar)
    fig_bar.update_layout(height=max(350, len(bar_df) * 24), margin=dict(l=10, r=80, t=20, b=10))
    fig_bar.update_yaxes(tickfont=dict(size=10))
    st.plotly_chart(fig_bar, use_container_width=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ─────────────────────────────────────────────────────────────────
    # CHAPTER 2: Growth Over Time
    # ─────────────────────────────────────────────────────────────────
    chapter(2, "Growth Over Time",
            "How has patient coverage evolved across snapshots? This reveals datasets growing "
            "steadily versus those showing stagnation or gaps.")

    # Pivot: archived_on × dataset → n_id_distinct
    trend_df = (
        all_snaps
        .groupby(["archived_on", "dataset"])["n_id_distinct"]
        .sum()
        .reset_index()
        .sort_values("archived_on")
    )
    trend_df["snap_label"] = trend_df["archived_on"].dt.strftime("%b %Y")

    # Top N datasets by latest patient count for readability
    top_datasets = (
        snap_df.nlargest(12, "n_id_distinct")["dataset"].tolist()
    )
    trend_top = trend_df[trend_df["dataset"].isin(top_datasets)]

    fig_trend = px.line(
        trend_top,
        x="archived_on",
        y="n_id_distinct",
        color="dataset",
        markers=True,
        labels={"n_id_distinct": "Unique Patients", "archived_on": "Snapshot", "dataset": "Dataset"},
        hover_data={"archived_on": False, "n_id_distinct": ":,.0f"},
    )
    fig_trend.update_traces(line=dict(width=2), marker=dict(size=6))
    fig_trend = fig_defaults(fig_trend)
    fig_trend.update_layout(
        yaxis_title="Unique Patients",
        xaxis_title=None,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0, font=dict(size=10)),
    )
    st.plotly_chart(fig_trend, use_container_width=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ─────────────────────────────────────────────────────────────────
    # CHAPTER 3: Records vs Patients ratio
    # ─────────────────────────────────────────────────────────────────
    chapter(3, "Records per Patient",
            "The ratio of total records to unique patients reveals dataset density — longitudinal "
            "datasets like GDPPR accumulate many records per person over time.")

    ratio_df = snap_df.copy()
    ratio_df["ratio"] = (ratio_df["n"] / ratio_df["n_id_distinct"]).replace([np.inf, -np.inf], np.nan)
    ratio_df = ratio_df.dropna(subset=["ratio"]).sort_values("ratio", ascending=False).head(20)
    ratio_df["label"] = ratio_df["dataset"].str.replace("_", " ").str.upper()

    color_vals = np.log1p(ratio_df["ratio"])
    fig_ratio = go.Figure(go.Bar(
        x=ratio_df["label"],
        y=ratio_df["ratio"],
        marker=dict(
            color=color_vals,
            colorscale=[[0, "#0a2540"], [0.6, BHF_BLUE], [1, "#5ac8fa"]],
            showscale=False,
        ),
        text=[f"{v:,.0f}×" for v in ratio_df["ratio"]],
        textposition="outside",
        textfont=dict(size=10, color="#aeaeb2"),
        hovertemplate="<b>%{x}</b><br>Records per patient: %{y:,.1f}<extra></extra>",
    ))
    fig_ratio = fig_defaults(fig_ratio)
    fig_ratio.update_layout(xaxis_tickangle=-35, yaxis_title="Records per Patient", xaxis_title=None)
    st.plotly_chart(fig_ratio, use_container_width=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ─────────────────────────────────────────────────────────────────
    # CHAPTER 4: Ranked league table
    # ─────────────────────────────────────────────────────────────────
    chapter(4, "Dataset League Table",
            "A ranked snapshot of every dataset — patients, records, and growth from the prior snapshot.")

    table_df = snap_df.copy()
    if not prev_df.empty:
        prev_map = prev_df.set_index("dataset")["n_id_distinct"].to_dict()
        table_df["prev_patients"] = table_df["dataset"].map(prev_map)
        table_df["Δ Patients"] = table_df.apply(
            lambda r: f"{((r['n_id_distinct'] - r['prev_patients']) / r['prev_patients'] * 100):+.1f}%"
            if pd.notna(r.get("prev_patients")) and r["prev_patients"] > 0 else "New",
            axis=1,
        )
    else:
        table_df["Δ Patients"] = "—"

    display_cols = {
        "dataset": "Dataset",
        "n": "Total Records",
        "n_id_distinct": "Unique Patients",
        "Δ Patients": "Δ vs Prior Snapshot",
    }
    table_out = (
        table_df[list(display_cols.keys())]
        .rename(columns=display_cols)
        .sort_values("Unique Patients", ascending=False)
        .reset_index(drop=True)
    )
    table_out.index += 1
    table_out["Total Records"] = table_out["Total Records"].apply(lambda v: f"{int(v):,}" if pd.notna(v) else "—")
    table_out["Unique Patients"] = table_out["Unique Patients"].apply(lambda v: f"{int(v):,}" if pd.notna(v) else "—")

    st.dataframe(table_out, use_container_width=True, height=min(600, (len(table_out) + 1) * 36))


# ─────────────────────────────────────────────────────────────────────────────
# DASHBOARD 2 — QUALITY HEALTH
# ─────────────────────────────────────────────────────────────────────────────
def render_quality(completeness_df, selected_snap, prev_snap, selected_datasets, threshold):
    # ── filter to snapshot
    snap_df = completeness_df[
        (completeness_df["archived_on"] == selected_snap) &
        (completeness_df["dataset"].isin(selected_datasets))
    ].copy()
    prev_df = completeness_df[
        (completeness_df["archived_on"] == prev_snap) &
        (completeness_df["dataset"].isin(selected_datasets))
    ].copy() if prev_snap is not None else pd.DataFrame()

    all_snaps = completeness_df[completeness_df["dataset"].isin(selected_datasets)].copy()

    # ── aggregate per dataset
    ds_summary = snap_df.groupby("dataset")["completeness"].agg(["mean", "min", "count"]).reset_index()
    ds_summary.columns = ["dataset", "avg_completeness", "min_completeness", "n_columns"]
    ds_summary["status"] = ds_summary["avg_completeness"].apply(
        lambda v: "Excellent" if v >= 90 else ("Good" if v >= threshold else "At Risk")
    )

    overall_avg  = ds_summary["avg_completeness"].mean()
    at_risk_count = (ds_summary["avg_completeness"] < threshold).sum()
    total_cols   = snap_df.shape[0]

    prev_avg = None
    if not prev_df.empty:
        prev_avg = prev_df.groupby("dataset")["completeness"].mean().mean()

    avg_delta = overall_avg - prev_avg if prev_avg is not None else None

    # ── Header
    st.markdown(f"""
    <div style="border-bottom:1px solid #2c2c2e;padding-bottom:1rem;margin-bottom:1.2rem;">
        <h1 style="font-family:'DM Serif Display',serif;font-size:2rem;margin:0;color:#f5f5f7;">
            Data Quality Health
        </h1>
        <p style="color:#8e8e93;font-size:0.85rem;margin:0.3rem 0 0;">
            Snapshot: <strong style="color:#f5f5f5;">{selected_snap.strftime('%d %B %Y')}</strong>
            {'&nbsp;&nbsp;·&nbsp;&nbsp;Threshold: ' + str(threshold) + '%'}
        </p>
    </div>
    """, unsafe_allow_html=True)

    # ── KPIs
    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("Portfolio Avg Completeness",
                  f"{overall_avg:.1f}%",
                  f"{avg_delta:+.1f}pp vs prior" if avg_delta is not None else None)
    with c2:
        st.metric("Datasets at Risk",
                  f"{at_risk_count} / {len(ds_summary)}",
                  f"Below {threshold}% threshold")
    with c3:
        st.metric("Variables Assessed", f"{total_cols:,}")

    st.markdown("<br>", unsafe_allow_html=True)

    # ─────────────────────────────────────────────────────────────────
    # CHAPTER 1: Dataset completeness overview
    # ─────────────────────────────────────────────────────────────────
    chapter(1, "Dataset Completeness at a Glance",
            "Average column completeness per dataset, ranked. Datasets below the configured threshold "
            "are flagged for attention.")

    ds_plot = ds_summary.sort_values("avg_completeness", ascending=True).copy()
    ds_plot["label"] = ds_plot["dataset"].str.replace("_", " ").str.upper()
    ds_plot["color"] = ds_plot["avg_completeness"].apply(
        lambda v: BHF_GREEN if v >= 90 else (BHF_AMBER if v >= threshold else BHF_RED)
    )

    fig_comp = go.Figure()
    fig_comp.add_vline(x=threshold, line=dict(dash="dot", color="#636366", width=1.5),
                       annotation_text=f"{threshold}% threshold",
                       annotation_font=dict(color="#636366", size=11))
    fig_comp.add_trace(go.Bar(
        y=ds_plot["label"],
        x=ds_plot["avg_completeness"],
        orientation="h",
        marker_color=ds_plot["color"],
        text=[f"{v:.1f}%" for v in ds_plot["avg_completeness"]],
        textposition="outside",
        textfont=dict(size=10, color="#aeaeb2"),
        hovertemplate="<b>%{y}</b><br>Avg completeness: %{x:.2f}%<extra></extra>",
    ))
    fig_comp.update_xaxes(range=[0, 110])
    fig_comp = fig_defaults(fig_comp)
    fig_comp.update_layout(height=max(350, len(ds_plot) * 28), margin=dict(r=80))
    st.plotly_chart(fig_comp, use_container_width=True)

    # Alert box for at-risk datasets
    at_risk_ds = ds_summary[ds_summary["avg_completeness"] < threshold]["dataset"].tolist()
    if at_risk_ds:
        st.markdown(
            f'<div class="alert-card">⚠️ <strong>{len(at_risk_ds)} dataset(s) below {threshold}% threshold:</strong> '
            f'{", ".join(at_risk_ds)}</div>',
            unsafe_allow_html=True,
        )

    st.markdown("<br>", unsafe_allow_html=True)

    # ─────────────────────────────────────────────────────────────────
    # CHAPTER 2: Quality trend over snapshots
    # ─────────────────────────────────────────────────────────────────
    chapter(2, "Quality Trend Over Time",
            "Has data quality been improving or regressing? This view tracks average completeness "
            "per dataset across every archived snapshot.")

    trend_df = (
        all_snaps
        .groupby(["archived_on", "dataset"])["completeness"]
        .mean()
        .reset_index()
        .sort_values("archived_on")
    )

    # Limit to top 12 datasets by current avg completeness for readability
    top_ds = ds_summary.nlargest(12, "avg_completeness")["dataset"].tolist()
    trend_top = trend_df[trend_df["dataset"].isin(top_ds)]

    fig_trend = px.line(
        trend_top,
        x="archived_on",
        y="completeness",
        color="dataset",
        markers=True,
        labels={"completeness": "Avg Completeness (%)", "archived_on": "Snapshot"},
    )
    fig_trend.add_hline(y=threshold, line=dict(dash="dot", color="#636366", width=1.5))
    fig_trend.update_traces(line=dict(width=2), marker=dict(size=6))
    fig_trend = fig_defaults(fig_trend)
    fig_trend.update_layout(
        yaxis=dict(range=[0, 105], title="Avg Completeness (%)"),
        xaxis_title=None,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0, font=dict(size=10)),
    )
    st.plotly_chart(fig_trend, use_container_width=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ─────────────────────────────────────────────────────────────────
    # CHAPTER 3: Variable completeness heatmap
    # ─────────────────────────────────────────────────────────────────
    chapter(3, "Variable Completeness Heatmap",
            "Which specific columns are dragging dataset quality down? Each cell shows the "
            "completeness % for a variable. Red cells are critical gaps.")

    # Pick datasets that have at least some data
    heatmap_datasets = ds_summary.nlargest(15, "n_columns")["dataset"].tolist()
    heat_df = snap_df[snap_df["dataset"].isin(heatmap_datasets)].copy()

    # Pivot: rows = dataset, cols = column_name
    pivot = heat_df.pivot_table(index="dataset", columns="column_name", values="completeness", aggfunc="mean")

    # Limit columns for display
    MAX_COLS = 60
    if pivot.shape[1] > MAX_COLS:
        # show the most variable columns first
        variance = pivot.var(axis=0).sort_values(ascending=False)
        pivot = pivot[variance.head(MAX_COLS).index]

    fig_heat = go.Figure(data=go.Heatmap(
        z=pivot.values,
        x=pivot.columns.tolist(),
        y=[d.replace("_", " ").upper() for d in pivot.index.tolist()],
        colorscale=[
            [0.0, "#3b0010"],
            [0.3, BHF_RED],
            [0.6, "#7d6000"],
            [0.75, BHF_AMBER],
            [1.0, BHF_GREEN],
        ],
        zmin=0, zmax=100,
        colorbar=dict(
            title="%",
            tickvals=[0, 25, 50, 75, 100],
            ticktext=["0", "25", "50", "75", "100"],
            thickness=12,
            len=0.8,
        ),
        hovertemplate="<b>%{y}</b><br>%{x}<br>Completeness: %{z:.1f}%<extra></extra>",
    ))
    fig_heat = fig_defaults(fig_heat)
    fig_heat.update_layout(
        height=max(300, len(pivot) * 30 + 80),
        xaxis=dict(tickfont=dict(size=9), tickangle=-60),
        yaxis=dict(tickfont=dict(size=10)),
    )
    st.plotly_chart(fig_heat, use_container_width=True)

    if pivot.shape[1] == MAX_COLS:
        st.caption(f"Showing the {MAX_COLS} most variable columns. All columns available in the table below.")

    st.markdown("<br>", unsafe_allow_html=True)

    # ─────────────────────────────────────────────────────────────────
    # CHAPTER 4: Worst-performing variables
    # ─────────────────────────────────────────────────────────────────
    chapter(4, "Critical Gaps — Variables Needing Attention",
            f"Variables with completeness below {threshold}%, ranked worst first. "
            "These represent the highest-priority curation targets.")

    gaps_df = snap_df[snap_df["completeness"] < threshold].copy()
    gaps_df = gaps_df.sort_values("completeness").head(50)
    gaps_df["label"] = gaps_df["dataset"] + " · " + gaps_df["column_name"]
    gaps_df["status_html"] = gaps_df["completeness"].apply(quality_pill)

    # Show as a styled table
    display = gaps_df[["dataset", "column_name", "completeness"]].copy()
    display.columns = ["Dataset", "Column", "Completeness (%)"]
    display["Completeness (%)"] = display["Completeness (%)"].apply(lambda v: f"{v:.2f}%")
    display = display.reset_index(drop=True)
    display.index += 1

    st.dataframe(display, use_container_width=True, height=min(500, (len(display) + 1) * 36))

    # Snapshot-over-snapshot quality improvement / regression
    if not prev_df.empty:
        st.markdown("<br>", unsafe_allow_html=True)
        chapter(5, "Movers — Quality Changes Since Prior Snapshot",
                "Which datasets improved or regressed most since the last data snapshot?")

        prev_avg_ds = prev_df.groupby("dataset")["completeness"].mean().rename("prev_avg")
        curr_avg_ds = snap_df.groupby("dataset")["completeness"].mean().rename("curr_avg")
        movers = pd.concat([prev_avg_ds, curr_avg_ds], axis=1).dropna()
        movers["change_pp"] = movers["curr_avg"] - movers["prev_avg"]
        movers = movers.sort_values("change_pp")

        colors = [BHF_GREEN if v >= 0 else BHF_RED for v in movers["change_pp"]]
        labels = [d.replace("_", " ").upper() for d in movers.index]

        fig_movers = go.Figure(go.Bar(
            x=labels,
            y=movers["change_pp"],
            marker_color=colors,
            text=[f"{v:+.1f}pp" for v in movers["change_pp"]],
            textposition="outside",
            textfont=dict(size=10),
            hovertemplate="<b>%{x}</b><br>Change: %{y:+.2f}pp<extra></extra>",
        ))
        fig_movers.add_hline(y=0, line=dict(color="#3a3a3c", width=1))
        fig_movers = fig_defaults(fig_movers)
        fig_movers.update_layout(xaxis_tickangle=-35, yaxis_title="Change (pp)")
        st.plotly_chart(fig_movers, use_container_width=True)


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main():
    with st.spinner("Loading data from Supabase..."):
        try:
            overall_df      = load_overall()
            completeness_df = load_completeness()
        except Exception as e:
            st.error(f"❌ Could not connect to Supabase. Check your secrets.\n\n`{e}`")
            st.code("""
# .streamlit/secrets.toml
SUPABASE_URL = "https://your-project.supabase.co"
SUPABASE_KEY = "your-anon-key"
""", language="toml")
            st.stop()

    dashboard, selected_snap, prev_snap, selected_datasets, comp_threshold = render_sidebar(
        overall_df, completeness_df
    )

    if dashboard == "📈 Coverage Journey":
        render_coverage(overall_df, selected_snap, prev_snap, selected_datasets)
    else:
        render_quality(completeness_df, selected_snap, prev_snap, selected_datasets, comp_threshold)


if __name__ == "__main__":
    main()

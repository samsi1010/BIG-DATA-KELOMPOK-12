import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine
from pymongo import MongoClient
from fpdf import FPDF
import io
import datetime

# ─────────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────────
st.set_page_config(
    page_title="ReiBig Data Dashboard",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ─────────────────────────────────────────────
# CUSTOM CSS
# ─────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;600;700&family=Space+Mono:wght@400;700&display=swap');

html, body, [class*="css"] { font-family: 'DM Sans', sans-serif; }
.stApp { background: #0a0d14; color: #e2e8f0; }

[data-testid="stSidebar"] { background: #0f1420 !important; border-right: 1px solid #1e2a3a; }
[data-testid="stSidebar"] * { color: #cbd5e1 !important; }
[data-testid="stSidebar"] .stSelectbox label,
[data-testid="stSidebar"] .stSlider label {
    color: #94a3b8 !important; font-size: 0.78rem !important;
    font-weight: 600 !important; text-transform: uppercase; letter-spacing: 0.05em;
}

[data-testid="metric-container"] {
    background: #111827; border: 1px solid #1e2a3a;
    border-radius: 12px; padding: 18px 20px; transition: border-color 0.2s;
}
[data-testid="metric-container"]:hover { border-color: #3b82f6; }
[data-testid="stMetricLabel"] {
    color: #64748b !important; font-size: 0.72rem !important;
    font-weight: 700 !important; text-transform: uppercase; letter-spacing: 0.08em;
}
[data-testid="stMetricValue"] {
    color: #f1f5f9 !important; font-family: 'Space Mono', monospace !important; font-size: 1.6rem !important;
}
[data-testid="stMetricDelta"] { font-size: 0.78rem !important; }

h2, h3 { color: #f1f5f9 !important; font-weight: 600 !important; letter-spacing: -0.02em; }

[data-testid="stTabs"] button {
    font-family: 'DM Sans', sans-serif !important; font-weight: 600 !important;
    font-size: 0.82rem !important; color: #64748b !important;
    text-transform: uppercase; letter-spacing: 0.06em;
}
[data-testid="stTabs"] button[aria-selected="true"] {
    color: #3b82f6 !important; border-bottom-color: #3b82f6 !important;
}

[data-testid="stContainer"] {
    background: #111827 !important; border: 1px solid #1e2a3a !important; border-radius: 12px !important;
}

.stButton > button {
    background: #1e2a3a !important; color: #94a3b8 !important;
    border: 1px solid #2d3f55 !important; border-radius: 8px !important;
    font-family: 'DM Sans', sans-serif !important; font-weight: 600 !important;
    font-size: 0.78rem !important; transition: all 0.2s !important;
}
.stButton > button:hover { background: #3b82f6 !important; color: #fff !important; border-color: #3b82f6 !important; }

.stAlert { border-radius: 10px !important; border-left-width: 4px !important; }
hr { border-color: #1e2a3a !important; }
[data-testid="stDataFrame"] { border: 1px solid #1e2a3a; border-radius: 10px; }

.section-pill {
    display: inline-block; background: #1e3a5f; color: #60a5fa;
    font-size: 0.68rem; font-weight: 700; letter-spacing: 0.1em;
    text-transform: uppercase; padding: 3px 10px; border-radius: 20px; margin-bottom: 6px;
}

.stProgress > div > div {
    background: linear-gradient(90deg, #3b82f6, #06b6d4) !important; border-radius: 99px !important;
}
.stProgress > div { background: #1e2a3a !important; border-radius: 99px !important; height: 6px !important; }

/* hide labels that must exist but shouldn't be visible */
.hide-label label { display: none !important; }

/* Pipeline cards */
.pipeline-box { border-radius: 12px; padding: 18px 20px; border: 1px solid; }
.pipeline-postgres { background: #0f2233; border-color: #1e4a6e; }
.pipeline-mongo    { background: #1a1f0e; border-color: #3a5c1a; }
.pipeline-spark    { background: #1a1218; border-color: #4a2040; }
.pipeline-label    { font-size: 0.65rem; font-weight: 700; letter-spacing: 0.12em; text-transform: uppercase; margin-bottom: 4px; }
.pipeline-value    { font-family: 'Space Mono', monospace; font-size: 1.5rem; font-weight: 700; margin: 2px 0 6px 0; }
.pipeline-desc     { font-size: 0.75rem; color: #64748b; line-height: 1.5; }
.pipeline-badge    { display: inline-block; font-size: 0.62rem; font-weight: 700; letter-spacing: 0.08em; text-transform: uppercase; padding: 2px 8px; border-radius: 20px; margin-top: 8px; }
.badge-postgres    { background: #1e4a6e; color: #60a5fa; }
.badge-mongo       { background: #2a4a10; color: #86efac; }
.badge-spark       { background: #3a1a40; color: #c084fc; }
.flow-arrow        { display: flex; align-items: center; justify-content: center; font-size: 1.4rem; color: #334155; padding: 0 4px; }
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────
# DATA LOADING
# ─────────────────────────────────────────────
@st.cache_data(ttl=60)
def load_postgres():
    try:
        engine = create_engine("postgresql+psycopg2://postgres:samuel@localhost:5432/retail_db")
        df = pd.read_sql("SELECT * FROM retail_transactions", engine)
        return df
    except Exception as e:
        st.error(f"Gagal koneksi PostgreSQL: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=60)
def load_mongodb():
    try:
        client   = MongoClient("mongodb://localhost:27017/")
        mongo_db = client["retail_bigdata"]
        coll     = mongo_db["retail_anomalies"]
        df       = pd.DataFrame(list(coll.find()))
        if "_id" in df.columns:
            df = df.drop("_id", axis=1)
        return df
    except Exception as e:
        st.error(f"Gagal koneksi MongoDB: {e}")
        return pd.DataFrame()

with st.spinner("Memuat data…"):
    df_sql   = load_postgres()
    df_mongo = load_mongodb()

if df_sql.empty:
    st.error("❌ Data PostgreSQL kosong. Periksa koneksi database.")
    st.stop()

# ─────────────────────────────────────────────
# VALIDASI KOLOM ANOMALY  ← FIX ERROR UTAMA
# ─────────────────────────────────────────────
if "anomaly" not in df_sql.columns:
    st.error("❌ Kolom 'anomaly' tidak ditemukan di PostgreSQL. Jalankan anomaly_detection.py terlebih dahulu.")
    st.stop()

# ─────────────────────────────────────────────
# PRE-PROCESSING
# ─────────────────────────────────────────────
if "InvoiceDate" in df_sql.columns:
    df_sql["InvoiceDate"] = pd.to_datetime(df_sql["InvoiceDate"], errors="coerce")
    df_sql["Hour"]        = df_sql["InvoiceDate"].dt.hour
    df_sql["Day"]         = df_sql["InvoiceDate"].dt.day
    df_sql["MonthNum"]    = df_sql["InvoiceDate"].dt.month
    df_sql["WeekdayNum"]  = df_sql["InvoiceDate"].dt.weekday
    df_sql["DayOfWeek"]   = df_sql["InvoiceDate"].dt.day_name()
    df_sql["Month"]       = df_sql["InvoiceDate"].dt.to_period("M").astype(str)

# ← FIX: cast SEMUA kolom numerik termasuk anomaly sebelum dropna
for col_name in ["Quantity", "UnitPrice", "TotalPrice", "anomaly"]:
    if col_name in df_sql.columns:
        df_sql[col_name] = pd.to_numeric(df_sql[col_name], errors="coerce")

df_sql = df_sql.dropna(subset=["Quantity", "UnitPrice", "TotalPrice", "anomaly"])

if "CustomerID" in df_sql.columns:
    df_sql["CustomerID"] = pd.to_numeric(df_sql["CustomerID"], errors="coerce")
    df_sql = df_sql.dropna(subset=["CustomerID"])

# Feature Engineering
if "Country" in df_sql.columns:
    df_sql["CountryEncoded"] = pd.Categorical(df_sql["Country"]).codes
if "StockCode" in df_sql.columns:
    pf = df_sql["StockCode"].value_counts().to_dict()
    df_sql["ProductFrequency"] = df_sql["StockCode"].map(pf).fillna(1).astype(int)
if "CustomerID" in df_sql.columns:
    cas = df_sql.groupby("CustomerID")["TotalPrice"].mean().to_dict()
    df_sql["AvgCustomerSpend"]  = df_sql["CustomerID"].map(cas).fillna(df_sql["TotalPrice"].mean())
    cf  = df_sql["CustomerID"].value_counts().to_dict()
    df_sql["CustomerFrequency"] = df_sql["CustomerID"].map(cf).fillna(1).astype(int)
    ctq = df_sql.groupby("CustomerID")["Quantity"].sum().to_dict()
    df_sql["CustomerTotalQty"]  = df_sql["CustomerID"].map(ctq).fillna(0)

# ─────────────────────────────────────────────
# SIDEBAR  ← FIX: label tidak boleh kosong
# ─────────────────────────────────────────────
st.sidebar.markdown("""
<div style="padding: 16px 0 8px 0;">
    <div style="font-family:'DM Sans',sans-serif; font-size:1.1rem; font-weight:700;
                color:#f1f5f9; letter-spacing:-0.01em;">🛒 ReiBig Dashboard</div>
    <div style="font-size:0.7rem; color:#475569; margin-top:2px;">Retail Analytics & Anomaly Detection</div>
</div>
""", unsafe_allow_html=True)
st.sidebar.markdown("---")

st.sidebar.markdown('<div class="section-pill">Tipe Transaksi</div>', unsafe_allow_html=True)
filter_type = st.sidebar.selectbox(
    "Tipe Transaksi",
    ["Semua", "Normal", "Anomali"],
    label_visibility="collapsed"
)

st.sidebar.markdown('<div class="section-pill" style="margin-top:12px;">Negara</div>', unsafe_allow_html=True)
if "Country" in df_sql.columns:
    countries        = ["Semua"] + sorted(df_sql["Country"].dropna().unique().tolist())
    selected_country = st.sidebar.selectbox(
        "Negara",
        countries,
        label_visibility="collapsed",
        key="country_sel"
    )
else:
    selected_country = "Semua"

st.sidebar.markdown('<div class="section-pill" style="margin-top:12px;">Rentang Harga (TotalPrice)</div>', unsafe_allow_html=True)
st.sidebar.caption("Filter transaksi berdasarkan nilai total belanja.")
price_min_abs = float(df_sql["TotalPrice"].min())
price_max_abs = float(df_sql["TotalPrice"].max())
price_range   = st.sidebar.slider(
    "Rentang Harga",
    min_value=price_min_abs,
    max_value=price_max_abs,
    value=(price_min_abs, price_max_abs),
    label_visibility="collapsed",
    key="price_range"
)

st.sidebar.markdown('<div class="section-pill" style="margin-top:12px;">Alert Threshold</div>', unsafe_allow_html=True)
st.sidebar.caption("Peringatan jika % anomaly melampaui batas ini.")
anomaly_threshold = st.sidebar.slider(
    "Alert Threshold",
    1, 50, 10,
    label_visibility="collapsed",
    key="anom_thresh"
)

st.sidebar.markdown("---")
if st.sidebar.button("🔄 Refresh Data", use_container_width=True):
    load_postgres.clear()
    load_mongodb.clear()
    st.rerun()

# ─────────────────────────────────────────────
# APPLY FILTERS
# ─────────────────────────────────────────────
filtered_df = df_sql.copy()
filtered_df = filtered_df[
    (filtered_df["TotalPrice"] >= price_range[0]) &
    (filtered_df["TotalPrice"] <= price_range[1])
]
if filter_type == "Normal":
    filtered_df = filtered_df[filtered_df["anomaly"] == 0]
elif filter_type == "Anomali":
    filtered_df = filtered_df[filtered_df["anomaly"] == 1]
if selected_country != "Semua" and "Country" in filtered_df.columns:
    filtered_df = filtered_df[filtered_df["Country"] == selected_country]

# Sidebar ringkasan
st.sidebar.markdown("---")
st.sidebar.markdown('<div class="section-pill">Ringkasan Filter</div>', unsafe_allow_html=True)
total_f   = len(filtered_df)
normal_f  = len(filtered_df[filtered_df["anomaly"] == 0])
anomaly_f = len(filtered_df[filtered_df["anomaly"] == 1])
anpct_f   = round(anomaly_f / total_f * 100, 1) if total_f > 0 else 0

st.sidebar.markdown(f"""
<div style="background:#111827; border:1px solid #1e2a3a; border-radius:10px; padding:12px 14px; margin-top:6px;">
    <div style="display:flex; justify-content:space-between; margin-bottom:6px;">
        <span style="color:#64748b; font-size:0.75rem;">Total</span>
        <span style="color:#f1f5f9; font-family:'Space Mono',monospace; font-size:0.85rem;">{total_f:,}</span>
    </div>
    <div style="display:flex; justify-content:space-between; margin-bottom:6px;">
        <span style="color:#64748b; font-size:0.75rem;">Normal</span>
        <span style="color:#22c55e; font-family:'Space Mono',monospace; font-size:0.85rem;">{normal_f:,}</span>
    </div>
    <div style="display:flex; justify-content:space-between;">
        <span style="color:#64748b; font-size:0.75rem;">Anomaly</span>
        <span style="color:#ef4444; font-family:'Space Mono',monospace; font-size:0.85rem;">{anomaly_f:,} ({anpct_f}%)</span>
    </div>
</div>
""", unsafe_allow_html=True)

if filtered_df.empty:
    st.warning("⚠️ Tidak ada data sesuai filter yang dipilih.")
    st.stop()

# ─────────────────────────────────────────────
# HEADER
# ─────────────────────────────────────────────
st.markdown("""
<div style="padding: 10px 0 4px 0;">
    <div style="font-size:0.72rem; color:#3b82f6; font-weight:700;
                letter-spacing:0.12em; text-transform:uppercase; margin-bottom:6px;">
        ● Live Dashboard
    </div>
    <h1 style="font-size:2rem; font-weight:700; color:#f1f5f9;
               letter-spacing:-0.03em; margin:0; line-height:1.1;">
        Retail Analytics & Anomaly Detection
    </h1>
    <p style="color:#475569; font-size:0.82rem; margin-top:6px;">
        Apache Spark · PostgreSQL · MongoDB · Isolation Forest · Streamlit
    </p>
</div>
""", unsafe_allow_html=True)
st.markdown("---")

# ─────────────────────────────────────────────
# COMPUTED METRICS
# ─────────────────────────────────────────────
total_transactions = len(filtered_df)
total_revenue      = round(filtered_df["TotalPrice"].sum(), 2)
total_anomaly      = len(filtered_df[filtered_df["anomaly"] == 1])
total_normal       = len(filtered_df[filtered_df["anomaly"] == 0])
avg_order_value    = round(filtered_df["TotalPrice"].mean(), 2)
anomaly_pct        = round((total_anomaly / total_transactions) * 100, 2) if total_transactions > 0 else 0
fraud_loss         = total_anomaly * avg_order_value

mid        = len(filtered_df) // 2
rev_first  = filtered_df.iloc[:mid]["TotalPrice"].sum() if mid > 0 else 0
rev_second = filtered_df.iloc[mid:]["TotalPrice"].sum() if mid > 0 else 0
rev_delta  = round(((rev_second - rev_first) / rev_first * 100), 2) if rev_first > 0 else 0

# ─────────────────────────────────────────────
# METRICS ROW
# ─────────────────────────────────────────────
st.markdown('<div class="section-pill">Key Metrics</div>', unsafe_allow_html=True)
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Total Transaksi",   f"{total_transactions:,}")
c2.metric("Total Revenue",     f"${total_revenue:,.0f}", delta=f"{rev_delta:+.1f}% vs periode sebelumnya")
c3.metric("Transaksi Normal",  f"{total_normal:,}")
c4.metric("Transaksi Anomaly", f"{total_anomaly:,}")
c5.metric("Avg Order Value",   f"${avg_order_value:,.2f}")

if anomaly_pct > anomaly_threshold:
    st.error(f"🚨 **ALERT:** Anomaly rate {anomaly_pct}% melampaui threshold {anomaly_threshold}% — segera tindak lanjuti!")
else:
    st.success(f"✅ Anomaly rate {anomaly_pct}% masih aman di bawah threshold {anomaly_threshold}%")

st.markdown("---")

# ─────────────────────────────────────────────
# DATA PIPELINE SECTION
# ─────────────────────────────────────────────
st.markdown('<div class="section-pill">Data Pipeline — Spark → PostgreSQL & MongoDB</div>', unsafe_allow_html=True)
st.markdown("<br>", unsafe_allow_html=True)

mongo_count      = len(df_mongo) if not df_mongo.empty else 0
postgres_total   = len(df_sql)
postgres_normal  = len(df_sql[df_sql["anomaly"] == 0])
postgres_anomaly = len(df_sql[df_sql["anomaly"] == 1])

pipe_col1, pipe_arrow1, pipe_col2, pipe_arrow2, pipe_col3 = st.columns([3, 0.4, 3, 0.4, 3])

with pipe_col1:
    st.markdown(f"""
    <div class="pipeline-box pipeline-spark">
        <div class="pipeline-label" style="color:#c084fc;">⚡ Apache Spark</div>
        <div class="pipeline-value" style="color:#e9d5ff;">{postgres_total:,}</div>
        <div class="pipeline-desc">
            Total transaksi diproses Spark.<br>
            Feature engineering + Isolation Forest anomaly detection.<br>
            Output: <code style="color:#c084fc;">advanced_anomaly.parquet</code>
        </div>
        <div class="pipeline-badge badge-spark">Isolation Forest</div>
    </div>
    """, unsafe_allow_html=True)

with pipe_arrow1:
    st.markdown("""
    <div class="flow-arrow" style="height:100%; min-height:140px; flex-direction:column; gap:4px; padding-top:50px;">
        <div style="font-size:1.6rem; color:#1e3a5f;">→</div>
        <div style="color:#3b82f6; font-size:0.58rem; font-weight:700; text-transform:uppercase; letter-spacing:.06em; text-align:center;">split</div>
    </div>
    """, unsafe_allow_html=True)

with pipe_col2:
    st.markdown(f"""
    <div class="pipeline-box pipeline-postgres">
        <div class="pipeline-label" style="color:#60a5fa;">🐘 PostgreSQL</div>
        <div class="pipeline-value" style="color:#bfdbfe;">{postgres_total:,}</div>
        <div class="pipeline-desc">
            <strong style="color:#93c5fd;">Semua transaksi</strong> (normal + anomaly) disimpan ke tabel
            <code style="color:#60a5fa;">retail_transactions</code>.<br>
            Sumber utama dashboard & analitik.
        </div>
        <div class="pipeline-badge badge-postgres">retail_transactions</div>
    </div>
    """, unsafe_allow_html=True)

with pipe_arrow2:
    st.markdown("""
    <div class="flow-arrow" style="height:100%; min-height:140px; flex-direction:column; gap:4px; padding-top:50px;">
        <div style="font-size:1.6rem; color:#14532d;">→</div>
        <div style="color:#22c55e; font-size:0.58rem; font-weight:700; text-transform:uppercase; letter-spacing:.06em; text-align:center;">anomaly only</div>
    </div>
    """, unsafe_allow_html=True)

with pipe_col3:
    st.markdown(f"""
    <div class="pipeline-box pipeline-mongo">
        <div class="pipeline-label" style="color:#86efac;">🍃 MongoDB</div>
        <div class="pipeline-value" style="color:#bbf7d0;">{mongo_count:,}</div>
        <div class="pipeline-desc">
            Hanya transaksi <strong style="color:#86efac;">anomaly = 1</strong> yang disimpan.<br>
            Collection: <code style="color:#86efac;">retail_anomalies</code> · DB: <code style="color:#86efac;">retail_bigdata</code>.<br>
            Batch insert 5.000 dokumen per iterasi via Spark.
        </div>
        <div class="pipeline-badge badge-mongo">retail_anomalies</div>
    </div>
    """, unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# Breakdown 4 kartu
pg1, pg2, pg3, pg4 = st.columns(4)
pg1.markdown(f"""
<div style="background:#0f1c2e; border:1px solid #1e3a5f; border-radius:10px; padding:14px 16px;">
    <div style="font-size:0.65rem; color:#64748b; font-weight:700; text-transform:uppercase; letter-spacing:.08em;">PostgreSQL · Total Baris</div>
    <div style="font-family:'Space Mono',monospace; font-size:1.3rem; color:#bfdbfe; margin:4px 0;">{postgres_total:,}</div>
    <div style="font-size:0.72rem; color:#475569;">Semua data (normal + anomaly)</div>
</div>
""", unsafe_allow_html=True)

pg2.markdown(f"""
<div style="background:#0f1c2e; border:1px solid #1e3a5f; border-radius:10px; padding:14px 16px;">
    <div style="font-size:0.65rem; color:#64748b; font-weight:700; text-transform:uppercase; letter-spacing:.08em;">PostgreSQL · Normal</div>
    <div style="font-family:'Space Mono',monospace; font-size:1.3rem; color:#22c55e; margin:4px 0;">{postgres_normal:,}</div>
    <div style="font-size:0.72rem; color:#475569;">Transaksi valid · anomaly = 0</div>
</div>
""", unsafe_allow_html=True)

pg3.markdown(f"""
<div style="background:#0f1c2e; border:1px solid #1e3a5f; border-radius:10px; padding:14px 16px;">
    <div style="font-size:0.65rem; color:#64748b; font-weight:700; text-transform:uppercase; letter-spacing:.08em;">PostgreSQL · Anomaly</div>
    <div style="font-family:'Space Mono',monospace; font-size:1.3rem; color:#ef4444; margin:4px 0;">{postgres_anomaly:,}</div>
    <div style="font-size:0.72rem; color:#475569;">Terdeteksi Isolation Forest · anomaly = 1</div>
</div>
""", unsafe_allow_html=True)

pg4.markdown(f"""
<div style="background:#0f1a10; border:1px solid #2a4a10; border-radius:10px; padding:14px 16px;">
    <div style="font-size:0.65rem; color:#64748b; font-weight:700; text-transform:uppercase; letter-spacing:.08em;">MongoDB · Dokumen</div>
    <div style="font-family:'Space Mono',monospace; font-size:1.3rem; color:#86efac; margin:4px 0;">{mongo_count:,}</div>
    <div style="font-size:0.72rem; color:#475569;">retail_anomalies · hanya anomaly saja</div>
</div>
""", unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)
if postgres_total > 0:
    normal_ratio  = postgres_normal  / postgres_total
    anomaly_ratio = postgres_anomaly / postgres_total
    st.markdown(f"""
    <div style="margin-bottom:6px; display:flex; justify-content:space-between; font-size:0.72rem; color:#64748b;">
        <span>🟢 Normal — {postgres_normal:,} ({normal_ratio*100:.1f}%)</span>
        <span>🔴 Anomaly — {postgres_anomaly:,} ({anomaly_ratio*100:.1f}%)</span>
    </div>
    <div style="background:#1e2a3a; border-radius:99px; height:10px; overflow:hidden;">
        <div style="display:flex; height:100%;">
            <div style="background:#22c55e; width:{normal_ratio*100:.1f}%; border-radius:99px 0 0 99px;"></div>
            <div style="background:#ef4444; width:{anomaly_ratio*100:.1f}%; border-radius:0 99px 99px 0;"></div>
        </div>
    </div>
    <div style="font-size:0.68rem; color:#334155; margin-top:6px;">
        Total data diproses Spark → split berdasarkan label anomaly → disimpan ke masing-masing database
    </div>
    """, unsafe_allow_html=True)

st.markdown("---")

# ─────────────────────────────────────────────
# KPI TRACKING
# ─────────────────────────────────────────────
st.markdown('<div class="section-pill">KPI Performance vs Target</div>', unsafe_allow_html=True)
st.markdown("<br>", unsafe_allow_html=True)

k1, k2, k3, k4 = st.columns(4)
with k1:
    rev_target = 550000
    rev_pct    = min((total_revenue / rev_target) * 100, 100)
    st.metric("Revenue vs Target", f"${total_revenue:,.0f}", delta=f"{rev_pct:.0f}% dari ${rev_target:,.0f}")
    st.progress(rev_pct / 100)

with k2:
    anm_target = 300
    anm_status = "❌ Melebihi batas" if total_anomaly > anm_target else "✅ Aman"
    st.metric("Anomaly vs Target", f"{total_anomaly:,}", delta=f"{anm_status} (target < {anm_target})")
    st.progress(min(total_anomaly / anm_target, 1.0))

with k3:
    aov_target = 50
    aov_pct    = min((avg_order_value / aov_target) * 100, 100)
    st.metric("AOV vs Target", f"${avg_order_value:.2f}", delta=f"{aov_pct:.0f}% dari ${aov_target}")
    st.progress(aov_pct / 100)

with k4:
    fraud_target = 2000
    fraud_status = "❌ Melebihi batas" if fraud_loss > fraud_target else "✅ Aman"
    st.metric("Estimasi Kerugian Fraud", f"${fraud_loss:,.0f}", delta=f"{fraud_status} (target < ${fraud_target:,.0f})")
    st.progress(min(fraud_loss / fraud_target, 1.0))

st.markdown("---")

# ─────────────────────────────────────────────
# BUSINESS INSIGHT CARDS
# ─────────────────────────────────────────────
st.markdown('<div class="section-pill">Business Insights</div>', unsafe_allow_html=True)
st.markdown("<br>", unsafe_allow_html=True)

bi1, bi2, bi3 = st.columns(3)

with bi1:
    with st.container(border=True):
        st.markdown("#### 🔴 Prioritas Tinggi")
        st.markdown(f"**{total_anomaly:,}** transaksi terdeteksi sebagai anomaly.")
        if total_anomaly > 0:
            st.markdown(f"Estimasi kerugian potensial: **${fraud_loss:,.0f}**")
        st.caption("→ Tinjau dan blokir transaksi berisiko tinggi segera.")

with bi2:
    with st.container(border=True):
        st.markdown("#### 🟢 Indikator Positif")
        st.markdown(f"Tren revenue: **{rev_delta:+.1f}%** dari periode sebelumnya.")
        st.markdown(f"Total revenue saat ini: **${total_revenue:,.0f}**")
        st.caption("→ Pertahankan strategi & eksplorasi peluang upselling.")

with bi3:
    with st.container(border=True):
        st.markdown("#### 🟡 Observasi")
        if "Hour" in filtered_df.columns:
            hs = filtered_df.groupby("Hour")["TotalPrice"].sum()
            if not hs.empty and hs.sum() > 0:
                peak_h = hs.idxmax()
                st.markdown(f"Jam puncak penjualan: **{peak_h}:00 – {peak_h+1}:00**")
        if "DayOfWeek" in filtered_df.columns:
            ds = filtered_df.groupby("DayOfWeek")["TotalPrice"].sum()
            if not ds.empty and ds.sum() > 0:
                peak_d = ds.idxmax()
                st.markdown(f"Hari terbaik: **{peak_d}**")
        st.caption("→ Optimalkan staf & stok untuk jam/hari puncak.")

st.markdown("---")

# ─────────────────────────────────────────────
# TABS
# ─────────────────────────────────────────────
tab1, tab3, tab4, tab5 = st.tabs([
    "📊 Overview",
    "📦 Produk & Customer",
    "📅 Time Series",
    "🗄️ Data & Export"
])

# ══════════════════════════════════════════════
# TAB 1 — OVERVIEW
# ══════════════════════════════════════════════
with tab1:

    col_a, col_b = st.columns(2)
    with col_a:
        anomaly_dist = filtered_df.groupby("anomaly").size().reset_index(name="count")
        anomaly_dist["anomaly"] = anomaly_dist["anomaly"].replace({0: "Normal", 1: "Anomaly"})
        fig_pie = px.pie(
            anomaly_dist, names="anomaly", values="count", hole=0.55,
            color="anomaly",
            color_discrete_map={"Normal": "#22c55e", "Anomaly": "#ef4444"},
            title="Distribusi Normal vs Anomaly"
        )
        fig_pie.update_layout(
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#94a3b8"), title_font_color="#f1f5f9",
            legend=dict(font=dict(color="#94a3b8"))
        )
        st.plotly_chart(fig_pie, use_container_width=True)

    with col_b:
        fig_hist = px.histogram(
            filtered_df, x="TotalPrice", nbins=60,
            color_discrete_sequence=["#3b82f6"],
            title="Distribusi Total Price"
        )
        fig_hist.update_layout(
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="#111827",
            font=dict(color="#94a3b8"), title_font_color="#f1f5f9",
            xaxis=dict(gridcolor="#1e2a3a"), yaxis=dict(gridcolor="#1e2a3a"),
            bargap=0.05
        )
        st.plotly_chart(fig_hist, use_container_width=True)

    st.markdown('<div class="section-pill" style="margin-top:12px;">Scatter: Quantity vs TotalPrice</div>', unsafe_allow_html=True)
    sdf = filtered_df.sample(min(2000, len(filtered_df)), random_state=42).copy()
    sdf["Label"] = sdf["anomaly"].replace({0: "Normal", 1: "Anomaly"})
    fig_sc = px.scatter(
        sdf, x="Quantity", y="TotalPrice", color="Label",
        color_discrete_map={"Normal": "#22c55e", "Anomaly": "#ef4444"},
        opacity=0.65, title="Quantity vs TotalPrice (sample 2.000 baris)",
        hover_data=["UnitPrice"] if "UnitPrice" in sdf.columns else None
    )
    fig_sc.update_layout(
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="#111827",
        font=dict(color="#94a3b8"), title_font_color="#f1f5f9",
        xaxis=dict(gridcolor="#1e2a3a"), yaxis=dict(gridcolor="#1e2a3a"),
        legend=dict(font=dict(color="#94a3b8"))
    )
    st.plotly_chart(fig_sc, use_container_width=True)

# ══════════════════════════════════════════════
# TAB 3 — PRODUCT & CUSTOMER
# ══════════════════════════════════════════════
with tab3:

    col_a, col_b = st.columns(2)
    with col_a:
        if "Description" in filtered_df.columns:
            top_prod = (
                filtered_df.groupby("Description")["Quantity"].sum()
                .reset_index().sort_values("Quantity", ascending=False).head(10)
            )
            fig_p = px.bar(
                top_prod, x="Quantity", y="Description", orientation="h",
                color="Quantity", color_continuous_scale="Blues",
                title="Top 10 Produk Terlaris (by Quantity)"
            )
            fig_p.update_layout(
                yaxis={"categoryorder":"total ascending"},
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="#111827",
                font=dict(color="#94a3b8"), title_font_color="#f1f5f9",
                xaxis=dict(gridcolor="#1e2a3a"), yaxis_title="",
                coloraxis_showscale=False, height=420
            )
            st.plotly_chart(fig_p, use_container_width=True)

    with col_b:
        if "CustomerID" in filtered_df.columns:
            top_cust = (
                filtered_df.groupby("CustomerID")["TotalPrice"].sum()
                .reset_index().sort_values("TotalPrice", ascending=False).head(10)
            )
            top_cust["CustomerID"] = top_cust["CustomerID"].astype(int).astype(str)
            fig_c = px.bar(
                top_cust, x="TotalPrice", y="CustomerID", orientation="h",
                color="TotalPrice", color_continuous_scale="Greens",
                title="Top 10 Customer by Revenue"
            )
            fig_c.update_layout(
                yaxis={"categoryorder":"total ascending"},
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="#111827",
                font=dict(color="#94a3b8"), title_font_color="#f1f5f9",
                xaxis=dict(gridcolor="#1e2a3a", title="Total Revenue"),
                yaxis_title="Customer ID",
                coloraxis_showscale=False, height=420
            )
            st.plotly_chart(fig_c, use_container_width=True)

    if "Description" in filtered_df.columns:
        col_e, col_f = st.columns(2)
        with col_e:
            top_rev_prod = (
                filtered_df.groupby("Description")["TotalPrice"].sum()
                .reset_index().sort_values("TotalPrice", ascending=False).head(10)
            )
            fig_rp = px.bar(
                top_rev_prod, x="TotalPrice", y="Description", orientation="h",
                color="TotalPrice", color_continuous_scale="Purples",
                title="Top 10 Produk by Revenue"
            )
            fig_rp.update_layout(
                yaxis={"categoryorder":"total ascending"},
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="#111827",
                font=dict(color="#94a3b8"), title_font_color="#f1f5f9",
                xaxis=dict(gridcolor="#1e2a3a", title="Total Revenue"),
                yaxis_title="", coloraxis_showscale=False, height=400
            )
            st.plotly_chart(fig_rp, use_container_width=True)

        with col_f:
            st.markdown('<div class="section-pill">Transaksi Tertinggi</div>', unsafe_allow_html=True)
            top10_cols = [c for c in ["InvoiceNo","CustomerID","Description","Quantity","UnitPrice","TotalPrice","Country"]
                          if c in filtered_df.columns]
            st.dataframe(
                filtered_df[top10_cols].sort_values("TotalPrice", ascending=False).head(10),
                use_container_width=True, hide_index=True, height=380
            )

# ══════════════════════════════════════════════
# TAB 4 — TIME SERIES
# ══════════════════════════════════════════════
with tab4:

    if "InvoiceDate" in filtered_df.columns:

        daily = (
            filtered_df.groupby(filtered_df["InvoiceDate"].dt.date)["TotalPrice"]
            .sum().reset_index()
        )
        daily.columns = ["Date", "Revenue"]
        fig_d = px.line(
            daily, x="Date", y="Revenue", markers=False,
            title="Tren Revenue Harian", color_discrete_sequence=["#3b82f6"]
        )
        fig_d.update_traces(line_width=2)
        fig_d.update_layout(
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="#111827",
            font=dict(color="#94a3b8"), title_font_color="#f1f5f9",
            xaxis=dict(gridcolor="#1e2a3a"), yaxis=dict(gridcolor="#1e2a3a"), height=320
        )
        st.plotly_chart(fig_d, use_container_width=True)

        col_a, col_b = st.columns(2)
        with col_a:
            hourly = filtered_df.groupby("Hour")["TotalPrice"].sum().reset_index()
            fig_h = px.bar(
                hourly, x="Hour", y="TotalPrice",
                color="TotalPrice", color_continuous_scale="Blues", title="Revenue per Jam"
            )
            fig_h.update_layout(
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="#111827",
                font=dict(color="#94a3b8"), title_font_color="#f1f5f9",
                xaxis=dict(gridcolor="#1e2a3a", dtick=2), yaxis=dict(gridcolor="#1e2a3a"),
                coloraxis_showscale=False
            )
            st.plotly_chart(fig_h, use_container_width=True)

        with col_b:
            day_order = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
            dow = (
                filtered_df.groupby("DayOfWeek")["TotalPrice"].sum()
                .reindex(day_order).reset_index()
            )
            dow.columns = ["Day", "Revenue"]
            fig_dow = px.bar(
                dow, x="Day", y="Revenue",
                color="Revenue", color_continuous_scale="Purples", title="Revenue per Hari"
            )
            fig_dow.update_layout(
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="#111827",
                font=dict(color="#94a3b8"), title_font_color="#f1f5f9",
                xaxis=dict(gridcolor="#1e2a3a"), yaxis=dict(gridcolor="#1e2a3a"),
                coloraxis_showscale=False
            )
            st.plotly_chart(fig_dow, use_container_width=True)

        monthly = (
            filtered_df.groupby("Month")["TotalPrice"].sum()
            .reset_index().sort_values("Month")
        )
        fig_mo = px.area(
            monthly, x="Month", y="TotalPrice",
            title="Tren Revenue Bulanan", color_discrete_sequence=["#8b5cf6"]
        )
        fig_mo.update_layout(
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="#111827",
            font=dict(color="#94a3b8"), title_font_color="#f1f5f9",
            xaxis=dict(gridcolor="#1e2a3a"), yaxis=dict(gridcolor="#1e2a3a"), height=320
        )
        st.plotly_chart(fig_mo, use_container_width=True)

    else:
        st.info("Kolom InvoiceDate tidak tersedia.")

# ══════════════════════════════════════════════
# TAB 5 — DATA & EXPORT
# ══════════════════════════════════════════════
with tab5:

    sub1, sub2, sub3 = st.tabs(["🐘 PostgreSQL", "🍃 MongoDB", "⬇️ Export"])

    with sub1:
        st.markdown('<div class="section-pill">PostgreSQL: retail_transactions</div>', unsafe_allow_html=True)
        ci1, ci2, ci3 = st.columns(3)
        ci1.metric("Total Baris",   f"{len(df_sql):,}")
        ci2.metric("Total Kolom",   len(df_sql.columns))
        ci3.metric("Ukuran Memori", f"~{df_sql.memory_usage(deep=True).sum()/1024**2:.1f} MB")
        st.markdown("---")

        all_cols = df_sql.columns.tolist()
        sel_cols = st.multiselect("Pilih kolom:", all_cols,
                                  default=all_cols[:10] if len(all_cols) > 10 else all_cols)
        disp_opt = st.radio("Tampilan:", ["Tabel", "Statistik", "Info Tipe Data"], horizontal=True)

        if sel_cols:
            if disp_opt == "Tabel":
                PAGE = 50
                tp   = max(1, (len(df_sql) - 1) // PAGE + 1)
                pg   = st.number_input(f"Halaman (total {tp})", 1, tp, 1, step=1)
                st.dataframe(df_sql[sel_cols].iloc[(pg-1)*PAGE:pg*PAGE],
                             use_container_width=True, height=400, hide_index=True)
                st.download_button("⬇️ Download CSV", df_sql[sel_cols].to_csv(index=False),
                                   f"postgresql_{datetime.date.today()}.csv", "text/csv")
            elif disp_opt == "Statistik":
                st.dataframe(df_sql[sel_cols].describe().T.round(2), use_container_width=True)
            else:
                info = pd.DataFrame({
                    "Kolom":    sel_cols,
                    "Tipe":     [str(df_sql[c].dtype) for c in sel_cols],
                    "Non-Null": [df_sql[c].count() for c in sel_cols],
                    "Null":     [df_sql[c].isnull().sum() for c in sel_cols],
                    "Null %":   [f"{df_sql[c].isnull().mean()*100:.1f}%" for c in sel_cols],
                })
                st.dataframe(info, use_container_width=True, hide_index=True)

    with sub2:
        st.markdown('<div class="section-pill">MongoDB: retail_anomalies</div>', unsafe_allow_html=True)
        if not df_mongo.empty:
            ci1, ci2 = st.columns(2)
            ci1.metric("Total Dokumen", f"{len(df_mongo):,}")
            ci2.metric("Total Field",   len(df_mongo.columns))
            st.markdown("---")

            mg_cols = df_mongo.columns.tolist()
            sel_mg  = st.multiselect("Pilih field:", mg_cols,
                                     default=mg_cols[:10] if len(mg_cols) > 10 else mg_cols,
                                     key="sel_mg")
            mg_opt  = st.radio("Tampilan:", ["Tabel", "Statistik", "JSON Preview"],
                               horizontal=True, key="mg_opt")
            if sel_mg:
                mg_disp = df_mongo[sel_mg].copy()
                for c in mg_disp.columns:
                    if mg_disp[c].dtype == "object":
                        mg_disp[c] = mg_disp[c].astype(str)

                if mg_opt == "Tabel":
                    PAGE = 50
                    tp   = max(1, (len(mg_disp) - 1) // PAGE + 1)
                    pg   = st.number_input(f"Halaman (total {tp})", 1, tp, 1, key="mg_pg")
                    st.dataframe(mg_disp.iloc[(pg-1)*PAGE:pg*PAGE],
                                 use_container_width=True, height=400, hide_index=True)
                    st.download_button("⬇️ Download CSV", mg_disp.to_csv(index=False),
                                       f"mongodb_{datetime.date.today()}.csv", "text/csv")
                elif mg_opt == "Statistik":
                    num_f = df_mongo[sel_mg].select_dtypes(include=["number"]).columns
                    if len(num_f):
                        st.dataframe(df_mongo[num_f].describe().T.round(2), use_container_width=True)
                    else:
                        st.info("Tidak ada kolom numerik.")
                else:
                    doc_n = st.number_input("Dokumen ke (0-indexed):", 0, len(df_mongo)-1, 0, key="mg_json")
                    st.json({k: str(v) for k, v in df_mongo.iloc[doc_n][sel_mg].to_dict().items()})
        else:
            st.warning("🍃 Data MongoDB kosong. Jalankan pipeline terlebih dahulu.")

    with sub3:
        st.markdown('<div class="section-pill">Export Data</div>', unsafe_allow_html=True)
        st.markdown("---")

        col_a, col_b = st.columns(2)
        with col_a:
            st.markdown("**PostgreSQL → CSV**")
            st.download_button("⬇️ Download PostgreSQL CSV", df_sql.to_csv(index=False),
                               f"postgresql_{datetime.date.today()}.csv", "text/csv",
                               use_container_width=True)
        with col_b:
            st.markdown("**MongoDB → CSV**")
            if not df_mongo.empty:
                st.download_button("⬇️ Download MongoDB CSV", df_mongo.to_csv(index=False),
                                   f"mongodb_{datetime.date.today()}.csv", "text/csv",
                                   use_container_width=True)
            else:
                st.info("MongoDB kosong.")

        st.markdown("---")
        st.markdown("**Filtered Data → Excel**")
        buf2 = io.BytesIO()
        with pd.ExcelWriter(buf2, engine="openpyxl") as wr:
            filtered_df.to_excel(wr, index=False, sheet_name="Transactions")
            if not df_mongo.empty:
                df_mongo.to_excel(wr, index=False, sheet_name="MongoDB Anomalies")
        buf2.seek(0)
        st.download_button("⬇️ Download Filtered Excel", buf2,
                           f"retail_filtered_{datetime.date.today()}.xlsx",
                           "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                           use_container_width=True)

        st.markdown("---")
        st.markdown("**Semua Data → Excel (2 sheet)**")
        buf_all = io.BytesIO()
        with pd.ExcelWriter(buf_all, engine="openpyxl") as wr:
            df_sql.to_excel(wr, index=False, sheet_name="PostgreSQL")
            if not df_mongo.empty:
                df_mongo.to_excel(wr, index=False, sheet_name="MongoDB")
        buf_all.seek(0)
        st.download_button("⬇️ Download Full Excel", buf_all,
                           f"retail_full_{datetime.date.today()}.xlsx",
                           "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                           use_container_width=True)

        st.markdown("---")
        st.markdown("**PDF Summary Report**")
        if st.button("🖨️ Generate PDF", use_container_width=True):
            pdf = FPDF()
            pdf.add_page()
            pdf.set_font("Arial", "B", 16)
            pdf.cell(0, 10, "Retail Big Data Dashboard — Summary Report", ln=True)
            pdf.set_font("Arial", "", 11)
            pdf.cell(0, 8, f"Generated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", ln=True)
            pdf.ln(4)
            pdf.set_font("Arial", "B", 13)
            pdf.cell(0, 8, "Key Metrics", ln=True)
            pdf.set_font("Arial", "", 11)
            for label, value in [
                ("Total Transactions",   f"{total_transactions:,}"),
                ("Total Revenue",        f"${total_revenue:,.2f}"),
                ("Normal Transactions",  f"{total_normal:,}"),
                ("Anomaly Transactions", f"{total_anomaly:,}"),
                ("Anomaly Rate",         f"{anomaly_pct}%"),
                ("Avg Order Value",      f"${avg_order_value:,.2f}"),
                ("Estimated Fraud Loss", f"${fraud_loss:,.2f}"),
                ("MongoDB Documents",    f"{mongo_count:,}"),
            ]:
                pdf.cell(0, 7, f"{label}: {value}", ln=True)
            pdf_bytes = pdf.output(dest="S").encode("latin-1")
            st.download_button("⬇️ Download PDF", pdf_bytes,
                               f"retail_summary_{datetime.date.today()}.pdf",
                               "application/pdf", use_container_width=True)

# ─────────────────────────────────────────────
# DEBUG
# ─────────────────────────────────────────────
with st.expander("🔍 Debug Info", expanded=False):
    st.write(f"Kolom PostgreSQL ({len(df_sql.columns)}): {list(df_sql.columns)}")
    st.write(f"Shape: {df_sql.shape}")
    st.write(f"Filtered shape: {filtered_df.shape}")
    st.write(f"Kolom 'anomaly' ada: {'anomaly' in df_sql.columns}")
    st.write(f"MongoDB rows: {mongo_count}")

# ─────────────────────────────────────────────
# FOOTER
# ─────────────────────────────────────────────
st.markdown("---")
st.markdown("""
<div style="text-align:center; color:#334155; font-size:0.75rem; padding:8px 0 16px 0;">
    Apache Spark &nbsp;·&nbsp; PostgreSQL &nbsp;·&nbsp; MongoDB &nbsp;·&nbsp;
    Isolation Forest &nbsp;·&nbsp; Streamlit<br>
    <span style="color:#1e3a5f;">ReiBig Retail Analytics & Anomaly Detection</span>
</div>
""", unsafe_allow_html=True)
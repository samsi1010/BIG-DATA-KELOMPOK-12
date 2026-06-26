import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine

st.set_page_config(
    page_title="Anomaly Analysis",
    page_icon="🚨",
    layout="wide"
)

st.title("🚨 Advanced Anomaly Analysis")

# =========================
# LOAD DATA
# =========================

@st.cache_data
def load_data():
    engine = create_engine(
        "postgresql+psycopg2://postgres:samuel@localhost:5432/retail_db"
    )
    df = pd.read_sql("SELECT * FROM retail_transactions", engine)
    return df

df = load_data()

# =========================
# VALIDASI KOLOM
# =========================

if "anomaly" not in df.columns:
    st.error("❌ Kolom anomaly tidak ditemukan. Jalankan anomaly_detection.py terlebih dahulu.")
    st.stop()

# =========================
# FEATURE ENGINEERING
# =========================

if "InvoiceDate" in df.columns:
    df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"], errors="coerce")
    df["Hour"]       = df["InvoiceDate"].dt.hour
    df["Day"]        = df["InvoiceDate"].dt.day
    df["MonthNum"]   = df["InvoiceDate"].dt.month
    df["WeekdayNum"] = df["InvoiceDate"].dt.weekday
    df["DayOfWeek"]  = df["InvoiceDate"].dt.day_name()
    df["Month"]      = df["InvoiceDate"].dt.to_period("M").astype(str)

if "CustomerID" in df.columns:
    df["CustomerID"] = pd.to_numeric(df["CustomerID"], errors="coerce")
    df = df.dropna(subset=["CustomerID"])

if "Country" in df.columns:
    df["CountryEncoded"] = pd.Categorical(df["Country"]).codes

if "StockCode" in df.columns:
    product_freq = df["StockCode"].value_counts().to_dict()
    df["ProductFrequency"] = df["StockCode"].map(product_freq).fillna(1).astype(int)

if "CustomerID" in df.columns:
    customer_avg  = df.groupby("CustomerID")["TotalPrice"].mean().to_dict()
    customer_freq = df["CustomerID"].value_counts().to_dict()
    customer_qty  = df.groupby("CustomerID")["Quantity"].sum().to_dict()
    customer_std  = df.groupby("CustomerID")["TotalPrice"].std().to_dict()

    df["AvgCustomerSpend"]  = df["CustomerID"].map(customer_avg).fillna(df["TotalPrice"].mean())
    df["CustomerFrequency"] = df["CustomerID"].map(customer_freq).fillna(1).astype(int)
    df["CustomerTotalQty"]  = df["CustomerID"].map(customer_qty).fillna(0)
    df["CustomerPriceStd"]  = df["CustomerID"].map(customer_std).fillna(0)

# =========================
# SPLIT ANOMALY / NORMAL
# =========================

anomaly_df = df[df["anomaly"] == 1].copy()
normal_df  = df[df["anomaly"] == 0].copy()

# =========================
# METRICS
# =========================

st.subheader("📊 Anomaly Summary")

total_tx      = len(df)
total_anomaly = len(anomaly_df)
anomaly_pct   = round((total_anomaly / total_tx) * 100, 2) if total_tx > 0 else 0
avg_price     = round(anomaly_df["TotalPrice"].mean(), 2)
max_price     = round(anomaly_df["TotalPrice"].max(), 2)
est_loss      = round(total_anomaly * df["TotalPrice"].mean(), 2)

col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Total Anomalies",   f"{total_anomaly:,}")
col2.metric("Anomaly %",         f"{anomaly_pct}%")
col3.metric("Avg Anomaly Price", f"${avg_price:,.2f}")
col4.metric("Highest Anomaly",   f"${max_price:,.2f}")
col5.metric("Estimated Loss",    f"${est_loss:,.2f}")

if anomaly_pct > 10:
    st.error(f"⚠️ Anomaly rate {anomaly_pct}% sangat tinggi! Segera investigasi.")
elif anomaly_pct > 5:
    st.warning(f"⚠️ Anomaly rate {anomaly_pct}% perlu diperhatikan.")
else:
    st.success(f"✅ Anomaly rate {anomaly_pct}% masih dalam batas wajar.")

st.markdown("---")

# =========================
# TABS
# =========================

tab1, tab2, tab3, tab4 = st.tabs([
    "📈 Visualisasi",
    "🌍 Geographic",
    "📅 Time Analysis",
    "🔍 Detail Data"
])

# ══════════════════════════════
# TAB 1 — VISUALISASI
# ══════════════════════════════
with tab1:

    col_a, col_b = st.columns(2)

    with col_a:
        st.subheader("Quantity vs TotalPrice")
        fig_scatter = px.scatter(
            anomaly_df,
            x="Quantity",
            y="TotalPrice",
            color="UnitPrice",
            size="TotalPrice",
            hover_data=["UnitPrice"],
            color_continuous_scale="Reds",
            title="Anomaly Transactions — Quantity vs TotalPrice"
        )
        st.plotly_chart(fig_scatter, use_container_width=True)

    with col_b:
        st.subheader(" Anomaly Price Distribution")
        fig_hist = px.histogram(
            anomaly_df,
            x="TotalPrice",
            nbins=40,
            color_discrete_sequence=["#e74c3c"],
            title="Anomaly TotalPrice Distribution"
        )
        st.plotly_chart(fig_hist, use_container_width=True)

    col_c, col_d = st.columns(2)

    with col_c:
        st.subheader("📦 Quantity Distribution — Anomaly vs Normal")
        fig_box = px.box(
            df,
            x=df["anomaly"].replace({0: "Normal", 1: "Anomaly"}),
            y="Quantity",
            color=df["anomaly"].replace({0: "Normal", 1: "Anomaly"}),
            color_discrete_map={"Normal": "#2ecc71", "Anomaly": "#e74c3c"},
            title="Quantity Distribution"
        )
        st.plotly_chart(fig_box, use_container_width=True)

    with col_d:
        st.subheader("💵 TotalPrice Violin — Anomaly vs Normal")
        fig_violin = px.violin(
            df,
            x=df["anomaly"].replace({0: "Normal", 1: "Anomaly"}),
            y="TotalPrice",
            color=df["anomaly"].replace({0: "Normal", 1: "Anomaly"}),
            box=True,
            color_discrete_map={"Normal": "#2ecc71", "Anomaly": "#e74c3c"},
            title="TotalPrice Violin"
        )
        st.plotly_chart(fig_violin, use_container_width=True)

    # Anomaly Reason (jika kolom tersedia dari anomaly_detection.py)
    if "anomaly_reason" in anomaly_df.columns:
        st.subheader("🔎 Anomaly Reason Breakdown")
        reason_counts = anomaly_df["anomaly_reason"].value_counts().reset_index()
        reason_counts.columns = ["Reason", "Count"]
        fig_reason = px.bar(
            reason_counts,
            x="Reason",
            y="Count",
            color="Count",
            color_continuous_scale="Reds",
            title="Penyebab Anomaly"
        )
        st.plotly_chart(fig_reason, use_container_width=True)

    # Anomaly Score (jika kolom tersedia dari anomaly_detection.py)
    if "anomaly_score" in anomaly_df.columns:
        st.subheader("📉 Anomaly Score Distribution")
        fig_score = px.histogram(
            anomaly_df,
            x="anomaly_score",
            nbins=40,
            color_discrete_sequence=["#e74c3c"],
            title="Distribusi Anomaly Score (lebih negatif = lebih anomali)"
        )
        st.plotly_chart(fig_score, use_container_width=True)


# ══════════════════════════════
# TAB 2 — GEOGRAPHIC
# ══════════════════════════════
with tab2:

    if "Country" in anomaly_df.columns:

        st.subheader("🌍 Anomaly per Negara")
        anomaly_country = (
            anomaly_df.groupby("Country")
            .size()
            .reset_index(name="count")
            .sort_values("count", ascending=False)
            .head(15)
        )

        col_a, col_b = st.columns(2)

        with col_a:
            fig_bar = px.bar(
                anomaly_country,
                x="Country",
                y="count",
                color="count",
                color_continuous_scale="Reds",
                title="Top 15 Negara — Anomaly Count"
            )
            st.plotly_chart(fig_bar, use_container_width=True)

        with col_b:
            fig_map = px.choropleth(
                anomaly_country,
                locations="Country",
                locationmode="country names",
                color="count",
                color_continuous_scale="Reds",
                title="Anomaly Transactions per Country"
            )
            st.plotly_chart(fig_map, use_container_width=True)

        st.subheader("💰 Anomaly Revenue per Negara")
        revenue_country = (
            anomaly_df.groupby("Country")["TotalPrice"]
            .sum()
            .reset_index()
            .sort_values("TotalPrice", ascending=False)
            .head(15)
        )
        fig_rev = px.bar(
            revenue_country,
            x="Country",
            y="TotalPrice",
            color="TotalPrice",
            color_continuous_scale="OrRd",
            title="Top 15 Negara — Anomaly Revenue"
        )
        st.plotly_chart(fig_rev, use_container_width=True)

    else:
        st.info("Kolom Country tidak tersedia.")

# ══════════════════════════════
# TAB 3 — TIME ANALYSIS
# ══════════════════════════════
with tab3:

    if "InvoiceDate" in anomaly_df.columns:

        st.subheader("📅 Daily Anomaly Trend")
        daily_anomaly = (
            anomaly_df.groupby(anomaly_df["InvoiceDate"].dt.date)
            .size()
            .reset_index(name="count")
        )
        daily_anomaly.columns = ["Date", "Anomaly Count"]
        fig_daily = px.line(
            daily_anomaly,
            x="Date",
            y="Anomaly Count",
            title="Daily Anomaly Count",
            markers=True,
            color_discrete_sequence=["#e74c3c"]
        )
        st.plotly_chart(fig_daily, use_container_width=True)

        col_a, col_b = st.columns(2)

        with col_a:
            st.subheader("🕐 Anomaly per Jam")
            hourly = (
                anomaly_df.groupby("Hour")
                .size()
                .reset_index(name="count")
            )
            fig_hour = px.bar(
                hourly,
                x="Hour",
                y="count",
                color="count",
                color_continuous_scale="Reds",
                title="Anomaly Count per Hour"
            )
            st.plotly_chart(fig_hour, use_container_width=True)

        with col_b:
            st.subheader("📆 Anomaly per Hari")
            day_order = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
            daily_dow = (
                anomaly_df.groupby("DayOfWeek")
                .size()
                .reindex(day_order)
                .reset_index(name="count")
            )
            daily_dow.columns = ["Day", "Anomaly Count"]
            fig_dow = px.bar(
                daily_dow,
                x="Day",
                y="Anomaly Count",
                color="Anomaly Count",
                color_continuous_scale="Reds",
                title="Anomaly Count per Day of Week"
            )
            st.plotly_chart(fig_dow, use_container_width=True)


# ══════════════════════════════
# TAB 4 — DETAIL DATA
# ══════════════════════════════
with tab4:

    st.subheader("🚨 Highest Anomaly Transactions")
    top_anomaly = anomaly_df.sort_values(by="TotalPrice", ascending=False).head(20)
    st.dataframe(top_anomaly, use_container_width=True, height=500)

    st.markdown("---")

    st.subheader("🔎 Filter & Cari Anomaly")
    search = st.text_input("Cari Invoice / Customer ID", placeholder="Ketik di sini...")

    filtered_anomaly = anomaly_df.copy()
    if search:
        mask = pd.Series(False, index=filtered_anomaly.index)
        for col in ["InvoiceNo", "CustomerID"]:
            if col in filtered_anomaly.columns:
                mask |= filtered_anomaly[col].astype(str).str.contains(
                    search, case=False, na=False
                )
        filtered_anomaly = filtered_anomaly[mask]

    st.write(f"Menampilkan **{len(filtered_anomaly):,}** anomaly transactions")
    st.dataframe(filtered_anomaly, use_container_width=True, height=400)

    st.markdown("---")

    st.subheader("⬇️ Export Anomaly Data")
    csv = anomaly_df.to_csv(index=False)
    st.download_button(
        label="⬇️ Download Anomaly CSV",
        data=csv,
        file_name="anomaly_transactions.csv",
        mime="text/csv",
        use_container_width=True
    )
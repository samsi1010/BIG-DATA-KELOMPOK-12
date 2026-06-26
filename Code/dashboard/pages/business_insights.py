import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine

st.set_page_config(
    page_title="Business Insights",
    page_icon="📊",
    layout="wide"
)

st.title("📊 Business Insights")

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
# FEATURE ENGINEERING
# =========================

try:
    if "InvoiceDate" in df.columns:
        df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"], errors="coerce")
        df["Hour"]        = df["InvoiceDate"].dt.hour
        df["Day"]         = df["InvoiceDate"].dt.day
        df["MonthNum"]    = df["InvoiceDate"].dt.month
        df["WeekdayNum"]  = df["InvoiceDate"].dt.weekday
        df["DayOfWeek"]   = df["InvoiceDate"].dt.day_name()
        df["Month"]       = df["InvoiceDate"].dt.to_period("M").astype(str)

    if "CustomerID" in df.columns:
        df["CustomerID"] = pd.to_numeric(df["CustomerID"], errors="coerce")
        df = df.dropna(subset=["CustomerID"])

    if "Country" in df.columns:
        try:
            df["CountryEncoded"] = pd.Categorical(df["Country"]).codes
        except:
            pass

    if "StockCode" in df.columns:
        try:
            product_freq = df["StockCode"].value_counts().to_dict()
            df["ProductFrequency"] = df["StockCode"].map(product_freq).fillna(1).astype(int)
        except:
            pass

    if "CustomerID" in df.columns:
        try:
            customer_avg_spend = df.groupby("CustomerID", dropna=False)["TotalPrice"].mean().to_dict()
            df["AvgCustomerSpend"] = df["CustomerID"].map(customer_avg_spend).fillna(df["TotalPrice"].mean())
        except:
            pass
        try:
            customer_freq = df["CustomerID"].value_counts().to_dict()
            df["CustomerFrequency"] = df["CustomerID"].map(customer_freq).fillna(1).astype(int)
        except:
            pass

    if "Quantity" in df.columns and "CustomerID" in df.columns:
        try:
            customer_total_qty = df.groupby("CustomerID", dropna=False)["Quantity"].sum().to_dict()
            df["CustomerTotalQty"] = df["CustomerID"].map(customer_total_qty).fillna(0)
        except:
            pass

    if "CustomerID" in df.columns and "TotalPrice" in df.columns:
        try:
            customer_price_std = df.groupby("CustomerID", dropna=False)["TotalPrice"].std().to_dict()
            df["CustomerPriceStd"] = df["CustomerID"].map(customer_price_std).fillna(0)
        except:
            pass

except Exception as e:
    st.error(f"⚠️ Error feature engineering: {e}")

required_columns = ["Quantity", "UnitPrice", "TotalPrice"]
missing_cols = [col for col in required_columns if col not in df.columns]
if missing_cols:
    st.error(f"Kolom tidak ditemukan: {missing_cols}")
    st.stop()

# Pisahkan anomaly jika kolom tersedia
has_anomaly = "anomaly" in df.columns
if has_anomaly:
    anomaly_df = df[df["anomaly"] == 1].copy()
    normal_df  = df[df["anomaly"] == 0].copy()

# =========================
# SECTION 1 — REVENUE & PENJUALAN
# =========================

st.markdown("---")
st.subheader("💰 1. Revenue & Penjualan")
st.caption("Gambaran besar kesehatan keuangan bisnis: total pendapatan, rata-rata transaksi, dan tren dari waktu ke waktu.")

total_revenue = df["TotalPrice"].sum()
avg_tx        = df["TotalPrice"].mean()
max_tx        = df["TotalPrice"].max()
total_tx      = len(df)

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Revenue",         f"${total_revenue:,.2f}")
col2.metric("Total Transaksi",        f"{total_tx:,}")
col3.metric("Rata-rata per Transaksi",f"${avg_tx:,.2f}")
col4.metric("Transaksi Tertinggi",    f"${max_tx:,.2f}")

col_a, col_b = st.columns(2)

with col_a:
    # Tren revenue harian
    if "InvoiceDate" in df.columns:
        daily_rev = (
            df.groupby(df["InvoiceDate"].dt.date)["TotalPrice"]
            .sum().reset_index()
        )
        daily_rev.columns = ["Tanggal", "Revenue"]
        fig_daily = px.line(
            daily_rev, x="Tanggal", y="Revenue",
            title="📅 Tren Revenue Harian",
            color_discrete_sequence=["#3b82f6"],
            markers=False
        )
        fig_daily.update_traces(line_width=2)
        fig_daily.update_layout(
            hovermode="x unified",
            yaxis_title="Revenue ($)",
            xaxis_title="",
            height=320
        )
        st.plotly_chart(fig_daily, use_container_width=True)

with col_b:
    # Tren revenue bulanan
    if "Month" in df.columns:
        monthly_rev = (
            df.groupby("Month")["TotalPrice"]
            .sum().reset_index().sort_values("Month")
        )
        fig_monthly = px.area(
            monthly_rev, x="Month", y="TotalPrice",
            title="📆 Tren Revenue Bulanan",
            color_discrete_sequence=["#8b5cf6"]
        )
        fig_monthly.update_layout(
            yaxis_title="Revenue ($)",
            xaxis_title="",
            height=320
        )
        st.plotly_chart(fig_monthly, use_container_width=True)

# Distribusi harga transaksi
fig_dist = px.histogram(
    df, x="TotalPrice", nbins=60,
    title="📊 Distribusi Nilai Transaksi — sebagian besar transaksi bernilai kecil, beberapa outlier bernilai sangat besar",
    color_discrete_sequence=["#3b82f6"]
)
fig_dist.update_layout(
    xaxis_title="Total Price ($)",
    yaxis_title="Jumlah Transaksi",
    bargap=0.05
)
st.plotly_chart(fig_dist, use_container_width=True)
st.info("💡 **Insight:** Distribusi condong ke kanan (right-skewed) menunjukkan mayoritas transaksi bernilai rendah. Transaksi bernilai sangat tinggi perlu diverifikasi — bisa peluang pelanggan premium atau indikasi anomali.")

# =========================
# SECTION 2 — DISTRIBUSI GEOGRAFIS
# =========================

st.markdown("---")
st.subheader("🌍 2. Distribusi Geografis & Lokasi Risiko")
st.caption("Negara mana yang menghasilkan penjualan terbesar? Dan di mana konsentrasi transaksi berisiko?")

if "Country" in df.columns:
    col_a, col_b = st.columns(2)

    with col_a:
        # Top 15 negara by revenue
        country_rev = (
            df.groupby("Country")["TotalPrice"].sum()
            .reset_index().sort_values("TotalPrice", ascending=False).head(15)
        )
        fig_crev = px.bar(
            country_rev, x="Country", y="TotalPrice",
            title="💰 Top 15 Negara by Revenue",
            color="TotalPrice",
            color_continuous_scale="Blues"
        )
        fig_crev.update_layout(
            xaxis_tickangle=-35,
            yaxis_title="Total Revenue ($)",
            xaxis_title="",
            coloraxis_showscale=False,
            height=380
        )
        st.plotly_chart(fig_crev, use_container_width=True)

    with col_b:
        # Anomaly per negara (jika tersedia)
        if has_anomaly:
            anomaly_country = (
                anomaly_df.groupby("Country").size()
                .reset_index(name="Anomali").sort_values("Anomali", ascending=False).head(15)
            )
            normal_country = (
                normal_df.groupby("Country").size()
                .reset_index(name="Normal")
            )
            merged = anomaly_country.merge(normal_country, on="Country", how="left").fillna(0)
            merged["Total"] = merged["Anomali"] + merged["Normal"]
            merged["% Anomali"] = (merged["Anomali"] / merged["Total"] * 100).round(1)

            fig_risk = px.bar(
                merged, x="Country", y="% Anomali",
                title="🚨 % Anomali per Negara (Top 15 Anomali Terbanyak)",
                color="% Anomali",
                color_continuous_scale="Reds"
            )
            fig_risk.update_layout(
                xaxis_tickangle=-35,
                yaxis_title="% Anomali",
                xaxis_title="",
                coloraxis_showscale=False,
                height=380
            )
            st.plotly_chart(fig_risk, use_container_width=True)
        else:
            # Jumlah transaksi per negara
            country_tx = (
                df.groupby("Country").size()
                .reset_index(name="Transaksi").sort_values("Transaksi", ascending=False).head(15)
            )
            fig_ctx = px.bar(
                country_tx, x="Country", y="Transaksi",
                title="🌐 Jumlah Transaksi per Negara",
                color="Transaksi",
                color_continuous_scale="Greens"
            )
            fig_ctx.update_layout(
                xaxis_tickangle=-35,
                coloraxis_showscale=False,
                height=380
            )
            st.plotly_chart(fig_ctx, use_container_width=True)

    # Choropleth map
    country_map = (
        df.groupby("Country")["TotalPrice"].sum()
        .reset_index(name="Revenue")
    )
    fig_map = px.choropleth(
        country_map,
        locations="Country",
        locationmode="country names",
        color="Revenue",
        color_continuous_scale="Blues",
        title="🗺️ Peta Revenue Global"
    )
    fig_map.update_layout(height=420)
    st.plotly_chart(fig_map, use_container_width=True)
    st.info("💡 **Insight:** Identifikasi negara dengan revenue tinggi namun % anomali tinggi — wilayah ini memerlukan pengawasan ketat karena potensi kerugian yang besar.")

else:
    st.warning("Kolom Country tidak tersedia.")

# =========================
# SECTION 3 — PRODUK TERLARIS
# =========================

st.markdown("---")
st.subheader("📦 3. Analisis Produk Terlaris")
st.caption("Produk mana yang paling banyak terjual? Dan produk mana yang menghasilkan revenue terbesar?")

if "Description" in df.columns:
    col_a, col_b = st.columns(2)

    with col_a:
        top_qty = (
            df.groupby("Description")["Quantity"].sum()
            .reset_index().sort_values("Quantity", ascending=False).head(10)
        )
        fig_tq = px.bar(
            top_qty, x="Quantity", y="Description", orientation="h",
            title="📦 Top 10 Produk — Volume Terjual",
            color="Quantity",
            color_continuous_scale="Blues"
        )
        fig_tq.update_layout(
            yaxis={"categoryorder": "total ascending"},
            yaxis_title="",
            coloraxis_showscale=False,
            height=400
        )
        st.plotly_chart(fig_tq, use_container_width=True)

    with col_b:
        top_rev = (
            df.groupby("Description")["TotalPrice"].sum()
            .reset_index().sort_values("TotalPrice", ascending=False).head(10)
        )
        fig_tr = px.bar(
            top_rev, x="TotalPrice", y="Description", orientation="h",
            title="💰 Top 10 Produk — Revenue",
            color="TotalPrice",
            color_continuous_scale="Greens"
        )
        fig_tr.update_layout(
            yaxis={"categoryorder": "total ascending"},
            yaxis_title="",
            xaxis_title="Total Revenue ($)",
            coloraxis_showscale=False,
            height=400
        )
        st.plotly_chart(fig_tr, use_container_width=True)

    # Scatter: Frekuensi vs Revenue per produk
    prod_summary = (
        df.groupby("Description")
        .agg(Volume=("Quantity", "sum"), Revenue=("TotalPrice", "sum"), Transaksi=("TotalPrice", "count"))
        .reset_index()
        .sort_values("Revenue", ascending=False)
        .head(50)
    )
    fig_bubble = px.scatter(
        prod_summary,
        x="Volume",
        y="Revenue",
        size="Transaksi",
        hover_name="Description",
        title="🔵 Produk: Volume vs Revenue (ukuran = frekuensi transaksi) — Top 50",
        color="Revenue",
        color_continuous_scale="Purples",
        size_max=40
    )
    fig_bubble.update_layout(
        xaxis_title="Total Volume Terjual",
        yaxis_title="Total Revenue ($)",
        coloraxis_showscale=False,
        height=420
    )
    st.plotly_chart(fig_bubble, use_container_width=True)
    st.info("💡 **Insight:** Produk di pojok kanan atas (volume tinggi + revenue tinggi) adalah produk bintang yang harus selalu tersedia di stok. Produk dengan revenue tinggi tapi volume rendah biasanya adalah produk bernilai tinggi yang perlu proteksi ekstra.")

else:
    st.warning("Kolom Description tidak tersedia.")

# =========================
# SECTION 4 — POLA WAKTU & JAM SIBUK
# =========================

st.markdown("---")
st.subheader("🕐 4. Pola Waktu & Jam Sibuk")
st.caption("Kapan pelanggan paling aktif bertransaksi? Gunakan ini untuk jadwal promosi, alokasi server, dan pengelolaan staf.")

if "Hour" in df.columns and "DayOfWeek" in df.columns:
    col_a, col_b = st.columns(2)

    with col_a:
        hourly = df.groupby("Hour")["TotalPrice"].sum().reset_index()
        hourly.columns = ["Jam", "Revenue"]
        fig_hour = px.bar(
            hourly, x="Jam", y="Revenue",
            title="🕐 Revenue per Jam",
            color="Revenue",
            color_continuous_scale="Blues"
        )
        fig_hour.update_layout(
            xaxis=dict(dtick=2, title="Jam"),
            yaxis_title="Revenue ($)",
            coloraxis_showscale=False,
            height=320
        )
        st.plotly_chart(fig_hour, use_container_width=True)

    with col_b:
        day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        daily_dow = (
            df.groupby("DayOfWeek")["TotalPrice"].sum()
            .reindex(day_order).reset_index()
        )
        daily_dow.columns = ["Hari", "Revenue"]
        fig_dow = px.bar(
            daily_dow, x="Hari", y="Revenue",
            title="📆 Revenue per Hari",
            color="Revenue",
            color_continuous_scale="Purples"
        )
        fig_dow.update_layout(
            xaxis_title="",
            yaxis_title="Revenue ($)",
            coloraxis_showscale=False,
            height=320
        )
        st.plotly_chart(fig_dow, use_container_width=True)

    # Heatmap: Jam vs Hari
    if "DayOfWeek" in df.columns:
        heatmap_data = (
            df.groupby(["DayOfWeek", "Hour"])["TotalPrice"]
            .sum().reset_index()
        )
        heatmap_pivot = heatmap_data.pivot(index="DayOfWeek", columns="Hour", values="TotalPrice").fillna(0)
        heatmap_pivot = heatmap_pivot.reindex([d for d in day_order if d in heatmap_pivot.index])

        fig_heat = px.imshow(
            heatmap_pivot,
            color_continuous_scale="Blues",
            title="🔥 Heatmap Aktivitas: Hari vs Jam — Temukan jam sibuk penjualan",
            labels=dict(x="Jam", y="Hari", color="Revenue ($)"),
            aspect="auto"
        )
        fig_heat.update_layout(height=350)
        st.plotly_chart(fig_heat, use_container_width=True)
        st.info("💡 **Insight:** Area biru gelap di heatmap adalah waktu puncak penjualan. Jadwalkan promosi flash sale atau kampanye email di jam tersebut untuk memaksimalkan konversi.")

# =========================
# SECTION 5 — RETUR BARANG
# =========================

st.markdown("---")
st.subheader("↩️ 5. Pemantauan Retur Barang")
st.caption("Transaksi dengan kuantitas negatif menandakan retur. Produk apa yang paling sering dikembalikan?")

retur_df = df[df["Quantity"] < 0].copy()
retur_df["Quantity_Abs"] = retur_df["Quantity"].abs()

if len(retur_df) > 0:
    col_a, col_b, col_c = st.columns(3)
    col_a.metric("Total Transaksi Retur", f"{len(retur_df):,}")
    col_b.metric("% dari Total Transaksi", f"{len(retur_df)/len(df)*100:.1f}%")
    col_c.metric("Total Nilai Retur",      f"${retur_df['TotalPrice'].abs().sum():,.2f}")

    col_x, col_y = st.columns(2)

    with col_x:
        if "Description" in retur_df.columns:
            top_retur = (
                retur_df.groupby("Description")["Quantity_Abs"].sum()
                .reset_index().sort_values("Quantity_Abs", ascending=False).head(10)
            )
            fig_ret = px.bar(
                top_retur, x="Quantity_Abs", y="Description", orientation="h",
                title="↩️ Top 10 Produk Paling Sering Diretur",
                color="Quantity_Abs",
                color_continuous_scale="Reds"
            )
            fig_ret.update_layout(
                yaxis={"categoryorder": "total ascending"},
                yaxis_title="",
                xaxis_title="Jumlah Unit Diretur",
                coloraxis_showscale=False,
                height=400
            )
            st.plotly_chart(fig_ret, use_container_width=True)

    with col_y:
        if "Country" in retur_df.columns:
            retur_country = (
                retur_df.groupby("Country")["Quantity_Abs"].sum()
                .reset_index().sort_values("Quantity_Abs", ascending=False).head(10)
            )
            fig_retc = px.bar(
                retur_country, x="Country", y="Quantity_Abs",
                title="🌍 Retur per Negara (Top 10)",
                color="Quantity_Abs",
                color_continuous_scale="Oranges"
            )
            fig_retc.update_layout(
                xaxis_tickangle=-35,
                yaxis_title="Jumlah Unit Diretur",
                coloraxis_showscale=False,
                height=400
            )
            st.plotly_chart(fig_retc, use_container_width=True)

    # Tren retur harian
    if "InvoiceDate" in retur_df.columns:
        daily_retur = (
            retur_df.groupby(retur_df["InvoiceDate"].dt.date)["Quantity_Abs"]
            .sum().reset_index()
        )
        daily_retur.columns = ["Tanggal", "Unit Diretur"]
        fig_rtren = px.line(
            daily_retur, x="Tanggal", y="Unit Diretur",
            title="📅 Tren Retur Harian",
            color_discrete_sequence=["#ef4444"]
        )
        fig_rtren.update_layout(height=280)
        st.plotly_chart(fig_rtren, use_container_width=True)

    st.info("💡 **Insight:** Produk dengan retur tinggi perlu evaluasi kualitas atau deskripsi produk yang lebih akurat. Lonjakan retur pada tanggal tertentu bisa mengindikasikan masalah pengiriman atau kampanye promosi yang tidak tepat sasaran.")

else:
    st.success("✅ Tidak ada transaksi retur ditemukan.")

# =========================
# SECTION 6 — ANOMALI (jika tersedia)
# =========================

if has_anomaly:
    st.markdown("---")
    st.subheader("🚨 6. Ringkasan Anomali Transaksi")
    st.caption("Transaksi yang terdeteksi mencurigakan oleh model Isolation Forest.")

    total_anomaly = len(anomaly_df)
    anomaly_pct   = round(total_anomaly / len(df) * 100, 2)
    est_loss      = round(total_anomaly * df["TotalPrice"].mean(), 2)

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Anomali",       f"{total_anomaly:,}")
    col2.metric("% Anomali",           f"{anomaly_pct}%")
    col3.metric("Avg Nilai Anomali",   f"${anomaly_df['TotalPrice'].mean():,.2f}")
    col4.metric("Estimasi Kerugian",   f"${est_loss:,.2f}")

    if anomaly_pct > 10:
        st.error(f"⚠️ Anomaly rate {anomaly_pct}% sangat tinggi! Segera investigasi.")
    elif anomaly_pct > 5:
        st.warning(f"⚠️ Anomaly rate {anomaly_pct}% perlu diperhatikan.")
    else:
        st.success(f"✅ Anomaly rate {anomaly_pct}% masih dalam batas wajar.")

    col_a, col_b = st.columns(2)

    with col_a:
        # Donut: Normal vs Anomali
        pie_data = pd.DataFrame({
            "Status": ["Normal", "Anomali"],
            "Jumlah": [len(normal_df), total_anomaly]
        })
        fig_pie = px.pie(
            pie_data, names="Status", values="Jumlah", hole=0.55,
            title="Distribusi Normal vs Anomali",
            color="Status",
            color_discrete_map={"Normal": "#22c55e", "Anomali": "#ef4444"}
        )
        st.plotly_chart(fig_pie, use_container_width=True)

    with col_b:
        # Scatter Anomali
        fig_sc = px.scatter(
            anomaly_df.sample(min(1000, len(anomaly_df)), random_state=42),
            x="Quantity", y="TotalPrice",
            color="UnitPrice",
            color_continuous_scale="Reds",
            title="Sebaran Transaksi Anomali (sample)",
            opacity=0.7
        )
        fig_sc.update_layout(height=350)
        st.plotly_chart(fig_sc, use_container_width=True)

    st.info("💡 **Insight:** Anomali terkonsentrasi pada transaksi dengan TotalPrice atau Quantity ekstrem. Prioritaskan investigasi pada transaksi dengan nilai tertinggi terlebih dahulu untuk efisiensi waktu tim fraud.")

# =========================
# SECTION 7 — CUSTOMER ANALYSIS
# =========================

st.markdown("---")
st.subheader("👤 7. Analisis Pelanggan")
st.caption("Siapa pelanggan paling bernilai? Bagaimana pola pengeluaran mereka?")

if "CustomerID" in df.columns:
    col_a, col_b = st.columns(2)

    with col_a:
        top_cust = (
            df.groupby("CustomerID")["TotalPrice"].sum()
            .reset_index().sort_values("TotalPrice", ascending=False).head(10)
        )
        top_cust["CustomerID"] = top_cust["CustomerID"].astype(int).astype(str)
        fig_tc = px.bar(
            top_cust, x="TotalPrice", y="CustomerID", orientation="h",
            title="🏆 Top 10 Pelanggan by Total Belanja",
            color="TotalPrice",
            color_continuous_scale="Greens"
        )
        fig_tc.update_layout(
            yaxis={"categoryorder": "total ascending"},
            yaxis_title="Customer ID",
            xaxis_title="Total Belanja ($)",
            coloraxis_showscale=False,
            height=380
        )
        st.plotly_chart(fig_tc, use_container_width=True)

    with col_b:
        top_freq = (
            df.groupby("CustomerID").size()
            .reset_index(name="Frekuensi").sort_values("Frekuensi", ascending=False).head(10)
        )
        top_freq["CustomerID"] = top_freq["CustomerID"].astype(int).astype(str)
        fig_tf = px.bar(
            top_freq, x="Frekuensi", y="CustomerID", orientation="h",
            title="🔁 Top 10 Pelanggan Paling Sering Bertransaksi",
            color="Frekuensi",
            color_continuous_scale="Blues"
        )
        fig_tf.update_layout(
            yaxis={"categoryorder": "total ascending"},
            yaxis_title="Customer ID",
            xaxis_title="Jumlah Transaksi",
            coloraxis_showscale=False,
            height=380
        )
        st.plotly_chart(fig_tf, use_container_width=True)

    # RFM-style: scatter frekuensi vs total spend
    rfm = (
        df.groupby("CustomerID")
        .agg(TotalSpend=("TotalPrice", "sum"), Frekuensi=("TotalPrice", "count"))
        .reset_index()
    )
    fig_rfm = px.scatter(
        rfm.sample(min(500, len(rfm)), random_state=42),
        x="Frekuensi", y="TotalSpend",
        title="💎 Segmentasi Pelanggan: Frekuensi vs Total Belanja",
        color="TotalSpend",
        color_continuous_scale="Purples",
        opacity=0.6,
        size="TotalSpend",
        size_max=20
    )
    fig_rfm.update_layout(
        xaxis_title="Jumlah Transaksi",
        yaxis_title="Total Belanja ($)",
        coloraxis_showscale=False,
        height=400
    )
    st.plotly_chart(fig_rfm, use_container_width=True)
    st.info("💡 **Insight:** Pelanggan di pojok kanan atas (frekuensi tinggi + belanja tinggi) adalah pelanggan VIP. Berikan program loyalitas khusus untuk mempertahankan mereka. Pelanggan frekuensi tinggi tapi belanja rendah bisa di-upsell ke produk premium.")

# =========================
# SECTION 8 — KORELASI FITUR
# =========================

# =========================
# CORRELATION
# =========================

st.subheader("📌 Correlation Matrix - Semua Numeric Features")

# Ambil SEMUA fitur numeric (include engineered features)
corr_cols = df.select_dtypes(include=['number']).columns.tolist()
corr = df[corr_cols].corr()

fig_corr = px.imshow(
    corr,
    text_auto=".2f",
    aspect="auto",
    color_continuous_scale="RdBu_r",
    zmin=-1,
    zmax=1,
    title="Correlation Matrix: ALL Numeric Features vs Anomaly Detection",
    labels=dict(color="Correlation")
)

# Styling untuk readability
num_features = len(corr_cols)
figure_height = max(600, num_features * 80)
fig_corr.update_traces(text=corr.values, texttemplate="%{text:.2f}", textfont={"size": 10})
fig_corr.update_layout(
    width=900,
    height=figure_height,
    font=dict(size=11),
    coloraxis_colorbar=dict(thickness=20, len=0.7),
    xaxis_tickangle=-45
)

st.plotly_chart(
    fig_corr,
    use_container_width=True
)

if "anomaly" in corr.columns:
    st.subheader("🔍 Top 10 Fitur Berkorelasi dengan Anomali")
    anomaly_corr = corr["anomaly"].drop("anomaly").sort_values(key=abs, ascending=False)
    top_10 = anomaly_corr.head(10).reset_index()
    top_10.columns = ["Fitur", "Korelasi"]
    top_10["Arah"] = top_10["Korelasi"].apply(lambda v: "Positif (→ Anomali)" if v > 0 else "Negatif (→ Normal)")
    top_10["Abs"]  = top_10["Korelasi"].abs()

    fig_bar_corr = px.bar(
        top_10, x="Abs", y="Fitur", orientation="h",
        color="Arah",
        color_discrete_map={"Positif (→ Anomali)": "#ef4444", "Negatif (→ Normal)": "#22c55e"},
        title="Kekuatan Korelasi Fitur terhadap Anomali",
        text=top_10["Korelasi"].round(4).astype(str)
    )
    fig_bar_corr.update_layout(
        yaxis={"categoryorder": "total ascending"},
        xaxis_title="Kekuatan Korelasi (Absolut)",
        yaxis_title="",
        height=420
    )
    st.plotly_chart(fig_bar_corr, use_container_width=True)
    st.info("💡 **Insight:** Fitur berwarna merah (positif) artinya nilai tinggi cenderung mengindikasikan anomali. Fitur berwarna hijau (negatif) artinya nilai tinggi justru ciri transaksi normal. Gunakan ini untuk membangun aturan bisnis atau threshold deteksi manual.")

# =========================
# DATA SAMPLE
# =========================

st.markdown("---")
st.subheader("🧾 Dataset Preview")
st.dataframe(df.head(100), use_container_width=True)
"""
Script untuk menghasilkan laporan output dari Feature Engineering
Digunakan untuk dokumentasi laporan penelitian
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pandas as pd
import os

spark = SparkSession.builder \
    .appName("OutputReport") \
    .getOrCreate()

print("\n" + "="*80)
print("GENERATING OUTPUT REPORT FOR DOCUMENTATION")
print("="*80)

# Baca featured data
print("\nMembaca featured data...")
df = spark.read.parquet("data/featured_data")

# PENTING: Bersihkan whitespace dari nama kolom
df = df.select([col(c).alias(c.strip()) for c in df.columns])

print(f"✓ Featured data berhasil dibaca: {df.count()} baris")

# ─────────────────────────────────────────────────────────────────────────────
# 1. MAPPING COUNTRY KE NUMERIK (PENTING UNTUK LAPORAN)
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "="*80)
print("1. MAPPING COUNTRY → COUNTRY ENCODED (BUKTI ENCODING)")
print("="*80)

country_mapping = df.select("Country", "CountryEncoded").distinct().orderBy("CountryEncoded")

# Convert to pandas untuk lebih readable
country_mapping_pd = country_mapping.toPandas()

# Simpan ke CSV
country_mapping_pd.to_csv("reports/01_country_mapping.csv", index=False)
print("✓ Mapping disimpan ke: reports/01_country_mapping.csv")

print("\nSample Mapping (20 negara pertama):")
print(country_mapping_pd.head(20).to_string(index=False))

print(f"\n✓ Total negara yang di-encode: {len(country_mapping_pd)}")

# ─────────────────────────────────────────────────────────────────────────────
# 2. SAMPLE DATA DENGAN SEMUA FITUR (BUKTI FEATURE ENGINEERING)
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "="*80)
print("2. SAMPLE DATA LENGKAP HASIL FEATURE ENGINEERING")
print("="*80)

important_features = [
    "InvoiceNo",
    "CustomerID",
    "StockCode",
    "Description",
    "Quantity",
    "UnitPrice",
    "TotalPrice",
    "Country",
    "InvoiceDate",
    "Hour",
    "Day",
    "MonthNum",
    "RevenueCategory",
    "CountryEncoded",
    "ProductFrequency",
    "AvgCustomerSpend",
    "CustomerFrequency",
    "CustomerTotalQty",
    "CustomerPriceStd"
]

# Filter kolom yang ada
available_features = [c for c in important_features if c in df.columns]

sample_data = df.select(available_features).limit(20).toPandas()
sample_data.to_csv("reports/02_sample_featured_data.csv", index=False)

print("✓ Sample data disimpan ke: reports/02_sample_featured_data.csv")
print("\nSample data (baris pertama):")
print(sample_data.head(5).to_string())

# ─────────────────────────────────────────────────────────────────────────────
# 3. STATISTIK FITUR YANG DIBUAT
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "="*80)
print("3. STATISTIK FITUR-FITUR YANG DIBUAT")
print("="*80)

# Time features
print("\n[TIME FEATURES]")
time_stats = df.select(
    min("Hour").alias("Min_Hour"),
    max("Hour").alias("Max_Hour"),
    avg("Hour").alias("Avg_Hour"),
    min("Day").alias("Min_Day"),
    max("Day").alias("Max_Day"),
    min("MonthNum").alias("Min_Month"),
    max("MonthNum").alias("Max_Month")
).toPandas()
print(time_stats.to_string(index=False))

# Revenue category
print("\n[REVENUE CATEGORY DISTRIBUTION]")
revenue_dist = df.groupBy("RevenueCategory").count().toPandas()
revenue_dist.columns = ["RevenueCategory", "Count"]
revenue_dist = revenue_dist.sort_values("Count", ascending=False)
print(revenue_dist.to_string(index=False))
revenue_dist.to_csv("reports/03_revenue_category_distribution.csv", index=False)

# Product frequency
print("\n[PRODUCT FREQUENCY STATISTICS]")
product_stats = df.select(
    min("ProductFrequency").alias("Min_Frequency"),
    max("ProductFrequency").alias("Max_Frequency"),
    avg("ProductFrequency").alias("Avg_Frequency"),
    stddev("ProductFrequency").alias("Stddev_Frequency")
).toPandas()
print(product_stats.to_string(index=False))

# Customer features
print("\n[CUSTOMER FEATURES STATISTICS]")
customer_stats = df.select(
    avg("AvgCustomerSpend").alias("Avg_AvgSpend"),
    min("AvgCustomerSpend").alias("Min_AvgSpend"),
    max("AvgCustomerSpend").alias("Max_AvgSpend"),
    avg("CustomerFrequency").alias("Avg_Frequency"),
    max("CustomerFrequency").alias("Max_Frequency"),
    avg("CustomerTotalQty").alias("Avg_TotalQty"),
    max("CustomerTotalQty").alias("Max_TotalQty")
).toPandas()
print(customer_stats.to_string(index=False))

# ─────────────────────────────────────────────────────────────────────────────
# 4. TOP PRODUCTS DAN TOP CUSTOMERS (UNTUK ANALISIS)
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "="*80)
print("4. TOP PRODUCTS & TOP CUSTOMERS")
print("="*80)

print("\n[TOP 10 MOST SOLD PRODUCTS]")
top_products = df.groupBy("StockCode", "Description").agg(
    count("*").alias("Transaction_Count"),
    sum("Quantity").alias("Total_Quantity"),
    avg("UnitPrice").alias("Avg_Price")
).orderBy(col("Transaction_Count").desc()).limit(10).toPandas()
print(top_products.to_string(index=False))
top_products.to_csv("reports/04_top_products.csv", index=False)

print("\n[TOP 10 CUSTOMERS BY SPENDING]")
top_customers = df.groupBy("CustomerID").agg(
    first("AvgCustomerSpend").alias("Avg_Spend"),
    first("CustomerFrequency").alias("Transaction_Count"),
    first("CustomerTotalQty").alias("Total_Qty"),
    first("Country").alias("Country")
).orderBy(col("Avg_Spend").desc()).limit(10).toPandas()
print(top_customers.to_string(index=False))
top_customers.to_csv("reports/05_top_customers.csv", index=False)

# ─────────────────────────────────────────────────────────────────────────────
# 5. DATA QUALITY SUMMARY
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "="*80)
print("5. DATA QUALITY SUMMARY")
print("="*80)

print("\nJumlah kolom setelah feature engineering:", len(df.columns))
print("Jumlah baris:", df.count())

# Check NULL values
print("\n[NULL VALUES CHECK]")
null_counts = {}
for column in df.columns:
    null_count = df.filter(col(column).isNull()).count()
    if null_count > 0:
        null_counts[column] = null_count

if null_counts:
    print("Ditemukan NULL values di kolom:")
    for col_name, count in null_counts.items():
        print(f"  - {col_name}: {count}")
else:
    print("✓ Tidak ada NULL values - Data siap untuk analisis")

# ─────────────────────────────────────────────────────────────────────────────
# 6. SCHEMA FINAL
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "="*80)
print("6. FINAL SCHEMA")
print("="*80)
df.printSchema()

print("\n" + "="*80)
print("✓ OUTPUT REPORT GENERATION COMPLETE")
print("="*80)
print("\n📁 Semua laporan disimpan di folder: reports/")
print("   - 01_country_mapping.csv")
print("   - 02_sample_featured_data.csv")
print("   - 03_revenue_category_distribution.csv")
print("   - 04_top_products.csv")
print("   - 05_top_customers.csv")

spark.stop()

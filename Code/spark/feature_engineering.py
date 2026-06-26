from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.ml.feature import StringIndexer
import os

spark = SparkSession.builder \
    .appName("FeatureEngineering") \
    .getOrCreate()

print("\n" + "="*80)
print("TAHAP 4.2: FEATURE ENGINEERING DENGAN PYSPARK")
print("="*80)

print("\n[TAHAP 4.2.1] MEMBACA DATA CLEANED")
print("-"*80)

df = spark.read.parquet("data/cleaned_data")

# PENTING: Bersihkan whitespace dari nama kolom
df = df.select([col(c).alias(c.strip()) for c in df.columns])

print(f"✓ Data berhasil dibaca dari: data/cleaned_data")
print(f"✓ Jumlah baris: {df.count()}")
print(f"✓ Jumlah kolom: {len(df.columns)}")
print(f"\nKolom yang tersedia:")
for col_name in df.columns:
    print(f"  - {col_name}")
print()

print("\n[TAHAP 4.2.2] PEMBUATAN FITUR WAKTU TRANSAKSI")
print("-"*80)

# Parse InvoiceDate dengan format "M/dd/yyyy H:mm"
df = df.withColumn("InvoiceDate", to_timestamp(col("InvoiceDate"), "M/dd/yyyy H:mm"))
print("✓ InvoiceDate berhasil dikonversi ke format timestamp")

df = df.withColumn("Hour", hour(col("InvoiceDate")).cast("double"))
df = df.withColumn("Day", dayofmonth(col("InvoiceDate")).cast("double"))
df = df.withColumn("MonthNum", month(col("InvoiceDate")).cast("double"))
df = df.withColumn("WeekdayNum", (dayofweek(col("InvoiceDate")) - 1).cast("double"))
df = df.withColumn("Month", date_format(col("InvoiceDate"), "yyyy-MM"))

print("✓ Fitur waktu berhasil dibuat: Hour, Day, MonthNum, WeekdayNum, Month")
print("\nSample data fitur waktu (5 baris pertama):")
df.select("InvoiceDate", "Hour", "Day", "MonthNum", "WeekdayNum", "Month").show(5, truncate=False)

print("\nStatistik fitur waktu:")
df.select(
    min("Hour").alias("Min_Hour"),
    max("Hour").alias("Max_Hour"),
    min("Day").alias("Min_Day"),
    max("Day").alias("Max_Day"),
    min("MonthNum").alias("Min_Month"),
    max("MonthNum").alias("Max_Month")
).show()
print()

print("\n[TAHAP 4.2.3] PEMBUATAN REVENUE CATEGORY")
print("-"*80)

df = df.withColumn(
    "RevenueCategory",
    when(col("TotalPrice") > 1000, "High")
    .when(col("TotalPrice") > 300, "Medium")
    .otherwise("Low")
)

print("✓ RevenueCategory berhasil dibuat")
print("  - High: TotalPrice > 1000")
print("  - Medium: TotalPrice > 300")
print("  - Low: TotalPrice <= 300")

print("\nDistribusi RevenueCategory:")
df.groupBy("RevenueCategory").count().orderBy(col("count").desc()).show()

print("\nSample data RevenueCategory (10 baris):")
df.select("TotalPrice", "RevenueCategory").distinct().limit(10).show()
print()

print("\n[TAHAP 4.2.4] ENCODING DATA NEGARA")
print("-"*80)

if "Country" in df.columns:
    print("✓ Kolom Country ditemukan")
    
    # Tampilkan negara unik sebelum encoding
    print("\nJumlah negara unik:", df.select("Country").distinct().count())
    print("\nSample negara sebelum encoding:")
    df.select("Country").distinct().limit(10).show()
    
    # Lakukan encoding
    indexer = StringIndexer(
        inputCol="Country",
        outputCol="CountryEncoded",
        handleInvalid="keep"
    )
    df = indexer.fit(df).transform(df)
    
    print("\n✓ Country berhasil di-encode menjadi CountryEncoded (numerik)")
    
    # Tampilkan mapping Country ke numerik
    print("\nMapping Country ke CountryEncoded (Sample 15 negara):")
    mapping_df = df.select("Country", "CountryEncoded").distinct().orderBy("CountryEncoded")
    mapping_df.limit(15).show(truncate=False)
    
    print("\n✓ Total mapping yang dihasilkan:", mapping_df.count())
print()

print("\n[TAHAP 4.2.5] PEMBUATAN PRODUCT FEATURES")
print("-"*80)

if "StockCode" in df.columns:
    stock_window = Window.partitionBy("StockCode")
    df = df.withColumn(
        "ProductFrequency",
        count("StockCode").over(stock_window)
    )
    
    print("✓ ProductFrequency berhasil dibuat")
    print("  Menghitung: Jumlah transaksi per produk (StockCode)")
    
    print("\nStatistik ProductFrequency:")
    df.select(
        min("ProductFrequency").alias("Min_Frequency"),
        max("ProductFrequency").alias("Max_Frequency"),
        avg("ProductFrequency").alias("Avg_Frequency")
    ).show()
    
    print("\nTop 10 produk paling sering dijual:")
    df.groupBy("StockCode").agg(
        first("ProductFrequency").alias("Frequency")
    ).orderBy(col("Frequency").desc()).limit(10).show()
print()

print("\n[TAHAP 4.2.6] PEMBUATAN CUSTOMER FEATURES")
print("-"*80)

if "CustomerID" in df.columns:
    cust_window = Window.partitionBy("CustomerID")

    df = df.withColumn(
        "AvgCustomerSpend",
        avg("TotalPrice").over(cust_window)
    )

    df = df.withColumn(
        "CustomerFrequency",
        count("CustomerID").over(cust_window)
    )

    df = df.withColumn(
        "CustomerTotalQty",
        sum("Quantity").over(cust_window)
    )

    df = df.withColumn(
        "CustomerPriceStd",
        stddev("TotalPrice").over(cust_window)
    )
    
    print("✓ Customer Features berhasil dibuat:")
    print("  - AvgCustomerSpend: Rata-rata pengeluaran per pelanggan")
    print("  - CustomerFrequency: Jumlah transaksi per pelanggan")
    print("  - CustomerTotalQty: Total barang yang dibeli pelanggan")
    print("  - CustomerPriceStd: Standar deviasi nilai transaksi pelanggan")
    
    print("\nStatistik Customer Features:")
    df.select(
        avg("AvgCustomerSpend").alias("Avg_CustomerSpend"),
        avg("CustomerFrequency").alias("Avg_CustomerFrequency"),
        max("CustomerFrequency").alias("Max_CustomerFrequency"),
        avg("CustomerTotalQty").alias("Avg_CustomerTotalQty"),
        max("CustomerTotalQty").alias("Max_CustomerTotalQty"),
        avg("CustomerPriceStd").alias("Avg_CustomerPriceStd")
    ).show()
    
    print("\nSample Customer Features (10 pelanggan unik):")
    df.select("CustomerID", "AvgCustomerSpend", "CustomerFrequency", "CustomerTotalQty", "CustomerPriceStd") \
        .distinct().limit(10).show(truncate=False)
print()

print("\n[TAHAP 4.2.7] PENANGANAN MISSING VALUES PADA FITUR BARU")
print("-"*80)

# ─────────────────────────────────────────────
# NULL HANDLING
# ─────────────────────────────────────────────
fill_map = {
    "CountryEncoded": 0,
    "ProductFrequency": 0,
    "AvgCustomerSpend": 0.0,
    "CustomerFrequency": 0,
    "CustomerTotalQty": 0,
    "CustomerPriceStd": 0.0
}

print("✓ Menangani Missing Values dengan nilai default:")
for col_name, value in fill_map.items():
    if col_name in df.columns:
        print(f"  - {col_name}: {value}")

for col_name, value in fill_map.items():
    if col_name in df.columns:
        df = df.fillna({col_name: value})

print("\n✓ Missing Values berhasil ditangani")
print()

print("\n[TAHAP 4.2.8] KONVERSI TIPE DATA NUMERIK")
print("-"*80)

# ─────────────────────────────────────────────
# FINAL CHECK: CAST ALL IMPORTANT NUMERIC
# ─────────────────────────────────────────────
numeric_cols = [
    "Quantity",
    "UnitPrice",
    "TotalPrice",
    "Hour",
    "Day",
    "MonthNum",
    "WeekdayNum",
    "CountryEncoded",
    "ProductFrequency",
    "AvgCustomerSpend",
    "CustomerFrequency",
    "CustomerTotalQty",
    "CustomerPriceStd"
]

print("✓ Mengkonversi tipe data ke Double:")
for c in numeric_cols:
    if c in df.columns:
        df = df.withColumn(c, col(c).cast("double"))
        print(f"  - {c}: ✓")

print("\nTipe data fitur setelah konversi:")
df.printSchema()
print()

print("\n[TAHAP 4.2.9] PENYIMPANAN DATASET HASIL FEATURE ENGINEERING")
print("-"*80)

# ─────────────────────────────────────────────
# SAVE TO PARQUET
# ─────────────────────────────────────────────
print("✓ Menyimpan dataset ke format Apache Parquet...")

df.write.mode("overwrite").parquet("data/featured_data")

print("✓ Dataset berhasil disimpan ke: data/featured_data")
print(f"✓ Jumlah baris: {df.count()}")
print(f"✓ Jumlah kolom: {len(df.columns)}")

# ─────────────────────────────────────────────
# EXPORT SAMPLE KE CSV UNTUK BUKTI VISUAL
# ─────────────────────────────────────────────
print("\n[TAMBAHAN] EXPORT SAMPLE DATA KE CSV UNTUK DOKUMENTASI")
print("-"*80)

# Pilih kolom penting untuk ditampilkan
important_cols = [
    "InvoiceNo",
    "StockCode",
    "Description",
    "Quantity",
    "InvoiceDate",
    "UnitPrice",
    "CustomerID",
    "Country",
    "TotalPrice",
    "Hour",
    "Day",
    "MonthNum",
    "RevenueCategory",
    "CountryEncoded",
    "ProductFrequency",
    "AvgCustomerSpend",
    "CustomerFrequency",
    "CustomerTotalQty"
]

# Filter kolom yang ada di dataframe
available_cols = [c for c in important_cols if c in df.columns]

# Ambil sample 100 baris untuk CSV
sample_df = df.select(available_cols).limit(100)

# Buat direktori jika belum ada
os.makedirs("data", exist_ok=True)

# Export ke CSV
sample_df.coalesce(1).write.mode("overwrite") \
    .option("header", "true") \
    .csv("data/featured_data_sample")

print("✓ Sample data (100 baris) berhasil di-export ke: data/featured_data_sample")
print("✓ File ini dapat dibuka di Excel untuk verifikasi manual")

print("\n" + "="*80)
print("FEATURE ENGINEERING SELESAI")
print("="*80)

spark.stop()
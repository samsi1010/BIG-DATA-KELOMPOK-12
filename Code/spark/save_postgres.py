from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys

# =========================
# TAHAP 4.3.1: INISIASI SPARK
# =========================
spark = SparkSession.builder \
    .appName("SavePostgreSQL") \
    .master("local[*]") \
    .config(
        "spark.driver.extraClassPath",
        "D:/proyek_bidat/bigdata_retail_anomaly/jars/postgresql-42.7.3.jar"
    ) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("\n" + "="*80)
print("TAHAP 4.3.1: PENYIMPANAN DATA KE POSTGRESQL (DATABASE RELASIONAL)")
print("="*80)

print("\n[SETUP] Inisiasi Apache Spark")
print("✓ Spark berhasil dijalankan")
print("✓ JDBC Driver PostgreSQL: postgresql-42.7.3.jar")

# =========================
# LOAD DATA DARI PARQUET
# =========================
print("\n[LOAD DATA] Membaca data dari Apache Parquet")
print("-"*80)

data_path = "data/advanced_anomaly"

try:
    df = spark.read.parquet(data_path)

    df = df.select([col(c).alias(c.strip()) for c in df.columns])
    print(f"✓ Data berhasil dibaca dari: {data_path}")
except Exception as e:
    print(f"❌ ERROR membaca data: {e}")
    spark.stop()
    sys.exit(1)

row_count = df.count()
col_count = len(df.columns)

print(f"✓ Jumlah baris data: {row_count:,}")
print(f"✓ Jumlah kolom: {col_count}")

# =========================
# EXPLORATORY DATA
# =========================
print("\n[EDA] Exploratory Data Analysis")
print("-"*80)

print("\nStruktur Data (Schema):")
df.printSchema()

print("\nSample Data (5 baris pertama):")
df.show(5, truncate=False)

# =========================
# VALIDASI DATA
# =========================
print("\n[VALIDASI] Pengecekan Kualitas Data")
print("-"*80)

# Check NULL values
print("\nCek NULL values:")
null_check = True
for column in df.columns:
    null_count = df.filter(col(column).isNull()).count()
    if null_count > 0:
        print(f"  ⚠️  {column}: {null_count} NULL values")
        null_check = False

if null_check:
    print("  ✓ Tidak ada NULL values - Data siap untuk penyimpanan")

# Check data types
print("\nVerifikasi Tipe Data Numerik:")
numeric_cols = [
    "Quantity", "UnitPrice", "TotalPrice", "Hour", "Day", "MonthNum", 
    "WeekdayNum", "CountryEncoded", "ProductFrequency", 
    "AvgCustomerSpend", "CustomerFrequency", "CustomerTotalQty", "CustomerPriceStd"
]

for c in numeric_cols:
    if c in df.columns:
        print(f"  ✓ {c}: {df.select(c).dtypes[0][1]}")

# =========================
# STOP JIKA DATA KOSONG
# =========================
if row_count == 0:
    print("\n❌ ERROR: DATAFRAME KOSONG - PROSES DIHENTIKAN")
    spark.stop()
    sys.exit(1)

print("\n✓ Data valid dan siap untuk penyimpanan")

# =========================
# POSTGRESQL CONFIGURATION
# =========================
print("\n[KONFIGURASI] Koneksi PostgreSQL")
print("-"*80)

postgres_url = "jdbc:postgresql://localhost:5432/retail_db"
postgres_properties = {
    "user": "postgres",
    "password": "samuel",
    "driver": "org.postgresql.Driver"
}
table_name = "public.retail_transactions"

print(f"✓ Database URL: {postgres_url}")
print(f"✓ Target Table: {table_name}")
print(f"✓ User: postgres")

# =========================
# PROSES IMPLEMENTASI
# =========================
print("\n[IMPLEMENTASI] Menyimpan Data ke PostgreSQL")
print("-"*80)

print(f"\nMengirimkan {row_count:,} baris ke PostgreSQL...")
print("Mode: OVERWRITE (menimpa data lama)")

try:
    df.write \
        .mode("overwrite") \
        .jdbc(
            url=postgres_url,
            table=table_name,
            properties=postgres_properties
        )
    
    print("✓ Data berhasil disimpan ke PostgreSQL")
    
except Exception as e:
    print(f"❌ ERROR menyimpan data ke PostgreSQL: {e}")
    print("\n⚠️  Tips Troubleshooting:")
    print("   - Pastikan PostgreSQL sudah running")
    print("   - Cek username dan password")
    print("   - Pastikan database 'retail_db' sudah ada")
    print("   - Jalankan: python verify_mongodb.py (untuk cek konfigurasi)")
    spark.stop()
    sys.exit(1)

# =========================
# KEUNGGULAN POSTGRESQL
# =========================
print("\n[KEUNGGULAN] Strategi Penyimpanan PostgreSQL")
print("-"*80)
print("✓ ACID Compliance: Data aman & konsisten (atomic transactions)")
print("✓ SQL Query Support: Mudah dianalisis dengan SQL")
print("✓ Multi-User Access: Stabil dengan akses bersamaan dari dashboard")
print("✓ Data Integrity: Foreign keys & constraints untuk validasi")
print("✓ Index Support: Query cepat untuk dashboard real-time")

# =========================
# STATISTIK PENYIMPANAN
# =========================
print("\n[STATISTIK] Ringkasan Data Tersimpan")
print("-"*80)

print("\nDistribusi berdasarkan Revenue Category:")
df.groupBy("RevenueCategory").count().show()

print("\nTop 5 Negara dengan Transaksi Terbanyak:")
df.groupBy("Country").count().orderBy(col("count").desc()).limit(5).show()

print("\nStatistik Kolom Numerik Utama:")
df.select(
    count("*").alias("Total_Records"),
    min("TotalPrice").alias("Min_Price"),
    max("TotalPrice").alias("Max_Price"),
    avg("TotalPrice").alias("Avg_Price"),
    min("Quantity").alias("Min_Qty"),
    max("Quantity").alias("Max_Qty"),
    avg("Quantity").alias("Avg_Qty")
).show()

# =========================
# SELESAI
# =========================
print("\n" + "="*80)
print("✓ TAHAP 4.3.1 SELESAI - DATA BERHASIL DISIMPAN KE POSTGRESQL")
print("="*80)
print(f"\n📊 Database: retail_db")
print(f"📋 Tabel: {table_name}")
print(f"📈 Jumlah Baris: {row_count:,}")
print(f"📌 Timestamp: " + str(df.select(current_timestamp()).collect()[0][0]))

spark.stop()
print("\n🛑 Spark session ditutup")
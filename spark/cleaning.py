from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os

spark = SparkSession.builder \
    .appName("RetailCleaning") \
    .getOrCreate()

print("\n" + "="*80)
print("TAHAP 4.1: PIPELINE DATA CLEANING DENGAN PYSPARK")
print("="*80)

print("\n[TAHAP 4.1.1] INISIASI SPARK DAN LOAD DATA")
print("-"*80)
print("✓ Apache Spark berhasil dijalankan")

df = spark.read.csv(
    "data/online_retail.csv.gz",
    header=True,
    inferSchema=True,
    sep=","
)

# PENTING: Bersihkan whitespace dari nama kolom!
df = df.select([col(c).alias(c.strip()) for c in df.columns])
               
print("✓ Dataset berhasil dibaca dari: data/online_retail.csv")

# Simpan count data awal
initial_count = df.count()
print(f"✓ Jumlah data awal: {initial_count} baris")

print("\n[TAHAP 4.1.2] EXPLORATORY DATA ANALYSIS (EDA)")
print("-"*80)

print("\nMENAMPILKAN SAMPLE DATA (5 baris pertama):")
df.show(5)

print("\nSTRUKTUR DATA (Schema):")
df.printSchema()

print("\nSTATISTIK QUANTITY DAN UNITPRICE:")

df.select(
    min("Quantity").alias("MinQuantity"),
    max("Quantity").alias("MaxQuantity"),
    min("UnitPrice").alias("MinUnitPrice"),
    max("UnitPrice").alias("MaxUnitPrice"),
    avg("Quantity").alias("AvgQuantity"),
    avg("UnitPrice").alias("AvgUnitPrice")
).show()

print("\nDISTRIBUSI TRANSAKSI BERDASARKAN NEGARA (Top 10):")

df.groupBy("Country") \
    .count() \
    .orderBy(col("count").desc()) \
    .show(10)

print("\n[TAHAP 4.1.3] PENANGANAN MISSING VALUES")
print("-"*80)

# Hitung missing values sebelum
print("Missing values SEBELUM cleaning:")
for column in df.columns:
    missing_count = df.filter(col(column).isNull()).count()
    if missing_count > 0:
        print(f"  - {column}: {missing_count}")

df = df.dropna()
after_dropna = df.count()
print(f"\n✓ Missing values berhasil dihapus")
print(f"✓ Jumlah data setelah dropna: {after_dropna} baris")
print(f"✓ Data yang dihapus: {initial_count - after_dropna} baris")

print("\n[TAHAP 4.1.4] PENGHAPUSAN DATA TIDAK VALID")
print("-"*80)

print("✓ Memfilter Quantity > 0 (menghapus retur/data tidak valid)...")
before_quantity = df.count()
df = df.filter(col("Quantity") > 0)
after_quantity = df.count()
print(f"  Data yang dihapus: {before_quantity - after_quantity} baris")

print("✓ Memfilter UnitPrice > 0 (memastikan harga valid)...")
before_price = df.count()
df = df.filter(col("UnitPrice") > 0)
after_price = df.count()
print(f"  Data yang dihapus: {before_price - after_price} baris")

print(f"\n✓ Jumlah data setelah filtering: {df.count()} baris")

print("\n[TAHAP 4.1.5] PERHITUNGAN NILAI TRANSAKSI")
print("-"*80)

df = df.withColumn(
    "TotalPrice",
    col("Quantity") * col("UnitPrice")
)

print("✓ Kolom TotalPrice berhasil dibuat")
print("  Formula: TotalPrice = Quantity × UnitPrice")

print("\nSample TotalPrice (10 baris):")
df.select("Quantity", "UnitPrice", "TotalPrice").limit(10).show()

print("\nStatistik TotalPrice:")
df.select(
    min("TotalPrice").alias("Min"),
    max("TotalPrice").alias("Max"),
    avg("TotalPrice").alias("Average"),
    stddev("TotalPrice").alias("StdDev")
).show()

print("\n[TAHAP 4.1.8] LAPORAN PERBANDINGAN SEBELUM-SESUDAH CLEANING")
print("-"*80)

final_count = df.count()
deleted_count = initial_count - final_count

print("\n╔════════════════════════════════════════════╗")
print("║        RINGKASAN PROSES DATA CLEANING      ║")
print("╠════════════════════════════════════════════╣")
print(f"║ Data Awal                  : {initial_count:>20} ║")
print(f"║ Data Setelah Cleaning      : {final_count:>20} ║")
print(f"║ Total Data Dihapus         : {deleted_count:>20} ║")
print(f"║ Persentase Data Tersisa    : {(final_count/initial_count*100):>19.1f}% ║")
print("╚════════════════════════════════════════════╝")

print("\n[TAHAP 4.1.9] EXPORT DATASET KE APACHE PARQUET")
print("-"*80)

print("✓ Menyimpan dataset ke format Apache Parquet...")

df.write.mode("overwrite").parquet(
    "data/cleaned_data"
)

print("✓ Dataset berhasil disimpan ke: data/cleaned_data")
print(f"✓ Jumlah baris akhir: {df.count()}")
print(f"✓ Jumlah kolom: {len(df.columns)}")

print("\n" + "="*80)
print("TAHAP 4.1 DATA CLEANING SELESAI")
print("="*80 + "\n")

spark.stop()
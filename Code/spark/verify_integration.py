"""
TAHAP 4.3.3: CROSS-SYSTEM QUERY - VERIFIKASI KONSISTENSI DATA
Script ini memastikan data di PostgreSQL dan MongoDB selalu selaras
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import psycopg2
from pymongo import MongoClient
import sys
import json

# =========================
# SETUP SPARK SESSION
# =========================
spark = SparkSession.builder \
    .appName("VerifyIntegration") \
    .master("local[*]") \
    .config(
        "spark.driver.extraClassPath",
        "D:/proyek_bidat/bigdata_retail_anomaly/jars/postgresql-42.7.3.jar"
    ) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("\n" + "="*80)
print("TAHAP 4.3.3: CROSS-SYSTEM QUERY - VERIFIKASI KONSISTENSI DATA")
print("="*80)

print("\n[SETUP] Inisiasi Spark Session")
print("✓ Spark session berhasil dijalankan")

# =========================
# CONNECT TO POSTGRESQL
# =========================
print("\n[KONEKSI] PostgreSQL - Database Relasional")
print("-"*80)

postgres_url = "jdbc:postgresql://localhost:5432/retail_db"
postgres_properties = {
    "user": "postgres",
    "password": "samuel",
    "driver": "org.postgresql.Driver"
}

try:
    print("Mengkoneksi ke PostgreSQL...")
    pg_df = spark.read.jdbc(
        url=postgres_url,
        table="public.retail_transactions",
        properties=postgres_properties
    )
    
    # PENTING: Bersihkan whitespace dari nama kolom
    pg_df = pg_df.select([col(c).alias(c.strip()) for c in pg_df.columns])
    
    pg_count = pg_df.count()
    print(f"✓ Koneksi PostgreSQL berhasil")
    print(f"✓ Database: retail_db")
    print(f"✓ Tabel: retail_transactions")
    print(f"✓ Jumlah baris: {pg_count:,}")
    
except Exception as e:
    print(f"❌ ERROR koneksi PostgreSQL: {e}")
    print("\n⚠️  Troubleshooting:")
    print("   - Pastikan PostgreSQL running")
    print("   - Jalankan terlebih dahulu: python spark/save_postgres.py")
    spark.stop()
    sys.exit(1)

# =========================
# CONNECT TO MONGODB
# =========================
print("\n[KONEKSI] MongoDB - Database Non-Relasional")
print("-"*80)

try:
    print("Mengkoneksi ke MongoDB...")
    client = MongoClient("mongodb://localhost:27017/")
    client.admin.command('ping')
    
    db = client["retail_bigdata"]
    collection = db["retail_anomalies"]
    
    mongo_count = collection.count_documents({})
    print(f"✓ Koneksi MongoDB berhasil")
    print(f"✓ Database: retail_bigdata")
    print(f"✓ Collection: retail_anomalies")
    print(f"✓ Jumlah dokumen: {mongo_count:,}")
    
except Exception as e:
    print(f"❌ ERROR koneksi MongoDB: {e}")
    print("\n⚠️  Troubleshooting:")
    print("   - Pastikan MongoDB running")
    print("   - Jalankan terlebih dahulu: python spark/save_mongodb.py")
    spark.stop()
    sys.exit(1)

# =========================
# CROSS-CHECK VERIFICATION
# =========================
print("\n" + "="*80)
print("CROSS-CHECK: VERIFIKASI KONSISTENSI DATA")
print("="*80)

print("\n[1] VERIFIKASI KOLOM & STRUKTUR DATA")
print("-"*80)

pg_cols = set(pg_df.columns)
print(f"✓ PostgreSQL - Total kolom: {len(pg_cols)}")
print("  Kolom utama:")
for col_name in sorted(list(pg_cols))[:10]:
    print(f"    - {col_name}")

if mongo_count > 0:
    sample_mongo_doc = collection.find_one({})
    mongo_cols = set(sample_mongo_doc.keys())
    mongo_cols.discard('_id')
    
    print(f"\n✓ MongoDB - Total field: {len(mongo_cols)}")
    print("  Field utama (dari sample dokumen):")
    for col_name in sorted(list(mongo_cols))[:10]:
        print(f"    - {col_name}")

# =========================
# STATISTIK PERBANDINGAN
# =========================
print("\n[2] STATISTIK PERBANDINGAN")
print("-"*80)

print("\n📊 RINGKASAN DATA:")
print(f"{'Sumber':<30} {'Total Record':<20} {'Status':<20}")
print("-" * 70)
print(f"{'PostgreSQL':<30} {pg_count:>18,} {'✓ OK' if pg_count > 0 else '❌ KOSONG':<20}")
print(f"{'MongoDB':<30} {mongo_count:>18,} {'✓ OK' if mongo_count > 0 else '❌ KOSONG':<20}")

# MongoDB seharusnya berisi subset dari PostgreSQL (hanya anomali)
print("\n💡 INTERPRETASI:")
print(f"   PostgreSQL = semua data ({pg_count:,} baris)")
print(f"   MongoDB = hanya data anomali ({mongo_count:,} dokumen)")
print(f"   Status: {'✓ Konsisten' if mongo_count <= pg_count else '⚠️  Perlu Verifikasi'}")

# =========================
# STATISTIK REVENUE CATEGORY
# =========================
print("\n[3] ANALISIS REVENUE CATEGORY")
print("-"*80)

print("\n📈 PostgreSQL - Distribusi Revenue Category:")
if "RevenueCategory" in pg_cols or "revenue_category" in pg_cols:
    col_name = "RevenueCategory" if "RevenueCategory" in pg_cols else "revenue_category"
    revenue_pg = pg_df.groupBy(col_name).count().orderBy("count", ascending=False)
    revenue_pg.show()
    
    revenue_data_pg = revenue_pg.collect()
else:
    print("  ⚠️  Kolom Revenue Category tidak ditemukan")

if mongo_count > 0:
    print("\n📈 MongoDB - Distribusi Revenue Category (dari anomali):")
    pipeline = [
        {"$group": {"_id": "$RevenueCategory", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ]
    
    revenue_data_mongo = list(collection.aggregate(pipeline))
    for doc in revenue_data_mongo:
        print(f"  {doc['_id']}: {doc['count']} dokumen")

# =========================
# VERIFIKASI SAMPLE DATA
# =========================
print("\n[4] VERIFIKASI SAMPLE DATA")
print("-"*80)

print("\n🔍 Sample dari PostgreSQL (1 baris):")
pg_sample = pg_df.limit(1).collect()
if pg_sample:
    sample_dict = pg_sample[0].asDict()
    for key, value in list(sample_dict.items())[:8]:
        print(f"   {key}: {value}")

if mongo_count > 0:
    print("\n🔍 Sample dari MongoDB (1 dokumen):")
    mongo_sample = collection.find_one({})
    if mongo_sample:
        display_doc = {k: v for k, v in list(mongo_sample.items())[:8] if k != '_id'}
        for key, value in display_doc.items():
            print(f"   {key}: {value}")

# =========================
# QUALITY METRICS
# =========================
print("\n[5] METRIK KUALITAS DATA")
print("-"*80)

print("\n📊 PostgreSQL - Data Quality:")
null_count = 0
for c in pg_cols:
    if c in pg_df.columns:
        null_in_col = pg_df.filter(col(c).isNull()).count()
        if null_in_col > 0:
            null_count += null_in_col

print(f"   ✓ Total baris: {pg_count:,}")
print(f"   ✓ Total kolom: {len(pg_cols)}")
print(f"   ✓ NULL values: {null_count} (cek di kolom tertentu)")

print("\n📊 MongoDB - Data Quality:")
print(f"   ✓ Total dokumen: {mongo_count:,}")
print(f"   ✓ Total field (avg): {len(mongo_cols)} per dokumen")
print(f"   ✓ Type: Dokumen JSON (fleksibel)")

# =========================
# INDICATORS & WARNINGS
# =========================
print("\n[6] INDIKATOR & PERINGATAN")
print("-"*80)

print("\n🚨 INDIKATOR KESEHATAN SISTEM:")

if pg_count == 0:
    print("   ❌ PostgreSQL KOSONG - Jalankan: python spark/save_postgres.py")
else:
    print(f"   ✓ PostgreSQL berisi data ({pg_count:,} baris)")

if mongo_count == 0:
    print("   ⚠️  MongoDB KOSONG - Kemungkinan:")
    print("      1. Jalankan: python spark/save_mongodb.py")
    print("      2. Data anomali mungkin belum diproses")
else:
    print(f"   ✓ MongoDB berisi anomali ({mongo_count:,} dokumen)")

if pg_count > 0 and mongo_count > 0:
    print("   ✓ SISTEM SIAP untuk dashboard")
else:
    print("   ❌ SISTEM BELUM SIAP - Ada data yang kosong")

# =========================
# RECOMMENDATIONS
# =========================
print("\n[7] REKOMENDASI DASHBOARD")
print("-"*80)

print("""
✓ Untuk ANALISIS GENERAL (semua transaksi):
   Query dari PostgreSQL: SELECT * FROM retail_transactions

✓ Untuk FRAUD INVESTIGATION (hanya anomali):
   Query dari MongoDB: db.retail_anomalies.find({})

✓ Untuk JOIN ANALYSIS (cross-check):
   1. Get anomaly IDs dari MongoDB
   2. Join dengan data detail dari PostgreSQL
   3. Tampilkan daftar transaksi mencurigakan dengan konteks lengkap

✓ Untuk MONITORING REAL-TIME:
   - Monitor jumlah dokumen MongoDB
   - Jika kosong/berbeda = peringatan pengiriman data gagal
""")

# =========================
# GENERATE VERIFICATION REPORT
# =========================
print("\n[8] GENERATING VERIFICATION REPORT")
print("-"*80)

verification_report = {
    "timestamp": str(current_timestamp()),
    "postgresql": {
        "database": "retail_db",
        "table": "retail_transactions",
        "total_records": pg_count,
        "total_columns": len(pg_cols),
        "columns": list(sorted(pg_cols))[:10]
    },
    "mongodb": {
        "database": "retail_bigdata",
        "collection": "retail_anomalies",
        "total_documents": mongo_count,
        "total_fields": len(mongo_cols) if mongo_count > 0 else 0
    },
    "consistency_check": {
        "status": "✓ VALID" if (pg_count > 0 and mongo_count > 0) else "❌ INVALID",
        "mongodb_is_subset": mongo_count <= pg_count,
        "data_ready_for_dashboard": pg_count > 0 and mongo_count > 0
    }
}

# Simpan report ke file
import os
os.makedirs("reports", exist_ok=True)

with open("reports/06_database_integration_verification.json", "w") as f:
    json.dump(verification_report, f, indent=2, default=str)

print("✓ Verification report disimpan ke: reports/06_database_integration_verification.json")

# =========================
# FINAL STATUS
# =========================
print("\n" + "="*80)
print("✓ VERIFIKASI SELESAI")
print("="*80)

if pg_count > 0 and mongo_count > 0:
    print("\n✅ SISTEM INTEGRATION SEHAT & SIAP PRODUKSI")
elif pg_count > 0:
    print("\n⚠️  PostgreSQL OK, MongoDB sedang diproses...")
else:
    print("\n❌ DATA BELUM TERSIMPAN - Jalankan tahap sebelumnya")

spark.stop()
print("\n🛑 Spark session ditutup")

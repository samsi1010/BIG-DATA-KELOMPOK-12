from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pymongo import MongoClient
import json
import sys

# =========================
# TAHAP 4.3.2: INISIASI SPARK
# =========================
spark = SparkSession.builder \
    .appName("SaveMongoDB_Anomaly") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("\n" + "="*80)
print("TAHAP 4.3.2: PENYIMPANAN DATA ANOMALI KE MONGODB (DATABASE NON-RELASIONAL)")
print("="*80)

print("\n[SETUP] Inisiasi Apache Spark")
print("✓ Spark berhasil dijalankan")

# =========================
# LOAD DATA DARI PARQUET
# =========================
print("\n[LOAD DATA] Membaca data dari Apache Parquet")
print("-"*80)

data_path = "data/advanced_anomaly"

try:
    df = spark.read.parquet(data_path)
    # PENTING: Bersihkan whitespace dari nama kolom
    df = df.select([col(c).alias(c.strip()) for c in df.columns])
    total_data = df.count()
    print(f"✓ Data berhasil dibaca dari: {data_path}")
    print(f"✓ Total data seluruhnya: {total_data:,} baris")
except Exception as e:
    print(f"❌ ERROR membaca data: {e}")
    spark.stop()
    sys.exit(1)

# =========================
# CEK KOLOM ANOMALY
# =========================
print("\n[VALIDASI] Pengecekan Kolom Data")
print("-"*80)

print(f"✓ Total kolom: {len(df.columns)}")
print("\nDaftar kolom:")
for i, col_name in enumerate(df.columns, 1):
    print(f"  {i}. {col_name}")

# Cek keberadaan kolom 'anomaly'
if "anomaly" not in df.columns:
    print("\n❌ ERROR: Kolom 'anomaly' TIDAK DITEMUKAN")
    print("⚠️  Pastikan advanced_anomaly.py sudah dijalankan terlebih dahulu")
    spark.stop()
    sys.exit(1)

print("\n✓ Kolom 'anomaly' ditemukan")

# =========================
# FILTER DATA ANOMALI
# =========================
print("\n[FILTER] Memilih Hanya Data Anomali")
print("-"*80)

print("\nMeng-filter data dengan kondisi: anomaly == 1")

anomaly_df = df.filter(col("anomaly") == 1)
anomaly_count = anomaly_df.count()

print(f"✓ Total data anomali: {anomaly_count:,} baris")
print(f"✓ Persentase anomali: {(anomaly_count/total_data*100):.2f}%")

print("\nSample Data Anomali (5 baris pertama):")
anomaly_df.select(
    "InvoiceNo", "CustomerID", "TotalPrice", "RevenueCategory", "anomaly"
).show(5, truncate=False)

# =========================
# STOP JIKA DATA KOSONG
# =========================
if anomaly_count == 0:
    print("\n❌ ERROR: Tidak ada data anomali")
    print("⚠️  Kemungkinan:")
    print("   - advanced_anomaly.py belum dijalankan")
    print("   - Model Isolation Forest belum memberikan label anomali")
    spark.stop()
    sys.exit(1)

# =========================
# MONGODB CONNECTION
# =========================
print("\n[KONFIGURASI] Koneksi MongoDB")
print("-"*80)

try:
    client = MongoClient("mongodb://localhost:27017/")
    db = client["retail_bigdata"]
    collection = db["retail_anomalies"]
    
    # Test connection
    client.admin.command('ping')
    print("✓ Koneksi MongoDB berhasil")
    print(f"✓ Database: retail_bigdata")
    print(f"✓ Collection: retail_anomalies")
    
except Exception as e:
    print(f"❌ ERROR koneksi MongoDB: {e}")
    print("\n⚠️  Tips Troubleshooting:")
    print("   - Pastikan MongoDB sudah running")
    print("   - Default: mongodb://localhost:27017/")
    print("   - Jalankan: python verify_mongodb.py (untuk cek konfigurasi)")
    spark.stop()
    sys.exit(1)

# =========================
# HAPUS DATA LAMA
# =========================
print("\n[PREPARASI] Membersihkan Data Lama")
print("-"*80)

old_count = collection.count_documents({})
print(f"Data lama di MongoDB: {old_count:,} dokumen")

collection.delete_many({})
print("✓ Data lama berhasil dihapus")

# =========================
# INSERT DATA ANOMALI
# =========================
print("\n[IMPLEMENTASI] Menyimpan Data Anomali ke MongoDB")
print("-"*80)

print(f"\nMengirimkan {anomaly_count:,} dokumen anomali ke MongoDB...")
print("Strategi: Batch insert (5000 dokumen per batch)")

batch_size = 5000
buffer = []
batch_count = 0

try:
    for row in anomaly_df.toLocalIterator():
        buffer.append(row.asDict())
        
        if len(buffer) >= batch_size:
            collection.insert_many(buffer)
            batch_count += 1
            print(f"  ✓ Batch {batch_count} ({len(buffer)} dokumen) berhasil disimpan")
            buffer.clear()
    
    # Insert data yang tersisa
    if buffer:
        collection.insert_many(buffer)
        batch_count += 1
        print(f"  ✓ Batch {batch_count} ({len(buffer)} dokumen) berhasil disimpan")
    
    print("\n✓ Semua data anomali berhasil disimpan ke MongoDB")
    
except Exception as e:
    print(f"\n❌ ERROR menyimpan data: {e}")
    spark.stop()
    sys.exit(1)

# =========================
# VALIDASI HASIL
# =========================
print("\n[VALIDASI] Verifikasi Data di MongoDB")
print("-"*80)

total_docs = collection.count_documents({})
print(f"✓ Total dokumen di collection: {total_docs:,}")
print(f"✓ Verifikasi: Data yang dikirim = Data yang disimpan → {anomaly_count == total_docs}")

# Statistik dokumen
print("\nStatistik Dokumen Anomali:")
anomaly_types = collection.aggregate([
    {"$group": {"_id": "$RevenueCategory", "count": {"$sum": 1}}},
    {"$sort": {"count": -1}}
])

print("  Revenue Category Distribution:")
for doc in anomaly_types:
    print(f"    - {doc['_id']}: {doc['count']} dokumen")

# Sample dokumen
print("\nSample Dokumen MongoDB (1 dokumen):")
sample_doc = collection.find_one({})
if sample_doc:
    # Remove _id untuk display yang lebih rapi
    display_doc = {k: v for k, v in sample_doc.items() if k != '_id'}
    print(json.dumps(display_doc, indent=2, default=str))

# =========================
# KEUNGGULAN MONGODB
# =========================
print("\n[KEUNGGULAN] Strategi Penyimpanan MongoDB")
print("-"*80)
print("✓ Flexible Schema: Mudah menambah field baru tanpa merusak data lain")
print("✓ Document-Oriented: Menyimpan data dalam format JSON-like")
print("✓ Nested Data Support: Cocok untuk data kompleks & hierarki")
print("✓ Scalability: Mudah di-scale untuk volume data besar")
print("✓ Fraud Investigation: Memudahkan fraud analyst menambah catatan investigasi")

# =========================
# SELESAI
# =========================
print("\n" + "="*80)
print("✓ TAHAP 4.3.2 SELESAI - DATA ANOMALI BERHASIL DISIMPAN KE MONGODB")
print("="*80)
print(f"\n📊 Database: retail_bigdata")
print(f"📋 Collection: retail_anomalies")
print(f"📈 Jumlah Dokumen: {total_docs:,}")
print(f"⚠️  Tipe Data: Anomali (anomaly flag = 1)")

spark.stop()
print("\n🛑 Spark session ditutup")
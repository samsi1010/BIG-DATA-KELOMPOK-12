"""
Script untuk menghasilkan OUTPUT REPORT dari Database Integration
Untuk dokumentasi laporan Tahap 4.3
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import psycopg2
from pymongo import MongoClient
import pandas as pd
import json
import os
import sys

spark = SparkSession.builder \
    .appName("DBIntegrationReport") \
    .master("local[*]") \
    .config(
        "spark.driver.extraClassPath",
        "D:/proyek_bidat/bigdata_retail_anomaly/jars/postgresql-42.7.3.jar"
    ) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("\n" + "="*80)
print("GENERATING DATABASE INTEGRATION OUTPUT REPORT")
print("="*80)

os.makedirs("reports", exist_ok=True)

# =========================
# 1. POSTGRESQL SAMPLE DATA
# =========================
print("\n[1/4] Extracting PostgreSQL Sample Data...")
print("-"*80)

postgres_url = "jdbc:postgresql://localhost:5432/retail_db"
postgres_properties = {
    "user": "postgres",
    "password": "samuel",
    "driver": "org.postgresql.Driver"
}

try:
    pg_df = spark.read.jdbc(
        url=postgres_url,
        table="public.retail_transactions",
        properties=postgres_properties
    )
    
    # PENTING: Bersihkan whitespace dari nama kolom
    pg_df = pg_df.select([col(c).alias(c.strip()) for c in pg_df.columns])
    
    pg_count = pg_df.count()
    print(f"✓ Total records di PostgreSQL: {pg_count:,}")
    
    # Export sample 50 baris
    pg_sample = pg_df.limit(50).toPandas()
    pg_sample.to_csv("reports/07_postgresql_sample_data.csv", index=False)
    print(f"✓ Exported: reports/07_postgresql_sample_data.csv (50 baris)")
    
except Exception as e:
    print(f"❌ ERROR: {e}")
    pg_count = 0

# =========================
# 2. MONGODB ANOMALY STATISTICS
# =========================
print("\n[2/4] Extracting MongoDB Anomaly Statistics...")
print("-"*80)

try:
    client = MongoClient("mongodb://localhost:27017/")
    db = client["retail_bigdata"]
    collection = db["retail_anomalies"]
    
    mongo_count = collection.count_documents({})
    print(f"✓ Total anomali di MongoDB: {mongo_count:,}")
    
    # Ambil sample dokumen
    mongo_samples = list(collection.find({}).limit(50))
    mongo_sample_df = pd.DataFrame(mongo_samples)
    
    # Remove _id column jika ada
    if '_id' in mongo_sample_df.columns:
        mongo_sample_df = mongo_sample_df.drop('_id', axis=1)
    
    mongo_sample_df.to_csv("reports/08_mongodb_anomaly_sample.csv", index=False)
    print(f"✓ Exported: reports/08_mongodb_anomaly_sample.csv (50 dokumen)")
    
    # Revenue category distribution dari MongoDB
    revenue_dist = list(collection.aggregate([
        {"$group": {"_id": "$RevenueCategory", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ]))
    
    revenue_dist_df = pd.DataFrame(revenue_dist)
    revenue_dist_df.columns = ["RevenueCategory", "AnomalyCount"]
    revenue_dist_df.to_csv("reports/09_mongodb_revenue_distribution.csv", index=False)
    print(f"✓ Exported: reports/09_mongodb_revenue_distribution.csv")
    
except Exception as e:
    print(f"❌ ERROR: {e}")
    mongo_count = 0

# =========================
# 3. INTEGRATION STATISTICS
# =========================
print("\n[3/4] Generating Integration Statistics...")
print("-"*80)

stats = {
    "PostgreSQL_Total_Records": pg_count,
    "MongoDB_Total_Documents": mongo_count,
    "Anomaly_Percentage": f"{(mongo_count/pg_count*100):.2f}%" if pg_count > 0 else "0%",
    "Integration_Status": "✓ VALID" if (pg_count > 0 and mongo_count > 0) else "❌ INVALID"
}

stats_df = pd.DataFrame([stats])
stats_df.to_csv("reports/10_integration_statistics.csv", index=False)
print("✓ Exported: reports/10_integration_statistics.csv")

print("\n📊 RINGKASAN STATISTIK:")
for key, value in stats.items():
    print(f"   {key}: {value}")

# =========================
# 4. SCHEMA INFORMATION
# =========================
print("\n[4/4] Documenting Database Schema...")
print("-"*80)

schema_info = {
    "PostgreSQL": {
        "Database": "retail_db",
        "Table": "retail_transactions",
        "Purpose": "Menyimpan semua data transaksi retail yang sudah diproses",
        "Type": "Relasional (ACID Compliance)",
        "Total_Records": pg_count,
        "Key_Features": [
            "ACID Compliance untuk data financial",
            "SQL Query support untuk analysis",
            "Multi-user access support",
            "Index untuk performa query"
        ]
    },
    "MongoDB": {
        "Database": "retail_bigdata",
        "Collection": "retail_anomalies",
        "Purpose": "Menyimpan data transaksi anomali untuk fraud investigation",
        "Type": "Non-Relasional (NoSQL, Document-oriented)",
        "Total_Documents": mongo_count,
        "Key_Features": [
            "Flexible schema untuk penambahan field",
            "Document-oriented format (JSON-like)",
            "Support nested data struktur",
            "Mudah di-scale untuk big data"
        ]
    }
}

with open("reports/11_database_schema_documentation.json", "w") as f:
    json.dump(schema_info, f, indent=2)

print("✓ Exported: reports/11_database_schema_documentation.json")

# =========================
# 5. COMPARISON TABLE
# =========================
print("\n[5/4] Creating Comparison Table...")
print("-"*80)

comparison_data = {
    "Aspek": [
        "Jenis Database",
        "Total Data",
        "Tujuan",
        "Format Penyimpanan",
        "Query Language",
        "Data Integrity",
        "Skalabilitas",
        "Schema",
        "Use Case"
    ],
    "PostgreSQL": [
        "Relasional (SQL)",
        f"{pg_count:,} baris",
        "Data warehouse utama",
        "Tabel (rows & columns)",
        "SQL",
        "ACID Compliance",
        "Vertikal (power)",
        "Fixed schema",
        "Analytics & General Query"
    ],
    "MongoDB": [
        "Non-Relasional (NoSQL)",
        f"{mongo_count:,} dokumen",
        "Anomali untuk investigasi",
        "Dokumen (JSON-like)",
        "MongoDB Query Language",
        "Eventual Consistency",
        "Horizontal (scale-out)",
        "Flexible schema",
        "Fraud Investigation & Complex Data"
    ]
}

comparison_df = pd.DataFrame(comparison_data)
comparison_df.to_csv("reports/12_postgresql_vs_mongodb_comparison.csv", index=False)
print("✓ Exported: reports/12_postgresql_vs_mongodb_comparison.csv")

# =========================
# SUMMARY
# =========================
print("\n" + "="*80)
print("✓ DATABASE INTEGRATION REPORT GENERATION COMPLETE")
print("="*80)

print("""
📁 Output Files di folder reports/:
   ├─ 07_postgresql_sample_data.csv (50 baris sample)
   ├─ 08_mongodb_anomaly_sample.csv (50 dokumen sample)
   ├─ 09_mongodb_revenue_distribution.csv (anomali per kategori)
   ├─ 10_integration_statistics.csv (ringkasan statistik)
   ├─ 11_database_schema_documentation.json (dokumentasi schema)
   └─ 12_postgresql_vs_mongodb_comparison.csv (perbandingan)

📊 STATISTIK UTAMA:
   • PostgreSQL: {pg_count:,} baris
   • MongoDB: {mongo_count:,} dokumen
   • Status: {'✓ SIAP PRODUKSI' if (pg_count > 0 and mongo_count > 0) else '⚠️  Sedang diproses'}
""")

spark.stop()
print("\n🛑 Spark session ditutup")

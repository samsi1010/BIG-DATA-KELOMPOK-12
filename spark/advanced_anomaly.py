from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from sklearn.ensemble import IsolationForest
import pandas as pd
import numpy as np

spark = SparkSession.builder \
    .appName("AdvancedAnomalyDetection") \
    .getOrCreate()

print("MEMBACA FEATURED DATA")

# =========================
# READ DATA
# =========================

df = spark.read.parquet(
    "data/featured_data"
)

# PENTING: Bersihkan whitespace dari nama kolom
df = df.select([col(c).alias(c.strip()) for c in df.columns])

print("🔍 KOLOM TERSEDIA DI FEATURED DATA:")
df.printSchema()

# =========================
# CONVERT TO PANDAS UNTUK ANOMALY DETECTION
# =========================

pdf = df.toPandas()

print("JUMLAH DATA SEBELUM DROPNA:")
print(len(pdf))
print("KOLUMN:", pdf.columns.tolist())
print("NULL COUNT PER KOLOM:\n", pdf.isnull().sum().loc[lambda x: x > 0])

# Hapus null hanya pada fitur model yang dipakai oleh IsolationForest
model_features = ["Quantity", "UnitPrice", "TotalPrice"]
pdf = pdf.dropna(subset=model_features)

print("JUMLAH DATA SETELAH DROPNA SUBSET:")
print(len(pdf))

# =========================
# FEATURES UNTUK ANOMALY DETECTION 
# =========================

features = pdf[
    [
        "Quantity",
        "UnitPrice",
        "TotalPrice"
    ]
]

print("MENJALANKAN ISOLATION FOREST")

# =========================
# ISOLATION FOREST MODEL
# =========================

model = IsolationForest(
    contamination=0.02,
    random_state=42,
    n_estimators=100
)

# =========================
# FIT MODEL
# =========================

model.fit(features)

# =========================
# PREDICT ANOMALY
# =========================

predictions = model.predict(features)

pdf["anomaly"] = np.where(
    predictions == -1,
    1,
    0
)

# =========================
# ANOMALY SCORE
# =========================

pdf["anomaly_score"] = model.decision_function(
    features
)

# =========================
# EXPLAINABLE ANOMALY
# =========================

pdf["anomaly_reason"] = "Normal"

pdf.loc[
    pdf["Quantity"] > 100,
    "anomaly_reason"
] = "Large Quantity"

pdf.loc[
    pdf["TotalPrice"] > 1000,
    "anomaly_reason"
] = "High Transaction"

pdf.loc[
    pdf["UnitPrice"] > 100,
    "anomaly_reason"
] = "Expensive Product"

# =========================
# ANOMALY SUMMARY
# =========================

total_data = len(pdf)

total_anomaly = len(
    pdf[pdf["anomaly"] == 1]
)

anomaly_percentage = round(
    (total_anomaly / total_data) * 100,
    2
)

print("TOTAL DATA:")
print(total_data)

print("TOTAL ANOMALY:")
print(total_anomaly)

print("ANOMALY PERCENTAGE:")
print(f"{anomaly_percentage}%")

# =========================
# CONVERT BACK TO SPARK (PRESERVE ORIGINAL SCHEMA)
# =========================

print("MENYIMPAN PARQUET DARI PANDAS")

# Save directly ke parquet dengan pandas (preserve types lebih baik)
import os
import shutil

# Cast timestamp ke string untuk compatibility dengan Spark
for col in pdf.columns:
    if pdf[col].dtype == 'datetime64[ns]':
        pdf[col] = pdf[col].astype(str)

output_path = "data/advanced_anomaly"

# Hapus folder lama jika ada
if os.path.exists(output_path):
    shutil.rmtree(output_path)

# Create folder baru
os.makedirs(output_path, exist_ok=True)

# Save dengan pyarrow engine untuk preserve types
pdf.to_parquet(
    f"{output_path}/part-00000.parquet",
    engine='pyarrow',
    index=False
)

print("ADVANCED ANOMALY DETECTION SELESAI")

spark.stop()
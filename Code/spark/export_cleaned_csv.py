from pyspark.sql import SparkSession
import os
import shutil

spark = SparkSession.builder \
    .appName("ExportCleanedCSV") \
    .getOrCreate()

print("=" * 80)
print("EXPORT CLEANED DATASET TO CSV")
print("=" * 80)

# ===================================
# BACA HASIL CLEANING
# ===================================

df = spark.read.parquet(
    "data/cleaned_data"
)

total_rows = df.count()

print(f"Jumlah data bersih : {total_rows:,}")

# ===================================
# EXPORT CSV
# ===================================

temp_folder = "data/temp_cleaned_csv"

df.coalesce(1) \
    .write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(temp_folder)

# ===================================
# RENAME FILE CSV
# ===================================

final_csv = "data/cleaned_retail.csv"

if os.path.exists(final_csv):
    os.remove(final_csv)

for file in os.listdir(temp_folder):

    if file.endswith(".csv"):

        shutil.copy(
            os.path.join(temp_folder, file),
            final_csv
        )

        break

print(f"CSV berhasil dibuat:")
print(final_csv)

print(f"Total baris: {total_rows:,}")

# ===================================
# HAPUS FOLDER TEMP
# ===================================

shutil.rmtree(temp_folder)

print("Export selesai")

spark.stop()
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, count, desc

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("RetailAnalysis") \
    .getOrCreate()

# load hasil cleaning
df = spark.read.option("header", True).csv("output/cleaned_data")

df = df.withColumn("TotalPrice", df["TotalPrice"].cast("double"))

# =========================
# TOTAL PENJUALAN
# =========================
df.groupBy("Country") \
  .agg(sum("TotalPrice").alias("TotalRevenue")) \
  .orderBy(desc("TotalRevenue")) \
  .show(10)

# =========================
# PRODUK TERLARIS
# =========================
df.groupBy("Description") \
  .agg(count("*").alias("TotalSold")) \
  .orderBy(desc("TotalSold")) \
  .show(10)

spark.stop()
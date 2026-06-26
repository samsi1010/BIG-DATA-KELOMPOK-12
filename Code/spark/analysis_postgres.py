from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, count, col
import os

# =========================
# ENVIRONMENT
# =========================
os.environ["JAVA_HOME"] = "C:\\Program Files\\Eclipse Adoptium\\jdk-17.0.19.10-hotspot"
os.environ["HADOOP_HOME"] = "C:\\hadoop-3.3.6"

print("STARTING ANALYSIS FROM POSTGRESQL...")

# =========================
# JDBC DRIVER PATH
# =========================
jdbc_path = "file:///D:/proyek_bidat/bigdata_retail_anomaly/jars/postgresql-42.7.3.jar"

# =========================
# SPARK SESSION
# =========================
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("AnalysisPostgres") \
    .config("spark.jars", jdbc_path) \
    .getOrCreate()

print("SPARK BERHASIL DIJALANKAN")

# =========================
# READ DATA FROM POSTGRESQL
# =========================
df = spark.read \
    .format("jdbc") \
    .option(
        "url",
        "jdbc:postgresql://localhost:5432/retaildb"
    ) \
    .option(
        "dbtable",
        "transactions"
    ) \
    .option(
        "user",
        "postgres"
    ) \
    .option(
        "password",
        "samuel"
    ) \
    .option(
        "driver",
        "org.postgresql.Driver"
    ) \
    .load()

print("DATA BERHASIL DIBACA DARI POSTGRESQL")

# =========================
# SHOW DATA
# =========================
df.show(5)

# =========================
# ANALISIS TOTAL PENJUALAN
# =========================
print("TOTAL PENJUALAN PER NEGARA")

sales_country = df.groupBy("Country") \
    .agg(
        sum("TotalPrice").alias("TotalSales")
    ) \
    .orderBy(col("TotalSales").desc())

sales_country.show()

# =========================
# ANALISIS PRODUK TERLARIS
# =========================
print("PRODUK TERLARIS")

best_product = df.groupBy("Description") \
    .agg(
        sum("Quantity").alias("TotalQty")
    ) \
    .orderBy(col("TotalQty").desc())

best_product.show(10)

# =========================
# STOP SPARK
# =========================
spark.stop()

print("SPARK STOPPED")
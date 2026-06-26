from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id

# =========================
# SPARK SESSION
# =========================

spark = SparkSession.builder \
    .appName("JoinAnalysis") \
    .config(
        "spark.jars",
        ",".join([
            "jars/postgresql-42.7.3.jar",
            "jars/mongo-spark-connector_2.12-10.3.0.jar",
            "jars/mongodb-driver-sync-5.1.0.jar",
            "jars/mongodb-driver-core-5.1.2.jar",
            "jars/bson-5.1.2.jar"
        ])
    ) \
    .config(
        "spark.mongodb.read.connection.uri",
        "mongodb://127.0.0.1:27017/retail_db.retail_anomalies"
    ) \
    .getOrCreate()

print("SPARK BERHASIL DIJALANKAN")

# =========================
# READ POSTGRESQL
# =========================

postgres_df = spark.read \
    .format("jdbc") \
    .option(
        "url",
        "jdbc:postgresql://localhost:5432/retail_db"
    ) \
    .option("dbtable", "retail_transactions") \
    .option("user", "postgres") \
    .option("password", "samuel") \
    .option("driver", "org.postgresql.Driver") \
    .load()

print("POSTGRESQL BERHASIL DIBACA")

postgres_df.show(5)

# =========================
# READ MONGODB
# =========================

mongo_df = spark.read \
    .format("mongodb") \
    .load()

print("MONGODB BERHASIL DIBACA")

mongo_df.show(5)

# =========================
# TAMBAH ROW ID
# =========================

postgres_df = postgres_df.withColumn(
    "row_id",
    monotonically_increasing_id()
)

mongo_df = mongo_df.withColumn(
    "row_id",
    monotonically_increasing_id()
)

# =========================
# JOIN DATA
# =========================

joined_df = postgres_df.alias("pg").join(
    mongo_df.select(
        "row_id",
        "anomaly"
    ).alias("mg"),
    on="row_id",
    how="inner"
).select(
    "row_id",
    "pg.Quantity",
    "pg.UnitPrice",
    "pg.TotalPrice",
    "mg.anomaly"
)

print("JOIN BERHASIL")

joined_df.show(10)

# =========================
# ANALISIS
# =========================

print("JUMLAH DATA:")
print(joined_df.count())

print("DATA ANOMALI:")

joined_df.filter(
    joined_df["anomaly"] == 1
).show(10)

# =========================
# SAVE HASIL JOIN
# =========================

joined_df.write.mode("overwrite").parquet(
    "data/joined_analysis"
)

print("HASIL JOIN DISIMPAN")

spark.stop()
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# =========================
# SPARK SESSION
# =========================

spark = SparkSession.builder \
    .appName("CustomerSegmentation") \
    .getOrCreate()

print("MEMBACA FEATURED DATA")

# =========================
# LOAD DATA
# =========================

df = spark.read.parquet(
    "data/featured_data"
)

# PENTING: Bersihkan whitespace dari nama kolom
df = df.select([col(c).alias(c.strip()) for c in df.columns])

# =========================
# PASTIKAN FORMAT TANGGAL
# =========================

df = df.withColumn(
    "InvoiceDate",
    to_timestamp("InvoiceDate")
)

# =========================
# HITUNG RECENCY
# =========================

max_date = df.agg(
    max("InvoiceDate")
).collect()[0][0]

# =========================
# CUSTOMER ANALYTICS
# =========================

customer_df = df.groupBy(
    "CustomerID"
).agg(

    # terakhir transaksi
    max("InvoiceDate").alias("LastPurchaseDate"),

    # jumlah transaksi
    countDistinct("InvoiceNo").alias("Frequency"),

    # total belanja
    round(sum("TotalPrice"), 2).alias("Monetary"),

    # rata-rata belanja
    round(avg("TotalPrice"), 2).alias("AvgTransaction"),

    # total quantity
    sum("Quantity").alias("TotalQuantity")

)

# =========================
# RECENCY
# =========================

customer_df = customer_df.withColumn(
    "Recency",
    datediff(
        lit(max_date),
        col("LastPurchaseDate")
    )
)

# =========================
# CUSTOMER SEGMENTATION
# =========================

customer_df = customer_df.withColumn(

    "Segment",

    when(
        (col("Monetary") > 5000) &
        (col("Frequency") > 50),
        "VIP Customer"
    )

    .when(
        (col("Monetary") > 2000),
        "Loyal Customer"
    )

    .when(
        (col("Frequency") > 20),
        "Frequent Buyer"
    )

    .when(
        (col("Recency") > 90),
        "Inactive Customer"
    )

    .otherwise(
        "Low Value Customer"
    )
)

# =========================
# SORTING
# =========================

customer_df = customer_df.orderBy(
    col("Monetary").desc()
)

# =========================
# SHOW SAMPLE
# =========================

print("HASIL CUSTOMER SEGMENTATION")

customer_df.show(20)

# =========================
# SAVE DATA
# =========================

customer_df.write.mode("overwrite").parquet(
    "data/customer_segments"
)

print("CUSTOMER SEGMENTATION BERHASIL DISIMPAN")

spark.stop()
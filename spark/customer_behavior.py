from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("CustomerBehaviorAnalytics") \
    .getOrCreate()

print("MEMBACA FEATURED DATA")

df = spark.read.parquet(
    "data/featured_data"
)

print("MEMBUAT CUSTOMER ANALYTICS")

customer_behavior = df.groupBy("CustomerID").agg(
    countDistinct("InvoiceNo").alias("TotalTransactions"),

    sum("TotalPrice").alias("TotalSpent"),

    avg("TotalPrice").alias("AverageTransactionValue"),

    avg("Quantity").alias("AverageQuantity"),

    max("InvoiceDate").alias("LastTransactionDate")
)

customer_behavior = customer_behavior.withColumn(
    "CustomerType",
    when(col("TotalSpent") > 5000, "VIP")
    .when(col("TotalSpent") > 1000, "Regular")
    .otherwise("Low Value")
)

print("MENYIMPAN CUSTOMER ANALYTICS")

customer_behavior.write.mode("overwrite").parquet(
    "data/customer_behavior"
)

print("CUSTOMER BEHAVIOR ANALYTICS SELESAI")

spark.stop()
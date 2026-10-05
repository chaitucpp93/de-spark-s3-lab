from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os


BUCKET = os.environ["S3_BUCKET"]
REGION = os.environ["AWS_REGION"]

INPUT_PATH = f"s3a://{BUCKET}/raw/sales_data.csv"
OUTPUT_PATH = f"s3a://{BUCKET}/processed/spark_sales_by_region/"

spark = (
    SparkSession.builder
    .appName("S3SalesByRegion")
    .config("spark.hadoop.fs.s3a.endpoint.region", REGION)
    .getOrCreate()
)
orders = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(INPUT_PATH)
)

sales_by_region = (
    orders.groupBy("region")
    .agg(F.sum("sales").alias("total_sales"))
    .orderBy("region")
)
try:
    (
        sales_by_region.write
        .mode("overwrite")
        .option("header", "true")
        .csv(OUTPUT_PATH)
    )


    print(f"Wrote output to {OUTPUT_PATH}")
finally:
    spark.stop()

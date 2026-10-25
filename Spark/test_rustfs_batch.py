import os
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("TelcoPulse-RustFS-Batch-Test")
    .config("spark.hadoop.fs.s3a.endpoint", "http://192.168.1.11:9000")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    )
    .config(
        "spark.hadoop.fs.s3a.access.key",
        os.environ["AWS_ACCESS_KEY_ID"]
    )
    .config(
        "spark.hadoop.fs.s3a.secret.key",
        os.environ["AWS_SECRET_ACCESS_KEY"]
    )
    .getOrCreate()
)

try:
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv("s3a://raw/Dataset_01_LTE_2100.csv")
    )

    print("COLUMNS:", df.columns)
    print("PARTITIONS:", df.rdd.getNumPartitions())
    print("ROW_COUNT:", df.count())
    df.show(3, truncate=False)
    print("RUSTFS_SPARK_READ_SUCCESS")

finally:
    spark.stop()

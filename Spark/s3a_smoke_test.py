from pyspark.sql import SparkSession

spark = (SparkSession.builder
    .appName("TelcoPulse-S3A-Smoke-Test")
    .master("spark://192.168.1.11:7077")
    .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

df = (spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .csv("s3a://raw/Dataset_01_LTE_2100.csv"))

print("INPUT_ROWS:", df.count(), flush=True)

(df.limit(1000)
    .write.mode("overwrite")
    .parquet("s3a://processed/spark_smoke_test/"))

print("SMOKE_TEST_SUCCESS", flush=True)
spark.stop()

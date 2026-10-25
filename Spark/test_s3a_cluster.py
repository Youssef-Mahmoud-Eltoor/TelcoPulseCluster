import os
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("TelcoPulse-Distributed-S3A-Test")
    .config("spark.hadoop.fs.s3a.endpoint", "http://192.168.1.12:9000")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    )
    .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
    .getOrCreate()
)

try:
    df = spark.read.text("s3a://raw/telcopulse-test.txt")
    print("PARTITIONS:", df.rdd.getNumPartitions())
    print("CONTENT:")
    df.show(truncate=False)

    # Force Spark tasks to execute on the cluster workers.
    print("DISTRIBUTED TASK COUNT:", df.rdd.mapPartitions(lambda rows: [1]).count())
    print("DISTRIBUTED_S3A_TEST_SUCCESS")
finally:
    spark.stop()

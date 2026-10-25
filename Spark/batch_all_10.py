import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp

spark = (
    SparkSession.builder
    .appName("TelcoPulse-Batch-10-Files")
    .master("spark://192.168.1.11:7077")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

files = [
    "Dataset_01_LTE_2100.csv",
    "Dataset_01_NR_1800.csv",
    "Dataset_02_LTE_1800.csv",
    "Dataset_02_NR_2100.csv",
    "Dataset_03_GSM_900.csv",
    "Dataset_03_LTE_1800.csv",
    "Dataset_03_LTE_700.csv",
    "Dataset_03_LTE_800.csv",
    "Dataset_03_NR_2100.csv",
    "Dataset_03_NR_3500.csv",
]

frames = []

for name in files:
    path = f"s3a://raw/{name}"
    print(f"Reading: {path}", flush=True)

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("mode", "PERMISSIVE")
        .csv(path)
    )

    tech = name.split("_")[2]
    frequency = name.removesuffix(".csv").split("_")[-1]

    df = (
        df.withColumn("source_file", lit(name))
          .withColumn("technology", lit(tech))
          .withColumn("frequency_mhz", lit(frequency))
          .withColumn("processed_at", current_timestamp())
    )

    frames.append(df)

combined = frames[0]
for frame in frames[1:]:
    combined = combined.unionByName(frame, allowMissingColumns=True)

output = "s3a://processed/telcopulse_batch_v1/"

print("Starting distributed Parquet write...", flush=True)

(
    combined
    .repartition(12)
    .write
    .mode("overwrite")
    .parquet(output)
)

print(f"BATCH_SUCCESS: {output}", flush=True)
spark.stop()

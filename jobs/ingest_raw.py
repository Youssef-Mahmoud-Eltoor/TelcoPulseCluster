import sys
from pyspark.sql import SparkSession, functions as F

ALL = [
    "Dataset_01_LTE_2100.csv", "Dataset_01_NR_1800.csv",
    "Dataset_02_LTE_1800.csv", "Dataset_02_NR_2100.csv",
    "Dataset_03_GSM_900.csv", "Dataset_03_LTE_1800.csv",
    "Dataset_03_LTE_700.csv", "Dataset_03_LTE_800.csv",
    "Dataset_03_NR_2100.csv", "Dataset_03_NR_3500.csv",
]
args = sys.argv[1:]
if not args or args[0] == "all":
    targets = ALL
else:
    targets = [a if a.endswith(".csv") else a + ".csv" for a in args]

spark = SparkSession.builder.appName("TelcoPulse-Ingest").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

for i, name in enumerate(targets, 1):
    stem = name[:-4]
    parts = stem.split("_")
    dst = "s3a://processed/ingested/" + stem + "/"
    print("INGEST [%d/%d] reading %s" % (i, len(targets), name), flush=True)
    df = (spark.read.option("header", "true").option("inferSchema", "false")
          .csv("s3a://raw/" + name)
          .withColumn("source_file", F.lit(name))
          .withColumn("technology", F.lit(parts[2]))
          .withColumn("frequency_mhz", F.lit(parts[-1]))
          .withColumn("processed_at", F.current_timestamp()))
    n_in = df.count()
    df.write.mode("overwrite").parquet(dst)
    n_out = spark.read.parquet(dst).count()
    status = "OK" if n_in == n_out else "MISMATCH"
    print("INGEST", name, "rows_in:", n_in, "rows_out:", n_out, status, "->", dst, flush=True)
spark.stop()

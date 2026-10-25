import sys
from functools import reduce
from pyspark.sql import SparkSession, functions as F

ALL = [
    "Dataset_01_LTE_2100.csv", "Dataset_01_NR_1800.csv",
    "Dataset_02_LTE_1800.csv", "Dataset_02_NR_2100.csv",
    "Dataset_03_GSM_900.csv", "Dataset_03_LTE_1800.csv",
    "Dataset_03_LTE_700.csv", "Dataset_03_LTE_800.csv",
    "Dataset_03_NR_2100.csv", "Dataset_03_NR_3500.csv",
]
PREFIX = {"LTE": "4G ", "NR": "5G ", "GSM": "2G "}
ENERGY = ["Radio unit energy consumption", "Baseband energy consumption"]
KEY = ["Base station", "Sector", "Timestamp"]
SRC = "s3a://processed/ingested/"
CURATED = "s3a://curated"

def q(c):
    return F.col("`" + c + "`")

def snake(s):
    return s.strip().lower().replace(" ", "_")

args = sys.argv[1:]
if not args or args[0] == "all":
    targets = ALL
else:
    targets = [a if a.endswith(".csv") else a + ".csv" for a in args]

spark = SparkSession.builder.appName("TelcoPulse-Clean-Curate").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
summary = []

for i, name in enumerate(targets, 1):
    stem = name[:-4]
    tech = stem.split("_")[2]
    prefix = PREFIX[tech]
    df = spark.read.parquet(SRC + stem + "/")
    metric = [c for c in df.columns if c.startswith(prefix)]
    numeric = ENERGY + metric
    total = df.count()
    print("CHECK [%d/%d] %s | technology: %s | rows: %d" % (i, len(targets), stem, tech, total), flush=True)

    aggs = []
    for k, c in enumerate(numeric):
        aggs.append(F.sum(F.when(q(c).isNull() | (F.trim(q(c)) == ""), 1).otherwise(0)).alias("e%d" % k))
        aggs.append(F.sum(F.when(q(c).isNotNull() & (F.trim(q(c)) != "") & q(c).cast("double").isNull(), 1).otherwise(0)).alias("b%d" % k))
    aggs.append(F.sum(F.when(F.to_timestamp(q("Timestamp")).isNull(), 1).otherwise(0)).alias("ts_bad"))
    aggs.append(F.sum(F.when(q("Base station").isNull() | q("Sector").isNull(), 1).otherwise(0)).alias("key_null"))
    r = df.agg(*aggs).collect()[0].asDict()
    print("CHECK", stem, "timestamp_parse_failures:", r["ts_bad"], "| null_base_station_or_sector:", r["key_null"], flush=True)
    for k, c in enumerate(numeric):
        if r["e%d" % k] or r["b%d" % k]:
            print("CHECK", stem, "col '%s' empty: %d | not_numeric: %d" % (c, r["e%d" % k], r["b%d" % k]), flush=True)

    dup = df.groupBy(*[q(c) for c in KEY]).count().filter(F.col("count") > 1)
    nd = dup.count()
    extra = dup.agg(F.sum(F.col("count") - 1)).collect()[0][0] or 0
    print("CHECK", stem, "duplicate_keys:", nd, "| extra_rows:", extra, "(kept, not deleted)", flush=True)

    cols = [q("Base station").alias("base_station"), q("Sector").alias("sector"),
            F.to_timestamp(q("Timestamp")).alias("ts"), F.col("source_file"),
            F.col("frequency_mhz").cast("int").alias("frequency_mhz")]
    cols += [q(c).cast("double").alias(snake(c)) for c in ENERGY]
    cols += [q(c).cast("double").alias(snake(c[len(prefix):])) for c in metric]
    has = reduce(lambda a, b: a | b, [q(c).cast("double").isNotNull() for c in metric])
    cols.append(has.alias("has_network_metrics"))

    dst = CURATED + "/" + tech.lower() + "/" + stem + "/"
    df.select(*cols).write.mode("overwrite").parquet(dst)
    back = spark.read.parquet(dst)
    n_back = back.count()
    with_m = back.filter(F.col("has_network_metrics")).count()
    status = "OK" if n_back == total else "MISMATCH"
    print("CLEAN", stem, "rows:", n_back, "input:", total, status, "| with_metrics:", with_m, "->", dst, flush=True)
    summary.append((stem, status, r["ts_bad"]))

print("DONE summary:", flush=True)
for s in summary:
    print("DONE", s[0], s[1], "ts_failures:", s[2], flush=True)
spark.stop()

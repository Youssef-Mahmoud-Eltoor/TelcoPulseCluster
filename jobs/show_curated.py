import sys
from pyspark.sql import SparkSession, functions as F

CURATED = "s3a://curated"
spark = SparkSession.builder.appName("TelcoPulse-Show-Curated").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

for t in ["lte", "nr", "gsm"]:
    path = CURATED + "/" + t + "/*"
    try:
        df = spark.read.parquet(path)
    except Exception:
        print("SHOW", t.upper(), "no data found at", path, flush=True)
        continue
    df = df.cache()
    n = df.count()
    print("SHOW ==========", t.upper(), "==========", flush=True)
    print("SHOW rows:", n, flush=True)
    print("SHOW schema:", flush=True)
    for f in df.schema.fields:
        print("SHOW    %-34s %s" % (f.name, f.dataType.simpleString()), flush=True)

    print("SHOW rows per source file:", flush=True)
    df.groupBy("source_file").count().orderBy("source_file").show(20, False)

    r = df.agg(F.min("ts"), F.max("ts"), F.countDistinct("base_station"), F.countDistinct("sector"),
               F.sum(F.col("has_network_metrics").cast("int"))).collect()[0]
    print("SHOW time_range:", r[0], "->", r[1], flush=True)
    print("SHOW distinct_base_stations:", r[2], "| distinct_sectors:", r[3], flush=True)
    print("SHOW rows_with_network_metrics:", r[4], "of", n, flush=True)

    nulls = df.agg(*[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns]).collect()[0].asDict()
    print("SHOW null counts (columns that have nulls):", flush=True)
    for c, v in nulls.items():
        if v:
            print("SHOW    %-34s %d" % (c, v), flush=True)

    num = [f.name for f in df.schema.fields if f.dataType.simpleString() == "double"]
    print("SHOW stats (count/min/mean/max):", flush=True)
    for i in range(0, len(num), 5):
        df.select(*num[i:i + 5]).summary("count", "min", "mean", "max").show(truncate=False)

    print("SHOW sample rows:", flush=True)
    df.select("base_station", "sector", "ts", "source_file", *num[:3]).limit(8).show(truncate=False)

    m = "data_volume_dl" if "data_volume_dl" in df.columns else "ts_utilization"
    ok = df.filter(F.col("has_network_metrics"))
    print("SHOW KPI preview: avg", m, "per frequency (rows with metrics only):", flush=True)
    ok.groupBy("frequency_mhz").agg(F.count("*").alias("rows"), F.round(F.avg(m), 3).alias("avg_" + m)).orderBy("frequency_mhz").show()
    print("SHOW KPI preview: avg", m, "per hour of day:", flush=True)
    ok.groupBy(F.hour("ts").alias("hour")).agg(F.round(F.avg(m), 3).alias("avg_" + m)).orderBy("hour").show(24)
    df.unpersist()
spark.stop()

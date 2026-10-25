#!/usr/bin/env python3
"""
TelcoPulse - LTE Capacity Forecasting v1

Reads the five known LTE curated datasets from RustFS, unions compatible
schemas, discovers a PRB/utilization target, trains a Random Forest using a
chronological holdout, compares it with a persistence baseline, and writes
predictions/metrics to s3a://ml/capacity_forecasting/.

This job does not overwrite or modify curated source data.
RustFS credentials must be supplied through the same Spark-submit/configuration
mechanism used by the successful TelcoPulse cleaning job.
"""

import sys
from datetime import datetime, timezone

from pyspark.sql import SparkSession, Window, functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator

CURATED_ROOT = "s3a://curated/lte"
OUTPUT_ROOT = "s3a://machine learning/capacity_forecasting"
MODEL_VERSION = "capacity_rf_v1"

# These paths match the LTE output folders created by the supplied cleaner.
LTE_DATASETS = [
    "Dataset_01_LTE_2100",
    "Dataset_02_LTE_1800",
    "Dataset_03_LTE_1800",
    "Dataset_03_LTE_700",
    "Dataset_03_LTE_800",
]

ENERGY_COLUMNS = [
    "radio_unit_energy_consumption",
    "baseband_energy_consumption",
]


def stop(spark, message):
    print("ERROR:", message, flush=True)
    spark.stop()
    sys.exit(1)


def main():
    spark = (
        SparkSession.builder
        .appName("TelcoPulse-Capacity-Forecasting-v1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("SOURCE ROOT:", CURATED_ROOT, flush=True)
    print("OUTPUT ROOT:", OUTPUT_ROOT, flush=True)

    # Read each known LTE folder separately. This avoids relying on root-level
    # schema inference when the individual curated datasets have different
    # metric columns.
    frames = []
    for stem in LTE_DATASETS:
        path = f"{CURATED_ROOT}/{stem}/"
        try:
            part = spark.read.parquet(path)
            if "base_station" not in part.columns or "sector" not in part.columns or "ts" not in part.columns:
                print("SKIP: required key columns missing:", path, part.columns, flush=True)
                continue
            part = part.withColumn("_dataset", F.lit(stem))
            frames.append(part)
            print("READ SCHEMA:", stem, "| columns:", len(part.columns), flush=True)
        except Exception as exc:
            print(
                "READ FAILED:", path, "|", type(exc).__name__, str(exc)[:1200],
                flush=True
            )
            stop(
                spark,
                f"Could not read curated LTE path {path}. Check the path and "
                "the same RustFS credentials/configuration used by the "
                "successful cleaning job."
            )

    if not frames:
        stop(spark, "No LTE curated dataset could be read.")

    raw = frames[0]
    for part in frames[1:]:
        raw = raw.unionByName(part, allowMissingColumns=True)

    print("UNIONED SOURCE SCHEMA:", flush=True)
    raw.printSchema()
    print("UNIONED ROW COUNT:", raw.count(), flush=True)

    required = ["base_station", "sector", "ts", "frequency_mhz"]
    missing = [c for c in required if c not in raw.columns]
    if missing:
        stop(spark, "Missing required curated columns: " + ", ".join(missing))

    # Prefer PRB utilization. The cleaner strips the leading technology prefix
    # and lowercases/replaces spaces, so common names include prb_utilization.
    candidates = [
        c for c in raw.columns
        if "utilization" in c.lower()
        and ("prb" in c.lower() or "rb" in c.lower())
    ]
    if not candidates:
        candidates = [c for c in raw.columns if "utilization" in c.lower()]

    if not candidates:
        stop(
            spark,
            "No utilization column found in the actual LTE curated schema. "
            "No ML output has been written."
        )

    target_col = candidates[0]
    print("TARGET COLUMN:", target_col, flush=True)
    if len(candidates) > 1:
        print("OTHER TARGET CANDIDATES:", candidates[1:], flush=True)

    # Parse/cast only the columns needed for modeling.
    df = (
        raw
        .withColumn("ts", F.to_timestamp("ts"))
        .withColumn("target", F.col(target_col).cast("double"))
        .withColumn("frequency_num", F.col("frequency_mhz").cast("double"))
        .filter(
            F.col("ts").isNotNull()
            & F.col("target").isNotNull()
            & F.col("base_station").isNotNull()
            & F.col("sector").isNotNull()
            & F.col("frequency_num").isNotNull()
        )
    )

    valid_rows = df.count()
    print("VALID SOURCE ROWS:", valid_rows, flush=True)
    if valid_rows == 0:
        stop(spark, "No valid rows remain after parsing timestamps and target.")

    # Only carry energy columns if they exist, and cast them safely.
    available_energy = [c for c in ENERGY_COLUMNS if c in df.columns]
    for c in available_energy:
        df = df.withColumn(c, F.col(c).cast("double"))

    # One observation per cell/sector/frequency/time. Average duplicates rather
    # than silently multiplying the same time point.
    group_keys = ["base_station", "sector", "frequency_num", "ts"]
    agg_exprs = [F.avg("target").alias("target")]
    for c in available_energy:
        agg_exprs.append(F.avg(c).alias(c))

    df = df.groupBy(*group_keys).agg(*agg_exprs)

    # Features are based on historical target observations only. Energy is
    # intentionally excluded from this first version to avoid target-time
    # leakage and to keep the model robust when energy fields are sparse.
    w = Window.partitionBy(*group_keys[:3]).orderBy("ts")
    df = (
        df
        .withColumn("prev_ts", F.lag("ts", 1).over(w))
        .withColumn(
            "gap_minutes",
            (F.col("ts").cast("long") - F.col("prev_ts").cast("long")) / 60.0
        )
        .withColumn("lag_1", F.lag("target", 1).over(w))
        .withColumn("lag_2", F.lag("target", 2).over(w))
        .withColumn("lag_4", F.lag("target", 4).over(w))
        .withColumn("hour", F.hour("ts").cast("double"))
        .withColumn("day_of_week", F.dayofweek("ts").cast("double"))
        .withColumn("frequency_feature", F.col("frequency_num"))
    )

    gap_stats = df.filter(F.col("gap_minutes").isNotNull()).agg(
        F.count("*").alias("gap_count"),
        F.expr("percentile_approx(gap_minutes, 0.5)").alias("median_gap_minutes"),
        F.sum(
            F.when(F.col("gap_minutes") != 60, 1).otherwise(0)
        ).alias("non_60_minute_gaps"),
    ).first()

    print(
        "TIME GAP CHECK:",
        f"rows_with_previous={gap_stats['gap_count']},",
        f"median_gap_minutes={gap_stats['median_gap_minutes']},",
        f"non_60_minute_gaps={gap_stats['non_60_minute_gaps']}",
        flush=True,
    )

    feature_cols = [
        "lag_1", "lag_2", "lag_4",
        "hour", "day_of_week", "frequency_feature",
    ]
    df = df.dropna(subset=["target"] + feature_cols)
    df = df.withColumn("ts_epoch", F.col("ts").cast("long"))

    usable_rows = df.count()
    print("USABLE ROWS AFTER FEATURES:", usable_rows, flush=True)
    if usable_rows < 100:
        stop(
            spark,
            f"Only {usable_rows} usable rows remain. Refusing to train on an "
            "insufficient sample."
        )

    # Split by global timestamp, not random rows, to avoid training on future
    # timestamps and testing on earlier timestamps.
    cutoff = df.approxQuantile("ts_epoch", [0.8], 0.001)[0]
    train = df.filter(F.col("ts_epoch") <= cutoff)
    test = df.filter(F.col("ts_epoch") > cutoff)

    train_count = train.count()
    test_count = test.count()
    print("CHRONOLOGICAL CUTOFF EPOCH:", cutoff, flush=True)
    print("TRAIN ROWS:", train_count, flush=True)
    print("TEST ROWS:", test_count, flush=True)
    if train_count == 0 or test_count == 0:
        stop(spark, "Chronological split produced an empty train or test set.")

    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol="features",
        handleInvalid="skip",
    )
    rf = RandomForestRegressor(
        featuresCol="features",
        labelCol="target",
        predictionCol="prediction",
        numTrees=30,
        maxDepth=8,
        seed=42,
    )
    pipeline = Pipeline(stages=[assembler, rf])
    model = pipeline.fit(train)
    predictions = model.transform(test)

    evaluator = RegressionEvaluator(
        labelCol="target",
        predictionCol="prediction",
        metricName="mae",
    )
    model_mae = evaluator.evaluate(predictions)
    evaluator.setMetricName("rmse")
    model_rmse = evaluator.evaluate(predictions)

    # Persistence baseline: predict the next observed value as the last value.
    scored = predictions.withColumn("baseline_prediction", F.col("lag_1"))
    evaluator.setPredictionCol("baseline_prediction")
    evaluator.setMetricName("mae")
    baseline_mae = evaluator.evaluate(scored)
    evaluator.setMetricName("rmse")
    baseline_rmse = evaluator.evaluate(scored)

    print("MODEL MAE:", model_mae, flush=True)
    print("MODEL RMSE:", model_rmse, flush=True)
    print("BASELINE MAE:", baseline_mae, flush=True)
    print("BASELINE RMSE:", baseline_rmse, flush=True)

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    prediction_path = f"{OUTPUT_ROOT}/predictions/run_id={run_id}/"
    metrics_path = f"{OUTPUT_ROOT}/metrics/run_id={run_id}/"

    result = scored.select(
        "ts",
        "base_station",
        "sector",
        F.col("frequency_num").cast("int").alias("frequency_mhz"),
        F.col("target").alias("actual_utilization"),
        F.col("prediction").alias("predicted_utilization"),
        F.col("baseline_prediction").alias("baseline_utilization"),
    ).withColumn("target_column", F.lit(target_col)) \
     .withColumn("model_version", F.lit(MODEL_VERSION)) \
     .withColumn("run_id", F.lit(run_id))

    print("WRITING PREDICTIONS:", prediction_path, flush=True)
    result.write.mode("errorifexists").parquet(prediction_path)

    metrics_rows = [
        (run_id, MODEL_VERSION, target_col, "MAE", float(model_mae)),
        (run_id, MODEL_VERSION, target_col, "RMSE", float(model_rmse)),
        (run_id, MODEL_VERSION, target_col, "BASELINE_MAE", float(baseline_mae)),
        (run_id, MODEL_VERSION, target_col, "BASELINE_RMSE", float(baseline_rmse)),
        (run_id, MODEL_VERSION, target_col, "TRAIN_ROWS", float(train_count)),
        (run_id, MODEL_VERSION, target_col, "TEST_ROWS", float(test_count)),
        (run_id, MODEL_VERSION, target_col, "SOURCE_VALID_ROWS", float(valid_rows)),
        (run_id, MODEL_VERSION, target_col, "USABLE_ROWS", float(usable_rows)),
        (
            run_id, MODEL_VERSION, target_col, "MEDIAN_GAP_MINUTES",
            float(gap_stats["median_gap_minutes"])
            if gap_stats["median_gap_minutes"] is not None else float("nan")
        ),
    ]
    metrics = spark.createDataFrame(
        metrics_rows,
        ["run_id", "model_version", "target_column", "metric", "value"],
    )
    print("WRITING METRICS:", metrics_path, flush=True)
    metrics.write.mode("errorifexists").parquet(metrics_path)

    print("SUCCESS: RustFS prediction and metric outputs written.", flush=True)
    print("RUN ID:", run_id, flush=True)
    print("PREDICTIONS PATH:", prediction_path, flush=True)
    print("METRICS PATH:", metrics_path, flush=True)
    print("MODEL BEATS BASELINE ON MAE:", model_mae < baseline_mae, flush=True)
    spark.stop()


if __name__ == "__main__":
    main()

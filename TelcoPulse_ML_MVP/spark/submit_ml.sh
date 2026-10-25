#!/usr/bin/env bash
# Run inside tp-spark-master (client mode: the driver is this container).
set -euo pipefail
/opt/spark/bin/spark-submit \
  --master spark://192.168.1.11:7077 --name TelcoPulse-ML \
  --driver-memory 4g --executor-cores 2 --executor-memory 2g --total-executor-cores 4 \
  --py-files /opt/telcopulse/jobs/common.py,/opt/telcopulse/ml/telcopulse_ml.py \
  /opt/telcopulse/ml/spark_ml_job.py --since 2026-01-01 --write-clickhouse

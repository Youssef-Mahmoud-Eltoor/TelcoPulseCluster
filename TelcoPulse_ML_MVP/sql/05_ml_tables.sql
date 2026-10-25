-- ML result tables (written by src/telcopulse_ml.py --write-clickhouse). Run once.
CREATE TABLE IF NOT EXISTS telecom_db.ml_forecasts
( cell String, ts DateTime, technology LowCardinality(String), forecast_rb Float32,
  congestion_risk UInt8, generated_at DateTime, model_version LowCardinality(String) )
ENGINE = MergeTree ORDER BY (generated_at, cell, ts);

CREATE TABLE IF NOT EXISTS telecom_db.ml_energy_waste
( cell String, technology LowCardinality(String), actual Float64, expected Float64,
  excess_per_hour Float64, hours UInt32, excess_pct Float64, robust_z Float64,
  waste_flag UInt8, potential_saving_units Float64, generated_at DateTime )
ENGINE = MergeTree ORDER BY (generated_at, cell);

CREATE TABLE IF NOT EXISTS telecom_db.ml_cell_profiles
( cell String, cluster_id UInt8, profile_label LowCardinality(String), peak_hour UInt8, generated_at DateTime )
ENGINE = MergeTree ORDER BY (generated_at, cell);

CREATE TABLE IF NOT EXISTS telecom_db.ml_model_metrics
( generated_at DateTime, model_version LowCardinality(String), metrics_json String )
ENGINE = MergeTree ORDER BY generated_at;

-- Dashboard 7 datasets (latest run only)
-- Next-24h congestion watch list
SELECT cell, technology, max(forecast_rb) AS peak_forecast, argMax(ts, forecast_rb) AS peak_hour
FROM telecom_db.ml_forecasts
WHERE generated_at = (SELECT max(generated_at) FROM telecom_db.ml_forecasts)
GROUP BY cell, technology HAVING peak_forecast >= 80 ORDER BY peak_forecast DESC;
-- Energy waste candidates
SELECT cell, technology, excess_pct, potential_saving_units
FROM telecom_db.ml_energy_waste
WHERE generated_at = (SELECT max(generated_at) FROM telecom_db.ml_energy_waste) AND waste_flag = 1
ORDER BY potential_saving_units DESC;

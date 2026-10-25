CREATE TABLE IF NOT EXISTS telecom_db.benchmark_advice
( generated_at DateTime, category LowCardinality(String), finding String, evidence String,
  recommendation String, confidence LowCardinality(String), expected_gain_pct Nullable(Float32) )
ENGINE = MergeTree ORDER BY (generated_at, category);

-- Dashboard dataset (latest advice, strongest confidence first)
SELECT category, confidence, finding, evidence, recommendation, expected_gain_pct
FROM telecom_db.benchmark_advice
WHERE generated_at = (SELECT max(generated_at) FROM telecom_db.benchmark_advice)
ORDER BY multiIf(confidence='high',0,confidence='medium',1,2), category;

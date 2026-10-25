CREATE DATABASE IF NOT EXISTS telecom_db;

-- 1. Batch Historical KPI Table (25-column multi-technology schema)
CREATE TABLE IF NOT EXISTS telecom_db.kpi_batch (
    ts DateTime,
    base_station LowCardinality(String),
    sector UInt8,
    technology LowCardinality(String),
    frequency_mhz UInt32,
    bandwidth_mhz Nullable(Float64),
    data_volume_dl Nullable(Float64),
    data_volume_ul Nullable(Float64),
    throughput_dl_mbps Nullable(Float64),
    throughput_ul_mbps Nullable(Float64),
    active_users_dl Nullable(Float64),
    active_users_ul Nullable(Float64),
    max_active_users_dl Nullable(Float64),
    rb_utilization Nullable(Float64),
    cqi_mean Nullable(Float64),
    mcs_mean Nullable(Float64),
    packet_loss_rate Nullable(Float64),
    latency_ms Nullable(Float64),
    radio_unit_energy_consumption Nullable(Float64),
    baseband_energy_consumption Nullable(Float64),
    cooling_energy_consumption Nullable(Float64),
    power_amplifier_temp Nullable(Float64),
    mimo_layers Nullable(UInt8),
    tx_power_dbm Nullable(Float64),
    coverage_distance_km Nullable(Float64)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY (base_station, technology, ts);

-- 2. Real-Time Streaming KPI Table (Windowed Kafka telemetry)
CREATE TABLE IF NOT EXISTS telecom_db.kpi_stream (
    window_start DateTime,
    window_end DateTime,
    base_station LowCardinality(String),
    sector UInt8,
    technology LowCardinality(String),
    data_volume_dl Float64,
    data_volume_ul Float64,
    throughput_dl_mbps Float64,
    throughput_ul_mbps Float64,
    rb_utilization Float64,
    active_users_dl Float64,
    packet_loss_rate Float64,
    latency_ms Float64,
    scenario_id LowCardinality(String),
    ingested_at DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toDate(window_start)
ORDER BY (base_station, window_start);

-- 3. Network Anomaly & Security Incident Alerts Table
CREATE TABLE IF NOT EXISTS telecom_db.anomalies (
    detected_at DateTime,
    base_station LowCardinality(String),
    sector UInt8,
    technology LowCardinality(String),
    kpi_name LowCardinality(String),
    observed_value Float64,
    expected_mean Float64,
    anomaly_score Float64,
    severity LowCardinality(String),
    scenario_id LowCardinality(String)
) ENGINE = MergeTree()
ORDER BY (detected_at, severity, base_station);

-- 4. Data Quality Assurance Reports Table
CREATE TABLE IF NOT EXISTS telecom_db.quality_report (
    report_id UUID DEFAULT generateUUIDv4(),
    check_timestamp DateTime DEFAULT now(),
    source_layer LowCardinality(String),
    total_records UInt64,
    null_violations UInt64,
    range_violations UInt64,
    status LowCardinality(String)
) ENGINE = MergeTree()
ORDER BY check_timestamp;

-- 5. System Benchmark Metrics (Throughput and Latency Verification)
CREATE TABLE IF NOT EXISTS telecom_db.benchmark_runs (
    run_id UUID DEFAULT generateUUIDv4(),
    executed_at DateTime DEFAULT now(),
    test_scenario LowCardinality(String),
    records_processed UInt64,
    ingestion_rate_eps Float64,
    query_latency_ms Float64,
    status LowCardinality(String)
) ENGINE = MergeTree()
ORDER BY executed_at;

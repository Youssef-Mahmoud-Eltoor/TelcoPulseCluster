"""
TelcoPulse Pro — Stream Processor & Anomaly Detector  v2.0
===========================================================
Consumes events from Kafka, writes KPIs to ClickHouse, and runs
multi-rule anomaly detection (upgraded from single-threshold v1.0).

Anomaly rules (Section 11.2 expansion)
---------------------------------------
  Rule 1 — RB Utilisation spike      (rb_utilization > 80 %)
  Rule 2 — Throughput collapse        (data_volume_dl < low_threshold while users > 5)
  Rule 3 — Radio quality degradation  (radio_quality_cqi < 6.0)
  Rule 4 — Energy anomaly             (radio_unit_energy > expected ceiling)

All detected anomalies are written to telecom_db.anomalies with a
consistent schema, including severity grading and a numeric anomaly_score.
"""

import json
from typing import Dict, Optional

import requests
from kafka import KafkaConsumer


# ─────────────────────────────────────────────────────────────────────────────
#  CONNECTION CONFIG
# ─────────────────────────────────────────────────────────────────────────────

KAFKA_BROKER  = "192.168.1.13:9092"
KAFKA_TOPIC   = "telecom_stream"

CLICKHOUSE_URL = "http://192.168.1.15:8123/"
CH_AUTH        = ("admin", "bigdata123")


# ─────────────────────────────────────────────────────────────────────────────
#  ANOMALY RULE THRESHOLDS
#  Reference baselines derived from the Zenodo 17815388 PM counter dataset.
# ─────────────────────────────────────────────────────────────────────────────

RB_UTIL_WARNING_PCT   = 80.0     # > 80 % → warning
RB_UTIL_CRITICAL_PCT  = 90.0     # > 90 % → critical

CQI_DEGRADED_THRESH   = 6.0      # below 6 → poor radio quality
CQI_CRITICAL_THRESH   = 4.0      # below 4 → critical RF degradation

# Expected mean RB utilisation for anomaly score normalisation
RB_EXPECTED_MEAN      = 28.0
CQI_EXPECTED_MEAN     = 11.0

# Energy ceilings per technology  (radio unit, in W)
# Exceeded = possible hardware fault or cooling failure
RU_ENERGY_CEILING: Dict[str, float] = {
    "4G": 125.0,
    "5G": 250.0,
}

# DL collapse detection: if DL < this threshold AND active users > LOW_USERS_MIN,
# it signals a throughput collapse (RF degradation or misconfiguration)
DL_COLLAPSE_FLOOR_MB       = 15.0
DL_COLLAPSE_MIN_USERS      = 5.0
# Expected DL at normal mid-load for anomaly score baseline
DL_EXPECTED_NORMAL_MB      = 120.0


# ─────────────────────────────────────────────────────────────────────────────
#  CLICKHOUSE WRITER
# ─────────────────────────────────────────────────────────────────────────────

def ch_insert(table: str, row: Dict) -> bool:
    """Insert a single JSON row into a ClickHouse table. Returns True on success."""
    query = f"INSERT INTO {table} FORMAT JSONEachRow"
    resp  = requests.post(
        f"{CLICKHOUSE_URL}?query={requests.utils.quote(query)}",
        auth=CH_AUTH,
        data=json.dumps(row),
        timeout=5,
    )
    if resp.status_code != 200:
        print(f"  [CH ERROR] {table} → {resp.status_code}: {resp.text.strip()[:120]}")
        return False
    return True


# ─────────────────────────────────────────────────────────────────────────────
#  KPI ROW BUILDER
# ─────────────────────────────────────────────────────────────────────────────

def build_kpi_row(event: Dict) -> Dict:
    """Normalise and type-cast the incoming Kafka event into a KPI row."""
    return {
        "window_start":       event.get("window_start",       "2023-01-01 00:00:00"),
        "window_end":         event.get("window_end",         "2023-01-01 00:05:00"),
        "base_station":       event.get("base_station",       "Unknown"),
        "sector":             int(event.get("sector",         1)),
        "technology":         event.get("technology",         "4G"),
        "frequency_band":     int(event.get("frequency_band", 1800)),
        "data_volume_dl":     float(event.get("data_volume_dl",     0.0)),
        "data_volume_ul":     float(event.get("data_volume_ul",     0.0)),
        "rb_utilization":     float(event.get("rb_utilization",     0.0)),
        "radio_quality_cqi":  float(event.get("radio_quality_cqi",  0.0)),
        "active_users_dl":    float(event.get("active_users_dl",    0.0)),
        "radio_unit_energy":  float(event.get("radio_unit_energy",  0.0)),
        "baseband_energy":    float(event.get("baseband_energy",    0.0)),
        "data_source":        event.get("data_source",        "synthetic_stream"),
        "is_synthetic":       int(event.get("is_synthetic",   1)),
        "scenario_id":        event.get("scenario_id",        "normal"),
        "generation_version": event.get("generation_version", "v1.0"),
    }


def _anomaly_severity(score: float) -> str:
    if score >= 1.5:
        return "critical"
    if score >= 1.0:
        return "warning"
    return "info"


# ─────────────────────────────────────────────────────────────────────────────
#  ANOMALY DETECTION RULES
# ─────────────────────────────────────────────────────────────────────────────

def detect_anomalies(kpi: Dict, event: Dict) -> None:
    """
    Run all anomaly detection rules against a normalised KPI row.
    Writes each detected anomaly to telecom_db.anomalies.
    """
    base_fields = {
        "event_timestamp": kpi["window_start"],
        "pipeline":        "streaming",
        "base_station":    kpi["base_station"],
        "sector":          kpi["sector"],
        "technology":      kpi["technology"],
        "detection_method":"threshold",
        "is_synthetic":    kpi["is_synthetic"],
        "injected_label":  int(event.get("injected_label", 1)),
        "scenario_id":     kpi["scenario_id"],
    }

    # ── Rule 1 : High RB Utilisation ────────────────────────────────────────
    rb = kpi["rb_utilization"]
    if rb > RB_UTIL_WARNING_PCT:
        score = round(rb / RB_UTIL_WARNING_PCT, 2)
        anomaly = {
            **base_fields,
            "kpi_name":       "rb_utilization",
            "observed_value": rb,
            "expected_mean":  RB_EXPECTED_MEAN,
            "anomaly_score":  score,
            "severity":       "critical" if rb > RB_UTIL_CRITICAL_PCT else "warning",
        }
        if ch_insert("telecom_db.anomalies", anomaly):
            severity_tag = "CRITICAL" if rb > RB_UTIL_CRITICAL_PCT else "WARNING"
            print(f"    [{severity_tag}] RB Util = {rb:.1f}%  →  anomaly_score={score}")

    # ── Rule 2 : Throughput Collapse ─────────────────────────────────────────
    dl     = kpi["data_volume_dl"]
    users  = kpi["active_users_dl"]
    if dl < DL_COLLAPSE_FLOOR_MB and users > DL_COLLAPSE_MIN_USERS:
        # Score: how far below normal → higher = worse collapse
        score = round(max(DL_EXPECTED_NORMAL_MB - dl, 1.0) / DL_EXPECTED_NORMAL_MB, 2)
        anomaly = {
            **base_fields,
            "kpi_name":       "data_volume_dl",
            "observed_value": dl,
            "expected_mean":  DL_EXPECTED_NORMAL_MB,
            "anomaly_score":  score,
            "severity":       _anomaly_severity(score),
        }
        if ch_insert("telecom_db.anomalies", anomaly):
            print(f"    [WARNING] DL Collapse: {dl:.1f} MB with {users:.0f} users  →  anomaly_score={score}")

    # ── Rule 3 : CQI Degradation ─────────────────────────────────────────────
    cqi = kpi["radio_quality_cqi"]
    if cqi < CQI_DEGRADED_THRESH:
        score = round(max(CQI_EXPECTED_MEAN - cqi, 0.1) / CQI_EXPECTED_MEAN, 2)
        anomaly = {
            **base_fields,
            "kpi_name":       "radio_quality_cqi",
            "observed_value": cqi,
            "expected_mean":  CQI_EXPECTED_MEAN,
            "anomaly_score":  score,
            "severity":       "critical" if cqi < CQI_CRITICAL_THRESH else "warning",
        }
        if ch_insert("telecom_db.anomalies", anomaly):
            severity_tag = "CRITICAL" if cqi < CQI_CRITICAL_THRESH else "WARNING"
            print(f"    [{severity_tag}] CQI = {cqi:.1f}  →  anomaly_score={score}")

    # ── Rule 4 : Radio Unit Energy Anomaly ───────────────────────────────────
    ru_energy = kpi["radio_unit_energy"]
    tech_key  = kpi["technology"][:2]            # "4G" or "5G"
    ru_ceiling = RU_ENERGY_CEILING.get(tech_key, 150.0)
    if ru_energy > ru_ceiling:
        score = round(ru_energy / ru_ceiling, 2)
        anomaly = {
            **base_fields,
            "kpi_name":       "radio_unit_energy",
            "observed_value": ru_energy,
            "expected_mean":  ru_ceiling * 0.65,  # typical mid-load energy
            "anomaly_score":  score,
            "severity":       _anomaly_severity(score),
        }
        if ch_insert("telecom_db.anomalies", anomaly):
            print(f"    [WARNING] RU Energy = {ru_energy:.1f} W  (ceiling {ru_ceiling} W)  →  anomaly_score={score}")


# ─────────────────────────────────────────────────────────────────────────────
#  MAIN CONSUMER LOOP
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset="latest",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    print(f"╔══ TelcoPulse Pro Stream Processor v2.0 ══╗")
    print(f"║  Kafka  : {KAFKA_BROKER}  │  Topic : {KAFKA_TOPIC}")
    print(f"║  CH     : {CLICKHOUSE_URL}")
    print(f"╚{'═' * 44}╝\n")

    for message in consumer:
        event = message.value
        kpi   = build_kpi_row(event)

        # ── Write KPI stream record ──────────────────────────────────────────
        ok = ch_insert("telecom_db.kpi_stream", kpi)
        if ok:
            print(
                f"[OK] {kpi['base_station']} S{kpi['sector']} "
                f"({kpi['technology']}@{kpi['frequency_band']} MHz)  "
                f"DL:{kpi['data_volume_dl']:>8.1f} MB  "
                f"RB:{kpi['rb_utilization']:>5.1f}%  "
                f"CQI:{kpi['radio_quality_cqi']:>4.1f}  "
                f"[{kpi['scenario_id']}]"
            )

        # ── Run anomaly detection rules ──────────────────────────────────────
        detect_anomalies(kpi, event)


if __name__ == "__main__":
    main()
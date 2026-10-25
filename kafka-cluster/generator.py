"""
TelcoPulse Pro — Synthetic RAN Data Generator  v2.0
=====================================================
Schema-identical drop-in replacement for v1.0.

Data realism grounded in:
  Zenodo 10.5281/zenodo.17815388  — Live 5G/4G/2G PM counters (15-min intervals)
  Zenodo 10.5281/zenodo.1219679   — 4G LTE field traces (0–173 Mbit/s)

Design pillars
--------------
1. Circadian traffic model   — dual-peak (business + evening), weekday/weekend split
2. Site-character profiles   — urban / suburban / rural KPI envelopes
3. Tech×Band calibration     — separate parameter sets for 4G@1800, 4G@2100, 5G@3500, 5G@1800
4. Correlated KPI chain      — load → users → RB-util → throughput(CQI) → energy
5. Markov anomaly engine     — 5 states, realistic transition probabilities
6. Log-normal traffic noise  — captures bursty real-world variance

Usage
-----
    pip install kafka-python numpy
    python generator.py
"""

import json
import math
import random
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, List, Tuple, Optional

import numpy as np
from kafka import KafkaProducer


# ─────────────────────────────────────────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────────────────────────────────────────

KAFKA_BROKER       = "localhost:9092"
KAFKA_TOPIC        = "telecom_stream"
EMIT_INTERVAL_S    = 0.10           # seconds between emitted events
WINDOW_DURATION_S  = 300            # each event represents a 5-min measurement window
GENERATION_VERSION = "v2.0"

# Set a fixed numpy seed for reproducibility during development (remove for live use)
# np.random.seed(42)


# ─────────────────────────────────────────────────────────────────────────────
#  SITE PROFILES
#  Each site has a character that determines its baseline load and tech stack.
# ─────────────────────────────────────────────────────────────────────────────

class SiteType(Enum):
    URBAN    = "urban"
    SUBURBAN = "suburban"
    RURAL    = "rural"


@dataclass
class SiteProfile:
    name:        str
    site_type:   SiteType
    # (technology, frequency_band) pairs served by this site
    tech_bands:  List[Tuple[str, int]]
    sectors:     int   = 3
    # Multiplier on the reference load curve (1.0 = reference urban density)
    load_scale:  float = 1.0
    # Gaussian noise seed offset so sites don't produce identical patterns
    seed_offset: int   = 0


SITES: List[SiteProfile] = [
    SiteProfile("Site 36",  SiteType.URBAN,    [("4G", 1800), ("5G", 3500)], load_scale=1.20, seed_offset=0),
    SiteProfile("Site 42",  SiteType.URBAN,    [("4G", 2100), ("5G", 3500)], load_scale=1.10, seed_offset=7),
    SiteProfile("Site 88",  SiteType.SUBURBAN, [("4G", 1800), ("4G", 2100)], load_scale=0.75, seed_offset=13),
    SiteProfile("Site 101", SiteType.RURAL,    [("4G", 1800)],               load_scale=0.35, seed_offset=19),
]


# ─────────────────────────────────────────────────────────────────────────────
#  TECHNOLOGY × BAND KPI ENVELOPES
#  Calibrated from Zenodo 17815388 PM counter statistics + 3GPP reference values.
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class TechBandProfile:
    """Realistic KPI parameter set for one technology-band combination."""

    # DL data volume (MB) per window at 100 % RB utilization (sector capacity)
    dl_capacity_mb:    float
    # Minimum DL volume even at near-zero load (system keep-alive traffic)
    dl_floor_mb:       float
    # Typical UL-to-DL ratio (asymmetric; UL is heavier in 5G uplink-heavy deployments)
    ul_dl_ratio:       float

    # RB utilization (%) — normal operating distribution
    rb_mu:             float    # mean at peak-hour load
    rb_sigma:          float    # standard deviation

    # CQI (0–15) — represents radio channel quality
    cqi_clean_mu:      float    # mean CQI at low-load (good channel)
    cqi_clean_sigma:   float
    cqi_congested_mu:  float    # degrades under high load / interference

    # Active DL users per sector at peak hour
    users_peak_mu:     float
    users_peak_sigma:  float

    # Radio unit energy proxy (W; scales with load)
    ru_energy_idle:    float    # static power at zero traffic
    ru_energy_peak:    float    # total power at full load

    # Baseband unit energy (relatively constant; slight load dependency)
    bb_energy_mu:      float
    bb_energy_sigma:   float


# Reference values (sources listed in module docstring)
TECH_BAND_PROFILES: Dict[Tuple[str, int], TechBandProfile] = {
    # ── 4G LTE @ 1800 MHz (Band 3 — coverage + capacity workhorse) ──────────
    ("4G", 1800): TechBandProfile(
        dl_capacity_mb=620.0, dl_floor_mb=2.5, ul_dl_ratio=0.12,
        rb_mu=30.0, rb_sigma=13.0,
        cqi_clean_mu=11.2, cqi_clean_sigma=1.5, cqi_congested_mu=7.5,
        users_peak_mu=38.0, users_peak_sigma=11.0,
        ru_energy_idle=28.0, ru_energy_peak=102.0,
        bb_energy_mu=87.0, bb_energy_sigma=3.5,
    ),
    # ── 4G LTE @ 2100 MHz (Band 1 — dense urban capacity) ───────────────────
    ("4G", 2100): TechBandProfile(
        dl_capacity_mb=850.0, dl_floor_mb=3.5, ul_dl_ratio=0.11,
        rb_mu=36.0, rb_sigma=15.0,
        cqi_clean_mu=10.8, cqi_clean_sigma=1.7, cqi_congested_mu=7.0,
        users_peak_mu=46.0, users_peak_sigma=13.0,
        ru_energy_idle=31.0, ru_energy_peak=112.0,
        bb_energy_mu=88.5, bb_energy_sigma=4.0,
    ),
    # ── 5G NR @ 3500 MHz (n78 — mid-band, primary 5G capacity) ─────────────
    ("5G", 3500): TechBandProfile(
        dl_capacity_mb=4800.0, dl_floor_mb=8.0, ul_dl_ratio=0.09,
        rb_mu=26.0, rb_sigma=11.0,          # 5G has more RBs → lower % at same Mbps
        cqi_clean_mu=12.5, cqi_clean_sigma=1.3, cqi_congested_mu=8.5,
        users_peak_mu=52.0, users_peak_sigma=15.0,
        ru_energy_idle=55.0, ru_energy_peak=218.0,
        bb_energy_mu=91.5, bb_energy_sigma=5.0,
    ),
    # ── 5G NR @ 1800 MHz (DSS / n3 — 5G coverage layer) ────────────────────
    ("5G", 1800): TechBandProfile(
        dl_capacity_mb=2100.0, dl_floor_mb=5.0, ul_dl_ratio=0.10,
        rb_mu=23.0, rb_sigma=10.0,
        cqi_clean_mu=12.0, cqi_clean_sigma=1.4, cqi_congested_mu=8.0,
        users_peak_mu=44.0, users_peak_sigma=12.0,
        ru_energy_idle=47.0, ru_energy_peak=185.0,
        bb_energy_mu=90.0, bb_energy_sigma=4.5,
    ),
}

_FALLBACK_PROFILE = TECH_BAND_PROFILES[("4G", 1800)]


# ─────────────────────────────────────────────────────────────────────────────
#  CIRCADIAN TRAFFIC MODEL
# ─────────────────────────────────────────────────────────────────────────────

def circadian_load_factor(dt: datetime, site_type: SiteType) -> float:
    """
    Return a [0.05, 1.0] load multiplier derived from time-of-day and site type.

    Model: sum of Gaussian bumps on a 24-hour axis, calibrated per site character.

    Reference shape (urban weekday):
        Morning business peak  ~10:00 (σ=1.8 h)
        Evening social peak    ~20:00 (σ=1.6 h)
        Night floor             ~0.08
    """
    h = dt.hour + dt.minute / 60.0
    is_weekend = dt.weekday() >= 5

    def gauss(center: float, sigma: float, amplitude: float) -> float:
        return amplitude * math.exp(-0.5 * ((h - center) / sigma) ** 2)

    if site_type == SiteType.RURAL:
        # Single broad peak in the early evening (residents return home)
        load = 0.06 + gauss(18.5, 3.0, 0.52)

    elif site_type == SiteType.SUBURBAN:
        if is_weekend:
            load = 0.08 + gauss(13.0, 3.5, 0.58) + gauss(20.5, 2.0, 0.30)
        else:
            load = 0.08 + gauss(9.5, 2.0, 0.62) + gauss(20.0, 1.8, 0.75)

    else:  # URBAN
        if is_weekend:
            # Later start, flatter single peak, stronger evening leisure traffic
            load = 0.10 + gauss(14.0, 3.5, 0.60) + gauss(21.0, 1.5, 0.55)
        else:
            # Classic business hours + strong evening social burst
            load = 0.08 + gauss(10.5, 1.8, 0.80) + gauss(20.0, 1.6, 0.95)

    # Small Gaussian jitter so consecutive events aren't perfectly smooth
    jitter = float(np.random.normal(0, 0.025))
    return float(np.clip(load + jitter, 0.05, 1.0))


# ─────────────────────────────────────────────────────────────────────────────
#  MARKOV ANOMALY STATE MACHINE
# ─────────────────────────────────────────────────────────────────────────────

class AnomalyState(Enum):
    NORMAL      = "normal"
    LOADING     = "loading"         # gradual traffic ramp before a peak
    CONGESTED   = "congested"       # RB utilisation > 80 %, CQI degrades
    SPIKE       = "traffic_spike"   # sudden burst (event / viral content)
    RF_DEGRADED = "rf_degradation"  # radio channel quality collapse


# Transition matrix — rows must sum to 1.0
# Tuned so anomaly states last plausible durations at 0.1 s emit rate
_TRANSITIONS: Dict[AnomalyState, Dict[AnomalyState, float]] = {
    AnomalyState.NORMAL:      {AnomalyState.NORMAL: 0.938, AnomalyState.LOADING: 0.042, AnomalyState.RF_DEGRADED: 0.020},
    AnomalyState.LOADING:     {AnomalyState.LOADING: 0.600, AnomalyState.CONGESTED: 0.240, AnomalyState.SPIKE: 0.110, AnomalyState.NORMAL: 0.050},
    AnomalyState.CONGESTED:   {AnomalyState.CONGESTED: 0.710, AnomalyState.NORMAL: 0.210, AnomalyState.SPIKE: 0.080},
    AnomalyState.SPIKE:       {AnomalyState.SPIKE: 0.280, AnomalyState.CONGESTED: 0.510, AnomalyState.NORMAL: 0.210},
    AnomalyState.RF_DEGRADED: {AnomalyState.RF_DEGRADED: 0.760, AnomalyState.NORMAL: 0.240},
}


class AnomalyEngine:
    """Per-cell Markov chain that advances one step per generated event."""

    def __init__(self):
        self.state = AnomalyState.NORMAL

    def step(self) -> AnomalyState:
        row    = _TRANSITIONS[self.state]
        states = list(row.keys())
        probs  = list(row.values())
        self.state = random.choices(states, weights=probs, k=1)[0]
        return self.state

    @property
    def scenario_id(self) -> str:
        return self.state.value


# ─────────────────────────────────────────────────────────────────────────────
#  CORRELATED KPI ENGINE
# ─────────────────────────────────────────────────────────────────────────────

class KPIEngine:
    """
    Generates physically correlated KPIs for one (site, tech, band) cell.

    Correlation chain
    -----------------
    load_factor  →  active_users
                 →  rb_utilization
                 →  cqi  (degrades with interference at high load)
                 →  data_volume_dl  (users × per-user throughput ∝ CQI)
                 →  data_volume_ul  (fixed ratio of DL)
                 →  radio_unit_energy  (idle + load-proportional)
                 →  baseband_energy    (near-constant with small noise)

    Noise model: log-normal multiplicative noise on data volume captures the
    bursty, right-skewed traffic distributions seen in real PM counter data.
    """

    def __init__(self, profile: TechBandProfile, site: SiteProfile):
        self.p    = profile
        self.site = site

    def generate(self, load_factor: float, anomaly: AnomalyState) -> Dict:
        p              = self.p
        eff_load       = float(np.clip(load_factor * self.site.load_scale, 0.0, 1.0))

        # ── Active DL users ──────────────────────────────────────────────────
        users_mu  = p.users_peak_mu * eff_load
        users     = float(np.clip(
            np.random.normal(users_mu, p.users_peak_sigma * 0.55),
            0.0, p.users_peak_mu * 2.8
        ))

        # ── CQI — radio channel quality ──────────────────────────────────────
        if anomaly == AnomalyState.RF_DEGRADED:
            cqi_raw = np.random.normal(4.0, 1.3)         # severe channel degradation
        elif anomaly in (AnomalyState.CONGESTED, AnomalyState.SPIKE):
            cqi_raw = np.random.normal(p.cqi_congested_mu, 1.6)
        else:
            # Slight CQI degradation with load (inter-user interference)
            interference_penalty = eff_load * 1.8
            cqi_raw = np.random.normal(
                p.cqi_clean_mu - interference_penalty, p.cqi_clean_sigma
            )
        cqi = float(np.clip(cqi_raw, 1.0, 15.0))

        # ── RB utilisation (%) ───────────────────────────────────────────────
        rb_base = p.rb_mu * eff_load + np.random.normal(0, p.rb_sigma * 0.45)
        if anomaly == AnomalyState.SPIKE:
            rb_base += random.uniform(38.0, 58.0)
        elif anomaly == AnomalyState.CONGESTED:
            rb_base += random.uniform(16.0, 33.0)
        elif anomaly == AnomalyState.LOADING:
            rb_base += random.uniform(7.0, 18.0)
        elif anomaly == AnomalyState.RF_DEGRADED:
            # More RBs needed to deliver same data rate → utilisation rises
            rb_base *= 1.35
        rb_util = float(np.clip(rb_base, 0.5, 99.5))

        # ── DL data volume ───────────────────────────────────────────────────
        # CQI maps to a spectral efficiency proxy (higher CQI → more bits/RB)
        # Reference: 3GPP TS 36.213 MCS-CQI table
        spectral_eff = 0.14 * math.exp(0.23 * cqi)    # ≈ 0.3–8 bits/s/Hz proxy
        vol_base     = (
            p.dl_floor_mb
            + (p.dl_capacity_mb - p.dl_floor_mb)
            * (rb_util / 100.0)
            * min(spectral_eff / 5.0, 1.0)
        )
        # Log-normal noise captures real traffic burstiness (σ=0.18 → ≈±18%)
        vol_dl = float(np.clip(
            vol_base * float(np.random.lognormal(0.0, 0.18)),
            p.dl_floor_mb * 0.5,
            p.dl_capacity_mb * 1.20
        ))
        if anomaly == AnomalyState.SPIKE:
            vol_dl = min(vol_dl * random.uniform(2.0, 3.8), p.dl_capacity_mb * 1.20)
        elif anomaly == AnomalyState.RF_DEGRADED:
            vol_dl *= random.uniform(0.20, 0.50)       # throughput collapses with bad CQI

        # ── UL data volume ───────────────────────────────────────────────────
        ul_ratio_noise = float(np.random.normal(0, 0.012))
        vol_ul = float(np.clip(
            vol_dl * (p.ul_dl_ratio + ul_ratio_noise),
            0.0, vol_dl * 0.35
        ))

        # ── Energy ───────────────────────────────────────────────────────────
        load_ratio  = rb_util / 100.0
        ru_range    = p.ru_energy_peak - p.ru_energy_idle
        ru_energy   = float(np.clip(
            p.ru_energy_idle + ru_range * load_ratio + np.random.normal(0, ru_range * 0.04),
            p.ru_energy_idle * 0.90,
            p.ru_energy_peak * 1.10
        ))
        bb_energy = float(np.clip(
            np.random.normal(p.bb_energy_mu, p.bb_energy_sigma),
            70.0, 120.0
        ))

        return {
            "data_volume_dl":    round(vol_dl,    2),
            "data_volume_ul":    round(vol_ul,    2),
            "rb_utilization":    round(rb_util,   2),
            "radio_quality_cqi": round(cqi,       2),
            "active_users_dl":   round(users,     1),
            "radio_unit_energy": round(ru_energy, 2),
            "baseband_energy":   round(bb_energy, 2),
        }


# ─────────────────────────────────────────────────────────────────────────────
#  CELL REGISTRY
# ─────────────────────────────────────────────────────────────────────────────

def build_cell_registry() -> List[Dict]:
    """Return one engine+anomaly context per (site, tech, band, sector) cell."""
    cells = []
    for site in SITES:
        for (tech, band) in site.tech_bands:
            profile = TECH_BAND_PROFILES.get((tech, band), _FALLBACK_PROFILE)
            for sector_id in range(1, site.sectors + 1):
                cells.append({
                    "site":    site,
                    "tech":    tech,
                    "band":    band,
                    "sector":  sector_id,
                    "engine":  KPIEngine(profile, site),
                    "anomaly": AnomalyEngine(),
                })
    return cells


# ─────────────────────────────────────────────────────────────────────────────
#  EVENT BUILDER
# ─────────────────────────────────────────────────────────────────────────────

def build_event(cell: Dict, now: datetime) -> Dict:
    """Compose a single Kafka payload — schema identical to v1.0."""
    site    = cell["site"]
    anomaly = cell["anomaly"].step()          # returns AnomalyState enum
    load_f  = circadian_load_factor(now, site.site_type)
    kpis    = cell["engine"].generate(load_f, anomaly)

    window_start = now.strftime("%Y-%m-%d %H:%M:%S")
    window_end   = (now + timedelta(seconds=WINDOW_DURATION_S)).strftime("%Y-%m-%d %H:%M:%S")

    return {
        "window_start":       window_start,
        "window_end":         window_end,
        "base_station":       site.name,
        "sector":             cell["sector"],
        "technology":         cell["tech"],
        "frequency_band":     cell["band"],
        **kpis,
        "data_source":        "synthetic_stream",
        "is_synthetic":       1,
        "scenario_id":        anomaly.value,          # FIX: was anomaly.scenario_id
        "generation_version": GENERATION_VERSION,
    }


# ─────────────────────────────────────────────────────────────────────────────
#  ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    # FIX: removed value_serializer to avoid DeprecationWarning;
    # bytes are serialized manually before each send() call instead.
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        acks=1,
        retries=3,
    )

    cells = build_cell_registry()

    print(f"╔══ TelcoPulse Pro Generator {GENERATION_VERSION} ══╗")
    print(f"║  Sites  : {len(SITES)}  │  Cells : {len(cells)}")
    print(f"║  Broker : {KAFKA_BROKER}  │  Topic : {KAFKA_TOPIC}")
    print(f"╚{'═' * 45}╝\n")

    try:
        while True:
            now  = datetime.now()
            cell = random.choice(cells)
            ev   = build_event(cell, now)

            # FIX: serialize manually — producer has no value_serializer
            producer.send(KAFKA_TOPIC, value=json.dumps(ev).encode("utf-8"))

            state_tag = f"[{ev['scenario_id']:<14}]"
            print(
                f"  {ev['base_station']} S{ev['sector']} "
                f"({ev['technology']}@{ev['frequency_band']} MHz) "
                f"DL:{ev['data_volume_dl']:>8.1f} MB  "
                f"RB:{ev['rb_utilization']:>5.1f}%  "
                f"CQI:{ev['radio_quality_cqi']:>4.1f}  "
                f"Users:{ev['active_users_dl']:>5.1f}  "
                f"RU:{ev['radio_unit_energy']:>6.1f} W  "
                f"{state_tag}"
            )

            time.sleep(EMIT_INTERVAL_S)

    except KeyboardInterrupt:
        print("\n[*] Shutting down — flushing remaining messages…")
        producer.flush()
        producer.close()
        print("[*] Done.")


if __name__ == "__main__":
    main()
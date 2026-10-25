"""TelcoPulse ML MVP: load forecasting, congestion alerts, energy-waste detection, cell profiling.
Input: hourly KPI frame with columns
  cell, ts, technology, rb_utilization, active_users_dl, energy   (energy = radio unit + baseband)
Sources: --source demo (synthetic, labelled), csv, clickhouse (telecom_db.kpi_batch_v)."""
import argparse, json, os, datetime as dt
import numpy as np, pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score, mean_absolute_error

# ---- Egypt-specific calendar settings (edit to official dates each year)
RAMADAN = [("2026-02-18", "2026-03-19")]
WEEKEND_DOW = (4, 5)               # Friday, Saturday (Monday=0)
CONGESTION_PCT = 80.0              # resource-block utilisation treated as congested
TEST_DAYS, HORIZON_H = 14, 24
MODEL_VERSION = "ml-1.0"


# ------------------------------------------------------------------ data
def ch_client():
    import clickhouse_connect
    return clickhouse_connect.get_client(host=os.environ.get("CH_HOST", "192.168.1.15"), port=8123,
        username=os.environ.get("CH_USER", "default"), password=os.environ.get("CH_PASSWORD", ""),
        database="telecom_db")


def load_clickhouse(since="2026-01-01"):
    q = """SELECT concat(base_station,'-',sector,'-',toString(frequency_mhz)) AS cell,
      toStartOfHour(ts) AS ts, any(technology) AS technology,
      avg(rb_utilization) AS rb_utilization, avg(active_users_dl) AS active_users_dl,
      avg(radio_unit_energy_consumption + baseband_energy_consumption) AS energy
      FROM kpi_batch_v WHERE ts >= {since:DateTime} AND technology != 'UNKNOWN'
      GROUP BY cell, ts"""
    return ch_client().query_df(q, parameters={"since": since})


def make_demo(n_cells=45, days=100, seed=7):
    """SYNTHETIC demo data with Egyptian patterns. Not real network data."""
    rng = np.random.default_rng(seed)
    ts = pd.date_range("2026-01-15", periods=days * 24, freq="h")
    kinds = ["residential", "business", "tourist"]
    rows, waste = [], set()
    for i in range(n_cells):
        kind, tech = kinds[i % 3], ["LTE", "NR", "GSM"][i % 3 if i % 5 else 0]
        base, grow = rng.uniform(25, 58), rng.choice([0, 0, 0.35]) / 100
        h, dow = ts.hour.values, ts.dayofweek.values
        if kind == "residential": shape = 0.5 + 0.9 * np.exp(-((h - 21) / 3.0) ** 2)
        elif kind == "business": shape = 0.4 + 1.0 * np.exp(-((h - 12) / 3.5) ** 2)
        else: shape = 0.6 + 0.6 * np.exp(-((h - 18) / 4.0) ** 2)
        wk = np.where(np.isin(dow, WEEKEND_DOW), 0.7 if kind == "business" else 1.15, 1.0)
        ram = np.zeros(len(ts), bool)
        for a, b in RAMADAN: ram |= (ts >= a) & (ts <= pd.Timestamp(b) + pd.Timedelta(hours=23))
        iftar = np.where(ram & (h >= 19) & (h <= 23), 1.3, np.where(ram & (h >= 1) & (h <= 6), 1.15, 1.0))
        trend = 1 + grow * np.arange(len(ts)) / 24
        rb = np.clip(base * shape * wk * iftar * trend + rng.normal(0, 2.5, len(ts)), 1, 100)
        users = rb * rng.uniform(1.2, 1.8) + rng.normal(0, 3, len(ts))
        wasteful = (i % 9 == 4)
        if wasteful: waste.add(f"CELL-{i:03d}")
        energy = (180 + 1.1 * rb + 0.15 * users) * (1.28 if wasteful else 1.0) + rng.normal(0, 4, len(ts))
        rows.append(pd.DataFrame(dict(cell=f"CELL-{i:03d}", ts=ts, technology=tech,
            rb_utilization=rb, active_users_dl=np.clip(users, 0, None), energy=energy)))
    df = pd.concat(rows, ignore_index=True)
    df.attrs["injected_waste"] = waste
    return df


# ------------------------------------------------------------------ forecasting
def calendar(ts):
    t = pd.DatetimeIndex(ts)
    ram = np.zeros(len(t), bool)
    for a, b in RAMADAN: ram |= (t >= a) & (t <= pd.Timestamp(b) + pd.Timedelta(hours=23))
    return pd.DataFrame({"hour": t.hour, "dow": t.dayofweek,
        "is_weekend": np.isin(t.dayofweek, WEEKEND_DOW).astype(int), "ramadan": ram.astype(int),
        "ramadan_evening": (ram & (t.hour >= 19) & (t.hour <= 23)).astype(int)})


def add_features(df):
    """Horizon-safe features: only information available 24 h before the target hour."""
    df = df.sort_values(["cell", "ts"]).reset_index(drop=True)
    g = df.groupby("cell")["rb_utilization"]
    df["lag_24"], df["lag_48"], df["lag_168"] = g.shift(24), g.shift(48), g.shift(168)
    df["roll_24_mean"] = g.transform(lambda s: s.shift(24).rolling(24, min_periods=12).mean())
    df["roll_168_mean"] = g.transform(lambda s: s.shift(24).rolling(168, min_periods=72).mean())
    df["users_lag_24"] = df.groupby("cell")["active_users_dl"].shift(24)
    df = pd.concat([df, calendar(df["ts"])], axis=1)
    return df


FEATS = ["hour", "dow", "is_weekend", "ramadan", "ramadan_evening", "lag_24", "lag_48", "lag_168",
         "roll_24_mean", "roll_168_mean", "users_lag_24", "cell_code"]


def with_codes(f, cells):
    f = f.copy(); f["cell_code"] = f["cell"].map({c: i for i, c in enumerate(cells)}).astype(int); return f


def fit_forecast(df, cells, cutoff=None):
    f = with_codes(add_features(df), cells).dropna(subset=["lag_168", "roll_168_mean", "users_lag_24"])
    cutoff = cutoff or f["ts"].max() - pd.Timedelta(days=TEST_DAYS)
    tr, te = f[f.ts <= cutoff], f[f.ts > cutoff].copy()
    m = HistGradientBoostingRegressor(max_iter=300, learning_rate=0.06, max_leaf_nodes=31,
        categorical_features=[FEATS.index("cell_code")], random_state=0)
    m.fit(tr[FEATS], tr["rb_utilization"])
    te["pred"] = m.predict(te[FEATS]).clip(0, 100); te["base_pred"] = te["lag_168"]
    mae_m = mean_absolute_error(te.rb_utilization, te.pred)
    mae_b = mean_absolute_error(te.rb_utilization, te.base_pred)
    per = te.groupby("cell").apply(lambda x: pd.Series({
        "m": mean_absolute_error(x.rb_utilization, x.pred),
        "b": mean_absolute_error(x.rb_utilization, x.base_pred)}), include_groups=False)
    ac, pc = te.rb_utilization >= CONGESTION_PCT, te.pred >= CONGESTION_PCT
    tp, fp, fn = int((ac & pc).sum()), int((~ac & pc).sum()), int((ac & ~pc).sum())
    metrics = {"mae_model": round(mae_m, 3), "mae_baseline_same_hour_last_week": round(mae_b, 3),
        "improvement_pct": round(100 * (mae_b - mae_m) / mae_b, 1),
        "cells_model_beats_baseline_pct": round(100 * float((per.m < per.b).mean()), 1),
        "congestion_threshold_pct": CONGESTION_PCT, "congested_hours_in_test": int(ac.sum()),
        "congestion_precision": round(tp / max(tp + fp, 1), 3), "congestion_recall": round(tp / max(tp + fn, 1), 3),
        "train_rows": int(len(tr)), "test_rows": int(len(te)), "test_days": TEST_DAYS}
    return m, metrics, te


def forecast_next_day(model, df, cells):
    last = df["ts"].max()
    fut = pd.DataFrame([(c, last + pd.Timedelta(hours=h)) for c in cells for h in range(1, HORIZON_H + 1)],
                       columns=["cell", "ts"])
    fut["rb_utilization"], fut["active_users_dl"] = np.nan, np.nan
    tech = df.drop_duplicates("cell").set_index("cell")["technology"]
    fut["technology"] = fut["cell"].map(tech)
    full = pd.concat([df[["cell", "ts", "technology", "rb_utilization", "active_users_dl"]], fut], ignore_index=True)
    full = with_codes(add_features(full), cells)
    out = full[full.ts > last].dropna(subset=["lag_24"]).copy()
    out["forecast_rb"] = model.predict(out[FEATS]).clip(0, 100)
    out["congestion_risk"] = (out.forecast_rb >= CONGESTION_PCT).astype(int)
    return out[["cell", "ts", "technology", "forecast_rb", "congestion_risk"]]


# ------------------------------------------------------------------ energy waste
def energy_waste(df):
    d = df.dropna(subset=["energy", "rb_utilization", "active_users_dl"]).reset_index(drop=True)
    X = pd.DataFrame({"rb": d.rb_utilization, "users": d.active_users_dl, "hour": d.ts.dt.hour,
                      "tech": d.technology.astype("category").cat.codes})
    m = HistGradientBoostingRegressor(max_iter=150, random_state=0, categorical_features=[3]).fit(X, d["energy"])
    d["expected_energy"] = m.predict(X); d["excess"] = d["energy"] - d["expected_energy"]
    cell = d.groupby("cell").agg(technology=("technology", "first"), actual=("energy", "mean"),
        expected=("expected_energy", "mean"), excess_per_hour=("excess", "mean"), hours=("energy", "size"))
    cell["excess_pct"] = 100 * cell.excess_per_hour / cell.expected
    med = cell.excess_pct.median(); mad = (cell.excess_pct - med).abs().median() or 1e-6
    cell["robust_z"] = 0.6745 * (cell.excess_pct - med) / mad
    cell["waste_flag"] = ((cell.excess_pct > 8) & (cell.robust_z > 3.5)).astype(int)
    cell["potential_saving_units"] = np.where(cell.waste_flag == 1, cell.excess_per_hour * cell.hours, 0.0)
    return cell.reset_index()


# ------------------------------------------------------------------ cell profiles
def cell_profiles(df):
    prof = df.assign(hour=df.ts.dt.hour).pivot_table(index="cell", columns="hour", values="rb_utilization", aggfunc="mean")
    shape = prof.div(prof.mean(axis=1), axis=0).fillna(1.0)
    best_s, best = -1, None
    for k in range(2, 7):
        km = KMeans(n_clusters=k, n_init=10, random_state=0).fit(shape)
        s = silhouette_score(shape, km.labels_)
        if s > best_s: best_s, best = s, km
    out = pd.DataFrame({"cell": shape.index, "cluster_id": best.labels_})
    names = {}
    for c in range(best.n_clusters):
        centre = best.cluster_centers_[c]; pk = int(np.argmax(centre)); amp = centre.max() - centre.min()
        names[c] = ("flat / mixed" if amp < 0.35 else "evening peak (residential)" if pk >= 19 or pk <= 1
                    else "daytime peak (business)" if 8 <= pk <= 15 else "afternoon-evening (leisure/tourist)")
    out["profile_label"] = out.cluster_id.map(names)
    out["peak_hour"] = out.cluster_id.map(lambda c: int(np.argmax(best.cluster_centers_[c])))
    return out, {"k": int(best.n_clusters), "silhouette": round(float(best_s), 3)}


# ------------------------------------------------------------------ outputs
def save_charts(te, waste_df, outdir):
    import matplotlib; matplotlib.use("Agg"); import matplotlib.pyplot as plt
    cell = te.groupby("cell").rb_utilization.max().idxmax()
    x = te[te.cell == cell].tail(24 * 5)
    f, a = plt.subplots(1, 2, figsize=(13, 4))
    a[0].plot(x.ts, x.rb_utilization, label="actual"); a[0].plot(x.ts, x.pred, label="model")
    a[0].plot(x.ts, x.base_pred, "--", label="same hour last week")
    a[0].axhline(CONGESTION_PCT, color="r", ls=":"); a[0].set_title(f"Hold-out forecast, {cell}")
    a[0].legend(fontsize=8); a[0].tick_params(axis="x", rotation=30)
    w = waste_df.sort_values("excess_pct", ascending=False).head(12)
    a[1].barh(w.cell[::-1], w.excess_pct[::-1], color=["#B23A48" if v else "#0E7C86" for v in w.waste_flag[::-1]])
    a[1].set_title("Energy above load-expected (%)  red = flagged"); f.tight_layout()
    f.savefig(os.path.join(outdir, "ml_summary.png"), dpi=130); plt.close(f)


def write_clickhouse(frames):
    c = ch_client()
    for table, f in frames.items(): c.insert_df(table, f)


def run(df, out="out", write_ch=False, source="custom"):
    """Full pipeline on an hourly pandas frame (used by the CLI and by the Spark job)."""
    os.makedirs(out, exist_ok=True)
    injected = df.attrs.get("injected_waste", set())
    cells = sorted(df["cell"].unique())
    model, metrics, te = fit_forecast(df, cells)
    fc = forecast_next_day(model, df, cells)
    waste = energy_waste(df); prof, pm = cell_profiles(df)
    now = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None, microsecond=0)
    if source == "demo":
        fl = set(waste[waste.waste_flag == 1].cell)
        metrics["DEMO_ONLY_injected_waste_cells"] = len(injected)
        metrics["DEMO_ONLY_waste_precision"] = round(len(injected & fl) / max(len(fl), 1), 3)
        metrics["DEMO_ONLY_waste_recall"] = round(len(injected & fl) / max(len(injected), 1), 3)
    metrics.update(cells=len(cells), clusters=pm, source=source,
                   cells_flagged_congestion_next_24h=int(fc.groupby("cell").congestion_risk.max().sum()),
                   cells_flagged_energy_waste=int(waste.waste_flag.sum()),
                   potential_saving_units=round(float(waste.potential_saving_units.sum()), 1))
    fc = fc.assign(generated_at=now, model_version=MODEL_VERSION)
    fc.to_csv(f"{out}/forecasts.csv", index=False)
    waste.assign(generated_at=now).to_csv(f"{out}/energy_waste.csv", index=False)
    prof.assign(generated_at=now).to_csv(f"{out}/cell_profiles.csv", index=False)
    json.dump(metrics, open(f"{out}/metrics.json", "w"), indent=2)
    save_charts(te, waste, out)
    if write_ch:
        write_clickhouse({"ml_forecasts": fc, "ml_energy_waste": waste.assign(generated_at=now),
            "ml_cell_profiles": prof.assign(generated_at=now),
            "ml_model_metrics": pd.DataFrame([{"generated_at": now, "model_version": MODEL_VERSION,
                "metrics_json": json.dumps(metrics)}])})
    print(json.dumps(metrics, indent=2))
    return metrics


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", choices=["demo", "csv", "clickhouse"], default="demo")
    ap.add_argument("--csv"); ap.add_argument("--out", default="out"); ap.add_argument("--write-clickhouse", action="store_true")
    a = ap.parse_args()
    df = {"demo": make_demo, "csv": lambda: pd.read_csv(a.csv, parse_dates=["ts"]), "clickhouse": load_clickhouse}[a.source]()
    run(df, a.out, a.write_clickhouse, a.source)


if __name__ == "__main__":
    main()

"""TelcoPulse Benchmark Advisor.
Reads telecom_db.benchmark_runs and produces evidence-backed recommendations.
Small-data design: interpretable log-log regression with bootstrap confidence intervals,
an Amdahl serial-fraction fit, matched network comparison, and threshold rules.
No recommendation is issued without stating its evidence and confidence."""
import argparse, json, os, re, datetime as dt
import numpy as np, pandas as pd

CFG = dict(
    MIN_BATCH_RUNS=8,            # below this only descriptive advice is produced
    RAM_LIMIT_MB=8192,           # memory available to the sampled host (match .wslconfig memory)
    CPU_HIGH=85.0, CPU_LOW=40.0, RAM_HIGH_FRAC=0.85,
    STREAM_FLOOR_MS=105_000,     # 60 s window + 30 s watermark + 15 s trigger (Chapter 16)
    NET_PENALTY_MIN_PCT=10.0, BOOT=400, SEED=11)


# ------------------------------------------------------------------ data
def load_clickhouse():
    import clickhouse_connect
    c = clickhouse_connect.get_client(host=os.environ.get("CH_HOST", "192.168.1.15"), port=8123,
        username=os.environ.get("CH_USER", "default"), password=os.environ.get("CH_PASSWORD", ""),
        database="telecom_db")
    return c.query_df("SELECT * FROM benchmark_runs ORDER BY run_timestamp")


def parse_notes(df):
    """notes may carry 'key=value' pairs (e.g. rate=50; exec_mem_gb=2; shuffle_partitions=48)."""
    kv = df["notes"].fillna("").map(lambda s: dict(re.findall(r"([A-Za-z_]+)=([0-9.]+)", s)))
    extra = pd.DataFrame(list(kv)).apply(pd.to_numeric, errors="coerce") if len(df) else pd.DataFrame()
    return pd.concat([df.reset_index(drop=True), extra.reset_index(drop=True)], axis=1), list(extra.columns)


def make_demo(seed=3):
    """SYNTHETIC benchmark runs with a known structure, for testing the advisor only."""
    rng = np.random.default_rng(seed); rows = []; t0 = dt.datetime(2026, 10, 1, 9)
    s = 0.18                                            # serial fraction built into the demo
    for net in ("ethernet", "wifi"):
        for n in (1, 2, 3, 4):
            for gb in (2, 5, 10):
                for rep in range(2):
                    par = (s + (1 - s) / n) * (1 + (0.14 * (n - 1) if net == "wifi" else 0.0))
                    rt = 25 + 42 * gb * par * rng.normal(1, 0.04)
                    rows.append(dict(experiment_id=f"EXP-W{n}-{net[:3].upper()}-{gb}G-r{rep}",
                        run_timestamp=t0 + dt.timedelta(minutes=len(rows) * 20), workload_size_gb=gb,
                        num_spark_workers=n, network_mode=net, runtime_seconds=rt,
                        throughput_rows_per_sec=gb * 1.5e6 / rt, streaming_delay_ms=0.0,
                        cpu_usage_pct=float(np.clip(70 - 6 * n + rng.normal(0, 4), 10, 99)),
                        ram_usage_mb=float(2600 + 180 * gb + rng.normal(0, 80)), notes="batch"))
    for n in (1, 2, 3):
        for rate in (50, 200, 400):
            delay = 105_000 + max(0, rate / (n * 160) - 1) * 90_000 + rng.normal(0, 3000)
            rows.append(dict(experiment_id=f"EXP-STREAM-W{n}-r{rate}", run_timestamp=t0 + dt.timedelta(hours=30 + len(rows)),
                workload_size_gb=0, num_spark_workers=n, network_mode="ethernet", runtime_seconds=0.0,
                throughput_rows_per_sec=0.0, streaming_delay_ms=float(delay), cpu_usage_pct=float(40 + rate / 12),
                ram_usage_mb=3000.0, notes=f"stream; rate={rate}"))
    return pd.DataFrame(rows)


# ------------------------------------------------------------------ models
def design(b, cols_meta, extra_cols):
    X = [np.ones(len(b))]; names = ["const"]
    if cols_meta["wl"]: X.append(np.log(b.workload_size_gb)); names.append("log_workload")
    if cols_meta["wk"]: X.append(np.log(b.num_spark_workers)); names.append("log_workers")
    for m in cols_meta["modes"]: X.append((b.network_mode == m).astype(float)); names.append(f"net_{m}")
    for c in extra_cols: X.append(np.log(b[c])); names.append(f"log_{c}")
    return np.column_stack(X), names


def fit_loglog(b, extra_cols):
    meta = dict(wl=b.workload_size_gb.nunique() > 1, wk=b.num_spark_workers.nunique() > 1,
                modes=sorted(b.network_mode.unique())[1:])
    extra = [c for c in extra_cols if c in b and b[c].notna().all() and b[c].nunique() > 1 and (b[c] > 0).all()]
    X, names = design(b, meta, extra); y = np.log(b.runtime_seconds.values)
    beta = np.linalg.lstsq(X, y, rcond=None)[0]
    rng = np.random.default_rng(CFG["SEED"]); boots = []
    for _ in range(CFG["BOOT"]):
        i = rng.integers(0, len(b), len(b))
        if np.linalg.matrix_rank(X[i]) < X.shape[1]: continue
        boots.append(np.linalg.lstsq(X[i], y[i], rcond=None)[0])
    boots = np.array(boots) if boots else np.tile(beta, (2, 1))
    lo, hi = np.percentile(boots, [5, 95], axis=0)
    resid = y - X @ beta
    return dict(meta=meta, extra=extra, names=names, beta=beta, lo=lo, hi=hi,
                r2=float(1 - resid.var() / max(y.var(), 1e-12)), ref=sorted(b.network_mode.unique())[0])


def predict(m, workload, workers, mode, extras=None):
    row = pd.DataFrame([dict(workload_size_gb=workload, num_spark_workers=workers, network_mode=mode, **(extras or {}))])
    X, _ = design(row, dict(m["meta"], modes=m["meta"]["modes"]), m["extra"])
    # design() builds dummies only for modes present in meta
    return float(np.exp(X @ m["beta"])[0])


def amdahl_serial_fraction(b):
    """Grid-fit s so that t(N) ∝ s + (1-s)/N matches median runtimes at the best-covered workload."""
    g = b.groupby(["workload_size_gb", "network_mode"]).num_spark_workers.nunique()
    if g.max() < 3: return None
    wl, mode = g.idxmax(); sub = b[(b.workload_size_gb == wl) & (b.network_mode == mode)]
    med = sub.groupby("num_spark_workers").runtime_seconds.median(); n0 = med.index.min()
    best = (None, 1e18)
    for s in np.linspace(0, 1, 201):
        f = lambda n: s + (1 - s) / n
        pred = np.array([f(n) / f(n0) for n in med.index]); err = float(((med.values / med.values[0] - pred) ** 2).sum())
        if err < best[1]: best = (s, err)
    return dict(s=float(best[0]), workload=float(wl), mode=mode, max_speedup=float(1 / max(best[0], 1e-6)))


def network_penalty(b):
    modes = sorted(b.network_mode.unique())
    if len(modes) < 2: return None
    ref = modes[0]; out = []
    med = b.groupby(["num_spark_workers", "workload_size_gb", "network_mode"]).runtime_seconds.median().unstack()
    for m in modes[1:]:
        if m in med and ref in med:
            r = (med[m] / med[ref]).dropna()
            if len(r): out.append(dict(mode=m, ref=ref, ratio=float(r.median()), cells=int(len(r)),
                                       by_workers=(med[m] / med[ref]).dropna().groupby(level=0).median().round(3).to_dict()))
    return out


def conf(n, ci_width):
    return "high" if n >= 20 and ci_width < 0.3 else "medium" if n >= 10 and ci_width < 0.6 else "low"


# ------------------------------------------------------------------ advice
def advise(df):
    df, extra_cols = parse_notes(df)
    batch = df[df.runtime_seconds > 0].copy(); stream = df[df.streaming_delay_ms > 0].copy()
    A = []
    add = lambda cat, finding, evidence, rec, c, gain=None: A.append(dict(category=cat, finding=finding,
        evidence=evidence, recommendation=rec, confidence=c, expected_gain_pct=gain))
    n = len(batch); model = None
    if n < CFG["MIN_BATCH_RUNS"]:
        add("DATA", f"Only {n} batch runs recorded", f"minimum {CFG['MIN_BATCH_RUNS']} required for model-based advice",
            "Run the experiment plan of Chapter 28 (worker counts 1..N, 2-3 workload sizes, ethernet and wifi, 2-3 repeats).", "high")
    else:
        model = fit_loglog(batch, extra_cols); nm = model["names"]
        # --- scaling
        if "log_workers" in nm:
            k = nm.index("log_workers"); b, lo, hi = model["beta"][k], model["lo"][k], model["hi"][k]; w = hi - lo
            ev = f"runtime ~ workers^{b:.2f} (90% CI {lo:.2f} to {hi:.2f}), R2={model['r2']:.2f}, n={n}"
            if b <= -0.8: add("SCALING", "Runtime scales almost linearly with Workers", ev, "Adding Worker laptops is the most effective lever; keep executor sizing constant when adding.", conf(n, w))
            elif b <= -0.4: add("SCALING", "Diminishing returns when adding Workers", ev, "Gains flatten: shuffle, driver work or storage reads are limiting. Check Spark UI stage times, reduce shuffle volume, raise spark.sql.shuffle.partitions only if tasks are few.", conf(n, w))
            else: add("SCALING", "More Workers barely reduce runtime", ev, "Do not add Workers yet. The bottleneck is elsewhere: RustFS read throughput, the network, driver-side steps, or data skew. Profile one run in the Spark UI first.", conf(n, w))
        am = amdahl_serial_fraction(batch)
        if am: add("SCALING", f"Serial (non-parallel) share is about {am['s']*100:.0f}%", f"Amdahl fit at {am['workload']:g} GB, {am['mode']}: max speed-up about {am['max_speedup']:.1f}x regardless of Workers",
                   "Reduce the serial part (driver collect, single-file writes, small-file listing, job start-up) before adding hardware.", conf(n, 0.4))
        # --- workload
        if "log_workload" in nm:
            k = nm.index("log_workload"); b, lo, hi = model["beta"][k], model["lo"][k], model["hi"][k]
            if b > 1.15 and lo > 1.0: add("WORKLOAD", "Runtime grows faster than data size", f"runtime ~ workload^{b:.2f} (90% CI {lo:.2f} to {hi:.2f})", "Spilling or shuffle blow-up at larger sizes: raise executor memory, filter early, or partition input by date.", conf(n, hi - lo))
            elif b < 0.95: add("WORKLOAD", "Fixed overhead dominates small workloads", f"runtime ~ workload^{b:.2f}", "Prefer batching more data per job; start-up cost is amortised.", conf(n, hi - lo))
        # --- network
        for p in network_penalty(batch) or []:
            pct = (p["ratio"] - 1) * 100
            if pct >= CFG["NET_PENALTY_MIN_PCT"]:
                add("NETWORK", f"{p['mode']} is {pct:.0f}% slower than {p['ref']}", f"median runtime ratio over {p['cells']} matched cells; by workers {p['by_workers']}",
                    f"Move the Worker and storage laptops to {p['ref']} (cable). Expected runtime reduction about {100*(1-1/p['ratio']):.0f}%.", conf(p["cells"] * 2, 0.4), round(100 * (1 - 1 / p["ratio"]), 1))
        # --- resources (sampled on the driver host only)
        ram_hi = batch.ram_usage_mb.median() / CFG["RAM_LIMIT_MB"]
        if ram_hi >= CFG["RAM_HIGH_FRAC"]: add("RESOURCES", "Memory pressure on the sampled host", f"median RAM {batch.ram_usage_mb.median():.0f} MB = {ram_hi*100:.0f}% of {CFG['RAM_LIMIT_MB']} MB", "Raise WSL 2 memory in .wslconfig or lower driver/executor memory and --since window; risk of OOM and swapping.", "medium")
        cpu = batch.cpu_usage_pct.median()
        if cpu >= CFG["CPU_HIGH"]: add("RESOURCES", "Sampled host is CPU-saturated", f"median CPU {cpu:.0f}%", "CPU-bound: add cores or Workers.", "low")
        elif cpu <= CFG["CPU_LOW"] and "log_workers" in nm and model["beta"][nm.index("log_workers")] > -0.5:
            add("RESOURCES", "Low CPU with poor scaling suggests waiting, not computing", f"median CPU {cpu:.0f}%", "Look at network and storage I/O, not compute.", "low")
        add("DATA", "CPU and RAM are sampled on one host only", "benchmark_runner.py samples the host that runs the command", "Add per-host sampling (docker stats on every Worker laptop) before trusting resource-based advice.", "high")
        # --- what-if
        if "log_workers" in nm:
            mw = int(batch.num_spark_workers.max()); wl = float(batch.workload_size_gb.median()); mode = model["ref"]
            t_now, t_next = predict(model, wl, mw, mode), predict(model, wl, mw + 1, mode)
            add("WHAT-IF", f"Adding Worker number {mw+1} (extrapolation beyond tested range)", f"model predicts {t_now:.0f}s -> {t_next:.0f}s at {wl:g} GB, {mode}",
                "Treat as a hypothesis and verify with an experiment.", "low", round(100 * (t_now - t_next) / t_now, 1))
    # --- streaming
    if len(stream):
        med = stream.streaming_delay_ms.median(); floor = CFG["STREAM_FLOOR_MS"]
        add("STREAMING", f"Median streaming delay {med/1000:.0f}s against a design floor of {floor/1000:.0f}s", f"{len(stream)} stream runs; floor = window 60 s + watermark 30 s + trigger 15 s",
            "Delay near the floor is set by window design: shorten window or watermark to cut latency (trade-off: more late-data loss)." if med < 1.3 * floor else "Delay far above the floor means backlog: add Workers or lower the event rate; check processing time per micro-batch in the Spark UI.", "medium")
        if "rate" in stream and stream["rate"].nunique() > 1:
            piv = stream.groupby(["num_spark_workers", "rate"]).streaming_delay_ms.median().unstack()
            over = [(w, r) for w in piv.index for r in piv.columns if piv.loc[w, r] > 1.3 * floor]
            if over: add("STREAMING", "Backlog appears at higher event rates", f"delay > 1.3x floor at (workers, rate): {over[:6]}", "Capacity per Worker is exceeded there; size Workers for the expected rate or reduce the rate.", "medium")
    # --- next experiments
    gaps = []
    if batch.num_spark_workers.nunique() < 3: gaps.append("at least 3 different Worker counts")
    if batch.workload_size_gb.nunique() < 3: gaps.append("at least 3 workload sizes")
    if batch.network_mode.nunique() < 2: gaps.append("both ethernet and wifi")
    if len(batch) and batch.groupby(["num_spark_workers", "workload_size_gb", "network_mode"]).size().median() < 2: gaps.append("repeat each condition at least twice")
    if not extra_cols: gaps.append("record config as key=value in notes (exec_mem_gb=2; exec_cores=2; shuffle_partitions=48) so config changes can be evaluated")
    if gaps: add("NEXT-EXPERIMENTS", "Evidence gaps limit the advice", "; ".join(gaps), "Run these before changing the platform.", "high")
    return pd.DataFrame(A), model, batch


def chart(batch, model, out):
    import matplotlib; matplotlib.use("Agg"); import matplotlib.pyplot as plt
    f, a = plt.subplots(1, 2, figsize=(12, 4)); wl = batch.workload_size_gb.median()
    for mode, g in batch.groupby("network_mode"):
        s = g[g.workload_size_gb == wl]; a[0].scatter(s.num_spark_workers, s.runtime_seconds, label=f"{mode} (runs)")
        if model is not None and "log_workers" in model["names"]:
            ws = np.arange(batch.num_spark_workers.min(), batch.num_spark_workers.max() + 1.01, 0.25)
            a[0].plot(ws, [predict(model, wl, w, mode) for w in ws], "--")
    a[0].set_xlabel("Spark Workers"); a[0].set_ylabel("runtime (s)"); a[0].set_title(f"Runtime vs Workers at {wl:g} GB"); a[0].legend(fontsize=8)
    t = batch.groupby("num_spark_workers").cpu_usage_pct.median(); a[1].bar(t.index.astype(str), t.values, color="#0E7C86")
    a[1].set_title("Median CPU % (sampled host only)"); a[1].set_xlabel("Spark Workers"); f.tight_layout()
    f.savefig(os.path.join(out, "advisor_summary.png"), dpi=130); plt.close(f)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", choices=["demo", "csv", "clickhouse"], default="demo")
    ap.add_argument("--csv"); ap.add_argument("--out", default="out"); ap.add_argument("--write-clickhouse", action="store_true")
    a = ap.parse_args(); os.makedirs(a.out, exist_ok=True)
    df = {"demo": make_demo, "csv": lambda: pd.read_csv(a.csv, parse_dates=["run_timestamp"]), "clickhouse": load_clickhouse}[a.source]()
    adv, model, batch = advise(df)
    now = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None, microsecond=0)
    adv = adv.assign(generated_at=now, source=a.source)
    adv.to_csv(f"{a.out}/advice.csv", index=False); adv.to_json(f"{a.out}/advice.json", orient="records", indent=2, force_ascii=False, date_format="iso")
    if len(batch): chart(batch, model, a.out)
    if a.write_clickhouse:
        import clickhouse_connect
        c = clickhouse_connect.get_client(host=os.environ.get("CH_HOST", "192.168.1.15"), port=8123, username=os.environ.get("CH_USER", "default"),
            password=os.environ.get("CH_PASSWORD", ""), database="telecom_db")
        ins = adv.copy(); ins["expected_gain_pct"] = ins["expected_gain_pct"].astype(object).where(ins["expected_gain_pct"].notna(), None)
        c.insert_df("benchmark_advice", ins[["generated_at", "category", "finding", "evidence", "recommendation", "confidence", "expected_gain_pct"]])
    pd.set_option("display.max_colwidth", 140, "display.width", 200)
    for _, r in adv.iterrows():
        print(f"[{r.category}|{r.confidence}] {r.finding}\n    evidence: {r.evidence}\n    -> {r.recommendation}")


if __name__ == "__main__":
    main()

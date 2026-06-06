#!/usr/bin/env python3
"""Statistical smoke tests for zmq-bench (throughput + latency modes).

Validates a set of full-suite runs: per-finding sanity, cross-benchmark
relationships, that latency mode actually measures transit (not queueing), that
throughput is plausible, and that CPU pinning is spread. Exits non-zero if any
hard family fails; Family I is informational (jitter/noise characteristics).

Requires numpy:  pip install numpy

Generate the inputs into a directory, then point this script at it. Example:

    DIR=/tmp/zb_smoke; mkdir -p "$DIR"
    # one run with histograms (per-finding raw latencies + pin log)
    ./target/release/zmq-bench run-benchmarks --save-hists \
        --output "$DIR/run_1.csv" > "$DIR/savehists.log" 2>&1
    mv indiv_*.rawlat.txt indiv_*.recv_cpus.txt "$DIR/"; rm -f indiv_*.hgrm
    # a couple more plain runs for cross-run stability
    ./target/release/zmq-bench run-benchmarks --output "$DIR/run_2.csv" >/dev/null 2>&1
    ./target/release/zmq-bench run-benchmarks --output "$DIR/run_3.csv" >/dev/null 2>&1
    python3 scripts/smoke.py "$DIR"

Reads from <dir>:
  run_*.csv            aggregate results, one full-suite run each (>=1; >=2 for
                       cross-run stability checks)
  indiv_*.rawlat.txt   per-finding raw latencies (from the --save-hists run)
  indiv_*.recv_cpus.txt   per-finding receive CPUs (placement checks)
  savehists.log        stdout of the --save-hists run (pin distribution)

Test families (hard unless noted):
  A  per-finding sanity     each raw-latency series on its own (drops, ordering,
                            TSC garbage, no fixed-delay plateau)
  B  cross-benchmark        payload scaling, cross-run p50 stability
  M  cross-mode             latency-mode p50 must beat throughput-mode p50
  L  latency structural     us-scale floor, router adds a hop, small-payload flat
  T  throughput sanity      msgs/s in a plausible band, router is the bottleneck,
                            bandwidth grows with payload
  P  placement              receiver core spread, pin distribution
  I  informational          latency tail ratio, cross-run throughput variance
"""
import csv, glob, os, re, sys
from collections import defaultdict
import numpy as np

DIR = sys.argv[1] if len(sys.argv) > 1 else "."
PATTERNS = ["PubSub", "Dealer", "DealerRouter"]
TRANSPORTS = ["IPC", "TCP"]
MODES = ["throughput", "latency"]
SIZES = [8, 64, 256, 1024, 4096]

results = []
def rec(fam, name, tgt, ok, detail):
    results.append((fam, name, tgt, "PASS" if ok else "FAIL", detail))

# ---------- load aggregate CSVs (per run) ----------
# agg[(pat,tr,mode,sz)] = list over runs of dict(min,p50,p99,max,msgs,mb)
agg = defaultdict(list)
run_csvs = sorted(glob.glob(f"{DIR}/run_*.csv"))
for path in run_csvs:
    for r in csv.DictReader(open(path)):
        k = (r["pattern"], r["transport"], r["mode"], int(r["payload_size_bytes"]))
        agg[k].append(dict(
            mn=int(r["min_latency_ns"]), p50=int(r["median_latency_ns"]),
            p99=int(r["p99_latency_ns"]), mx=int(r["max_latency_ns"]),
            msgs=float(r["msgs_per_sec"]), mb=float(r["mb_per_sec"])))
def med(k, field):
    xs = [d[field] for d in agg.get(k, [])]
    return float(np.median(xs)) if xs else None

# ---------- load per-finding raw latencies (one save-hists run) ----------
def key_of(fn):
    m = re.search(r"indiv_(\w+?)_(IPC|TCP)_(throughput|latency)_(\d+)_\d+_\d+\.rawlat\.txt", fn)
    return (m.group(1), m.group(2), m.group(3), int(m.group(4))) if m else None
raw = {}
for fn in glob.glob(f"{DIR}/indiv_*.rawlat.txt"):
    k = key_of(os.path.basename(fn))
    if k:
        raw[k] = np.loadtxt(fn)
def pct(a, p): return float(np.percentile(a, p))

EXPECTED_N = 10000

# ============================================================
# FAMILY A — per-finding sanity
# ============================================================
for k in sorted(raw, key=lambda x: (PATTERNS.index(x[0]), x[1], x[2], x[3])):
    a = raw[k]; tgt = "/".join(map(str, k)); n = len(a)
    mn, p50, p99, mx = a.min(), pct(a, 50), pct(a, 99), a.max()
    mean, std = a.mean(), a.std(); cv = std/mean if mean else 0
    rec("A", "sample_count", tgt, n == EXPECTED_N,
        f"n={n}" + ("" if n == EXPECTED_N else f" (DROPPED {EXPECTED_N-n})"))
    rec("A", "ordering_positivity", tgt,
        mn > 0 and mn <= p50 <= p99 <= mx and np.isfinite(a).all(),
        f"min={mn:.0f} p50={p50:.0f} p99={p99:.0f} max={mx:.0f}")
    rec("A", "no_tsc_garbage", tgt, int((a > 1e9).sum()) == 0,
        f"{int((a>1e9).sum())} samples > 1s")
    # high-magnitude near-constant series = fixed-delay artifact (throughput only;
    # latency-mode tight clustering is expected and excluded by the p50>5ms guard)
    plateau = (p50 > 5e6) and (cv < 0.05) and ((mx-mn)/p50 < 0.1)
    rec("A", "not_constant_plateau", tgt, not plateau,
        f"p50={p50/1e6:.2f}ms cv={cv:.3f}")

# ============================================================
# FAMILY B — cross-benchmark (within a mode)
# ============================================================
for mode in MODES:
    for pat in PATTERNS:
        # Endpoint payload scaling: the largest payload must cost at least as much
        # as the smallest (adjacent-pair checks are too noise-sensitive, and in
        # latency mode small payloads are legitimately ~flat / transit-bound).
        lo = med((pat, "IPC", mode, SIZES[0]), "p50")
        hi = med((pat, "IPC", mode, SIZES[-1]), "p50")
        if lo is not None and hi is not None:
            rec("B", f"payload_scaling[{mode}]", f"{pat}/IPC", hi >= lo * 0.9,
                f"{SIZES[0]}B p50={lo/1e3:.0f}us -> {SIZES[-1]}B p50={hi/1e3:.0f}us")
    # cross-run p50 stability
    for k in agg:
        if k[2] != mode: continue
        xs = [d["p50"] for d in agg[k]]
        if len(xs) >= 2:
            m = np.median(xs); spread = (max(xs)-min(xs))/m if m else 0
            rec("B", f"cross_run_p50[{mode}]", "/".join(map(str, k)), spread <= 0.6,
                f"spread={spread:.2f} {[int(v) for v in xs]}")

# ============================================================
# FAMILY M — cross-mode: latency mode must beat throughput mode
# ============================================================
for pat in PATTERNS:
    for tr in TRANSPORTS:
        for sz in SIZES:
            lt = med((pat, tr, "latency", sz), "p50")
            tp = med((pat, tr, "throughput", sz), "p50")
            if lt is not None and tp is not None:
                rec("M", "latency_below_throughput", f"{pat}/{tr}/{sz}", lt < tp,
                    f"latency p50={lt/1e3:.0f}us vs throughput p50={tp/1e3:.0f}us")

# ============================================================
# FAMILY L — latency-mode structural
# ============================================================
for pat in PATTERNS:
    for tr in TRANSPORTS:
        # us-scale floor: the best message should be tens of us, not ms
        mn = med((pat, tr, "latency", 8), "mn")
        if mn is not None:
            rec("L", "us_floor", f"{pat}/{tr}", mn < 200_000,
                f"latency min={mn/1e3:.1f}us (expect < 200us)")
    # extra hop: DealerRouter latency p50 > Dealer latency p50
    for tr in TRANSPORTS:
        dr = med(("DealerRouter", tr, "latency", 64), "p50")
        de = med(("Dealer", tr, "latency", 64), "p50")
        if dr is not None and de is not None:
            rec("L", "router_adds_latency", f"{tr}", dr > de,
                f"DealerRouter p50={dr/1e3:.0f}us vs Dealer p50={de/1e3:.0f}us")
    # small payloads are transit-bound, so latency should be ~flat from 8B..1024B;
    # a big jump there means bandwidth/queueing leaked into the latency measurement
    for pat in PATTERNS:
        for tr in TRANSPORTS:
            lo = med((pat, tr, "latency", 8), "p50")
            mid = med((pat, tr, "latency", 1024), "p50")
            if lo and mid:
                rec("L", "small_payload_flat", f"{pat}/{tr}", mid <= lo * 3.0,
                    f"8B p50={lo/1e3:.0f}us -> 1024B p50={mid/1e3:.0f}us (ratio {mid/lo:.1f})")

# ============================================================
# FAMILY T — throughput sanity (throughput mode)
# ============================================================
for k in agg:
    if k[2] != "throughput": continue
    m = med(k, "msgs")
    if m is not None:
        # plausible band for this box; a regression like the affinity collapse
        # would push msg/s far below the floor.
        rec("T", "throughput_band", "/".join(map(str, k)), 10_000 <= m <= 60_000_000,
            f"{m/1e6:.2f}M msg/s")
# router is the bottleneck: DealerRouter throughput < Dealer throughput
for tr in TRANSPORTS:
    for sz in SIZES:
        dr = med(("DealerRouter", tr, "throughput", sz), "msgs")
        de = med(("Dealer", tr, "throughput", sz), "msgs")
        if dr is not None and de is not None:
            rec("T", "router_is_bottleneck", f"{tr}/{sz}", dr < de,
                f"DealerRouter {dr/1e6:.2f}M/s vs Dealer {de/1e6:.2f}M/s")
# bandwidth grows with payload (throughput mode): 4096B MB/s > 8B MB/s
for pat in PATTERNS:
    for tr in TRANSPORTS:
        lo = med((pat, tr, "throughput", 8), "mb")
        hi = med((pat, tr, "throughput", 4096), "mb")
        if lo is not None and hi is not None:
            rec("T", "bandwidth_grows", f"{pat}/{tr}", hi > lo,
                f"8B={lo:.0f} MB/s -> 4096B={hi:.0f} MB/s")

# ============================================================
# FAMILY P — CPU placement (from save-hists run)
# ============================================================
recv_files = glob.glob(f"{DIR}/indiv_*.recv_cpus.txt")
core0 = sum(1 for fn in recv_files
            if set(int(x) for x in open(fn) if x.strip()) == {0})
if recv_files:
    rec("P", "receiver_core_spread", "all",
        not (core0 == len(recv_files)),
        f"{core0}/{len(recv_files)} receivers ran entirely on core 0")
log = f"{DIR}/savehists.log"
if os.path.exists(log):
    pins = re.findall(r"pinned current thread to cpu (\d+)", open(log).read())
    if pins:
        from collections import Counter
        c = Counter(pins); core, nn = c.most_common(1)[0]
        rec("P", "pin_distribution", "savehists",
            nn/len(pins) <= 0.5,
            f"{nn}/{len(pins)} ({nn/len(pins):.0%}) on cpu {core}; {len(c)} cores used")

# ============================================================
# FAMILY I — informational (reported, never fails the run)
# ============================================================
# Cross-run throughput stability. Stable for bottlenecked/large configs; fast
# small-payload IPC configs drain in <1ms at N=10k so the timed span is jitter-
# dominated (up to ~3x variance). Informational: it flags where the throughput
# number is only approximate, not a regression.
for k in agg:
    if k[2] != "throughput": continue
    xs = [d["msgs"] for d in agg[k]]
    if len(xs) >= 2 and np.median(xs) > 0:
        m = np.median(xs); spread = (max(xs)-min(xs))/m
        rec("I", "throughput_cross_run", "/".join(map(str, k)), spread <= 0.75,
            f"msgs/s spread={spread:.2f} {[int(v) for v in xs]}")
# Latency-mode tail health: p99/p50. Lock-step patterns are tight (~1-2x); a
# large ratio flags a jittery distribution (e.g. paced PUB/SUB has no flow
# control). Informational because it reflects OS/transport jitter, not a bug.
for pat in PATTERNS:
    for tr in TRANSPORTS:
        p50 = med((pat, tr, "latency", 8), "p50")
        p99 = med((pat, tr, "latency", 8), "p99")
        if p50 and p99:
            ratio = p99 / p50
            rec("I", "latency_tail_ratio", f"{pat}/{tr}/8", ratio <= 5.0,
                f"p99/p50={ratio:.1f} (p50={p50/1e3:.0f}us p99={p99/1e3:.0f}us)")

# ============================================================
# Report
# ============================================================
TITLES = {
    "A": "per-finding sanity", "B": "cross-benchmark", "M": "cross-mode",
    "L": "latency structural", "T": "throughput sanity", "P": "CPU placement",
    "I": "informational (tail health)",
}
hard_fail = 0
for fam in ["A", "B", "M", "L", "T", "P", "I"]:
    rows = [r for r in results if r[0] == fam]
    flagged = [r for r in rows if r[3] == "FAIL"]
    if fam != "I":
        hard_fail += len(flagged)
    label = "FLAG" if fam == "I" else "FAIL"
    print(f"\n{'='*80}\nFAMILY {fam} — {TITLES[fam]}  ({len(rows)} tests, {len(flagged)} {label})\n{'='*80}")
    for r in flagged:
        print(f"  {label}  {r[1]:<24} {r[2]:<22} {r[4]}")
    if not flagged:
        print("  (all clear)")
print(f"\n{'#'*80}\nHARD FAILURES: {hard_fail}  (Family I is informational)\n{'#'*80}")
sys.exit(1 if hard_fail else 0)

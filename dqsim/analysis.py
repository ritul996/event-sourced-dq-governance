"""Aggregate results: statistics, tables (CSV/Markdown) and figures.
Usage: python analysis.py <results_dir>"""
import json
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from scipy.stats import mannwhitneyu

rng = np.random.default_rng(0)


def boot_ci(x, f=np.median, B=2000):
    x = np.asarray(x)
    s = [f(rng.choice(x, len(x))) for _ in range(B)]
    return float(np.percentile(s, 2.5)), float(np.percentile(s, 97.5))


def cliffs_delta(a, b, max_n=4000):
    a = rng.choice(a, min(len(a), max_n), replace=False)
    b = rng.choice(b, min(len(b), max_n), replace=False)
    gt = (a[:, None] > b[None, :]).sum()
    lt = (a[:, None] < b[None, :]).sum()
    return float((gt - lt) / (len(a) * len(b)))


def summarize(x):
    x = np.asarray(x)
    lo, hi = boot_ci(x)
    return {"n": int(len(x)), "median": float(np.median(x)), "median_ci": [lo, hi],
            "p95": float(np.percentile(x, 95)), "p99": float(np.percentile(x, 99)),
            "mean": float(np.mean(x)), "max": float(np.max(x))}


def main():
    res = Path(sys.argv[1])
    out = {}
    stress = json.loads((res / "es_stress.json").read_text())
    light = json.loads((res / "es_light.json").read_text())
    poll = json.loads((res / "es_poll.json").read_text()) if (res / "es_poll.json").exists() else []
    bl = json.loads((res / "baseline" / "baseline_latency.json").read_text())
    bt = json.loads((res / "baseline" / "baseline_build_times.json").read_text())

    # ---- latency ----
    lat = {"ES light load (N=20)": np.concatenate([m["latency_s"] for m in light])}
    for n in (1, 5, 10, 20):
        lat[f"ES saturated (N={n})"] = np.concatenate(
            [m["latency_s"] for m in stress if m["n_authors"] == n])
    names = {"scheduled_5min": "Baseline scheduled Δ=5 min",
             "scheduled_15min": "Baseline scheduled Δ=15 min",
             "scheduled_30min": "Baseline scheduled Δ=30 min",
             "on_demand_selective_10ph": "Baseline on-demand (10/h)",
             "on_demand_selective": "Baseline on-demand (50/h)"}
    for k, label in names.items():
        lat[label] = np.asarray(bl[k])
    out["latency"] = {k: summarize(v) for k, v in lat.items()}

    es = lat["ES light load (N=20)"]
    tests = {}
    for label in names.values():
        b = lat[label]
        u = mannwhitneyu(es, b, alternative="less")
        tests[label] = {"U": float(u.statistic), "p": float(u.pvalue),
                        "cliffs_delta": cliffs_delta(es, b),
                        "median_speedup": float(np.median(b) / np.median(es))}
    out["tests_vs_es_light"] = tests

    # ---- build times ----
    out["build_times"] = {kind: {e: {"mean": float(np.mean(v)), "sd": float(np.std(v, ddof=1))}
                                 for e, v in d.items()} for kind, d in bt.items()}
    out["pipeline_full_mean_s"] = sum(np.mean(v) for v in bt["full"].values())
    out["pipeline_selective_mean_s"] = sum(np.mean(v) for v in bt["selective"].values())

    # ---- contention (Zipf hot rules, saturated) ----
    thr = {}
    for n in (1, 5, 10, 20):
        ms = [m for m in stress if m["n_authors"] == n]
        thr[n] = {"commands_accepted_per_s": float(np.mean([m["handler_stats"]["accepted"] / m["cmd_phase_s"] for m in ms])),
                  "conflicts_per_command": float(np.mean([m["conflicts"] / m["n_commands"] for m in ms])),
                  "retries_exhausted": int(sum(m["retries_exhausted"] for m in ms)),
                  "approved": int(sum(m["approved"] for m in ms)),
                  "reached_prod": int(sum(m["reached_prod"] for m in ms)),
                  "superseded_before_prod": int(sum(m.get("superseded_before_prod", 0) for m in ms)),
                  "events_per_run": float(np.mean([m["events"] for m in ms]))}
    out["contention"] = thr
    # ---- offered-load sweep ----
    load = json.loads((res / "es_load.json").read_text()) if (res / "es_load.json").exists() else []
    lsweep = {}
    for lab in sorted({m["label"] for m in load}, key=lambda s: float(s[4:])):
        ms = [m for m in load if m["label"] == lab]
        l = np.concatenate([m["latency_s"] for m in ms])
        lsweep[lab] = {"offered_per_s": float(lab[4:]),
                       "achieved_per_s": float(np.mean([m["reached_prod"] / m["wall_s"] for m in ms])),
                       "arrival_per_s": float(np.mean([m["approved"] / m["cmd_phase_s"] for m in ms])),
                       "latency": summarize(l)}
    out["load_sweep"] = lsweep
    out["baseline_max_changes_per_min_on_demand"] = 60 / out["pipeline_selective_mean_s"]
    out["baseline_max_changes_per_s_on_demand"] = 1 / out["pipeline_selective_mean_s"]

    # ---- integrity ----
    allruns = stress + light + poll + load
    out["integrity"] = {
        "runs": len(allruns),
        "audit_state_checks": int(sum(m["audit"]["state_checks"] for m in allruns)),
        "audit_matched": int(sum(m["audit"]["matched"] for m in allruns)),
        "chain_valid_all": all(m["audit"]["chain_valid"] for m in allruns),
        "version_anomalies": int(sum(m["audit"]["rules_with_version_anomaly"] for m in allruns)),
        "final_projection_matches_truth_all": all(m["final_projection_matches_truth"] for m in allruns),
        "dup_delivered": int(sum(m["dup_delivered"] for m in allruns)),
        "dup_skipped": int(sum(m["dup_skipped"] for m in allruns)),
        "injected": {k: int(sum(m["injected"][k] for m in allruns)) for k in allruns[0]["injected"]},
        "blocked": {k: int(sum(m["blocked"][k] for m in allruns)) for k in allruns[0]["blocked"]},
        "rollback_latency": summarize(np.concatenate([m["rollback_latency_s"] for m in allruns])),
    }
    tam = [m["tamper"] for m in stress if "tamper" in m]
    out["tamper"] = tam[0] if tam else None
    if poll:
        out["poll_sensitivity"] = {
            str(p): summarize(np.concatenate([m["latency_s"] for m in poll if m["poll"] == p]))
            for p in sorted({m["poll"] for m in poll})}
        out["poll_sensitivity"]["0.1"] = out["latency"]["ES light load (N=20)"]
    det = res / "detection.json"
    if det.exists():
        out["detection"] = json.loads(det.read_text())
    (res / "summary.json").write_text(json.dumps(out, indent=1))

    # ---- figures ----
    figd = res / "figures"
    figd.mkdir(exist_ok=True)
    plt.rcParams.update({"font.size": 9, "font.family": "serif"})

    fig, ax = plt.subplots(figsize=(3.5, 2.6))
    for label, style in [("ES light load (N=20)", "-"), ("ES saturated (N=20)", "--"),
                         ("Baseline on-demand (10/h)", "-."),
                         ("Baseline scheduled Δ=5 min", ":"),
                         ("Baseline scheduled Δ=15 min", ":"), ("Baseline scheduled Δ=30 min", ":")]:
        x = np.sort(lat[label])
        ax.plot(x, np.arange(1, len(x) + 1) / len(x), style, label=label, lw=1.2)
    ax.set_xscale("log")
    ax.set_xlabel("Approval → active in prod (s, log)")
    ax.set_ylabel("Cumulative fraction")
    ax.legend(fontsize=6, loc="lower right")
    ax.grid(alpha=.3, which="both")
    fig.tight_layout()
    fig.savefig(figd / "fig3_latency_cdf.png", dpi=300)
    fig.savefig(figd / "fig3_latency_cdf.pdf")

    fig, ax1 = plt.subplots(figsize=(3.5, 2.4))
    if lsweep:
        xs = [v["arrival_per_s"] for v in lsweep.values()]
        ax1.plot(xs, [v["latency"]["median"] for v in lsweep.values()], "o-", label="ES median")
        ax1.plot(xs, [v["latency"]["p95"] for v in lsweep.values()], "s--", label="ES p95")
        ax1.set_xscale("log")
        ax1.set_yscale("log")
        ax1.axvline(out["baseline_max_changes_per_s_on_demand"], ls=":", color="gray",
                    label="Baseline on-demand capacity")
        ax1.set_xlabel("Offered load (approved changes / s, log)")
        ax1.set_ylabel("Propagation latency (s, log)")
        ax1.legend(fontsize=6)
        ax1.grid(alpha=.3, which="both")
    fig.tight_layout()
    fig.savefig(figd / "fig4_throughput.png", dpi=300)
    fig.savefig(figd / "fig4_throughput.pdf")
    print(json.dumps(out, indent=1)[:6000])


if __name__ == "__main__":
    main()

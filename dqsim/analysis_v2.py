"""Aggregate all results (original + review-v2 experiments) into numbers.json and figures.
Usage: python analysis_v2.py <data_dir> <results_dir> <out_dir>"""
import json
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from scipy.stats import mannwhitneyu

rng = np.random.default_rng(0)


def boot_ci(x, B=2000):
    x = np.asarray(x)
    s = [np.median(rng.choice(x, len(x))) for _ in range(B)]
    return [float(np.percentile(s, 2.5)), float(np.percentile(s, 97.5))]


def cliffs(a, b, max_n=4000):
    a = rng.choice(a, min(len(a), max_n), replace=False)
    b = rng.choice(b, min(len(b), max_n), replace=False)
    return float(((a[:, None] > b[None, :]).sum() - (a[:, None] < b[None, :]).sum()) / (len(a) * len(b)))


def summ(x):
    x = np.asarray(x, dtype=float)
    return {"n": int(len(x)), "median": float(np.median(x)), "ci": boot_ci(x),
            "p95": float(np.percentile(x, 95)), "p99": float(np.percentile(x, 99)), "mean": float(x.mean())}


def lat(ms):
    return np.concatenate([m["latency_s"] for m in ms])


def high_risk_share(run_dir):
    n = h = 0
    for f in (Path(run_dir) / "log").glob("p*.jsonl"):
        for line in f.read_text().splitlines():
            ev = json.loads(line)
            if ev["type"] == "APPROVED":
                n += 1
                h += bool(ev.get("justification"))
    return h / n if n else None


def main():
    data_dir, res, out = Path(sys.argv[1]), Path(sys.argv[2]), Path(sys.argv[3])
    out.mkdir(parents=True, exist_ok=True)
    N = {}
    ds = json.loads((data_dir / "data_summary.json").read_text())
    N["data"] = ds
    old = json.loads((res / "summary.json").read_text())
    light = json.loads((res / "es_light.json").read_text())
    bl = json.loads((res / "baseline" / "baseline_latency.json").read_text())

    # ---------- latency (RQ1) ----------
    L = {"es_light": lat(light)}
    rep = json.loads((res / "v2_es_light.json").read_text())
    b2 = json.loads((res / "v2_b2_light.json").read_text())
    L["es_rep"] = lat(rep)
    L["b2"] = lat(b2)
    for k in ("on_demand_selective_10ph", "on_demand_selective", "scheduled_5min", "scheduled_15min",
              "scheduled_30min"):
        L[k] = np.asarray(bl[k])
    N["latency"] = {k: summ(v) for k, v in L.items()}
    tests = {}
    for k in ("b2", "on_demand_selective_10ph", "on_demand_selective", "scheduled_5min",
              "scheduled_15min", "scheduled_30min"):
        u = mannwhitneyu(L["es_light"], L[k], alternative="two-sided")
        tests[k] = {"p": float(u.pvalue), "delta": cliffs(L["es_light"], L[k]),
                    "ratio": float(np.median(L[k]) / np.median(L["es_light"]))}
    u = mannwhitneyu(L["es_rep"], L["b2"], alternative="two-sided")
    tests["rep_vs_b2"] = {"p": float(u.pvalue), "delta": cliffs(L["es_rep"], L["b2"]),
                          "diff_median_ms": float((np.median(L["es_rep"]) - np.median(L["b2"])) * 1000)}
    u = mannwhitneyu(L["es_light"], L["es_rep"], alternative="two-sided")
    tests["light_vs_rep"] = {"p": float(u.pvalue), "delta": cliffs(L["es_light"], L["es_rep"])}
    N["tests"] = tests
    N["build_times"] = old["build_times"]
    N["pipeline_full_s"] = old["pipeline_full_mean_s"]
    N["pipeline_selective_s"] = old["pipeline_selective_mean_s"]
    N["baseline_capacity_per_s"] = old["baseline_max_changes_per_s_on_demand"]

    # ---------- capacity / contention / poll (RQ2) ----------
    N["load_sweep"] = old["load_sweep"]
    N["contention"] = old["contention"]
    stress = json.loads((res / "es_stress.json").read_text())
    N["contention_latency"] = {n: summ(lat([m for m in stress if m["n_authors"] == n])) for n in (1, 5, 10, 20)}
    N["poll"] = {k: {"median": v["median"], "p95": v["p95"], "n": v["n"]} for k, v in old["poll_sensitivity"].items()}

    # ---------- auditability (RQ3) ----------
    N["integrity_v1"] = {k: old["integrity"][k] for k in ("runs", "audit_state_checks", "audit_matched",
                                                          "chain_valid_all", "version_anomalies",
                                                          "final_projection_matches_truth_all",
                                                          "dup_delivered", "dup_skipped", "injected",
                                                          "blocked")}
    N["tamper_es"] = old["tamper"]
    N["tamper_b2"] = b2[0]["b2_tamper"]
    N["b2_recon"] = {"checks": int(sum(m["audit"]["state_checks"] for m in b2)),
                     "matched": int(sum(m["audit"]["matched"] for m in b2)),
                     "final_ok": all(m["final_projection_matches_truth"] for m in b2)}
    N["rep_recon"] = {"checks": int(sum(m["audit"]["state_checks"] for m in rep)),
                      "matched": int(sum(m["audit"]["matched"] for m in rep)),
                      "final_ok": all(m["final_projection_matches_truth"] for m in rep),
                      "chain_ok": all(m["audit"]["chain_valid"] for m in rep)}
    inv = json.loads((res / "v2_invariants.json").read_text())
    runs = inv["runs"]
    tot = {c: sum(r["violations"][c] for r in runs.values()) for c in [f"C{i}" for i in range(1, 11)]}
    N["invariants"] = {"runs": len(runs), "events": sum(r["events"] for r in runs.values()),
                       "violations": tot, "self_test": inv["self_test"], "trials_per_kind": 40}

    # ---------- policy (RQ4) ----------
    def pol(ms):
        inj = {k: sum(m["injected"][k] for m in ms) for k in ms[0]["injected"]}
        blk = {k: sum(m["blocked"][k] for m in ms) for k in ms[0]["blocked"]}
        return {"injected": inj, "blocked": blk, "dup_delivered": sum(m["dup_delivered"] for m in ms),
                "dup_skipped": sum(m["dup_skipped"] for m in ms)}
    mixr = {}
    for f in sorted(res.glob("v2_mix_*.json")):
        name = f.stem[len("v2_mix_"):]
        ms = json.loads(f.read_text())
        mixr[name] = ms
    allv2_es = rep + [m for ms in mixr.values() for m in ms]
    N["policy_v2_es"] = pol(allv2_es)
    N["policy_b2"] = pol(b2)
    N["rollback"] = {"es_light": summ(np.concatenate([m["rollback_latency_s"] for m in light])),
                     "es_rep": summ(np.concatenate([m["rollback_latency_s"] for m in rep])),
                     "b2": summ(np.concatenate([m["rollback_latency_s"] for m in b2]))}

    # ---------- workload mix (RQ5) ----------
    mixes = {"baseline": rep}
    mixes.update(mixr)
    N["mix"] = {}
    for name, ms in mixes.items():
        tag = "es_light" if name == "baseline" else f"mix_{name}"
        shares = [high_risk_share(res / "_keep" / f"{tag}_r{i}") for i in range(len(ms))]
        inv_v = sum(sum(runs[f"{tag}_r{i}"]["violations"].values()) for i in range(len(ms)) if f"{tag}_r{i}" in runs)
        x = lat(ms)
        N["mix"][name] = {"mix": ms[0]["mix"], "reps": len(ms), **summ(x),
                          "high_risk_share": float(np.mean(shares)),
                          "rollbacks": int(sum(m["rollbacks"] for m in ms)),
                          "recon": float(sum(m["audit"]["matched"] for m in ms) / sum(m["audit"]["state_checks"] for m in ms)),
                          "final_ok": all(m["final_projection_matches_truth"] for m in ms),
                          "invariant_violations": int(inv_v)}
    N["detection"] = old["detection"]
    (out / "numbers.json").write_text(json.dumps(N, indent=1))

    # ---------- figures ----------
    plt.rcParams.update({"font.family": "serif", "font.serif": ["TeX Gyre Termes", "Liberation Serif"],
                         "mathtext.fontset": "stix", "font.size": 7.5, "axes.linewidth": 0.6,
                         "xtick.major.width": 0.6, "ytick.major.width": 0.6})
    fig, ax = plt.subplots(figsize=(3.45, 2.35))
    series = [("es_light", "ES (event-sourced)", "-", "#1f4e79", 1.4),
              ("b2", "B2: table + trigger", "-", "#b5651d", 1.2),
              ("on_demand_selective_10ph", "B1: on-demand, 10/h", "--", "#444444", 1.0),
              ("scheduled_5min", "B1: every 5 min", ":", "#666666", 1.1),
              ("scheduled_15min", "B1: every 15 min", "-.", "#888888", 1.0),
              ("scheduled_30min", "B1: every 30 min", (0, (5, 1, 1, 1, 1, 1)), "#aaaaaa", 1.0)]
    for k, label, ls, c, lw in series:
        x = np.sort(L[k])
        ax.plot(x, np.arange(1, len(x) + 1) / len(x), ls=ls, color=c, lw=lw, label=label)
    ax.set_xscale("log")
    ax.set_xlim(1e-4, 3e3)
    ax.set_xlabel("Approval-to-production latency (s, log scale)")
    ax.set_ylabel("Cumulative fraction")
    ax.grid(alpha=.25, which="major", lw=0.5)
    ax.legend(fontsize=5.6, loc="center", bbox_to_anchor=(0.556, 0.28), frameon=False, handlelength=2.0, handletextpad=0.5)
    fig.tight_layout(pad=0.3)
    fig.savefig(out / "fig3_latency_cdf.png", dpi=600)
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(3.45, 2.15))
    ls_ = old["load_sweep"]
    keys = sorted(ls_, key=lambda k: ls_[k]["arrival_per_s"])
    xs = [ls_[k]["arrival_per_s"] for k in keys]
    ax.plot(xs, [ls_[k]["latency"]["median"] for k in keys], "o-", ms=3, lw=1.1, color="#1f4e79", label="ES median")
    ax.plot(xs, [ls_[k]["latency"]["p95"] for k in keys], "s--", ms=3, lw=1.0, color="#1f4e79", alpha=.7, label="ES p95")
    cap = old["baseline_max_changes_per_s_on_demand"]
    ax.axvline(cap, ls=":", color="#444", lw=1.0)
    ax.text(cap * 1.12, 0.35, "B1 on-demand\ncapacity", fontsize=5.6, va="center")
    ax.set_xscale("log"); ax.set_yscale("log")
    ax.set_xlim(0.02, 60)
    ax.set_xlabel("Approved changes per second (log scale)")
    ax.set_ylabel("Latency (s, log scale)")
    ax.grid(alpha=.25, which="major", lw=0.5)
    ax.legend(fontsize=6, loc="upper center", frameon=False)
    fig.tight_layout(pad=0.3)
    fig.savefig(out / "fig4_load.png", dpi=600)
    plt.close(fig)
    print(json.dumps({k: N[k] for k in ("tests", "invariants", "tamper_b2", "b2_recon", "rep_recon")}, indent=1))
    print(json.dumps({k: {kk: vv for kk, vv in v.items() if kk in ("median", "p95", "n", "high_risk_share", "recon", "invariant_violations", "rollbacks")} for k, v in N["mix"].items()}, indent=1))
    print({k: (round(v["median"], 4), round(v["p95"], 4), v["n"]) for k, v in N["latency"].items()})


if __name__ == "__main__":
    main()

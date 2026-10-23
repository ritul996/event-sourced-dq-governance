"""Review-v3 analysis: adds per-run statistics, baseline B3, the impact gate, ablations,
verification cost and an approval-time lead-time model to the v2 numbers.
Usage: python analysis_v3.py <results_dir> <v2_numbers.json> <out_dir>"""
import json
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from scipy.stats import mannwhitneyu

rng = np.random.default_rng(7)
CS = ["C1", "C2", "C3", "C4", "C5", "C6", "C7", "C8", "C9", "C10"]


def cliffs(a, b):
    a, b = np.asarray(a), np.asarray(b)
    return float(((a[:, None] > b[None, :]).sum() - (a[:, None] < b[None, :]).sum()) / (len(a) * len(b)))


def boot_ci(x, B=2000):
    x = np.asarray(x)
    s = [np.median(rng.choice(x, len(x))) for _ in range(B)]
    return [float(np.percentile(s, 2.5)), float(np.percentile(s, 97.5))]


def summ(x):
    x = np.asarray(x, dtype=float)
    return {"n": int(len(x)), "median": float(np.median(x)), "ci": boot_ci(x), "p95": float(np.percentile(x, 95)),
            "p99": float(np.percentile(x, 99)), "mean": float(x.mean())}


def main():
    res, v2p, out = Path(sys.argv[1]), Path(sys.argv[2]), Path(sys.argv[3])
    out.mkdir(parents=True, exist_ok=True)
    N = json.loads(v2p.read_text())
    load = lambda f: json.loads((res / f).read_text())
    light, rep, b2 = load("es_light.json"), load("v2_es_light.json"), load("v2_b2_light.json")
    b3, gate = load("v3_b3_light.json"), load("v3_es_gate.json")
    bl = load("baseline/baseline_latency.json")

    # ---------------- per-run medians (fixes pooling of non-independent changes) ----------------
    runs = {"es_light": [float(np.median(m["latency_s"])) for m in light],
            "es_rep": [float(np.median(m["latency_s"])) for m in rep],
            "b2": [float(np.median(m["latency_s"])) for m in b2],
            "b3": [float(np.median(m["latency_s"])) for m in b3],
            "es_gate": [float(np.median(m["latency_normal_s"])) for m in gate]}
    for k in ("on_demand_selective_10ph", "on_demand_selective", "scheduled_5min", "scheduled_15min", "scheduled_30min"):
        x = np.asarray(bl[k])
        runs[k] = [float(np.median(c)) for c in np.split(x, 10)]
    N["run_medians"] = {k: {"values": v, "mean": float(np.mean(v)), "sd": float(np.std(v, ddof=1)),
                            "min": float(np.min(v)), "max": float(np.max(v))} for k, v in runs.items()}
    pairs = [("es_light", k) for k in ("b2", "b3", "on_demand_selective_10ph", "on_demand_selective",
                                       "scheduled_5min", "scheduled_15min", "scheduled_30min")]
    pairs += [("es_rep", "b2"), ("es_rep", "b3"), ("b2", "b3"), ("es_light", "es_rep"), ("es_rep", "es_gate")]
    N["run_tests"] = {f"{a}__{b}": {"p": float(mannwhitneyu(runs[a], runs[b], alternative="two-sided").pvalue),
                                    "delta": cliffs(runs[a], runs[b]),
                                    "diff_ms": float((np.mean(runs[a]) - np.mean(runs[b])) * 1000)} for a, b in pairs}

    # ---------------- B3 ----------------
    N["latency"]["b3"] = summ(np.concatenate([m["latency_s"] for m in b3]))
    N["b3_recon"] = {"checks": int(sum(m["audit"]["state_checks"] for m in b3)),
                     "matched": int(sum(m["audit"]["matched"] for m in b3)),
                     "final_ok": all(m["final_projection_matches_truth"] for m in b3)}
    N["tamper_b3"] = b3[0]["b3_tamper"]
    N["policy_b3"] = {"injected": {k: sum(m["injected"][k] for m in b3) for k in b3[0]["injected"]},
                      "blocked": {k: sum(m["blocked"][k] for m in b3) for k in b3[0]["blocked"]}}

    # ---------------- impact gate ----------------
    g = {k: sum(m["gate"][k] for m in gate) for k in ("faulty_approved", "faulty_blocked", "faulty_reached_prod",
                                                       "normal_approved", "normal_blocked", "gate_checks")}
    g["faulty_blocked_env"] = {e: sum(m["gate"]["faulty_blocked_env"][e] for m in gate) for e in ("dev", "uat", "prod_test")}
    g["faulty_superseded"] = g["faulty_approved"] - g["faulty_blocked"] - g["faulty_reached_prod"]
    g["latency_normal"] = summ(np.concatenate([m["latency_normal_s"] for m in gate]))
    g["recon"] = float(sum(m["audit"]["matched"] for m in gate) / sum(m["audit"]["state_checks"] for m in gate))
    g["final_ok"] = all(m["final_projection_matches_truth"] for m in gate)
    g["runs"] = len(gate)
    g["errors"] = int(sum(m.get("projector_errors", 0) for m in gate))
    N["gate"] = g
    N["policy_gate"] = {"injected": {k: sum(m["injected"][k] for m in gate) for k in gate[0]["injected"]},
                        "blocked": {k: sum(m["blocked"][k] for m in gate) for k in gate[0]["blocked"]},
                        "dup_delivered": sum(m["dup_delivered"] for m in gate),
                        "dup_skipped": sum(m["dup_skipped"] for m in gate)}
    # which faulty versions slipped through, and why (template and in-scope rows in dev)
    N["gate_policy"] = {"rule": "block if newly flagged > max(5, 10% of in-scope rows) in the gate environment"}

    # ---------------- invariants on gate + ablations ----------------
    inv = load("v3_invariants.json")
    def agg(prefix):
        rs = {k: v for k, v in inv.items() if k.startswith(prefix)}
        return {"runs": len(rs), "violations": {c: int(sum(r["violations"][c] for r in rs.values())) for c in CS},
                "events": int(sum(r["events"] for r in rs.values())),
                "duplicate_promotions": int(sum(r["duplicate_promotions"] for r in rs.values()))}
    N["inv_gate"] = agg("es_gate")
    abl = {}
    for tag in ("abl_nodedup", "abl_nopolicy", "abl_randpart"):
        ms = load(f"v3_{tag}.json")
        abl[tag] = {**agg(tag), "final_ok_runs": int(sum(m["final_projection_matches_truth"] for m in ms)),
                    "recon": float(sum(m["audit"]["matched"] for m in ms) / sum(m["audit"]["state_checks"] for m in ms)),
                    "errors": int(sum(m.get("projector_errors", 0) for m in ms)),
                    "median_s": float(np.median(np.concatenate([m["latency_s"] for m in ms]))),
                    "accepted_violations": {k: int(sum(m["injected"][k] - m["blocked"][k] for m in ms)) for k in ms[0]["injected"]}}
    N["ablation"] = abl

    # ---------------- verification and hashing cost ----------------
    cost = load("v3_cost.json")
    es_ev = sum(c["events"] for c in cost["es"])
    N["cost"] = {"es_events": es_ev, "es_logs": len(cost["es"]),
                 "chain_us_per_event": 1e6 * sum(c["chain_s"] for c in cost["es"]) / es_ev,
                 "replay_us_per_event": 1e6 * sum(c["replay_s"] for c in cost["es"]) / es_ev,
                 "verifier_us_per_event": 1e6 * sum(c["verifier_s"] for c in cost["es"]) / es_ev,
                 "b3_rows": sum(c["rows"] for c in cost["b3"]),
                 "b3_chain_us_per_row": 1e6 * sum(c["chain_s"] for c in cost["b3"]) / sum(c["rows"] for c in cost["b3"]),
                 "all_ok": all(c["ok"] for c in cost["es"] + cost["b3"]),
                 "append_hash_us_per_event": 1e6 * sum(m["hash_seconds"] for m in gate) / sum(m["events"] for m in gate)}

    # ---------------- approval time + propagation: end-to-end lead time model ----------------
    pools = {"ES": np.concatenate([m["latency_s"] for m in light]),
             "B3": np.concatenate([m["latency_s"] for m in b3]),
             "B1 on-demand": np.asarray(bl["on_demand_selective_10ph"]),
             "B1 every 5 min": np.asarray(bl["scheduled_5min"]),
             "B1 every 30 min": np.asarray(bl["scheduled_30min"])}
    lead = {}
    for med_min in (15, 120, 1440):
        A = rng.lognormal(np.log(med_min * 60), 1.0, 200000)
        lead[str(med_min)] = {}
        for k, pool in pools.items():
            l = rng.choice(pool, len(A))
            L = A + l
            lead[str(med_min)][k] = {"median_min": float(np.median(L) / 60), "p95_min": float(np.percentile(L, 95) / 60),
                                     "prop_share": float(l.mean() / L.mean())}
    N["lead_time"] = lead

    (out / "numbers_v3.json").write_text(json.dumps(N, indent=1))

    # ---------------- figures (journal width) ----------------
    plt.rcParams.update({"font.family": "serif", "font.serif": ["TeX Gyre Termes", "Liberation Serif"],
                         "mathtext.fontset": "stix", "font.size": 9.5, "axes.linewidth": 0.7})
    L = {"es": np.concatenate([m["latency_s"] for m in light]), "b2": np.concatenate([m["latency_s"] for m in b2]),
         "b3": np.concatenate([m["latency_s"] for m in b3])}
    for k in ("on_demand_selective_10ph", "scheduled_5min", "scheduled_15min", "scheduled_30min"):
        L[k] = np.asarray(bl[k])
    fig, ax = plt.subplots(figsize=(6.0, 3.1))
    series = [("es", "ES (event-sourced)", "-", "#1f4e79", 1.6), ("b2", "B2: table + audit trigger", "-", "#b5651d", 1.3),
              ("b3", "B3: table + hash-chained audit", "--", "#7a3e9d", 1.3),
              ("on_demand_selective_10ph", "B1: dbt on-demand, 10/h", "--", "#444444", 1.1),
              ("scheduled_5min", "B1: dbt every 5 min", ":", "#666666", 1.3),
              ("scheduled_15min", "B1: dbt every 15 min", "-.", "#888888", 1.1),
              ("scheduled_30min", "B1: dbt every 30 min", (0, (5, 1, 1, 1, 1, 1)), "#aaaaaa", 1.1)]
    for k, lab, ls, c, lw in series:
        x = np.sort(L[k])
        ax.plot(x, np.arange(1, len(x) + 1) / len(x), ls=ls, color=c, lw=lw, label=lab)
    ax.set_xscale("log"); ax.set_xlim(1e-4, 3e3)
    ax.set_xlabel("Approval-to-production latency (s, log scale)"); ax.set_ylabel("Cumulative fraction")
    ax.grid(alpha=.25, lw=0.5)
    ax.legend(fontsize=8, loc="center", bbox_to_anchor=(0.555, 0.30), frameon=False, handlelength=2.2)
    fig.tight_layout(pad=0.3); fig.savefig(out / "fig3_latency_cdf.png", dpi=600); plt.close(fig)

    ls_ = N["load_sweep"]
    keys = sorted(ls_, key=lambda k: ls_[k]["arrival_per_s"])
    fig, ax = plt.subplots(figsize=(6.0, 2.8))
    xs = [ls_[k]["arrival_per_s"] for k in keys]
    ax.plot(xs, [ls_[k]["latency"]["median"] for k in keys], "o-", ms=4, lw=1.3, color="#1f4e79", label="ES median")
    ax.plot(xs, [ls_[k]["latency"]["p95"] for k in keys], "s--", ms=4, lw=1.1, color="#1f4e79", alpha=.7, label="ES 95th percentile")
    cap = N["baseline_capacity_per_s"]
    ax.axvline(cap, ls=":", color="#444", lw=1.1)
    ax.text(cap * 1.12, 0.35, "B1 on-demand\ncapacity", fontsize=8.5, va="center")
    ax.set_xscale("log"); ax.set_yscale("log"); ax.set_xlim(0.02, 60)
    ax.set_xlabel("Approved changes per second (log scale)"); ax.set_ylabel("Latency (s, log scale)")
    ax.grid(alpha=.25, lw=0.5); ax.legend(fontsize=8.5, loc="upper center", frameon=False)
    fig.tight_layout(pad=0.3); fig.savefig(out / "fig4_load.png", dpi=600); plt.close(fig)

    print(json.dumps({k: N[k] for k in ("run_tests", "gate", "ablation", "cost", "tamper_b3", "b3_recon")}, indent=1)[:6000])
    print({k: (round(v["mean"], 4), round(v["sd"], 4)) for k, v in N["run_medians"].items()})
    print(json.dumps(N["lead_time"], indent=0)[:1500])


if __name__ == "__main__":
    main()

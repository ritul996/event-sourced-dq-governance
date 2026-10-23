"""Review-v3 experiments.
  b3     ledger-style baseline B3 (B2 + hash-chained audit + anchors), same seeds as ES/B2
  gate   ES with the impact-based promotion gate and 5% injected faulty (typo) changes
  abl    ablations: no deduplication, no command-layer policy, random (not rule-keyed) partitions
  cost   verification / replay / hashing cost on all retained logs
Usage: python run_v3.py <data_dir> <results_dir> [parts]"""
import json
import sys
import time
from pathlib import Path

import audit
import b2_audit
import b3_audit
import invariants
from workload import run_es

ENVS = ["dev", "uat", "prod_test", "prod"]
LIGHT = {"n_authors": 20, "n_commands": 300, "pace": 0.25, "target": "idle"}


def main():
    data_dir, res = Path(sys.argv[1]), Path(sys.argv[2])
    only = sys.argv[3].split(",") if len(sys.argv) > 3 else ["b3", "gate", "abl", "inv", "cost"]
    rules = json.loads((data_dir / "rules_initial.json").read_text())
    env_paths = {e: data_dir / f"{e}.duckdb" for e in ENVS}
    keep = res / "_keep"
    keep.mkdir(parents=True, exist_ok=True)
    pids = invariants.patient_ids(data_dir)

    def runs(tag, kw, n, extra=None, audit_fn=None):
        out = []
        for i in range(n):
            rd = keep / f"{tag}_r{i}"
            t0 = time.time()
            m = run_es(rd, env_paths, rules, seed=1000 + i, **kw)
            m["audit"] = audit_fn(rd) if audit_fn else audit.reconstruction_accuracy(rd, n_samples=200, seed=1000 + i)
            m["label"] = tag
            if extra and i == 0:
                m.update(extra(rd))
            out.append(m)
            lat = sorted(m["latency_s"])
            print(f"[{tag}] rep {i}: {time.time() - t0:.0f}s reached={m['reached_prod']}/{m['approved']} "
                  f"median={lat[len(lat) // 2]:.4f}s audit={m['audit']['accuracy']:.4f} "
                  f"final_ok={m['final_projection_matches_truth']} gate={m.get('gate')} err={m.get('projector_errors')}",
                  flush=True)
            (res / f"v3_{tag}.json").write_text(json.dumps(out))
        return out

    if "b3" in only:
        runs("b3_light", dict(LIGHT, system="b3"), 10,
             extra=lambda rd: {"b3_tamper": b3_audit.tamper_experiment(rd, trials=200)},
             audit_fn=lambda rd: b2_audit.reconstruction_accuracy(rd, n_samples=200))
    if "gate" in only:
        runs("es_gate", dict(LIGHT, gate="impact", fault_rate=0.05), 10)
    if "abl" in only:
        runs("abl_nodedup", dict(LIGHT, idempotent=False), 3)
        runs("abl_nopolicy", dict(LIGHT, enforce_policy=False), 3)
        runs("abl_randpart", dict(LIGHT, key_mode="random"), 3)
    if "inv" in only:
        per = {}
        for d in sorted(keep.iterdir()):
            if not (d / "log").exists() or not d.name.startswith(("es_gate", "abl_")):
                continue
            v, ex, info = invariants.check(*invariants.load_run(d), pids)
            parts = audit.load_log(d / "log")
            seen, dup_prom = set(), 0
            for part in parts:
                for ev in part:
                    if ev["type"] == "PROMOTED":
                        k = (ev["rule_id"], ev["version"], ev["env"])
                        dup_prom += k in seen
                        seen.add(k)
            per[d.name] = {"violations": v, "examples": ex, "duplicate_promotions": dup_prom, **info}
            print(d.name, v, "dup_prom", dup_prom, flush=True)
        (res / "v3_invariants.json").write_text(json.dumps(per, indent=1))
    if "cost" in only:
        cost = {"es": [], "b3": []}
        for d in sorted(keep.iterdir()):
            if (d / "log").exists() and d.name.startswith(("es_light", "mix_", "es_gate")):
                parts = audit.load_log(d / "log")
                n = sum(len(p) for p in parts)
                t0 = time.perf_counter(); ok = audit.verify(parts, audit.anchors_every(parts, 50)); t1 = time.perf_counter()
                hist = audit.replay_history(parts); t2 = time.perf_counter()
                v = invariants.check(*invariants.load_run(d), pids); t3 = time.perf_counter()
                cost["es"].append({"run": d.name, "events": n, "chain_s": t1 - t0, "replay_s": t2 - t1,
                                   "verifier_s": t3 - t2, "ok": ok})
            elif (d / "b2.sqlite").exists() and d.name.startswith("b3_"):
                rows = b3_audit.load_rows(d / "b2.sqlite")
                t0 = time.perf_counter(); ok = b3_audit.verify(rows, b3_audit.anchors_every(rows, 50)); t1 = time.perf_counter()
                cost["b3"].append({"run": d.name, "rows": len(rows), "chain_s": t1 - t0, "ok": ok})
        (res / "v3_cost.json").write_text(json.dumps(cost, indent=1))
        print("cost done", len(cost["es"]), len(cost["b3"]), flush=True)


if __name__ == "__main__":
    main()

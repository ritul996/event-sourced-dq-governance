"""Additional experiments requested in review (v2).

  1. Replication of the light-load event-sourced runs, keeping every log for invariant checks.
  2. Baseline B2 (mutable parameter table + trigger audit) on the identical workload/seeds.
  3. Workload-mix sensitivity (four alternative command mixes).
  4. Independent C1-C10 invariant checks on every kept event-sourced log + mutation self-test.
  5. B2 audit reconstruction and tamper detection.
Usage: python run_new.py <data_dir> <results_dir> [parts=es,b2,mix,inv]
"""
import json
import sys
import time
from pathlib import Path

import audit
import b2_audit
import invariants
from workload import run_es

ENVS = ["dev", "uat", "prod_test", "prod"]
MIXES = {
    "tighten_heavy": {"update": .70, "relax": .10, "create": .10, "rollback": .05, "retire": .05},
    "relax_heavy": {"update": .10, "relax": .70, "create": .10, "rollback": .05, "retire": .05},
    "rollback_heavy": {"update": .30, "relax": .20, "create": .10, "rollback": .35, "retire": .05},
    "create_heavy": {"update": .20, "relax": .15, "create": .55, "rollback": .05, "retire": .05},
}
LIGHT = {"n_authors": 20, "n_commands": 300, "pace": 0.25, "target": "idle"}


def main():
    data_dir, res = Path(sys.argv[1]), Path(sys.argv[2])
    only = sys.argv[3].split(",") if len(sys.argv) > 3 else ["es", "b2", "mix", "inv"]
    rules = json.loads((data_dir / "rules_initial.json").read_text())
    env_paths = {e: data_dir / f"{e}.duckdb" for e in ENVS}
    keep = res / "_keep"
    keep.mkdir(parents=True, exist_ok=True)

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
                  f"final_ok={m['final_projection_matches_truth']}", flush=True)
            (res / f"v2_{tag}.json").write_text(json.dumps(out))
        return out

    if "es" in only:
        runs("es_light", LIGHT, 10)
    if "b2" in only:
        runs("b2_light", dict(LIGHT, system="b2"), 10,
             extra=lambda rd: {"b2_tamper": b2_audit.tamper_experiment(rd, trials=200)},
             audit_fn=lambda rd: b2_audit.reconstruction_accuracy(rd, n_samples=200))
    if "mix" in only:
        for name, mix in MIXES.items():
            runs(f"mix_{name}", dict(LIGHT, mix=mix), 3)
    if "inv" in only:
        pids = invariants.patient_ids(data_dir)
        dirs = sorted(d for d in keep.iterdir() if (d / "log").exists())
        per_run = {}
        for d in dirs:
            v, ex, info = invariants.check(*invariants.load_run(d), pids)
            per_run[d.name] = {"violations": v, "examples": ex, **info}
            print(d.name, v, info, flush=True)
        st = invariants.self_test(dirs, pids, trials_per_kind=40)
        print("self-test", st, flush=True)
        (res / "v2_invariants.json").write_text(json.dumps({"runs": per_run, "self_test": st}, indent=1))


if __name__ == "__main__":
    main()

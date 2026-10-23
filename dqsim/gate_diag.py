"""Gate diagnostics: re-run es_gate seeds with per-check details, to explain misses and false blocks.
Usage: python gate_diag.py <data_dir> <results_dir> <n_reps>"""
import json
import sys
from collections import Counter
from pathlib import Path

from workload import run_es

ENVS = ["dev", "uat", "prod_test", "prod"]
LIGHT = {"n_authors": 20, "n_commands": 300, "pace": 0.25, "target": "idle"}

data_dir, res, n = Path(sys.argv[1]), Path(sys.argv[2]), int(sys.argv[3])
rules = json.loads((data_dir / "rules_initial.json").read_text())
env_paths = {e: data_dir / f"{e}.duckdb" for e in ENVS}
out = []
for i in range(n):
    m = run_es(res / "_diag" / f"gate_r{i}", env_paths, rules, seed=1000 + i, gate="impact", fault_rate=0.05,
               gate_detail=True, **LIGHT)
    out.append({"seed": 1000 + i, "gate": m["gate"], "gate_detail": m["gate_detail"], "faulty_detail": m["faulty_detail"]})
    print(f"rep {i}: {m['gate']}", flush=True)
(res / "v3_gate_diag.json").write_text(json.dumps(out))

# summary
miss = [(r["seed"], f) for r in out for f in r["faulty_detail"] if f["reached_prod"]]
print("faulty reaching prod:", len(miss))
for seed, f in miss:
    checks = [d for r in out if r["seed"] == seed for d in r["gate_detail"] if d["rule"] == f["rule"] and d["version"] == f["version"]]
    print(" ", seed, f["rule"], [(d["env"], d["flagged"], d["baseline"], d["scope"]) for d in checks])
fb = [d for r in out for d in r["gate_detail"] if d["blocked"] and not d["faulty"]]
print("false blocks:", len(fb), Counter(d["kind"] for d in fb), Counter(d["env"] for d in fb))
for d in fb:
    print("  ", d["rule"], d["kind"], d["env"], d["flagged"], d["baseline"], d["scope"])

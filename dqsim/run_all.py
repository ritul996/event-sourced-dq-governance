"""Run the full evaluation. Usage: python run_all.py <data_dir> <results_dir> [reps]"""
import json
import random
import shutil
import sys
import time
from pathlib import Path

import audit
import baseline
from workload import run_es

ENVS = ["dev", "uat", "prod_test", "prod"]


def main():
    data_dir, res = Path(sys.argv[1]), Path(sys.argv[2])
    reps = int(sys.argv[3]) if len(sys.argv) > 3 else 10
    only = sys.argv[4].split(",") if len(sys.argv) > 4 else ["stress", "light", "load", "poll", "baseline"]
    res.mkdir(parents=True, exist_ok=True)
    rules = json.loads((data_dir / "rules_initial.json").read_text())
    env_paths = {e: data_dir / f"{e}.duckdb" for e in ENVS}
    scratch = res / "_runs"

    def es_runs(tag, configs, n_reps=None):
        out = []
        for cfg in configs:
            for i in range(n_reps or reps):
                seed = 1000 + i
                rd = scratch / f"{tag}_{cfg['label']}_r{i}"
                t0 = time.time()
                m = run_es(rd, env_paths, rules, seed=seed, **cfg["kw"])
                m["audit"] = audit.reconstruction_accuracy(rd, n_samples=200, seed=seed)
                m["label"] = cfg["label"]
                if tag == "stress" and cfg["kw"]["n_authors"] == 20 and i == 0:
                    m["tamper"] = audit.tamper_experiment(rd, trials=200)
                    shutil.copytree(rd / "log", res / "sample_log", dirs_exist_ok=True)
                shutil.rmtree(rd)
                out.append(m)
                print(f"[{tag}] {cfg['label']} rep {i}: {time.time() - t0:.0f}s, "
                      f"reached={m['reached_prod']}/{m['approved']}, "
                      f"conflicts={m['conflicts']}, audit={m['audit']['accuracy']:.4f}", flush=True)
                (res / f"es_{tag}.json").write_text(json.dumps(out))
        return out

    if "stress" in only:
        es_runs("stress", [{"label": f"N{n}", "kw": {"n_authors": n, "n_commands": 2000}}
                           for n in (1, 5, 10, 20)])
    if "light" in only:
        # light load: ~4 commands/s system-wide from 20 authors
        es_runs("light", [{"label": "N20_pace", "kw": {"n_authors": 20, "n_commands": 300,
                                                         "pace": 0.25, "target": "idle"}}])
    if "load" in only:
        # offered load sweep (changes/s); 60 s of arrivals per level
        es_runs("load", [{"label": f"rate{r}", "kw": {"n_authors": 20, "n_commands": max(120, 60 * r),
                                                      "pace": 1.0 / r, "target": "idle"}}
                         for r in (1, 2, 5, 10, 20, 40)], n_reps=min(reps, 3))
    if "poll" in only:
        es_runs("poll", [{"label": f"poll{p}", "kw": {"n_authors": 20, "n_commands": 300,
                                                      "pace": 0.25, "poll": p, "target": "idle"}}
                         for p in (0.01, 0.05, 0.5, 1.0)], n_reps=min(reps, 3))
    if "baseline" in only:
        bdir = res / "baseline"
        proj = baseline.gen_project(rules, bdir / "dbt_project", env_paths)
        m = baseline.measure(proj, reps=5)
        (bdir / "baseline_build_times.json").write_text(json.dumps(m, indent=1))
        sims = {}
        for i in range(reps):
            rng = random.Random(2000 + i)
            arr = baseline.poisson_arrivals(2000, 50, rng)   # 50 approved changes per hour
            for d in (5, 15, 30):
                sims.setdefault(f"scheduled_{d}min", []).extend(
                    baseline.sim_scheduled(arr, d * 60, m["full"], rng))
            sims.setdefault("on_demand_selective", []).extend(
                baseline.sim_on_demand(arr, m["selective"], rng))
            arr10 = baseline.poisson_arrivals(2000, 10, rng)  # light load for on-demand
            sims.setdefault("on_demand_selective_10ph", []).extend(
                baseline.sim_on_demand(arr10, m["selective"], rng))
        (bdir / "baseline_latency.json").write_text(json.dumps(sims))
        print({k: sorted(v)[len(v) // 2] for k, v in sims.items()})


if __name__ == "__main__":
    main()

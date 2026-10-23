"""Conventional pipeline-based baseline.

Rules live as code in a dbt project. Deploying a change means running `dbt build` in each
environment in order (dev -> uat -> prod_test -> prod). Build durations are MEASURED by actually
running dbt-core on the DuckDB environment files. Waiting for the scheduler is modeled with a
discrete-event simulation over a Poisson stream of approvals, using the measured build times.
"""
import json
import random
import shutil
import subprocess
import sys
import time
from pathlib import Path

from rules import violation_sql

ENVS = ["dev", "uat", "prod_test", "prod"]


def gen_project(rules, proj_dir, env_paths):
    proj = Path(proj_dir)
    if proj.exists():
        shutil.rmtree(proj)
    (proj / "models" / "rules").mkdir(parents=True)
    (proj / "dbs").mkdir()
    (proj / "dbt_project.yml").write_text(
        "name: dq_rules\nversion: '1.0'\nconfig-version: 2\nprofile: dq_rules\n"
        "model-paths: ['models']\nmodels:\n  dq_rules:\n    +materialized: table\n")
    targets = []
    for e in ENVS:
        dst = proj / "dbs" / f"{e}.duckdb"
        shutil.copy(env_paths[e], dst)
        targets.append(f"    {e}:\n      type: duckdb\n      path: {dst.resolve()}\n      threads: 4\n")
    (proj / "profiles.yml").write_text(
        "dq_rules:\n  target: dev\n  outputs:\n" + "".join(targets))
    for r in rules:
        # {% raw %} stops dbt's Jinja parser from reading UCUM units such as "{#}" as template syntax
        (proj / "models" / "rules" / f"rule_{r['rule_id']}.sql").write_text(
            "{% raw %}\n" + violation_sql(r) + "\n{% endraw %}\n")
    return proj


def dbt_build(proj, target, select=None):
    cmd = ["dbt", "build", "--target", target, "--profiles-dir", str(proj),
           "--project-dir", str(proj), "--quiet"]
    if select:
        cmd += ["--select", select]
    t0 = time.perf_counter()
    res = subprocess.run(cmd, capture_output=True, text=True)
    dt = time.perf_counter() - t0
    if res.returncode != 0:
        raise RuntimeError(res.stdout[-2000:] + res.stderr[-2000:])
    return dt


def measure(proj, reps=3, sel_rule="rule_R0001"):
    out = {"full": {e: [] for e in ENVS}, "selective": {e: [] for e in ENVS}}
    dbt_build(proj, "dev")  # warm-up (parse / manifest)
    for _ in range(reps):
        for e in ENVS:
            out["full"][e].append(dbt_build(proj, e))
        for e in ENVS:
            out["selective"][e].append(dbt_build(proj, e, select=sel_rule))
    return out


def poisson_arrivals(n, rate_per_hour, rng):
    t, arr = 0.0, []
    for _ in range(n):
        t += rng.expovariate(rate_per_hour / 3600.0)
        arr.append(t)
    return arr


def sample_pipeline(build, rng):
    """One deployment = sequential builds in all four environments (sampled from measurements)."""
    return sum(rng.choice(build[e]) for e in ENVS)


def sim_scheduled(arrivals, delta_s, build, rng):
    """Batch job every delta_s seconds (random phase). A tick is skipped while a job is running.
    A change approved at time t goes live when the first job that STARTS after t finishes."""
    phase = rng.uniform(0, delta_s)
    horizon = arrivals[-1] + 10 * delta_s
    jobs, busy_until, tick = [], -1.0, phase
    while tick <= horizon:
        if tick >= busy_until:
            dur = sample_pipeline(build, rng)
            jobs.append((tick, tick + dur))
            busy_until = tick + dur
        tick += delta_s
    lat, j = [], 0
    for t in arrivals:
        while jobs[j][0] < t:
            j += 1
        lat.append(jobs[j][1] - t)
    return lat


def sim_on_demand(arrivals, build_sel, rng, runners=1):
    """Each approval triggers its own selective pipeline run; FIFO over `runners` CI runners."""
    free = [0.0] * runners
    lat = []
    for t in arrivals:
        i = min(range(runners), key=lambda k: free[k])
        start = max(t, free[i])
        dur = sample_pipeline(build_sel, rng)
        free[i] = start + dur
        lat.append(free[i] - t)
    return lat


if __name__ == "__main__":
    data_dir, out_dir = Path(sys.argv[1]), Path(sys.argv[2])
    out_dir.mkdir(parents=True, exist_ok=True)
    rules = json.loads((data_dir / "rules_initial.json").read_text())
    env_paths = {e: data_dir / f"{e}.duckdb" for e in ENVS}
    proj = gen_project(rules, out_dir / "dbt_project", env_paths)
    m = measure(proj, reps=int(sys.argv[3]) if len(sys.argv) > 3 else 3)
    (out_dir / "baseline_build_times.json").write_text(json.dumps(m, indent=1))
    for k in m:
        print(k, {e: [round(x, 1) for x in v] for e, v in m[k].items()})

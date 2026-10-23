"""Workload driver: concurrent authors and approvers issuing rule-lifecycle commands."""
import copy
import json
import queue
import random
import shutil
import threading
import time
from pathlib import Path

import numpy as np

from es_system import (ENVS, CommandHandler, Conflict, PartitionedLog, PolicyViolation,
                       Projector)

MIX = {"update": 0.40, "relax": 0.30, "create": 0.15, "rollback": 0.10, "retire": 0.05}
INJECT = {"self_approve": 0.02, "missing_justification": 0.02, "author_promotes": 0.01}


def zipf_pick(rng, items, s=1.1):
    n = len(items)
    w = 1.0 / np.arange(1, n + 1) ** s
    return items[rng.choices(range(n), weights=w)[0]]


def make_params(rng, kind, cur):
    p = copy.deepcopy(cur)
    if kind == "relax":
        p["pass_threshold"] = round(max(0.50, float(p["pass_threshold"]) - 0.05), 2)
    elif kind == "update":
        if p["template"] in ("T1_impossible", "T2_implausible") and rng.random() < 0.6:
            span = float(p["hi"]) - float(p["lo"])
            d = span * rng.uniform(0.01, 0.05) * rng.choice([-1, 1])
            p["lo"], p["hi"] = round(float(p["lo"]) - d, 4), round(float(p["hi"]) + d, 4)
        else:
            p["pass_threshold"] = round(min(0.99, float(p["pass_threshold"]) + 0.01), 2)
    return p


def make_faulty(cur):
    """A schema-valid but harmful parameter change (a 'typo'): it would flag most in-scope records.
    All of these count as low risk under the risk policy, so no justification is required."""
    p = copy.deepcopy(cur)
    t = p["template"]
    if t in ("T1_impossible", "T2_implausible"):
        span = float(p["hi"]) - float(p["lo"])
        p["lo"], p["hi"] = round(float(p["lo"]) + 0.4 * span, 4), round(float(p["hi"]) - 0.4 * span, 4)
    elif t == "T5_unit":
        p["unit"] = "INVALID_UNIT"
    elif t == "T3_sex":
        p["allowed_sex"] = "M" if p["allowed_sex"] == "F" else "F"
    elif t == "T4_age":
        p["min_age"] = min(120, int(p["min_age"]) + 40)
    elif t == "T6_med_dx":
        p["required_cond"] = "0000000"
    return p


def run_es(run_dir, env_paths, rules, n_authors, n_commands, seed, poll=0.1, dup_rate=0.05,
           pace=None, anchor_every=50, timeout=1800, target="zipf", mix=None, system="es",
           enforce_policy=True, idempotent=True, key_mode="rule", gate="exec", fault_rate=0.0, gate_detail=False):
    """system='es': event-sourced design. system='b2': mutable parameter table + audit triggers
    (same command-layer policy checks, synchronous application, no log)."""
    run_dir = Path(run_dir)
    if run_dir.exists():
        shutil.rmtree(run_dir)
    run_dir.mkdir(parents=True)
    rng = random.Random(seed)
    mix = dict(mix or MIX)
    if fault_rate:
        mix["faulty"] = fault_rate / (1 - fault_rate)
    if system == "es":
        log = PartitionedLog(run_dir / "log", anchor_every=anchor_every, key_mode=key_mode, seed=seed)
        h = CommandHandler(log, enforce_policy=enforce_policy)
        proj = Projector(log, h, run_dir / "governance.duckdb", env_paths, poll_interval=poll,
                         dup_rate=dup_rate, seed=seed, idempotent=idempotent, gate=gate)
    elif system == "b2":
        from b2_system import B2Handler
        h = B2Handler(run_dir / "b2.sqlite", env_paths)
        proj = h
    else:
        from b3_system import B3Handler
        h = B3Handler(run_dir / "b2.sqlite", env_paths, anchor_every=anchor_every)
        proj = h
    proj.start()
    for r in rules:
        h.bootstrap(r)
    while proj.lag() > 0:
        time.sleep(0.05)

    cmds = queue.Queue()
    kinds = list(mix)
    for k in rng.choices(kinds, weights=[mix[k] for k in kinds], k=n_commands):
        cmds.put(k)
    approvals = queue.Queue()
    lock = threading.Lock()
    rec = {"approve_ts": {}, "submit": [], "conflicts": 0, "retries_exhausted": 0,
           "injected": {k: 0 for k in INJECT}, "blocked": {k: 0 for k in INJECT},
           "rollback_cmd": 0, "errors": [], "rejected_invalid": 0, "faulty": set(), "kind": {}}
    next_id = [len(rules)]
    t_start = time.perf_counter()

    def live_rules():
        with h.aggs_lock:
            ids = [rid for rid, a in h.aggs.items()]
        return ids

    def idle(a):
        if a.active["prod"] is None:
            return False
        return a.active["prod"] == a.latest or a.versions[a.latest]["status"] in ("rolled_back", "blocked")

    def author(aid):
        arng = random.Random(seed * 1000 + aid)
        actor = f"author{aid}"
        while True:
            try:
                kind = cmds.get_nowait()
            except queue.Empty:
                return
            if pace:
                time.sleep(arng.expovariate(1.0 / (pace * n_authors)))
            for attempt in range(20):
                try:
                    if kind == "create":
                        with lock:
                            next_id[0] += 1
                            rid = f"R{next_id[0]:04d}"
                        src = h.agg(arng.choice(live_rules()))
                        base = src.versions[src.active["prod"] or 1]["params"] or rules[0]
                        p = dict(base, rule_id=rid)
                        _, v, r = h.submit_change(actor, "author", "create", rid, 0, p)
                        approvals.put((rid, v, r, actor))
                    else:
                        ids = sorted(live_rules())
                        if target == "idle":   # no in-flight change on the rule
                            ids = [i for i in ids if idle(h.agg(i))] or ids
                            rid = arng.choice(ids)
                        else:
                            rid = zipf_pick(arng, ids)
                        a = h.agg(rid)
                        exp_v = a.latest
                        prod_v = a.active["prod"]
                        if prod_v is None:
                            raise Conflict()  # retired rule; pick another
                        cur = a.versions[prod_v]["params"]
                        if kind == "rollback":
                            try:
                                h.rollback(actor, "author", rid, exp_v)
                                with lock:
                                    rec["rollback_cmd"] += 1
                            except PolicyViolation:
                                kind = "update"
                                continue
                        elif kind == "retire":
                            _, v, r = h.submit_change(actor, "author", "retire", rid, exp_v, None)
                            approvals.put((rid, v, r, actor))
                        else:
                            p = make_faulty(cur) if kind == "faulty" else make_params(arng, kind, cur)
                            _, v, r = h.submit_change(actor, "author", "update", rid, exp_v, p)
                            with lock:
                                rec["kind"][(rid, v)] = kind
                            if kind == "faulty":
                                with lock:
                                    rec["faulty"].add((rid, v))
                            approvals.put((rid, v, r, actor))
                    break
                except PolicyViolation:
                    with lock:
                        rec["rejected_invalid"] += 1
                    break
                except Conflict:
                    with lock:
                        rec["conflicts"] += 1
                    continue
            else:
                with lock:
                    rec["retries_exhausted"] += 1
            # injected policy violation: author tries to promote directly to prod
            if arng.random() < INJECT["author_promotes"]:
                with lock:
                    rec["injected"]["author_promotes"] += 1
                try:
                    h.promote(actor, "author", "R0001", 1, "prod")
                except PolicyViolation:
                    with lock:
                        rec["blocked"]["author_promotes"] += 1

    def approver(qid, stop):
        prng = random.Random(seed * 7 + qid)
        while not (stop.is_set() and approvals.empty()):
            try:
                rid, v, r, author_actor = approvals.get(timeout=0.05)
            except queue.Empty:
                continue
            just = f"client-confirmation-{rid}-v{v}" if r == "high" else None
            try:
                if prng.random() < INJECT["self_approve"]:
                    with lock:
                        rec["injected"]["self_approve"] += 1
                    try:
                        h.approve(author_actor, "approver", rid, v, just)
                    except PolicyViolation as e:
                        if e.kind == "sod":
                            with lock:
                                rec["blocked"]["self_approve"] += 1
                if r == "high" and prng.random() < INJECT["missing_justification"]:
                    with lock:
                        rec["injected"]["missing_justification"] += 1
                    try:
                        h.approve(f"approver{qid}", "approver", rid, v, None)
                    except PolicyViolation as e:
                        if e.kind == "c4":
                            with lock:
                                rec["blocked"]["missing_justification"] += 1
                ev = h.approve(f"approver{qid}", "approver", rid, v, just)
                with lock:
                    rec["approve_ts"][(rid, v)] = ev["ts"]
            except (Conflict, PolicyViolation):
                pass  # superseded by a newer version before approval

    stop = threading.Event()
    n_appr = max(1, n_authors // 5)
    athreads = [threading.Thread(target=author, args=(i,)) for i in range(n_authors)]
    pthreads = [threading.Thread(target=approver, args=(i, stop)) for i in range(n_appr)]
    for t in athreads + pthreads:
        t.start()
    for t in athreads:
        t.join()
    stop.set()
    for t in pthreads:
        t.join()
    t_cmds_done = time.perf_counter()
    deadline = time.time() + timeout
    while proj.lag() > 0 and time.time() < deadline:
        time.sleep(0.05)
    time.sleep(max(0.5, poll * 5))
    while proj.lag() > 0 and time.time() < deadline:
        time.sleep(0.05)
    t_end = time.perf_counter()
    proj.stop()
    if system == "es":
        log.close()

    # ----- metrics -----
    lat, reached = [], 0
    for (rid, v), ta in rec["approve_ts"].items():
        tp = proj.applied_at.get((rid, v, "PROMOTED", "prod"))
        if tp is not None:
            lat.append(tp - ta)
            reached += 1
    final_proj = {(e, r): v for e, r, v in proj.gov.execute(
        "SELECT env, rule_id, version FROM " + ("active_rules" if system == "es" else "rule_params")).fetchall()}
    final_truth = {}
    for ts, r, e, v in sorted(h.truth.history):
        final_truth[(e, r)] = v
    final_truth = {k: v for k, v in final_truth.items() if v is not None}
    proj.gov.close()
    out = {
        "n_authors": n_authors, "n_commands": n_commands, "seed": seed, "poll": poll,
        "target": target, "system": system, "mix": mix,
        "pace": pace, "dup_rate": dup_rate,
        "wall_s": t_end - t_start, "cmd_phase_s": t_cmds_done - t_start,
        "approved": len(rec["approve_ts"]), "reached_prod": reached,
        "latency_s": lat, "rollback_latency_s": proj.rollback_lat,
        "rollbacks": rec["rollback_cmd"], "rejected_invalid": rec["rejected_invalid"],
        "superseded_before_prod": len(rec["approve_ts"]) - reached,
        "conflicts": rec["conflicts"], "retries_exhausted": rec["retries_exhausted"],
        "handler_stats": h.stats, "injected": rec["injected"], "blocked": rec["blocked"],
        "dup_delivered": proj.dup_delivered, "dup_skipped": proj.dup_skipped,
        "gate_runs": proj.gate_runs, "gate_seconds": proj.gate_seconds,
        "events": sum(len(p) for p in log.parts) if system == "es" else h.n_writes,
        "final_projection_matches_truth": final_proj == final_truth,
        "final_pairs": len(final_truth),
    }
    # ---- promotion-gate outcome (impact gate only) ----
    if getattr(proj, "gate_log", None) is not None and system == "es":
        blocked = {(r, v): e for r, v, e, *_rest, b in proj.gate_log if b}
        approved_keys = set(rec["approve_ts"])
        faulty_appr = rec["faulty"] & approved_keys
        normal_appr = approved_keys - rec["faulty"]
        out["gate"] = {
            "mode": gate, "faulty_approved": len(faulty_appr),
            "faulty_blocked": len([k for k in faulty_appr if k in blocked]),
            "faulty_reached_prod": len([k for k in faulty_appr if (k[0], k[1], "PROMOTED", "prod") in proj.applied_at]),
            "faulty_blocked_env": {e: sum(1 for k in faulty_appr if blocked.get(k) == e) for e in ENVS[:3]},
            "normal_approved": len(normal_appr),
            "normal_blocked": len([k for k in normal_appr if k in blocked]),
            "gate_checks": len(proj.gate_log)}
        if gate_detail:
            out["gate_detail"] = [{"rule": r, "version": v, "env": e, "flagged": c, "baseline": b, "scope": sc,
                                   "blocked": bl, "faulty": (r, v) in rec["faulty"], "kind": rec["kind"].get((r, v)),
                                   "reached_prod": (r, v, "PROMOTED", "prod") in proj.applied_at}
                                  for r, v, e, c, b, sc, bl in proj.gate_log]
            out["faulty_detail"] = [{"rule": r, "version": v, "blocked_env": blocked.get((r, v)),
                                     "reached_prod": (r, v, "PROMOTED", "prod") in proj.applied_at}
                                    for r, v in sorted(faulty_appr)]
        out["latency_normal_s"] = [proj.applied_at[(r, v, "PROMOTED", "prod")] - ta
                                   for (r, v), ta in rec["approve_ts"].items()
                                   if (r, v) not in rec["faulty"] and (r, v, "PROMOTED", "prod") in proj.applied_at]
    if system == "es":
        out["hash_seconds"] = log.hash_seconds
        out["projector_errors"] = proj.errors
        out["projector_error_samples"] = proj.error_samples[:5]
    (run_dir / "truth.json").write_text(json.dumps(h.truth.history))
    (run_dir / "metrics.json").write_text(json.dumps(out))
    return out

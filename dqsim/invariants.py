"""Independent invariant checker for the event-sourced design.

It reads only persisted artifacts: the raw partition files, the external anchor file, the
projection tables and the source data's patient identifiers. It shares no code with the
command handler, the projector or the oracle; the hash function and the risk policy are
re-implemented here from the written specification. Each constraint C1-C10 of the problem
formulation is checked directly on the recorded history.

A mutation self-test injects histories that violate one constraint each (with the hash chain
re-computed, as an actor with append rights could) and confirms the checker flags them.
"""
import copy
import hashlib
import json
import random
import re
import uuid
from pathlib import Path

import duckdb

ENVS = ("dev", "uat", "prod_test", "prod")
GENESIS = "0" * 64
SUBMIT = ("CREATED", "UPDATED", "RETIRE_REQUESTED")
ROLE_OF = {"BOOTSTRAPPED": "system", "CREATED": "author", "UPDATED": "author",
           "RETIRE_REQUESTED": "author", "ROLLED_BACK": "author", "APPROVED": "approver",
           "PROMOTED": "system", "GATE_FAILED": "system"}
EVENT_KEYS = {"event_id", "rule_id", "version", "type", "actor", "role", "env", "justification",
              "payload", "partition", "offset", "ts", "prev_hash", "hash"}
PARAM_KEYS = {"template", "code", "desc", "rule_id", "pass_threshold", "lo", "hi", "unit", "required_desc",
              "allowed_sex", "min_age", "required_cond"}
UUID_RE = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
SSN_RE = re.compile(r"\b\d{3}-\d{2}-\d{4}\b")
DATE_RE = re.compile(r"\b(19|20)\d{2}-\d{2}-\d{2}\b")


def sha(prev, ev):
    body = json.dumps({k: v for k, v in ev.items() if k != "hash"}, sort_keys=True,
                      separators=(",", ":"), default=str)
    return hashlib.sha256((prev + body).encode()).hexdigest()


def relaxes(old, new):
    """Specification of a high-risk change: the check becomes less strict or disappears."""
    if new is None:
        return True
    if old is None:
        return False
    if float(new["pass_threshold"]) < float(old["pass_threshold"]):
        return True
    if new["template"] in ("T1_impossible", "T2_implausible") and (
            float(new["lo"]) < float(old["lo"]) or float(new["hi"]) > float(old["hi"])):
        return True
    return new["template"] == "T4_age" and int(new["min_age"]) < int(old["min_age"])


def load_run(run_dir, P=8):
    run_dir = Path(run_dir)
    parts = []
    for p in range(P):
        f = run_dir / "log" / f"p{p}.jsonl"
        parts.append([json.loads(x) for x in f.read_text().splitlines()] if f.exists() else [])
    anchors = [tuple(a) for a in json.loads((run_dir / "log" / "anchors.json").read_text())]
    g = duckdb.connect(str(run_dir / "governance.duckdb"), read_only=True)
    proj = {(e, r): v for e, r, v in g.execute("SELECT env, rule_id, version FROM active_rules").fetchall()}
    applied = [x[0] for x in g.execute("SELECT event_id FROM events_applied").fetchall()]
    g.close()
    return parts, anchors, proj, applied


def check(parts, anchors, proj, applied, patient_ids):
    viol = {f"C{i}": 0 for i in range(1, 11)}
    examples = {}

    def flag(c, msg):
        viol[c] += 1
        examples.setdefault(c, msg)

    # ---- C1 append-only, totally ordered per partition; each event recorded once ----
    seen, rule_part = set(), {}
    for p, part in enumerate(parts):
        for i, ev in enumerate(part):
            if ev.get("offset") != i or ev.get("partition") != p:
                flag("C1", f"offset/partition mismatch p{p}@{i}")
            if i and ev["ts"] < part[i - 1]["ts"]:
                flag("C1", f"timestamp regression p{p}@{i}")
            if ev["event_id"] in seen:
                flag("C1", f"duplicate event id {ev['event_id']}")
            seen.add(ev["event_id"])
            if rule_part.setdefault(ev["rule_id"], p) != p:
                flag("C1", f"rule {ev['rule_id']} split across partitions")
            if ev["type"] not in ROLE_OF or set(ev) - EVENT_KEYS:
                flag("C1", f"unknown event shape p{p}@{i}")

    # ---- C2 tamper evidence: hash chain and external anchors ----
    for p, part in enumerate(parts):
        prev = GENESIS
        for i, ev in enumerate(part):
            if ev["prev_hash"] != prev or sha(prev, ev) != ev["hash"]:
                flag("C2", f"chain break p{p}@{i}")
                break
            prev = ev["hash"]
    for p, off, h in anchors:
        if off >= len(parts[p]) or parts[p][off]["hash"] != h:
            flag("C2", f"anchor mismatch p{p}@{off}")

    # ---- C3-C6: replay each rule's lifecycle in log order ----
    final = {}
    for part in parts:
        S = {}
        for ev in part:
            t, r, v, actor = ev["type"], ev["rule_id"], ev["version"], ev["actor"]
            s = S.setdefault(r, {"latest": 0, "ver": {}, "active": dict.fromkeys(ENVS), "hist": []})
            if ev["role"] != ROLE_OF.get(t):
                flag("C3", f"{t} by role {ev['role']} ({r} v{v})")
            if t == "BOOTSTRAPPED":
                if v != s["latest"] + 1:
                    flag("C6", f"version gap {r} v{v}")
                s["latest"] = v
                s["ver"][v] = {"params": ev["payload"], "status": "approved", "author": actor, "high": False}
                for e in ENVS:
                    s["active"][e] = v
                s["hist"].append(v)
            elif t in SUBMIT:
                if v != s["latest"] + 1:
                    flag("C6", f"lost update / version gap {r} v{v} after v{s['latest']}")
                new = ev["payload"]["params"]
                pv = s["active"]["prod"]
                old = s["ver"][pv]["params"] if pv is not None and pv in s["ver"] else None
                high = relaxes(old, new)
                if (ev["payload"].get("risk") == "high") != high:
                    flag("C4", f"risk mislabelled {r} v{v}")
                s["latest"] = max(s["latest"], v)
                s["ver"][v] = {"params": new, "status": "submitted", "author": actor, "high": high}
            elif t == "APPROVED":
                x = s["ver"].get(v)
                if x is None:
                    flag("C5", f"approval of unknown version {r} v{v}")
                    continue
                if actor == x["author"]:
                    flag("C3", f"self-approval {r} v{v} by {actor}")
                if x["high"] and not ev.get("justification"):
                    flag("C4", f"high-risk approval without justification {r} v{v}")
                if x["status"] != "submitted" or v != s["latest"]:
                    flag("C6", f"stale approval {r} v{v}")
                x["status"] = "approved"
            elif t == "PROMOTED":
                x = s["ver"].get(v)
                if x is None or x["status"] != "approved":
                    flag("C5", f"promotion without valid approval {r} v{v} -> {ev['env']}")
                if v != s["latest"]:
                    flag("C6", f"promotion of superseded version {r} v{v}")
                e = ev["env"]
                k = ENVS.index(e) if e in ENVS else -1
                tgt = v if (x is not None and x["params"] is not None) else None
                if k < 0 or (k > 0 and s["active"][ENVS[k - 1]] != tgt):
                    flag("C5", f"out-of-order promotion {r} v{v} -> {e}")
                if k >= 0:
                    s["active"][e] = tgt
                if e == "prod" and tgt is not None:
                    s["hist"].append(v)
            elif t == "GATE_FAILED":
                x = s["ver"].get(v)
                if x is None or x["status"] != "approved":
                    flag("C5", f"gate failure recorded for a non-approved version {r} v{v}")
                else:
                    x["status"] = "blocked"   # a blocked version may never be promoted further
            elif t == "ROLLED_BACK":
                frm = (ev.get("payload") or {}).get("from")
                if v not in s["hist"] or v == s["active"]["prod"]:
                    flag("C5", f"rollback target never active in prod {r} v{v}")
                if frm != s["active"]["prod"]:
                    flag("C6", f"rollback from stale state {r}")
                for vv, x in s["ver"].items():
                    if vv > v and x["status"] in ("submitted", "approved"):
                        x["status"] = "rolled_back"
                for e in ENVS:
                    s["active"][e] = v
                s["hist"].append(v)
        for r, s in S.items():
            for e in ENVS:
                if s["active"][e] is not None:
                    final[(e, r)] = s["active"][e]

    # ---- C8 data minimisation: no patient-level data in governance events ----
    for part in parts:
        for ev in part:
            raw = json.dumps(ev, default=str)
            if any(u in patient_ids for u in UUID_RE.findall(raw)) or SSN_RE.search(raw) \
                    or DATE_RE.search(raw):
                flag("C8", f"patient-level value in event {ev['event_id']}")
            pl = ev.get("payload")
            params = pl.get("params") if isinstance(pl, dict) and "params" in pl else (
                pl if ev["type"] == "BOOTSTRAPPED" else None)
            if isinstance(params, dict) and set(params) - PARAM_KEYS:
                flag("C8", f"non-schema field in parameters {set(params) - PARAM_KEYS}")

    # ---- C7 idempotent application: every event applied exactly once ----
    log_ids = [ev["event_id"] for part in parts for ev in part]
    if len(applied) != len(set(applied)) or set(applied) != set(log_ids):
        flag("C7", f"applied {len(applied)} rows / {len(set(applied))} distinct vs {len(set(log_ids))} logged")

    # ---- C10 reconstructability: replaying the log reproduces the projection exactly ----
    if final != proj:
        diff = {k for k in set(final) | set(proj) if final.get(k) != proj.get(k)}
        flag("C10", f"{len(diff)} (env, rule) pairs differ, e.g. {sorted(diff)[:2]}")

    # C9 (retention) is a deployment property: the prototype exposes no delete operation and
    # C1 above confirms no event was removed; the six-year horizon itself cannot be tested here.
    return viol, examples, {"events": len(log_ids), "rules": len(rule_part), "final_pairs": len(final)}


# ------------------------------------------------------------------ mutation self-test
def _append(parts, p, ev):
    part = parts[p]
    ev = dict(ev, event_id=uuid.uuid4().hex, partition=p, offset=len(part),
              ts=(part[-1]["ts"] if part else 0) + 1e-3,
              prev_hash=part[-1]["hash"] if part else GENESIS)
    ev["hash"] = sha(ev["prev_hash"], ev)
    part.append(ev)
    return ev


def _live_rule(parts, rng):
    """Pick a rule whose replayed prod state is live and whose latest version is in prod."""
    cands = []
    for p, part in enumerate(parts):
        latest, prod, params, types = {}, {}, {}, {}
        for ev in part:
            r = ev["rule_id"]
            if ev["type"] in SUBMIT + ("BOOTSTRAPPED",):
                latest[r] = ev["version"]
                params[(r, ev["version"])] = ev["payload"] if ev["type"] == "BOOTSTRAPPED" \
                    else ev["payload"]["params"]
            if ev["type"] == "PROMOTED" and ev["env"] == "prod":
                prod[r] = ev["version"]
            if ev["type"] in ("BOOTSTRAPPED", "ROLLED_BACK"):
                prod[r] = ev["version"]
        for r, v in latest.items():
            if prod.get(r) == v and params.get((r, v)) is not None:
                cands.append((p, r, v, params[(r, v)]))
    return rng.choice(cands)


def _base(r, v, t, actor, role, payload=None, env=None, just=None):
    return {"rule_id": r, "version": v, "type": t, "actor": actor, "role": role, "env": env,
            "justification": just, "payload": payload}


def mutate(kind, parts, anchors, proj, applied, rng):
    parts = [list(x) for x in parts]
    proj, applied = dict(proj), list(applied)
    p, r, v, params = _live_rule(parts, rng)
    tight = dict(params, pass_threshold=round(min(0.99, float(params["pass_threshold"]) + 0.01), 2))
    loose = dict(params, pass_threshold=round(max(0.5, float(params["pass_threshold"]) - 0.05), 2))
    if float(loose["pass_threshold"]) >= float(params["pass_threshold"]):
        loose["pass_threshold"] = float(params["pass_threshold"]) - 0.01
    nv = v + 1

    def submit(pp, risk, actor="authorX"):
        _append(parts, p, _base(r, nv, "UPDATED", actor, "author", {"params": pp, "risk": risk}))

    def promote_all(envs=ENVS, role="system"):
        for e in envs:
            _append(parts, p, _base(r, nv, "PROMOTED", role, role, env=e))

    if kind == "C1_duplicate_event":
        dup = copy.deepcopy(rng.choice(parts[p]))
        dup.update(offset=len(parts[p]), ts=parts[p][-1]["ts"] + 1e-3, prev_hash=parts[p][-1]["hash"])
        dup["hash"] = sha(dup["prev_hash"], dup)
        parts[p].append(dup)
    elif kind == "C2_edit_without_rehash":
        i = rng.randrange(len(parts[p]))
        parts[p][i] = dict(parts[p][i], actor="intruder")
    elif kind == "C3_self_approval":
        submit(tight, "low", actor="authorX")
        _append(parts, p, _base(r, nv, "APPROVED", "authorX", "approver"))
    elif kind == "C3_author_promotes":
        submit(tight, "low")
        _append(parts, p, _base(r, nv, "APPROVED", "approverY", "approver"))
        promote_all(role="author")
    elif kind == "C4_no_justification":
        submit(loose, "high")
        _append(parts, p, _base(r, nv, "APPROVED", "approverY", "approver", just=None))
    elif kind == "C4_risk_mislabelled":
        submit(loose, "low")
    elif kind == "C5_skip_environment":
        submit(tight, "low")
        _append(parts, p, _base(r, nv, "APPROVED", "approverY", "approver"))
        _append(parts, p, _base(r, nv, "PROMOTED", "system", "system", env="prod"))
    elif kind == "C5_promote_unapproved":
        submit(tight, "low")
        promote_all(envs=("dev",))
    elif kind == "C6_lost_update":
        _append(parts, p, _base(r, v + 2, "UPDATED", "authorX", "author", {"params": tight, "risk": "low"}))
    elif kind == "C7_event_not_applied":
        applied.remove(rng.choice(applied))
    elif kind == "C8_patient_identifier":
        submit(dict(tight, desc=f"exclude patient {rng.choice(sorted(_PIDS))}"), "low")
    elif kind == "C10_projection_drift":
        k = rng.choice(sorted(proj))
        proj[k] = proj[k] + 100
    return parts, anchors, proj, applied


MUTATIONS = ["C1_duplicate_event", "C2_edit_without_rehash", "C3_self_approval",
             "C3_author_promotes", "C4_no_justification", "C4_risk_mislabelled",
             "C5_skip_environment", "C5_promote_unapproved", "C6_lost_update",
             "C7_event_not_applied", "C8_patient_identifier", "C10_projection_drift"]
_PIDS = set()


def patient_ids(data_dir):
    c = duckdb.connect(str(Path(data_dir) / "clean.duckdb"), read_only=True)
    ids = {x[0] for x in c.execute("SELECT patient FROM patients").fetchall()}
    c.close()
    return ids


def self_test(run_dirs, pids, trials_per_kind=20, seed=0):
    _PIDS.clear()
    _PIDS.update(pids)
    rng = random.Random(seed)
    loaded = [load_run(d) for d in run_dirs]
    out = {}
    for kind in MUTATIONS:
        target = kind.split("_")[0]
        hit = 0
        for t in range(trials_per_kind):
            run = loaded[t % len(loaded)]
            viol, _, _ = check(*mutate(kind, *run, rng), pids)
            hit += viol[target] > 0
        out[kind] = hit / trials_per_kind
    return out

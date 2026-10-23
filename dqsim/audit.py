"""Audit reconstruction and tamper-detection experiments over a persisted run log."""
import copy
import json
import random
from bisect import bisect_right
from pathlib import Path

from es_system import ENVS, GENESIS, chain_hash


def load_log(log_dir, P=8):
    parts = []
    for p in range(P):
        f = Path(log_dir) / f"p{p}.jsonl"
        parts.append([json.loads(l) for l in f.read_text().splitlines()] if f.exists() else [])
    return parts


def anchors_every(parts, k):
    return [(p, i, part[i]["hash"]) for p, part in enumerate(parts)
            for i in range(k - 1, len(part), k)]


def verify(parts, anchors):
    """Return True if the log is consistent with its hash chain and the external anchors."""
    for part in parts:
        prev = GENESIS
        for i, ev in enumerate(part):
            if ev["offset"] != i or ev["prev_hash"] != prev or chain_hash(prev, ev) != ev["hash"]:
                return False
            prev = ev["hash"]
    for p, off, h in anchors:
        if off >= len(parts[p]) or parts[p][off]["hash"] != h:
            return False
    return True


def replay_history(parts):
    """Rebuild activation history purely from the log (independent of handler and projector)."""
    hist = []
    for part in parts:
        params_of = {}
        for ev in part:
            rid, v, t = ev["rule_id"], ev["version"], ev["type"]
            if t in ("CREATED", "UPDATED", "RETIRE_REQUESTED"):
                params_of[(rid, v)] = ev["payload"]["params"]
            elif t == "BOOTSTRAPPED":
                params_of[(rid, v)] = ev["payload"]
                for e in ENVS:
                    hist.append((ev["ts"], rid, e, v))
            elif t == "PROMOTED":
                val = v if params_of.get((rid, v)) is not None else None
                hist.append((ev["ts"], rid, ev["env"], val))
            elif t == "ROLLED_BACK":
                for e in ENVS:
                    hist.append((ev["ts"], rid, e, v))
    return hist


def index_history(hist):
    idx = {}
    for ts, rid, e, v in sorted(hist, key=lambda x: x[0]):
        idx.setdefault((rid, e), ([], []))
        idx[(rid, e)][0].append(ts)
        idx[(rid, e)][1].append(v)
    return idx


def state_at(idx, T):
    s = {}
    for k, (tss, vs) in idx.items():
        i = bisect_right(tss, T)
        if i:
            s[k] = vs[i - 1]
    return s


def reconstruction_accuracy(run_dir, n_samples=500, seed=0):
    parts = load_log(Path(run_dir) / "log")
    truth = [tuple(x) for x in json.loads((Path(run_dir) / "truth.json").read_text())]
    rec_idx, truth_idx = index_history(replay_history(parts)), index_history(truth)
    t0 = min(t for t, *_ in truth)
    t1 = max(t for t, *_ in truth)
    rng = random.Random(seed)
    checked = matched = 0
    for _ in range(n_samples):
        T = rng.uniform(t0, t1)
        a, b = state_at(rec_idx, T), state_at(truth_idx, T)
        keys = set(a) | set(b)
        checked += len(keys)
        matched += sum(1 for k in keys if a.get(k) == b.get(k))
    # lost-update check: per rule, submitted versions are exactly 1..n with no gaps/duplicates
    versions = {}
    for part in parts:
        for ev in part:
            if ev["type"] in ("BOOTSTRAPPED", "CREATED", "UPDATED", "RETIRE_REQUESTED"):
                versions.setdefault(ev["rule_id"], []).append(ev["version"])
    lost = sum(1 for vs in versions.values() if sorted(vs) != list(range(min(vs), min(vs) + len(vs))))
    return {"state_checks": checked, "matched": matched, "accuracy": matched / checked,
            "rules_with_version_anomaly": lost, "chain_valid": verify(parts, anchors_every(parts, 50))}


def tamper(parts, kind, rng):
    candidates = [p for p, part in enumerate(parts) if len(part) >= 3]
    p = rng.choice(candidates)
    t = list(parts)
    t[p] = [dict(e) for e in parts[p]]   # copy only the partition being tampered with
    part = t[p]
    n = len(part)
    i = rng.randrange(n)
    if kind == "modify_field":
        part[i]["actor"] = "intruder"
    elif kind == "modify_and_rehash":
        part[i]["actor"] = "intruder"
        part[i]["hash"] = chain_hash(part[i]["prev_hash"], part[i])
    elif kind == "delete_event":
        del part[i]
    elif kind == "reorder_adjacent":
        i = rng.randrange(n - 1)
        part[i], part[i + 1] = part[i + 1], part[i]
    elif kind == "insert_forged":
        forged = dict(part[i], event_id="forged", actor="intruder")
        part.insert(i, forged)
    elif kind == "rewrite_suffix":   # insider recomputes every hash after the change
        part[i]["actor"] = "intruder"
        prev = part[i]["prev_hash"]
        for j in range(i, n):
            part[j]["prev_hash"] = prev
            part[j]["hash"] = chain_hash(prev, part[j])
            prev = part[j]["hash"]
    elif kind == "truncate_tail":
        k = rng.randint(1, max(1, n // 10))
        del part[n - k:]
    return t


KINDS = ["modify_field", "modify_and_rehash", "delete_event", "reorder_adjacent",
         "insert_forged", "rewrite_suffix", "truncate_tail"]


def tamper_experiment(run_dir, trials=200, anchor_ks=(1, 10, 50, 100), seed=0):
    parts = load_log(Path(run_dir) / "log")
    rng = random.Random(seed)
    res = {}
    for k in anchor_ks:
        anchors = anchors_every(parts, k)
        assert verify(parts, anchors)
        res[k] = {}
        for kind in KINDS:
            det = sum(1 for _ in range(trials) if not verify(tamper(parts, kind, rng), anchors))
            res[k][kind] = det / trials
    # chain only, no external anchors
    res["no_anchor"] = {kind: sum(1 for _ in range(trials)
                                  if not verify(tamper(parts, kind, rng), [])) / trials
                        for kind in KINDS}
    return res

"""Audit experiments for baseline B2 (mutable parameter table + trigger audit log).

Reconstruction: replay the trigger-written audit rows of `rule_params` and compare the
state at random instants with the same oracle used for the event-sourced design.

Tamper detection: the B2 audit table carries no cryptographic binding. The strongest check
available without one is structural: (i) audit ids are contiguous from 1, (ii) the highest id
equals the table's AUTOINCREMENT sequence value, and (iii) timestamps never decrease in id
order. We apply the same seven attack procedures used on the event log, adapted to rows.
"""
import json
import random
import sqlite3
from pathlib import Path

from audit import index_history, state_at

COLS = ["id", "tbl", "op", "env", "rule_id", "old_version", "new_version", "old_params",
        "new_params", "old_status", "new_status", "changed_by", "changed_at"]


def load_audit(db_path):
    c = sqlite3.connect(str(db_path))
    rows = [dict(zip(COLS, r)) for r in c.execute(f"SELECT {','.join(COLS)} FROM audit_log ORDER BY id")]
    seq = c.execute("SELECT seq FROM sqlite_sequence WHERE name='audit_log'").fetchone()[0]
    c.close()
    return rows, seq


def replay_history(rows):
    hist = []
    for r in rows:
        if r["tbl"] != "rule_params":
            continue
        v = r["new_version"] if r["op"] in ("INSERT", "UPDATE") else None
        hist.append((r["changed_at"], r["rule_id"], r["env"], v))
    return hist


def reconstruction_accuracy(run_dir, n_samples=200, seed=0):
    rows, _ = load_audit(Path(run_dir) / "b2.sqlite")
    truth = [tuple(x) for x in json.loads((Path(run_dir) / "truth.json").read_text())]
    rec_idx, truth_idx = index_history(replay_history(rows)), index_history(truth)
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
    return {"state_checks": checked, "matched": matched, "accuracy": matched / checked,
            "audit_rows": len(rows)}


def verify(rows, seq):
    """Best structural integrity check available for a mutable audit table."""
    for i, r in enumerate(rows):
        if r["id"] != i + 1:
            return False
        if i and r["changed_at"] < rows[i - 1]["changed_at"]:
            return False
    return (rows[-1]["id"] if rows else 0) == seq


def tamper(rows, seq, kind, rng):
    t = [dict(r) for r in rows]
    n = len(t)
    i = rng.randrange(n)
    if kind == "modify_field":            # alter who made a change
        t[i]["changed_by"] = "intruder"
    elif kind == "modify_and_rehash":     # no per-record hash exists: alter a recorded value
        t[i]["new_version"] = (t[i]["new_version"] or 0) + 1
    elif kind == "delete_event":
        del t[i]
    elif kind == "reorder_adjacent":      # swap the content of two neighbouring rows, keep ids
        i = rng.randrange(n - 1)
        a, b = dict(t[i]), dict(t[i + 1])
        a["id"], b["id"] = t[i + 1]["id"], t[i]["id"]
        t[i], t[i + 1] = b, a
    elif kind == "insert_forged":         # back-dated forged row: the database assigns the next id
        forged = dict(t[i], changed_by="intruder", id=seq + 1)
        t.append(forged)
        seq += 1
    elif kind == "rewrite_suffix":        # insider rewrites history consistently from row i on
        t[i]["changed_by"] = "intruder"   # nothing binds later rows to row i; no repair needed
    elif kind == "truncate_tail":         # insider deletes the newest rows and resets the sequence
        k = rng.randint(1, max(1, n // 10))
        del t[n - k:]
        seq = t[-1]["id"]
    return t, seq


KINDS = ["modify_field", "modify_and_rehash", "delete_event", "reorder_adjacent",
         "insert_forged", "rewrite_suffix", "truncate_tail"]


def tamper_experiment(run_dir, trials=200, seed=0):
    rows, seq = load_audit(Path(run_dir) / "b2.sqlite")
    assert verify(rows, seq)
    rng = random.Random(seed)
    return {kind: sum(1 for _ in range(trials) if not verify(*tamper(rows, seq, kind, rng))) / trials
            for kind in KINDS}

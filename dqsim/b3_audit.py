"""Reconstruction and tamper detection for baseline B3 (hash-chained audit table + anchors).
The seven attacks are the same procedures applied to the event log of the event-sourced design."""
import random
import sqlite3
from pathlib import Path

from b3_system import GENESIS, ROW_FIELDS, row_hash


def load_rows(db_path):
    c = sqlite3.connect(str(db_path))
    rows = [dict(zip(ROW_FIELDS + ["prev_hash", "hash"], r)) for r in c.execute(
        f"SELECT {','.join(ROW_FIELDS)}, prev_hash, hash FROM audit_log ORDER BY id")]
    c.close()
    return rows


def rehash(r, prev):
    r["prev_hash"] = prev
    r["hash"] = row_hash(prev, [r[f] for f in ROW_FIELDS[0:]])
    return r["hash"]


def verify(rows, anchors):
    prev = GENESIS
    for i, r in enumerate(rows):
        if r["id"] != i + 1 or r["prev_hash"] != prev or row_hash(prev, [r[f] for f in ROW_FIELDS]) != r["hash"]:
            return False
        prev = r["hash"]
    by_id = {r["id"]: r["hash"] for r in rows}
    return all(by_id.get(i) == h for i, h in anchors)


def anchors_every(rows, k):
    return [(r["id"], r["hash"]) for r in rows if r["id"] % k == 0]


def tamper(rows, kind, rng):
    t = [dict(r) for r in rows]
    n = len(t)
    i = rng.randrange(n)
    if kind == "modify_field":
        t[i]["changed_by"] = "intruder"
    elif kind == "modify_and_rehash":
        t[i]["changed_by"] = "intruder"
        rehash(t[i], t[i]["prev_hash"])
    elif kind == "delete_event":
        del t[i]
    elif kind == "reorder_adjacent":
        i = rng.randrange(n - 1)
        t[i], t[i + 1] = t[i + 1], t[i]
    elif kind == "insert_forged":
        t.insert(i, dict(t[i], changed_by="intruder"))
    elif kind == "rewrite_suffix":          # insider recomputes every later hash
        t[i]["changed_by"] = "intruder"
        prev = t[i]["prev_hash"]
        for j in range(i, n):
            prev = rehash(t[j], prev)
    elif kind == "truncate_tail":
        k = rng.randint(1, max(1, n // 10))
        del t[n - k:]
    return t


KINDS = ["modify_field", "modify_and_rehash", "delete_event", "reorder_adjacent",
         "insert_forged", "rewrite_suffix", "truncate_tail"]


def tamper_experiment(run_dir, trials=200, anchor_ks=(1, 10, 50, 100), seed=0):
    rows = load_rows(Path(run_dir) / "b2.sqlite")
    rng = random.Random(seed)
    res = {}
    for k in list(anchor_ks) + ["no_anchor"]:
        anchors = [] if k == "no_anchor" else anchors_every(rows, k)
        assert verify(rows, anchors)
        res[str(k)] = {kind: sum(1 for _ in range(trials) if not verify(tamper(rows, kind, rng), anchors)) / trials
                       for kind in KINDS}
    return res

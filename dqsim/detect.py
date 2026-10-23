"""Rule detection quality (precision / recall vs. injected ground truth) and the control check
that both deployment paths evaluate identical rules identically."""
import json
import sys
from pathlib import Path

import duckdb

from rules import violation_sql


def detection(db_path, rules):
    con = duckdb.connect(str(db_path), read_only=True)
    gt = {}
    for t, rid in con.execute("SELECT template, rec_id FROM gt_labels").fetchall():
        gt.setdefault(t, set()).add(rid)
    out = {}
    for t in sorted({r["template"] for r in rules}):
        flagged = set()
        for r in (r for r in rules if r["template"] == t):
            flagged |= {x for (x,) in con.execute(violation_sql(r)).fetchall()}
        g = gt.get(t, set())
        tp = len(flagged & g)
        out[t] = {"flagged": len(flagged), "injected": len(g), "tp": tp,
                  "precision": tp / len(flagged) if flagged else None,
                  "recall": tp / len(g) if g else None}
    con.close()
    return out


def control_check(dbt_prod_db, rules):
    """dbt-materialized violation tables (baseline) vs runtime-rendered SQL from projected params
    (event-sourced). Returns number of rules whose violation sets differ."""
    con = duckdb.connect(str(dbt_prod_db), read_only=True)
    diff = 0
    for r in rules:
        a = {x for (x,) in con.execute(f"SELECT rec_id FROM main.rule_{r['rule_id']}").fetchall()}
        b = {x for (x,) in con.execute(violation_sql(json.loads(json.dumps(r)))).fetchall()}
        diff += a != b
    con.close()
    return {"rules_compared": len(rules), "rules_differing": diff}


if __name__ == "__main__":
    data_dir, out_dir = Path(sys.argv[1]), Path(sys.argv[2])
    rules = json.loads((data_dir / "rules_initial.json").read_text())
    res = {rate: detection(data_dir / f"prod_{rate}.duckdb", rules) for rate in ["01", "05", "10"]}
    dbt_db = out_dir / "baseline" / "dbt_project" / "dbs" / "prod.duckdb"
    if dbt_db.exists():
        res["control_check"] = control_check(dbt_db, rules)
    (out_dir / "detection.json").write_text(json.dumps(res, indent=1))
    print(json.dumps(res, indent=1))

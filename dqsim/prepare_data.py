"""Prepare synthetic evaluation data for the event-sourced DQ rule governance study.

Steps
1. Load Synthea CSV output (patients, observations, conditions, medications) into DuckDB.
2. Derive a 240-rule library from 6 logic templates (adapted from Wang et al. 2020) using
   knowledge (parameter) tables learned from the CLEAN data.
3. Inject labeled errors at record-level rates {1%, 5%, 10%} and write one DuckDB file per rate.
4. Build smaller dev / uat / prod_test environment samples from the 5% file.

Usage: python prepare_data.py <synthea_csv_dir> <out_dir>
"""
import json
import random
import sys
from pathlib import Path

import duckdb
import numpy as np

SEED = 42
RULES_PER_TEMPLATE = 40
TOTAL_RULES = 240
ERROR_RATES = [0.01, 0.05, 0.10]
ENV_SAMPLES = {"dev": 0.02, "uat": 0.05, "prod_test": 0.10}  # fraction of patients
DEFAULT_PASS_THRESHOLD = 0.80


def load_clean(con, csv_dir):
    con.execute(f"""
        CREATE OR REPLACE TABLE patients AS
        SELECT Id AS patient, GENDER AS gender, CAST(BIRTHDATE AS DATE) AS birthdate
        FROM read_csv_auto('{csv_dir}/patients.csv', header=true);
        CREATE OR REPLACE TABLE obs AS
        SELECT row_number() OVER () AS rec_id, PATIENT AS patient, CAST(DATE AS DATE) AS dt,
               CODE AS code, DESCRIPTION AS description, TRY_CAST(VALUE AS DOUBLE) AS value,
               UNITS AS units
        FROM read_csv_auto('{csv_dir}/observations.csv', header=true, all_varchar=true)
        WHERE TYPE = 'numeric' AND TRY_CAST(VALUE AS DOUBLE) IS NOT NULL;
        CREATE OR REPLACE TABLE cond AS
        SELECT row_number() OVER () AS rec_id, PATIENT AS patient, CAST(START AS DATE) AS dt,
               CAST(CODE AS VARCHAR) AS code, DESCRIPTION AS description
        FROM read_csv_auto('{csv_dir}/conditions.csv', header=true, all_varchar=true);
        CREATE OR REPLACE TABLE meds AS
        SELECT row_number() OVER () AS rec_id, PATIENT AS patient, CAST(START AS DATE) AS dt,
               CAST(CODE AS VARCHAR) AS code, DESCRIPTION AS description
        FROM read_csv_auto('{csv_dir}/medications.csv', header=true, all_varchar=true);
    """)


def derive_rules(con):
    """Build knowledge tables (rule parameters) from clean data. Returns list of rule dicts."""
    rules = []

    # Numeric observation codes ranked by frequency, with a dominant unit.
    numeric = con.execute("""
        SELECT code, any_value(description) AS d, mode(units) AS unit, count(*) AS n,
               min(value) AS mn, max(value) AS mx,
               quantile_cont(value, 0.001) AS p001, quantile_cont(value, 0.999) AS p999
        FROM obs GROUP BY code HAVING count(*) >= 200
           AND quantile_cont(value, 0.001) < quantile_cont(value, 0.999) ORDER BY n DESC
    """).fetchall()

    # T3 diagnosis-sex: conditions observed in only one sex (n >= 20).
    for code, d, sex in con.execute("""
        SELECT c.code, any_value(c.description), any_value(p.gender)
        FROM cond c JOIN patients p USING (patient)
        GROUP BY c.code HAVING count(DISTINCT p.gender) = 1 AND count(*) >= 20
        ORDER BY count(*) DESC LIMIT ?""", [RULES_PER_TEMPLATE]).fetchall():
        rules.append(dict(template="T3_sex", code=code, desc=d, allowed_sex=sex))

    # T4 age-diagnosis: conditions whose clean onset age is always >= 18.
    for code, d, min_age in con.execute("""
        SELECT c.code, any_value(c.description), min(date_diff('year', p.birthdate, c.dt))
        FROM cond c JOIN patients p USING (patient)
        GROUP BY c.code
        HAVING min(date_diff('year', p.birthdate, c.dt)) >= 18 AND count(*) >= 20
        ORDER BY count(*) DESC LIMIT ?""", [RULES_PER_TEMPLATE]).fetchall():
        rules.append(dict(template="T4_age", code=code, desc=d, min_age=int(min_age)))

    # T6 medication-diagnosis: med codes almost always co-occurring with one condition.
    for mcode, md, ccode, cd, ratio in con.execute("""
        WITH mp AS (SELECT DISTINCT code AS mcode, patient FROM meds),
             cp AS (SELECT DISTINCT code AS ccode, patient FROM cond),
             mcnt AS (SELECT mcode, count(*) AS n FROM mp GROUP BY mcode HAVING count(*) >= 50),
             co AS (SELECT mp.mcode, cp.ccode, count(*) AS k
                    FROM mp JOIN cp USING (patient) GROUP BY 1, 2),
             best AS (SELECT co.mcode, co.ccode, co.k * 1.0 / m.n AS ratio,
                             row_number() OVER (PARTITION BY co.mcode ORDER BY co.k DESC) AS rk
                      FROM co JOIN mcnt m USING (mcode))
        SELECT b.mcode, (SELECT any_value(description) FROM meds WHERE code=b.mcode),
               b.ccode, (SELECT any_value(description) FROM cond WHERE code=b.ccode), b.ratio
        FROM best b WHERE rk = 1 AND ratio >= 0.98 ORDER BY ratio DESC LIMIT ?""",
                                            [RULES_PER_TEMPLATE]).fetchall():
        rules.append(dict(template="T6_med_dx", code=mcode, desc=md, required_cond=ccode,
                          required_desc=cd))

    # Numeric-observation templates fill the library to TOTAL_RULES (T3 is capped by how few
    # sex-specific conditions exist in the data).
    deficit = TOTAL_RULES - len(rules)
    n_num = -(-deficit // 3)
    extra = 3 * n_num - deficit  # drop overshoot from T5
    # T1 physical impossibility: value outside a wide physically-possible envelope.
    for code, d, unit, n, mn, mx, p001, p999 in numeric[:n_num]:
        lo = 0.0 if mn >= 0 else mn - 5 * abs(mn)
        hi = mx * 3 if mx > 0 else mx + 100
        rules.append(dict(template="T1_impossible", code=code, desc=d, lo=round(lo, 4), hi=round(hi, 4)))
    # T2 implausible range: outside clean 0.1th-99.9th percentile.
    for code, d, unit, n, mn, mx, p001, p999 in numeric[:n_num]:
        rules.append(dict(template="T2_implausible", code=code, desc=d, lo=round(p001, 4), hi=round(p999, 4)))
    # T5 lab-unit consistency: unit must equal the dominant unit.
    for code, d, unit, n, mn, mx, p001, p999 in numeric[:n_num - extra]:
        rules.append(dict(template="T5_unit", code=code, desc=d, unit=unit))

    for i, r in enumerate(rules, 1):
        r["rule_id"] = f"R{i:04d}"
        r["pass_threshold"] = DEFAULT_PASS_THRESHOLD
    return rules


def inject_errors(con, rules, rate, rng):
    """Corrupt a copy of the clean tables; return ground-truth labels (template, rec_id)."""
    labels = []
    by_t = {}
    for r in rules:
        by_t.setdefault(r["template"], []).append(r)

    # Observation errors (T1/T2/T5) on rows in scope of the numeric rules.
    t1 = {r["code"]: r for r in by_t.get("T1_impossible", [])}
    t2 = {r["code"]: r for r in by_t.get("T2_implausible", [])}
    t5 = {r["code"]: r for r in by_t.get("T5_unit", [])}
    scope = con.execute("SELECT rec_id, code FROM obs WHERE code IN (SELECT unnest(?))",
                        [list(t1)]).fetchall()
    k = int(len(scope) * rate)
    picks = rng.sample(scope, k)
    updates = []
    for rec_id, code in picks:
        kind = rng.choice(["T1_impossible", "T2_implausible", "T5_unit"])
        if kind == "T5_unit" and code not in t5:
            kind = "T1_impossible"
        if kind == "T1_impossible":
            updates.append((rec_id, t1[code]["hi"] * rng.uniform(1.5, 3.0) + 1, None))
        elif kind == "T2_implausible" and code in t2:
            lo1, hi1 = t1[code]["lo"], t1[code]["hi"]
            lo2, hi2 = t2[code]["lo"], t2[code]["hi"]
            # implausible but physically possible: between plausible max and possible max
            v = hi2 + (hi1 - hi2) * rng.uniform(0.05, 0.6)
            updates.append((rec_id, v, None))
        else:
            kind = "T5_unit"
            updates.append((rec_id, None, "WRONG_UNIT"))
        labels.append((kind, rec_id))
    con.execute("CREATE TEMP TABLE upd(rec_id BIGINT, value DOUBLE, units VARCHAR)")
    con.executemany("INSERT INTO upd VALUES (?, ?, ?)", updates)
    con.execute("""UPDATE obs SET value = coalesce(upd.value, obs.value),
                                  units = coalesce(upd.units, obs.units)
                   FROM upd WHERE obs.rec_id = upd.rec_id""")
    con.execute("DROP TABLE upd")

    pats = [p for (p,) in con.execute("SELECT patient FROM patients").fetchall()]
    sex = dict(con.execute("SELECT patient, gender FROM patients").fetchall())
    bdate = dict(con.execute("SELECT patient, birthdate FROM patients").fetchall())
    max_c = con.execute("SELECT max(rec_id) FROM cond").fetchone()[0]
    max_m = con.execute("SELECT max(rec_id) FROM meds").fetchone()[0]
    new_cond, new_meds = [], []
    n_p = int(len(pats) * rate)

    # T3: add a sex-specific condition to a patient of the other sex.
    for p in rng.sample(pats, n_p):
        cands = [r for r in by_t.get("T3_sex", []) if r["allowed_sex"] != sex[p]]
        if not cands:
            continue
        r = rng.choice(cands)
        max_c += 1
        new_cond.append((max_c, p, "2024-01-01", r["code"], r["desc"]))
        labels.append(("T3_sex", max_c))
    # T4: add an adult-only condition with onset before its minimum age.
    for p in rng.sample(pats, n_p):
        r = rng.choice(by_t["T4_age"])
        onset_age = rng.randint(0, max(0, r["min_age"] - 2))
        y = bdate[p].year + onset_age
        max_c += 1
        new_cond.append((max_c, p, f"{y:04d}-06-15", r["code"], r["desc"]))
        labels.append(("T4_age", max_c))
    # T6: add a medication to a patient lacking the required condition.
    has_cond = {}
    for p, c in con.execute("SELECT DISTINCT patient, code FROM cond").fetchall():
        has_cond.setdefault(p, set()).add(c)
    for p in rng.sample(pats, n_p):
        cands = [r for r in by_t.get("T6_med_dx", []) if r["required_cond"] not in has_cond.get(p, set())]
        if not cands:
            continue
        r = rng.choice(cands)
        max_m += 1
        new_meds.append((max_m, p, "2024-01-01", r["code"], r["desc"]))
        labels.append(("T6_med_dx", max_m))
    con.executemany("INSERT INTO cond VALUES (?, ?, CAST(? AS DATE), ?, ?)", new_cond)
    con.executemany("INSERT INTO meds VALUES (?, ?, CAST(? AS DATE), ?, ?)", new_meds)
    return labels


def main():
    csv_dir, out_dir = Path(sys.argv[1]), Path(sys.argv[2])
    out_dir.mkdir(parents=True, exist_ok=True)
    clean_path = out_dir / "clean.duckdb"
    clean_path.unlink(missing_ok=True)
    con = duckdb.connect(str(clean_path))
    load_clean(con, csv_dir)
    stats = {t: con.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
             for t in ["patients", "obs", "cond", "meds"]}
    rules = derive_rules(con)
    con.close()
    (out_dir / "rules_initial.json").write_text(json.dumps(rules, indent=1, default=str))
    per_t = {}
    for r in rules:
        per_t[r["template"]] = per_t.get(r["template"], 0) + 1
    print("clean stats", stats)
    print("rules per template", per_t, "total", len(rules))

    summary = {"clean_counts": stats, "rules_per_template": per_t, "n_rules": len(rules), "rates": {}}
    for rate in ERROR_RATES:
        rng = random.Random(SEED + int(rate * 1000))
        path = out_dir / f"prod_{int(rate * 100):02d}.duckdb"
        path.unlink(missing_ok=True)
        con = duckdb.connect(str(path))
        con.execute(f"ATTACH '{clean_path}' AS c (READ_ONLY)")
        for t in ["patients", "obs", "cond", "meds"]:
            con.execute(f"CREATE TABLE {t} AS SELECT * FROM c.{t}")
        con.execute("DETACH c")
        labels = inject_errors(con, rules, rate, rng)
        con.execute("CREATE TABLE gt_labels(template VARCHAR, rec_id BIGINT)")
        con.executemany("INSERT INTO gt_labels VALUES (?, ?)", labels)
        con.close()
        summary["rates"][str(rate)] = {"labels": len(labels)}
        print(f"rate {rate}: {len(labels)} injected errors -> {path.name}")

    # Environment samples from the 5% file.
    src = out_dir / "prod_05.duckdb"
    for env, frac in ENV_SAMPLES.items():
        path = out_dir / f"{env}.duckdb"
        path.unlink(missing_ok=True)
        con = duckdb.connect(str(path))
        con.execute(f"ATTACH '{src}' AS s (READ_ONLY)")
        con.execute(f"""CREATE TABLE patients AS SELECT * FROM s.patients
                        WHERE hash(patient || '{SEED}') % 1000 < {int(frac * 1000)}""")
        for t in ["obs", "cond", "meds"]:
            con.execute(f"CREATE TABLE {t} AS SELECT * FROM s.{t} WHERE patient IN (SELECT patient FROM patients)")
        n = con.execute("SELECT count(*) FROM patients").fetchone()[0]
        con.execute("DETACH s")
        con.close()
        summary.setdefault("env_patients", {})[env] = n
        print(f"{env}: {n} patients")
    import shutil
    shutil.copy(src, out_dir / "prod.duckdb")
    (out_dir / "data_summary.json").write_text(json.dumps(summary, indent=1))


if __name__ == "__main__":
    main()

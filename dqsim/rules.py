"""Rule templates (adapted from Wang et al. 2020): render a rule's parameters to SQL."""


def q(s):
    return "'" + str(s).replace("'", "''") + "'"


def violation_sql(r):
    t = r["template"]
    if t in ("T1_impossible", "T2_implausible"):
        return (f"SELECT rec_id FROM obs WHERE code = {q(r['code'])} "
                f"AND (value < {float(r['lo'])} OR value > {float(r['hi'])})")
    if t == "T5_unit":
        return f"SELECT rec_id FROM obs WHERE code = {q(r['code'])} AND units <> {q(r['unit'])}"
    if t == "T3_sex":
        return (f"SELECT c.rec_id FROM cond c JOIN patients p USING (patient) "
                f"WHERE c.code = {q(r['code'])} AND p.gender <> {q(r['allowed_sex'])}")
    if t == "T4_age":
        return (f"SELECT c.rec_id FROM cond c JOIN patients p USING (patient) "
                f"WHERE c.code = {q(r['code'])} AND date_diff('year', p.birthdate, c.dt) < {int(r['min_age'])}")
    if t == "T6_med_dx":
        return (f"SELECT m.rec_id FROM meds m WHERE m.code = {q(r['code'])} AND NOT EXISTS "
                f"(SELECT 1 FROM cond c WHERE c.patient = m.patient AND c.code = {q(r['required_cond'])})")
    raise ValueError(t)


def scope_sql(r):
    t = r["template"]
    table = "obs" if t in ("T1_impossible", "T2_implausible", "T5_unit") else (
        "meds" if t == "T6_med_dx" else "cond")
    return f"SELECT count(*) FROM {table} WHERE code = {q(r['code'])}"


def validate_params(r):
    """Schema validation applied by the command handler before any event is appended."""
    t = r["template"]
    thr = r.get("pass_threshold")
    if thr is None or not (0.0 < float(thr) <= 1.0):
        return False
    if t in ("T1_impossible", "T2_implausible"):
        return float(r["lo"]) < float(r["hi"])
    if t == "T4_age":
        return 0 <= int(r["min_age"]) <= 120
    return True


PARAM_KEYS = ("lo", "hi", "unit", "allowed_sex", "min_age", "required_cond", "pass_threshold")


def risk(old, new):
    """Risk tier of a parameter change: relaxing a rule (fewer violations / lower bar) is HIGH."""
    if new is None:  # retirement removes a check entirely
        return "high"
    if old is None:  # creating a new (additional) check
        return "low"
    if float(new["pass_threshold"]) < float(old["pass_threshold"]):
        return "high"
    t = new["template"]
    if t in ("T1_impossible", "T2_implausible"):
        if float(new["lo"]) < float(old["lo"]) or float(new["hi"]) > float(old["hi"]):
            return "high"  # widened bounds accept more values
    if t == "T4_age" and int(new["min_age"]) < int(old["min_age"]):
        return "high"
    return "low"

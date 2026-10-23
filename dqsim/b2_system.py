"""Baseline B2: mutable parameter table + trigger-based audit log (no event log, no hash chain).

This is the strongest simple alternative to the event-sourced design. It keeps the same
template/parameter separation and the same command-layer policy checks (role permissions,
separation of duties, risk-tiered approval, optimistic concurrency, ordered gated promotion),
but stores the current parameters in a mutable table and records changes through database
triggers into an audit table, as conventional change-data-capture style auditing does.

Promotion is applied synchronously by the approving request: dev -> uat -> prod_test -> prod,
running the same execution gate in each environment before moving to the next.
"""
import json
import sqlite3
import threading
import time

import duckdb

from es_system import ENVS, CommandHandler
from rules import scope_sql, violation_sql

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=OFF;
CREATE TABLE ctx(id INTEGER PRIMARY KEY CHECK (id = 1), actor TEXT, ts REAL);
INSERT INTO ctx VALUES (1, NULL, NULL);
CREATE TABLE change_requests(rule_id TEXT, version INT, params TEXT, risk TEXT, author TEXT,
    status TEXT, approver TEXT, justification TEXT, updated_by TEXT, updated_at REAL,
    PRIMARY KEY (rule_id, version));
CREATE TABLE rule_params(env TEXT, rule_id TEXT, version INT, params TEXT, updated_by TEXT,
    updated_at REAL, PRIMARY KEY (env, rule_id));
CREATE TABLE audit_log(id INTEGER PRIMARY KEY AUTOINCREMENT, tbl TEXT, op TEXT, env TEXT,
    rule_id TEXT, old_version INT, new_version INT, old_params TEXT, new_params TEXT,
    old_status TEXT, new_status TEXT, changed_by TEXT, changed_at REAL);
CREATE TRIGGER rp_ins AFTER INSERT ON rule_params BEGIN
  INSERT INTO audit_log(tbl, op, env, rule_id, old_version, new_version, old_params, new_params,
      changed_by, changed_at)
  SELECT 'rule_params', 'INSERT', NEW.env, NEW.rule_id, NULL, NEW.version, NULL, NEW.params,
      actor, ts FROM ctx;
END;
CREATE TRIGGER rp_upd AFTER UPDATE ON rule_params BEGIN
  INSERT INTO audit_log(tbl, op, env, rule_id, old_version, new_version, old_params, new_params,
      changed_by, changed_at)
  SELECT 'rule_params', 'UPDATE', NEW.env, NEW.rule_id, OLD.version, NEW.version, OLD.params,
      NEW.params, actor, ts FROM ctx;
END;
CREATE TRIGGER rp_del AFTER DELETE ON rule_params BEGIN
  INSERT INTO audit_log(tbl, op, env, rule_id, old_version, new_version, old_params, new_params,
      changed_by, changed_at)
  SELECT 'rule_params', 'DELETE', OLD.env, OLD.rule_id, OLD.version, NULL, OLD.params, NULL,
      actor, ts FROM ctx;
END;
CREATE TRIGGER cr_ins AFTER INSERT ON change_requests BEGIN
  INSERT INTO audit_log(tbl, op, rule_id, new_version, new_params, new_status, changed_by,
      changed_at)
  SELECT 'change_requests', 'INSERT', NEW.rule_id, NEW.version, NEW.params, NEW.status,
      actor, ts FROM ctx;
END;
CREATE TRIGGER cr_upd AFTER UPDATE ON change_requests BEGIN
  INSERT INTO audit_log(tbl, op, rule_id, old_version, new_version, old_status, new_status,
      changed_by, changed_at)
  SELECT 'change_requests', 'UPDATE', NEW.rule_id, OLD.version, NEW.version, OLD.status,
      NEW.status, actor, ts FROM ctx;
END;
"""


UPSERT = ("INSERT INTO rule_params VALUES (?,?,?,?,?,?) ON CONFLICT(env, rule_id) DO UPDATE SET"
          " version=excluded.version, params=excluded.params, updated_by=excluded.updated_by,"
          " updated_at=excluded.updated_at")


class B2Handler(CommandHandler):
    """Same command-layer policy as the event-sourced handler; state lives in mutable tables."""

    def __init__(self, db_path, env_paths, clock=time.perf_counter):
        super().__init__(log=None)
        self.clock = clock
        self.db_path = str(db_path)
        self.db = sqlite3.connect(self.db_path, check_same_thread=False, isolation_level=None)
        self.db.executescript(SCHEMA)
        self.gov = self.db
        self.db_lock = threading.Lock()
        self.env_conn = {e: duckdb.connect(str(p), read_only=True) for e, p in env_paths.items()
                         if e != "prod"}
        self.local = threading.local()
        self.applied_at = {}
        self.rollback_lat = []
        self.dup_delivered = 0
        self.dup_skipped = 0
        self.gate_runs = 0
        self.gate_seconds = 0.0
        self.n_writes = 0
        self.gate_lock = threading.Lock()

    # projector-compatible no-ops (B2 applies synchronously)
    def start(self):
        pass

    def stop(self):
        pass

    def lag(self):
        return 0

    def _cur(self, env):
        if not hasattr(self.local, "cur"):
            self.local.cur = {e: c.cursor() for e, c in self.env_conn.items()}
        return self.local.cur[env]

    def _tx(self, stmts, actor):
        with self.db_lock:
            ts = self.clock()
            self.db.execute("BEGIN")
            # session context read by the audit triggers (who and when)
            self.db.execute("UPDATE ctx SET actor=?, ts=? WHERE id=1", [actor, ts])
            for sql, args in stmts:
                self.db.execute(sql, [ts if a is _TS else a for a in args])
            self.db.execute("COMMIT")
            self.n_writes += 1
            return ts

    def _emit(self, a, etype, version, actor, role, payload=None, env=None, justification=None):
        rid = a.rule_id
        ver = a.versions.get(version, {})
        params = ver.get("params")
        pj = json.dumps(params, default=str) if params is not None else None
        st = []
        if etype == "BOOTSTRAPPED":
            st.append(("INSERT INTO change_requests VALUES (?,?,?,?,?,?,?,?,?,?)",
                       [rid, version, pj, "low", actor, "approved", None, None, actor, _TS]))
            for e in ENVS:
                st.append((UPSERT, [e, rid, version, pj, actor, _TS]))
        elif etype in ("CREATED", "UPDATED", "RETIRE_REQUESTED"):
            st.append(("INSERT INTO change_requests VALUES (?,?,?,?,?,?,?,?,?,?)",
                       [rid, version, pj, payload["risk"], actor, "submitted", None, None, actor,
                        _TS]))
        elif etype == "APPROVED":
            st.append(("UPDATE change_requests SET status='approved', approver=?, justification=?,"
                       " updated_by=?, updated_at=? WHERE rule_id=? AND version=?",
                       [actor, justification, actor, _TS, rid, version]))
        elif etype == "PROMOTED":
            if params is None:
                st.append(("DELETE FROM rule_params WHERE env=? AND rule_id=?", [env, rid]))
            else:
                st.append((UPSERT, [env, rid, version, pj, actor, _TS]))
        elif etype == "ROLLED_BACK":
            for v, vv in a.versions.items():
                if v > version and vv["status"] == "rolled_back":
                    st.append(("UPDATE change_requests SET status='rolled_back', updated_by=?,"
                               " updated_at=? WHERE rule_id=? AND version=? AND status<>'rolled_back'",
                               [actor, _TS, rid, v]))
            for e in ENVS:
                st.append((UPSERT, [e, rid, version, pj, actor, _TS]))
        ts = self._tx(st, actor)
        self._bump("accepted")
        return {"rule_id": rid, "version": version, "type": etype, "ts": ts}

    def _gate(self, env, params):
        t0 = self.clock()
        c = self._cur(env)
        c.execute(f"SELECT count(*) FROM ({violation_sql(params)})").fetchone()
        c.execute(scope_sql(params)).fetchone()
        with self.gate_lock:
            self.gate_runs += 1
            self.gate_seconds += self.clock() - t0

    def approve(self, actor, role, rule_id, version, justification=None):
        rec = super().approve(actor, role, rule_id, version, justification)
        a = self.agg(rule_id)
        params = a.versions[version]["params"]
        if params is not None:   # compile check against dev before the first promotion
            self._cur("dev").execute(f"EXPLAIN {violation_sql(params)}")
        for k, env in enumerate(ENVS):
            prec = self.promote("system", "system", rule_id, version, env)
            if prec is None:     # superseded by a newer version: stop promoting this one
                break
            self.applied_at[(rule_id, version, "PROMOTED", env)] = prec["ts"]
            if k < 3 and params is not None:
                self._gate(env, params)
        return rec

    def rollback(self, actor, role, rule_id, expected_version):
        t0 = self.clock()
        rec = super().rollback(actor, role, rule_id, expected_version)
        self.rollback_lat.append(rec["ts"] - t0)
        return rec


class _TSMarker:
    pass


_TS = _TSMarker()

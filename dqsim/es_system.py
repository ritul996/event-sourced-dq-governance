"""Event-sourced DQ rule governance: partitioned hash-chained log, command handler, projector.

The log is an in-process stand-in that reproduces the Kafka properties the design relies on:
partitions keyed by rule id (per-rule total order), offset-addressed append-only storage,
consumer-tracked offsets, and at-least-once delivery (duplicates are injected deliberately).
"""
import hashlib
import json
import threading
import time
import uuid
from pathlib import Path

import duckdb

from rules import risk, scope_sql, validate_params, violation_sql

ENVS = ["dev", "uat", "prod_test", "prod"]
GENESIS = "0" * 64


def canonical(ev):
    return json.dumps({k: v for k, v in ev.items() if k != "hash"}, sort_keys=True,
                      separators=(",", ":"), default=str)


def chain_hash(prev, ev):
    return hashlib.sha256((prev + canonical(ev)).encode()).hexdigest()


class PartitionedLog:
    def __init__(self, directory, partitions=8, anchor_every=50, clock=time.perf_counter,
                 key_mode="rule", seed=0):
        import random as _r
        self.key_mode = key_mode          # "rule" (design) or "random" (ablation)
        self._rng = _r.Random(seed)
        self.hash_seconds = 0.0
        self.dir = Path(directory)
        self.dir.mkdir(parents=True, exist_ok=True)
        self.P = partitions
        self.anchor_every = anchor_every
        self.clock = clock
        self.parts = [[] for _ in range(partitions)]
        self.heads = [GENESIS] * partitions
        self.locks = [threading.Lock() for _ in range(partitions)]
        self.files = [open(self.dir / f"p{p}.jsonl", "w") for p in range(partitions)]
        self.anchors = []  # (partition, offset, hash) published to a separate store

    def partition_of(self, key):
        if self.key_mode == "random":
            return self._rng.randrange(self.P)
        return int(hashlib.md5(key.encode()).hexdigest(), 16) % self.P

    def append(self, key, ev):
        p = self.partition_of(key)
        with self.locks[p]:
            ev["partition"] = p
            ev["offset"] = len(self.parts[p])
            ev["ts"] = self.clock()
            ev["prev_hash"] = self.heads[p]
            _t = time.perf_counter()
            ev["hash"] = chain_hash(self.heads[p], ev)
            self.hash_seconds += time.perf_counter() - _t
            self.files[p].write(json.dumps(ev, default=str) + "\n")
            self.files[p].flush()
            self.parts[p].append(ev)
            self.heads[p] = ev["hash"]
            if (ev["offset"] + 1) % self.anchor_every == 0:
                self.anchors.append((p, ev["offset"], ev["hash"]))
            return ev

    def fetch(self, p, offset, max_n=500):
        return self.parts[p][offset:offset + max_n]

    def close(self):
        for p in range(self.P):
            with self.locks[p]:
                # final anchor for every non-empty partition at close (end-of-run checkpoint)
                self.files[p].close()
        (self.dir / "anchors.json").write_text(json.dumps(self.anchors))


class Conflict(Exception):
    pass


class PolicyViolation(Exception):
    def __init__(self, kind):
        super().__init__(kind)
        self.kind = kind


ROLE_PERMS = {
    "author": {"create", "update", "retire", "rollback"},
    "approver": {"approve", "reject"},
    "system": {"promote", "bootstrap"},
}


class Aggregate:
    def __init__(self, rule_id):
        self.rule_id = rule_id
        self.lock = threading.Lock()
        self.latest = 0
        self.versions = {}          # v -> dict(params, status, author, risk)
        self.active = {e: None for e in ENVS}
        self.prod_history = []      # versions that have been active in prod, in order


class Truth:
    """Independent oracle: activation changes recorded at decision time by the handler."""

    def __init__(self):
        self.lock = threading.Lock()
        self.history = []  # (ts, rule_id, env, version or None)

    def record(self, ts, rule_id, env, version):
        with self.lock:
            self.history.append((ts, rule_id, env, version))


class CommandHandler:
    def __init__(self, log, enforce_policy=True):
        self.log = log
        self.enforce_policy = enforce_policy
        self.aggs = {}
        self.aggs_lock = threading.Lock()
        self.truth = Truth()
        self.stats = {"accepted": 0, "conflict": 0, "violation_sod": 0, "violation_c4": 0,
                      "violation_role": 0, "violation_order": 0, "invalid_params": 0,
                      "superseded": 0}
        self.stats_lock = threading.Lock()

    def _bump(self, k):
        with self.stats_lock:
            self.stats[k] += 1

    def _check_role(self, role, action):
        if self.enforce_policy and action not in ROLE_PERMS.get(role, set()):
            self._bump("violation_role")
            raise PolicyViolation("role")

    def agg(self, rule_id, create=False):
        with self.aggs_lock:
            a = self.aggs.get(rule_id)
            if a is None and create:
                a = self.aggs[rule_id] = Aggregate(rule_id)
            return a

    def _emit(self, a, etype, version, actor, role, payload=None, env=None, justification=None):
        ev = {"event_id": uuid.uuid4().hex, "rule_id": a.rule_id, "version": version,
              "type": etype, "actor": actor, "role": role, "env": env,
              "justification": justification, "payload": payload}
        rec = self.log.append(a.rule_id, ev)
        self._bump("accepted")
        return rec

    # ----- commands -----
    def bootstrap(self, params):
        a = self.agg(params["rule_id"], create=True)
        # Each rule starts with a short approved history (v1 stricter, v2 current) so that
        # rollback targets exist from the start of the workload.
        v1 = dict(params, pass_threshold=round(min(0.99, float(params["pass_threshold"]) + 0.05), 2))
        with a.lock:
            a.latest = 2
            a.versions[1] = dict(params=v1, status="approved", author="bootstrap", risk="low")
            a.versions[2] = dict(params=params, status="approved", author="bootstrap", risk="low")
            rec1 = self._emit(a, "BOOTSTRAPPED", 1, "bootstrap", "system", payload=v1)
            for e in ENVS:  # seeded history: v1 was active before v2 (oracle must record it)
                self.truth.record(rec1["ts"], a.rule_id, e, 1)
            rec = self._emit(a, "BOOTSTRAPPED", 2, "bootstrap", "system", payload=params)
            for e in ENVS:
                a.active[e] = 2
                self.truth.record(rec["ts"], a.rule_id, e, 2)
            a.prod_history += [1, 2]
            return rec

    def submit_change(self, actor, role, action, rule_id, expected_version, params):
        """create / update / retire: append a new submitted version."""
        self._check_role(role, action)
        if params is not None and not validate_params(params):
            self._bump("invalid_params")
            raise PolicyViolation("invalid_params")
        a = self.agg(rule_id, create=(action == "create"))
        with a.lock:
            if expected_version != a.latest:
                self._bump("conflict")
                raise Conflict()
            prod_v = a.active["prod"]
            old = a.versions[prod_v]["params"] if prod_v else None
            v = a.latest + 1
            r = risk(old, params)
            a.latest = v
            a.versions[v] = dict(params=params, status="submitted", author=actor, risk=r)
            etype = {"create": "CREATED", "update": "UPDATED", "retire": "RETIRE_REQUESTED"}[action]
            rec = self._emit(a, etype, v, actor, role, payload={"params": params, "risk": r})
            return rec, v, r

    def approve(self, actor, role, rule_id, version, justification=None):
        self._check_role(role, "approve")
        a = self.agg(rule_id)
        with a.lock:
            ver = a.versions.get(version)
            # policy checks first (deny before any state check)
            if self.enforce_policy and ver is not None and actor == ver["author"]:
                self._bump("violation_sod")
                raise PolicyViolation("sod")
            if self.enforce_policy and ver is not None and ver["risk"] == "high" and not justification:
                self._bump("violation_c4")
                raise PolicyViolation("c4")
            if ver is None or ver["status"] != "submitted" or version != a.latest:
                self._bump("superseded")
                raise Conflict()
            ver["status"] = "approved"
            ver["approver"] = actor
            return self._emit(a, "APPROVED", version, actor, role, justification=justification)

    def promote(self, actor, role, rule_id, version, env):
        self._check_role(role, "promote")
        a = self.agg(rule_id)
        with a.lock:
            ver = a.versions.get(version)
            if ver is None or ver["status"] != "approved" or version != a.latest:
                self._bump("superseded")
                return None
            k = ENVS.index(env)
            if k > 0 and a.active[ENVS[k - 1]] != (version if ver["params"] is not None else None):
                self._bump("violation_order")
                raise PolicyViolation("order")
            rec = self._emit(a, "PROMOTED", version, actor, role, env=env)
            a.active[env] = version if ver["params"] is not None else None
            self.truth.record(rec["ts"], rule_id, env, a.active[env])
            if env == "prod" and ver["params"] is not None:
                a.prod_history.append(version)
            return rec

    def gate_fail(self, rule_id, version, env, evidence):
        """A promotion gate rejected an approved version: record it and stop its promotion."""
        a = self.agg(rule_id)
        with a.lock:
            ver = a.versions.get(version)
            if ver is None or ver["status"] != "approved" or version != a.latest:
                return None
            ver["status"] = "blocked"
            return self._emit(a, "GATE_FAILED", version, "system", "system", env=env, payload=evidence)

    def rollback(self, actor, role, rule_id, expected_version):
        """Pre-authorized rollback to the previously active prod version (already approved)."""
        self._check_role(role, "rollback")
        a = self.agg(rule_id)
        with a.lock:
            if expected_version != a.latest:
                self._bump("conflict")
                raise Conflict()
            hist = [v for v in a.prod_history if v != a.active["prod"]]
            if a.active["prod"] is None or not hist:
                raise PolicyViolation("nothing_to_rollback")
            target = hist[-1]
            for v, ver in a.versions.items():
                if v > target and ver["status"] in ("submitted", "approved"):
                    ver["status"] = "rolled_back"
            rec = self._emit(a, "ROLLED_BACK", target, actor, role, env="*",
                             payload={"from": a.active["prod"], "to": target})
            for e in ENVS:
                a.active[e] = target
                self.truth.record(rec["ts"], rule_id, e, target)
            a.prod_history.append(target)
            return rec


class Projector:
    """Per-partition idempotent consumers that maintain per-environment active-rule projections
    in the analytical store and drive gated promotion dev -> uat -> prod_test -> prod."""

    def __init__(self, log, handler, gov_path, env_paths, poll_interval=0.1, dup_rate=0.05,
                 seed=0, clock=time.perf_counter, idempotent=True, gate="exec",
                 gate_rel=0.10, gate_min=5):
        import random
        self.idempotent = idempotent      # False only in the ablation study
        self.gate_mode = gate             # "exec": compiles and runs; "impact": also bounds new flags
        self.gate_rel, self.gate_min = gate_rel, gate_min
        self.gate_log = []                # (rule, version, env, cand_viol, base_viol, scope, blocked)
        self.log, self.h = log, handler
        self.poll, self.dup_rate = poll_interval, dup_rate
        self.clock = clock
        self.rng = random.Random(seed)
        self.gov = duckdb.connect(str(gov_path))
        pk = "PRIMARY KEY" if idempotent else ""
        self.gov.execute(f"""
            CREATE TABLE IF NOT EXISTS events_applied(event_id VARCHAR {pk}, rule_id VARCHAR,
                version INT, type VARCHAR, actor VARCHAR, role VARCHAR, env VARCHAR,
                justification VARCHAR, partition_id INT, log_offset BIGINT, hash VARCHAR, applied_at DOUBLE);
            CREATE TABLE IF NOT EXISTS active_rules(env VARCHAR, rule_id VARCHAR, version INT,
                params VARCHAR, activated_at DOUBLE, PRIMARY KEY (env, rule_id));
        """)
        self.envs = {e: duckdb.connect(str(p), read_only=True) for e, p in env_paths.items()
                     if e != "prod"}
        self.offsets = [0] * log.P
        self.processed = set()
        self.proc_lock = threading.Lock()
        self.applied_at = {}  # (rule_id, version, type, env) -> apply time
        self.rollback_lat = []
        self.dup_delivered = 0
        self.dup_skipped = 0
        self.gate_runs = 0
        self.gate_seconds = 0.0
        self.errors = 0
        self.error_samples = []
        self.running = False
        self.threads = []

    def start(self):
        self.running = True
        for p in range(self.log.P):
            t = threading.Thread(target=self._loop, args=(p,), daemon=True)
            t.start()
            self.threads.append(t)

    def stop(self):
        self.running = False
        for t in self.threads:
            t.join()

    def lag(self):
        return sum(len(self.log.parts[p]) - self.offsets[p] for p in range(self.log.P))

    def _loop(self, p):
        cur = self.gov.cursor()
        env_cur = {e: c.cursor() for e, c in self.envs.items()}
        while self.running:
            batch = self.log.fetch(p, self.offsets[p])
            if not batch:
                time.sleep(self.poll)
                continue
            for ev in batch:
                try:
                    self._deliver(cur, env_cur, ev)
                    if self.rng.random() < self.dup_rate:   # at-least-once: redeliver
                        self.dup_delivered += 1
                        self._deliver(cur, env_cur, ev)
                except Exception as exc:   # never expected in the design; counted in ablations
                    self.errors += 1
                    self.error_samples.append(repr(exc)[:200])
                self.offsets[p] += 1

    def _deliver(self, cur, env_cur, ev):
        with self.proc_lock:
            if self.idempotent and ev["event_id"] in self.processed:
                self.dup_skipped += 1
                return
            self.processed.add(ev["event_id"])
        self._apply(cur, env_cur, ev)

    def _gate(self, env_cur, env, params):
        t0 = self.clock()
        c = env_cur[env]
        viol = c.execute(f"SELECT count(*) FROM ({violation_sql(params)})").fetchone()[0]
        scope = c.execute(scope_sql(params)).fetchone()[0]
        self.gate_runs += 1
        self.gate_seconds += self.clock() - t0
        return viol, scope

    def _upsert_active(self, cur, env, rule_id, version, params, now):
        if params is None:
            cur.execute("DELETE FROM active_rules WHERE env=? AND rule_id=?", [env, rule_id])
        else:
            cur.execute("INSERT OR REPLACE INTO active_rules VALUES (?, ?, ?, ?, ?)",
                        [env, rule_id, version, json.dumps(params, default=str), now])

    def _apply(self, cur, env_cur, ev):
        a = self.h.agg(ev["rule_id"])
        t, rid, v = ev["type"], ev["rule_id"], ev["version"]
        cur.execute("INSERT INTO events_applied VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                    [ev["event_id"], rid, v, t, ev["actor"], ev["role"], ev["env"],
                     ev["justification"], ev["partition"], ev["offset"], ev["hash"], self.clock()])
        if t == "BOOTSTRAPPED":
            now = self.clock()
            for e in ENVS:
                self._upsert_active(cur, e, rid, v, ev["payload"], now)
        elif t == "APPROVED":
            params = a.versions[v]["params"]
            if params is not None:  # compile check against dev before first promotion
                env_cur["dev"].execute(f"EXPLAIN {violation_sql(params)}")
            self.h.promote("system", "system", rid, v, "dev")
        elif t == "PROMOTED":
            params = a.versions[v]["params"]
            now = self.clock()
            self._upsert_active(cur, ev["env"], rid, v, params, now)
            self.applied_at[(rid, v, "PROMOTED", ev["env"])] = now
            k = ENVS.index(ev["env"])
            if k < 3:
                if params is not None:
                    cand, scope = self._gate(env_cur, ev["env"], params)  # C5: gate in e_k first
                    if self.gate_mode == "impact":
                        base_v = a.active[ENVS[k + 1]]
                        base_p = a.versions[base_v]["params"] if base_v is not None else None
                        base = (env_cur[ev["env"]].execute(
                            f"SELECT count(*) FROM ({violation_sql(base_p)})").fetchone()[0]
                            if base_p is not None else 0)
                        blocked = cand - base > max(self.gate_min, self.gate_rel * scope)
                        self.gate_log.append((rid, v, ev["env"], cand, base, scope, blocked))
                        if blocked:
                            self.h.gate_fail(rid, v, ev["env"], {"flagged": cand, "baseline": base,
                                                                 "scope": scope})
                            return
                try:
                    self.h.promote("system", "system", rid, v, ENVS[k + 1])
                except PolicyViolation:
                    pass
        elif t == "ROLLED_BACK":
            params = a.versions[v]["params"]
            now = self.clock()
            for e in ENVS:
                self._upsert_active(cur, e, rid, v, params, now)
            self.rollback_lat.append(now - ev["ts"])

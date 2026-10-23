"""Baseline B3: B2 plus a hash-chained audit table and external anchors ("ledger-style" table).

Same mutable parameter table, triggers, command-layer policy, and synchronous promotion as B2.
In addition, every audit row stores prev_hash and hash = SHA-256(prev_hash || canonical(row)),
computed inside the database by an AFTER INSERT trigger, and the head (id, hash) is published to
an external anchor list every k rows. This mirrors how ledger tables in commercial databases make
a mutable table's history tamper-evident.
"""
import hashlib
import json
import time

from b2_system import B2Handler

GENESIS = "0" * 64
ROW_FIELDS = ["id", "tbl", "op", "env", "rule_id", "old_version", "new_version", "old_params",
              "new_params", "old_status", "new_status", "changed_by", "changed_at"]


def row_hash(prev, values):
    return hashlib.sha256((prev + json.dumps(list(values), separators=(",", ":"),
                                             default=str)).encode()).hexdigest()


CHAIN_SQL = """
ALTER TABLE audit_log ADD COLUMN prev_hash TEXT;
ALTER TABLE audit_log ADD COLUMN hash TEXT;
CREATE TRIGGER audit_chain AFTER INSERT ON audit_log BEGIN
  UPDATE audit_log SET
    prev_hash = coalesce((SELECT hash FROM audit_log WHERE id < NEW.id ORDER BY id DESC LIMIT 1), '{g}'),
    hash = chain(coalesce((SELECT hash FROM audit_log WHERE id < NEW.id ORDER BY id DESC LIMIT 1), '{g}'),
                 NEW.id, NEW.tbl, NEW.op, NEW.env, NEW.rule_id, NEW.old_version, NEW.new_version,
                 NEW.old_params, NEW.new_params, NEW.old_status, NEW.new_status, NEW.changed_by,
                 NEW.changed_at)
  WHERE id = NEW.id;
END;
""".format(g=GENESIS)


class B3Handler(B2Handler):
    def __init__(self, db_path, env_paths, anchor_every=50, clock=time.perf_counter):
        self._hash_seconds = 0.0

        def chain(prev, *values):
            t0 = time.perf_counter()
            h = row_hash(prev, values)
            self._hash_seconds += time.perf_counter() - t0
            return h

        self._chain = chain
        super().__init__(db_path, env_paths, clock=clock)
        self.db.create_function("chain", 14, chain, deterministic=True)
        self.db.executescript(CHAIN_SQL)
        self.anchor_every = anchor_every
        self.anchors = []
        self._last_anchor_id = 0

    def _tx(self, stmts, actor):
        ts = super()._tx(stmts, actor)
        if self.anchor_every:
            with self.db_lock:
                rows = self.db.execute(
                    "SELECT id, hash FROM audit_log WHERE id > ? AND id % ? = 0 ORDER BY id",
                    [self._last_anchor_id, self.anchor_every]).fetchall()
                for rid, h in rows:
                    self.anchors.append((rid, h))
                    self._last_anchor_id = rid
        return ts

    @property
    def hash_seconds(self):
        return self._hash_seconds

"""SQLite persistence. Every state transition is a conditional UPDATE (compare-and-set),
so concurrent drivers/approvers (threads or processes) cannot double-claim a step.

A new connection is opened per operation (cheap for SQLite) so the store is safe to use
from any thread; WAL mode + busy_timeout handle concurrent writers.
"""
import contextlib
import json
import os
import sqlite3
import time
import uuid

SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    run_id        TEXT PRIMARY KEY,
    workflow_id   TEXT NOT NULL,
    workflow_json TEXT NOT NULL,        -- snapshot of the normalized config used by this run
    input_json    TEXT NOT NULL,
    status        TEXT NOT NULL,        -- RUNNING/WAITING_APPROVAL/SUCCEEDED/FAILED/CANCELLED/REJECTED
    reason        TEXT,
    result_json   TEXT,
    created_at    REAL NOT NULL,
    updated_at    REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS node_states (
    run_id      TEXT NOT NULL,
    node_id     TEXT NOT NULL,
    node_type   TEXT NOT NULL,
    status      TEXT NOT NULL,          -- PENDING/RUNNING/SUCCEEDED/FAILED/SKIPPED/WAITING_APPROVAL/CANCELLED/REJECTED
    attempts    INTEGER NOT NULL DEFAULT 0,
    output_json TEXT,
    error       TEXT,
    reason      TEXT,
    owner       TEXT,                   -- engine instance that claimed the node (pid:uuid)
    generation  INTEGER NOT NULL DEFAULT 0,  -- bumped on rerun
    started_at  REAL,
    finished_at REAL,
    updated_at  REAL NOT NULL,
    PRIMARY KEY (run_id, node_id)
);
CREATE TABLE IF NOT EXISTS attempts (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id      TEXT NOT NULL,
    node_id     TEXT NOT NULL,
    attempt     INTEGER NOT NULL,
    generation  INTEGER NOT NULL,
    status      TEXT NOT NULL,          -- RUNNING/SUCCEEDED/FAILED/CANCELLED/INTERRUPTED/WAITING_APPROVAL
    error       TEXT,
    owner       TEXT,
    started_at  REAL NOT NULL,
    finished_at REAL
);
CREATE TABLE IF NOT EXISTS approvals (
    approval_id  TEXT PRIMARY KEY,
    run_id       TEXT NOT NULL,
    node_id      TEXT NOT NULL,
    generation   INTEGER NOT NULL,
    content      TEXT NOT NULL,         -- exactly what the approver is shown
    content_hash TEXT NOT NULL,         -- sha256(content)
    status       TEXT NOT NULL,         -- PENDING/APPROVED/REJECTED/SUPERSEDED/CANCELLED
    decided_by   TEXT,
    comment      TEXT,
    created_at   REAL NOT NULL,
    decided_at   REAL
);
CREATE TABLE IF NOT EXISTS events (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id    TEXT NOT NULL,
    node_id   TEXT,
    kind      TEXT NOT NULL,
    detail    TEXT,
    ts        REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_attempts_run ON attempts(run_id, node_id);
CREATE INDEX IF NOT EXISTS idx_approvals_run ON approvals(run_id, node_id);
CREATE INDEX IF NOT EXISTS idx_events_run ON events(run_id);
"""

RUN_ACTIVE = ("RUNNING", "WAITING_APPROVAL")
RUN_TERMINAL = ("SUCCEEDED", "FAILED", "CANCELLED", "REJECTED")
NODE_TERMINAL = ("SUCCEEDED", "FAILED", "SKIPPED", "CANCELLED", "REJECTED")


def _dumps(v):
    return None if v is None else json.dumps(v, ensure_ascii=False, sort_keys=True)


def _loads(s):
    return None if s is None else json.loads(s)


class Store:
    def __init__(self, path):
        self.path = path
        d = os.path.dirname(os.path.abspath(path))
        os.makedirs(d, exist_ok=True)
        with self._conn() as c:
            c.execute("PRAGMA journal_mode=WAL")
            c.executescript(SCHEMA)

    @contextlib.contextmanager
    def _conn(self):
        conn = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000")
        try:
            yield conn
        finally:
            conn.close()

    @contextlib.contextmanager
    def _tx(self):
        """Explicit write transaction (BEGIN IMMEDIATE takes the write lock up front)."""
        with self._conn() as c:
            c.execute("BEGIN IMMEDIATE")
            try:
                yield c
                c.execute("COMMIT")
            except BaseException:
                c.execute("ROLLBACK")
                raise

    # ------------------------------------------------------------------ runs
    def create_run(self, workflow, run_input):
        run_id = "run_" + time.strftime("%Y%m%d%H%M%S") + "_" + uuid.uuid4().hex[:6]
        now = time.time()
        with self._tx() as c:
            c.execute("INSERT INTO runs VALUES (?,?,?,?,?,?,?,?,?)",
                      (run_id, workflow["id"], _dumps(workflow), _dumps(run_input), "RUNNING",
                       None, None, now, now))
            for n in workflow["nodes"]:
                c.execute("INSERT INTO node_states (run_id,node_id,node_type,status,updated_at) VALUES (?,?,?,?,?)",
                          (run_id, n["id"], n["type"], "PENDING", now))
            c.execute("INSERT INTO events (run_id,node_id,kind,detail,ts) VALUES (?,?,?,?,?)",
                      (run_id, None, "run_created", workflow["id"], now))
        return run_id

    def _run_row(self, r):
        if r is None:
            return None
        d = dict(r)
        d["workflow"] = _loads(d.pop("workflow_json"))
        d["input"] = _loads(d.pop("input_json"))
        d["result"] = _loads(d.pop("result_json"))
        return d

    def get_run(self, run_id):
        with self._conn() as c:
            return self._run_row(c.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone())

    def get_run_status(self, run_id):
        with self._conn() as c:
            r = c.execute("SELECT status FROM runs WHERE run_id=?", (run_id,)).fetchone()
            return r["status"] if r else None

    def list_runs(self, limit=50):
        with self._conn() as c:
            rows = c.execute("SELECT run_id, workflow_id, status, reason, created_at, updated_at FROM runs "
                             "ORDER BY created_at DESC LIMIT ?", (limit,)).fetchall()
            return [dict(r) for r in rows]

    def cas_run(self, run_id, from_statuses, to_status, reason=None, result=None, set_reason=True):
        """Atomically move a run from one of ``from_statuses`` to ``to_status``. Return True if done."""
        q = "UPDATE runs SET status=?, updated_at=?"
        args = [to_status, time.time()]
        if set_reason:
            q += ", reason=?"
            args.append(reason)
        if result is not None:
            q += ", result_json=?"
            args.append(_dumps(result))
        q += " WHERE run_id=? AND status IN (%s)" % ",".join("?" * len(from_statuses))
        args += [run_id] + list(from_statuses)
        with self._conn() as c:
            return c.execute(q, args).rowcount == 1

    def set_run_input(self, run_id, run_input):
        with self._conn() as c:
            c.execute("UPDATE runs SET input_json=?, updated_at=? WHERE run_id=?",
                      (_dumps(run_input), time.time(), run_id))

    # ----------------------------------------------------------------- nodes
    def get_nodes(self, run_id):
        with self._conn() as c:
            rows = c.execute("SELECT * FROM node_states WHERE run_id=?", (run_id,)).fetchall()
        out = {}
        for r in rows:
            d = dict(r)
            d["output"] = _loads(d.pop("output_json"))
            out[d["node_id"]] = d
        return out

    def claim_node(self, run_id, node_id, owner):
        """PENDING -> RUNNING. Only one caller can win (rowcount check)."""
        now = time.time()
        with self._conn() as c:
            return c.execute("UPDATE node_states SET status='RUNNING', owner=?, started_at=?, finished_at=NULL, "
                             "error=NULL, reason=NULL, updated_at=? WHERE run_id=? AND node_id=? AND status='PENDING'",
                             (owner, now, now, run_id, node_id)).rowcount == 1

    def skip_node(self, run_id, node_id, reason):
        now = time.time()
        with self._conn() as c:
            return c.execute("UPDATE node_states SET status='SKIPPED', reason=?, finished_at=?, updated_at=? "
                             "WHERE run_id=? AND node_id=? AND status='PENDING'",
                             (reason, now, now, run_id, node_id)).rowcount == 1

    def finish_node(self, run_id, node_id, owner, status, output=None, error=None, reason=None):
        """RUNNING(owner) -> terminal status. No-op (False) if someone else owns/changed it."""
        now = time.time()
        with self._conn() as c:
            return c.execute("UPDATE node_states SET status=?, output_json=?, error=?, reason=?, finished_at=?, "
                             "updated_at=? WHERE run_id=? AND node_id=? AND status='RUNNING' AND owner=?",
                             (status, _dumps(output), error, reason, now, now, run_id, node_id, owner)).rowcount == 1

    def cas_node(self, run_id, node_id, from_status, to_status, output=None, reason=None, error=None,
                 set_output=False):
        now = time.time()
        q = "UPDATE node_states SET status=?, reason=?, error=?, updated_at=?"
        args = [to_status, reason, error, now]
        if to_status in NODE_TERMINAL:
            q += ", finished_at=?"
            args.append(now)
        if set_output:
            q += ", output_json=?"
            args.append(_dumps(output))
        q += " WHERE run_id=? AND node_id=? AND status=?"
        args += [run_id, node_id, from_status]
        with self._conn() as c:
            return c.execute(q, args).rowcount == 1

    def cancel_open_nodes(self, run_id, reason, statuses=("PENDING",)):
        """Mark nodes in ``statuses`` CANCELLED; return their ids."""
        now = time.time()
        with self._tx() as c:
            ph = ",".join("?" * len(statuses))
            ids = [r["node_id"] for r in c.execute(
                "SELECT node_id FROM node_states WHERE run_id=? AND status IN (%s)" % ph,
                [run_id] + list(statuses)).fetchall()]
            c.execute("UPDATE node_states SET status='CANCELLED', reason=?, finished_at=?, updated_at=? "
                      "WHERE run_id=? AND status IN (%s)" % ph, [reason, now, now, run_id] + list(statuses))
            c.execute("UPDATE approvals SET status='CANCELLED', decided_at=? WHERE run_id=? AND status='PENDING'",
                      (now, run_id))
        return ids

    def reset_nodes(self, run_id, node_ids):
        """Put nodes back to PENDING for a rerun (bumps generation, keeps attempt counters)."""
        now = time.time()
        with self._tx() as c:
            for nid in node_ids:
                c.execute("UPDATE node_states SET status='PENDING', output_json=NULL, error=NULL, reason=NULL, "
                          "owner=NULL, started_at=NULL, finished_at=NULL, generation=generation+1, updated_at=? "
                          "WHERE run_id=? AND node_id=? AND status!='RUNNING'", (now, run_id, nid))
            c.execute("UPDATE approvals SET status='SUPERSEDED', decided_at=? WHERE run_id=? AND status='PENDING' "
                      "AND node_id IN (%s)" % ",".join("?" * len(node_ids)), [now, run_id] + list(node_ids))

    # -------------------------------------------------------------- attempts
    def start_attempt(self, run_id, node_id, owner):
        now = time.time()
        with self._tx() as c:
            row = c.execute("SELECT attempts, generation, status, owner FROM node_states WHERE run_id=? AND node_id=?",
                            (run_id, node_id)).fetchone()
            if row is None or row["status"] != "RUNNING" or row["owner"] != owner:
                return None
            n = row["attempts"] + 1
            c.execute("UPDATE node_states SET attempts=?, updated_at=? WHERE run_id=? AND node_id=?",
                      (n, now, run_id, node_id))
            cur = c.execute("INSERT INTO attempts (run_id,node_id,attempt,generation,status,owner,started_at) "
                            "VALUES (?,?,?,?,?,?,?)", (run_id, node_id, n, row["generation"], "RUNNING", owner, now))
            return cur.lastrowid, n

    def finish_attempt(self, attempt_row_id, status, error=None):
        with self._conn() as c:
            c.execute("UPDATE attempts SET status=?, error=?, finished_at=? WHERE id=? AND status='RUNNING'",
                      (status, error, time.time(), attempt_row_id))

    def interrupt_attempts(self, run_id, node_id, reason):
        with self._conn() as c:
            c.execute("UPDATE attempts SET status='INTERRUPTED', error=?, finished_at=? WHERE run_id=? AND node_id=? "
                      "AND status='RUNNING'", (reason, time.time(), run_id, node_id))

    def get_attempts(self, run_id, node_id=None):
        with self._conn() as c:
            if node_id:
                rows = c.execute("SELECT * FROM attempts WHERE run_id=? AND node_id=? ORDER BY id",
                                 (run_id, node_id)).fetchall()
            else:
                rows = c.execute("SELECT * FROM attempts WHERE run_id=? ORDER BY id", (run_id,)).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------- approvals
    def enter_waiting_approval(self, run_id, node_id, owner, content, content_hash, attempt_row_id):
        """RUNNING(owner) -> WAITING_APPROVAL + new PENDING approval request, in one transaction."""
        now = time.time()
        approval_id = "apr_" + uuid.uuid4().hex[:10]
        with self._tx() as c:
            row = c.execute("SELECT generation FROM node_states WHERE run_id=? AND node_id=? AND status='RUNNING' "
                            "AND owner=?", (run_id, node_id, owner)).fetchone()
            if row is None:
                return None
            c.execute("UPDATE approvals SET status='SUPERSEDED', decided_at=? WHERE run_id=? AND node_id=? "
                      "AND status='PENDING'", (now, run_id, node_id))
            c.execute("INSERT INTO approvals VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                      (approval_id, run_id, node_id, row["generation"], content, content_hash, "PENDING",
                       None, None, now, None))
            c.execute("UPDATE node_states SET status='WAITING_APPROVAL', updated_at=? WHERE run_id=? AND node_id=?",
                      (now, run_id, node_id))
            c.execute("UPDATE attempts SET status='WAITING_APPROVAL', finished_at=? WHERE id=?", (now, attempt_row_id))
        return approval_id

    def add_approval(self, run_id, node_id, content, content_hash):
        approval_id = "apr_" + uuid.uuid4().hex[:10]
        with self._tx() as c:
            gen = c.execute("SELECT generation FROM node_states WHERE run_id=? AND node_id=?",
                            (run_id, node_id)).fetchone()["generation"]
            c.execute("INSERT INTO approvals VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                      (approval_id, run_id, node_id, gen, content, content_hash, "PENDING", None, None,
                       time.time(), None))
        return approval_id

    def requeue_interrupted(self, run_id, node_id, owner, reason):
        with self._conn() as c:
            return c.execute("UPDATE node_states SET status='PENDING', owner=NULL, reason=?, updated_at=? "
                             "WHERE run_id=? AND node_id=? AND status='RUNNING' AND owner=?",
                             (reason, time.time(), run_id, node_id, owner)).rowcount == 1

    def get_approvals(self, run_id, node_id=None):
        with self._conn() as c:
            if node_id:
                rows = c.execute("SELECT * FROM approvals WHERE run_id=? AND node_id=? ORDER BY created_at, rowid",
                                 (run_id, node_id)).fetchall()
            else:
                rows = c.execute("SELECT * FROM approvals WHERE run_id=? ORDER BY created_at, rowid",
                                 (run_id,)).fetchall()
        return [dict(r) for r in rows]

    def latest_approval(self, run_id, node_id):
        rows = self.get_approvals(run_id, node_id)
        return rows[-1] if rows else None

    def decide_approval(self, approval_id, content_hash, decision, user, comment=None):
        """PENDING -> APPROVED/REJECTED only if the hash matches. Atomic; returns True for exactly one caller."""
        with self._conn() as c:
            return c.execute("UPDATE approvals SET status=?, decided_by=?, comment=?, decided_at=? "
                             "WHERE approval_id=? AND status='PENDING' AND content_hash=?",
                             (decision, user, comment, time.time(), approval_id, content_hash)).rowcount == 1

    def supersede_approval(self, approval_id):
        with self._conn() as c:
            c.execute("UPDATE approvals SET status='SUPERSEDED', decided_at=? WHERE approval_id=? AND status='PENDING'",
                      (time.time(), approval_id))

    # ---------------------------------------------------------------- events
    def event(self, run_id, kind, node_id=None, detail=None):
        if detail is not None and not isinstance(detail, str):
            detail = _dumps(detail)
        with self._conn() as c:
            c.execute("INSERT INTO events (run_id,node_id,kind,detail,ts) VALUES (?,?,?,?,?)",
                      (run_id, node_id, kind, detail, time.time()))

    def get_events(self, run_id):
        with self._conn() as c:
            return [dict(r) for r in c.execute("SELECT * FROM events WHERE run_id=? ORDER BY id", (run_id,)).fetchall()]

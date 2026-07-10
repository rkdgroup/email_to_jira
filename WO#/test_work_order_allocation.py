"""Branch-coverage test for WorkOrderManager.allocate_and_create — no DB access.

Uses a fake cursor/connection to drive every path of the collision-safe loop:
  * happy path
  * same-suffix race (-803 -> retry higher)
  * cross-suffix race (COUNT>1 -> delete own row -> reassign higher)
  * exhaustion (-803 every attempt -> RuntimeError)
  * dry_run (scan only, no writes)

Run standalone:  python WO#/test_work_order_allocation.py
Or under pytest: pytest WO#/test_work_order_allocation.py
"""
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import work_order as wo
from work_order import WorkOrderManager, _WO_MAX

_ARGS = dict(wccust=20011, worde3=70926, mailer_name="ZZ Test",
             manager_po="PO1", mailer_po="ZZTEST_COLLIDE")


class _DuplicateKeyError(Exception):
    """Mimics a Db2 for i unique-key violation message (SQLCODE -803)."""
    def __init__(self):
        super().__init__("SQL0803 - Duplicate key value specified for INDEX.")


class _FakeDB:
    def __init__(self, rows):
        self.rows = set(rows)            # set of (wworko, wsufx)
        self.race_same_suffix = 0        # inject N same-suffix racers (one per insert)
        self.race_cross_suffix = False   # inject a human '01' row right after our insert
        self.pepbk = None                # PEPBK# counter value (None -> unreadable -> floor 0)


class _FakeCursor:
    def __init__(self, db):
        self.db = db
        self._fetch = None

    def execute(self, sql, params=None):
        s = " ".join(sql.upper().split())
        if "DATA_AREA_INFO" in s:
            self._fetch = (self.db.pepbk,)   # PEPBK# read; None => floor 0
        elif s.startswith("SELECT MAX(WWORKO)"):
            live = [w for (w, _) in self.db.rows if w < _WO_MAX]
            self._fetch = (max(live) if live else None,)
        elif s.startswith("SELECT WWORKO FROM") and "WHERE WWORKO = ?" in s:
            wworko = params[0]
            taken = any(w == wworko for (w, _) in self.db.rows)
            self._fetch = (wworko,) if taken else None
        elif s.startswith("INSERT"):
            wworko = params[1]
            if self.db.race_same_suffix > 0:
                self.db.race_same_suffix -= 1
                self.db.rows.add((wworko, "  "))     # racer committed our exact key
                raise _DuplicateKeyError()
            if (wworko, "  ") in self.db.rows:
                raise _DuplicateKeyError()
            self.db.rows.add((wworko, "  "))
            if self.db.race_cross_suffix:
                self.db.race_cross_suffix = False
                self.db.rows.add((wworko, "01"))     # human keyed same WWORKO, diff suffix
        elif s.startswith("SELECT COUNT(*)"):
            wworko = params[0]
            self._fetch = (sum(1 for (w, _) in self.db.rows if w == wworko),)
        elif s.startswith("DELETE"):
            self.db.rows.discard((params[0], "  "))
        else:
            raise AssertionError(f"Unexpected SQL: {s}")

    def fetchone(self):
        return self._fetch


class _FakeJConn:
    def setAutoCommit(self, val):  # noqa: N802 (mimics Java method name)
        pass


class _FakeConn:
    def __init__(self, db):
        self.db = db
        self.jconn = _FakeJConn()

    def cursor(self):
        return _FakeCursor(self.db)

    def close(self):
        pass


class _FakeManager(WorkOrderManager):
    def __init__(self, db):
        self._db = db

    def _connect(self):
        return _FakeConn(self._db)


class _Capture(logging.Handler):
    def __init__(self):
        super().__init__()
        self.records = []

    def emit(self, record):
        self.records.append(record)


_cap = _Capture()
wo.log.addHandler(_cap)
wo.log.setLevel(logging.DEBUG)


def _warnings():
    return [r.getMessage() for r in _cap.records if r.levelno >= logging.WARNING]


def test_happy_path():
    _cap.records.clear()
    db = _FakeDB({(1000, "  ")})
    got = _FakeManager(db).allocate_and_create(**_ARGS)
    assert got == 1001
    assert (1001, "  ") in db.rows
    assert _warnings() == []


def test_same_suffix_retry():
    _cap.records.clear()
    db = _FakeDB({(1000, "  ")})
    db.race_same_suffix = 1                  # first insert (1001) hits -803
    got = _FakeManager(db).allocate_and_create(**_ARGS)
    assert got == 1002                       # reassigned higher
    assert (1001, "  ") in db.rows           # racer's row survives
    assert (1002, "  ") in db.rows           # ours lands at next free
    assert len(_warnings()) == 1 and "same-suffix" in _warnings()[0]


def test_cross_suffix_reassign():
    _cap.records.clear()
    db = _FakeDB({(1000, "  ")})
    db.race_cross_suffix = True              # human keys 1001/'01' after our insert
    got = _FakeManager(db).allocate_and_create(**_ARGS)
    assert got == 1002                       # reassigned higher
    assert (1001, "  ") not in db.rows       # our row backed out
    assert (1001, "01") in db.rows           # human's row untouched
    assert (1002, "  ") in db.rows           # ours lands clean
    assert len(_warnings()) == 1 and "cross-suffix" in _warnings()[0]


def test_exhaustion_raises():
    _cap.records.clear()
    db = _FakeDB({(1000, "  ")})
    db.race_same_suffix = 99                  # every insert hits -803
    try:
        _FakeManager(db).allocate_and_create(max_attempts=5, **_ARGS)
        raise AssertionError("expected RuntimeError")
    except RuntimeError as e:
        assert "after 5 attempts" in str(e)
    assert any(r.levelno == logging.ERROR for r in _cap.records)


def test_pepbk_floor():
    # Counter reserved 1005 ahead of committed MAX (1000); allocate at the floor, not 1001.
    _cap.records.clear()
    db = _FakeDB({(1000, "  ")})
    db.pepbk = 1005
    got = _FakeManager(db).allocate_and_create(**_ARGS)
    assert got == 1005, got
    assert (1005, "  ") in db.rows
    assert (1001, "  ") not in db.rows       # skipped the reserved gap
    assert _warnings() == []


def test_pepbk_floor_below_max_ignored():
    # Stale/low counter must not pull allocation below MAX+1.
    _cap.records.clear()
    db = _FakeDB({(1000, "  ")})
    db.pepbk = 500                            # behind the table
    got = _FakeManager(db).allocate_and_create(**_ARGS)
    assert got == 1001, got                   # MAX+1 wins


def test_dry_run_no_writes():
    _cap.records.clear()
    db = _FakeDB({(1000, "  ")})
    before = set(db.rows)
    got = _FakeManager(db).allocate_and_create(dry_run=True, **_ARGS)
    assert got == 1001
    assert db.rows == before, "dry_run must not write"


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"PASS: {name}")
    print("\nALL PASSED")

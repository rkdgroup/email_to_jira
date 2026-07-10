"""
Work Order creation for IBM i (DMIJOBS.ARWRKSCH).

Usage:
    from work_order import create_work_order

    wo = create_work_order(
        billable   = 'T11',       # Billable account code
        mailer_name= 'My Mailer', # Stored as acronym in WDESC (e.g. "American Conservative Union" -> "ACU")
        manager_po = 'PO12345',   # Manager PO, up to 9 chars
        mailer_po  = 'MPO678',    # Mailer PO, up to 15 chars
    )
    print(wo)  # {'wo_number': 461447, 'wccust': 1020}

Collision safety
----------------
(WWORKO, WSUFX) is a DB-enforced composite primary key; the pipeline always
writes a blank suffix ('  '). Two different orders can therefore still end up
sharing one WWORKO if a human keys the same number with a *different* suffix —
that is the real collision this module guards against. allocate_and_create()
does the whole scan -> insert -> verify -> auto-reassign cycle on a single
connection so the pipeline never leaves a WWORKO shared with another order.
"""

from __future__ import annotations
import logging
from dataclasses import dataclass
from datetime import date
from base import IBMiBase

log = logging.getLogger(__name__)

_WO_LIBRARY = "DMIJOBS"
_WO_MAX     = 500_000

# Single source of truth for the INSERT shape. Blank suffix ('  ') is a literal
# so the pipeline's row is always uniquely (WWORKO, '  ').
_INSERT_SQL = f"""
    INSERT INTO {_WO_LIBRARY}.ARWRKSCH (
        WTYPE, WWORKO, WSUFX, WSTORE, WCCUST, WSIZE,
        WORDE3, WCOM2, WVOLUM, WMAIL3, WDESC, WRELES,
        WDNB, WREASN, WXPOST, WADVAN, "WXCOD#", WXPOSO,
        WXPCOD, WESTBL, WESTPS, WPSTCD, WORDER, WCOMP,
        WMAIL, WMAILR
    ) VALUES (
        ?, ?, '  ', 0, ?, '     ',
        ?, 0, 0, ?, ?, 'A',
        ' ', '                             ', 0, 0, ?, ' ',
        ' ', 0, 0, 'L', 0, 0,
        0, ?
    )
"""


def _billable_to_wccust(code: str) -> int:
    """'K40' -> 11040  ('T11' -> 20011).  letter_pos * 1000 + trailing."""
    letter_pos = ord(code[0].upper()) - ord("A") + 1
    trailing   = int(code[1:].strip())
    return letter_pos * 1000 + trailing


_WDESC_OVERRIDES: dict[str, str] = {
    "60 PLUS ASSOCIATION": "60+ ASSN",
}


def _make_acronym(name: str, max_len: int = 19) -> str:
    """'American Conservative Union' -> 'ACU'  (falls back to truncation if single word)."""
    override = _WDESC_OVERRIDES.get(name.upper().strip())
    if override:
        return override[:max_len]
    words = name.split()
    if len(words) > 1:
        return "".join(w[0].upper() for w in words if w)[:max_len]
    return name[:max_len]


def _today_mmddyy() -> int:
    return int(date.today().strftime("%m%d%y"))


def _is_duplicate_key(exc: Exception) -> bool:
    """True if a JDBC/jaydebeapi exception is a Db2 for i unique-key violation.

    A same-suffix race (another writer already committed our exact (WWORKO, '  '))
    surfaces as SQLCODE -803 / SQLSTATE 23505. Everything else is a real error and
    must propagate so we never mask bugs as retryable collisions.
    """
    msg = str(exc).upper()
    return ("SQL0803" in msg or "-803" in msg
            or "23505" in msg or "DUPLICATE KEY" in msg)


@dataclass
class WorkOrderResult:
    wo_number: int
    wccust: int


class WorkOrderManager(IBMiBase):

    # --- cursor-level helpers (all share one connection/cursor) -------------

    def _scan_next_free(self, cur) -> int:
        """MAX(WWORKO)+1, then climb past any WWORKO already present with ANY suffix."""
        cur.execute(
            f"SELECT MAX(WWORKO) FROM {_WO_LIBRARY}.ARWRKSCH WHERE WWORKO < {_WO_MAX}"
        )
        row = cur.fetchone()
        candidate = int(row[0] or 460000) + 1
        while True:
            cur.execute(
                f"SELECT WWORKO FROM {_WO_LIBRARY}.ARWRKSCH "
                f"WHERE WWORKO = ? FETCH FIRST 1 ROW ONLY",
                [candidate],
            )
            if cur.fetchone() is None:
                return candidate
            candidate += 1

    def _insert_row(self, cur, wo_number, wccust, worde3, mailer_name,
                    manager_po, mailer_po) -> None:
        cur.execute(
            _INSERT_SQL,
            ["M1", wo_number, wccust, worde3, worde3,
             _make_acronym(mailer_name), manager_po[:9], mailer_po[:15]],
        )

    def _count_wworko(self, cur, wo_number) -> int:
        """Rows sharing this WWORKO across ALL suffixes (>1 means a cross-suffix clash)."""
        cur.execute(
            f"SELECT COUNT(*) FROM {_WO_LIBRARY}.ARWRKSCH WHERE WWORKO = ?",
            [wo_number],
        )
        return int(cur.fetchone()[0])

    def _delete_our_row(self, cur, wo_number) -> None:
        """Remove exactly the pipeline's row. The PK guarantees only one (WWORKO, '  ')
        exists, so this can never touch a human's row (their suffix differs)."""
        cur.execute(
            f"DELETE FROM {_WO_LIBRARY}.ARWRKSCH WHERE WWORKO = ? AND WSUFX = '  '",
            [wo_number],
        )

    # --- public allocation --------------------------------------------------

    def allocate_and_create(
        self,
        *,
        wccust: int,
        worde3: int,
        mailer_name: str,
        manager_po: str,
        mailer_po: str,
        dry_run: bool = False,
        max_attempts: int = 5,
    ) -> int:
        """Allocate a WWORKO that no other order uses and INSERT the pipeline's row.

        Runs scan -> insert -> verify -> auto-reassign on ONE connection so the
        pipeline never leaves a WWORKO shared with another order:
          * same-suffix race  -> the PK rejects our INSERT (-803); rescan higher.
          * cross-suffix race -> our INSERT succeeds but COUNT(WWORKO) > 1; delete
            our own row and rescan higher.
        Returns the final unique WWORKO. Raises if no free number after
        max_attempts. dry_run scans and returns a candidate without writing.
        """
        conn = self._connect()
        try:
            # Autocommit ON (jt400 default): each INSERT/DELETE is immediately
            # durable/visible, so the verify SELECT sees a racing writer's row.
            try:
                conn.jconn.setAutoCommit(True)
            except AttributeError:
                pass  # already the driver default

            cur = conn.cursor()

            if dry_run:
                return self._scan_next_free(cur)

            for attempt in range(1, max_attempts + 1):
                candidate = self._scan_next_free(cur)
                if candidate >= _WO_MAX:
                    raise RuntimeError(f"WO number space exhausted (reached {candidate})")

                try:
                    self._insert_row(cur, candidate, wccust, worde3,
                                     mailer_name, manager_po, mailer_po)
                except Exception as exc:
                    if _is_duplicate_key(exc):
                        log.warning(
                            "WO %d already taken (same-suffix race); reassigning (attempt %d/%d)",
                            candidate, attempt, max_attempts,
                        )
                        continue
                    raise

                shared = self._count_wworko(cur, candidate)
                if shared == 1:
                    log.info("Allocated WO %d for %s", candidate, mailer_name)
                    return candidate

                # Cross-suffix collision: a human keyed this WWORKO with a
                # different suffix in the race window. Back our row out and retry.
                self._delete_our_row(cur, candidate)
                log.warning(
                    "WO %d collided cross-suffix (%d rows share it); removed our row, "
                    "reassigning (attempt %d/%d)",
                    candidate, shared, attempt, max_attempts,
                )

            log.error("Could not allocate a unique WO after %d attempts", max_attempts)
            raise RuntimeError(f"Could not allocate a unique WO after {max_attempts} attempts")
        finally:
            conn.close()


def create_work_order(
    billable: str,
    mailer_name: str,
    manager_po: str = "",
    mailer_po: str  = "",
    dry_run: bool   = False,
) -> WorkOrderResult:
    """
    Allocate a unique WO# and insert it into DMIJOBS.ARWRKSCH.

    Parameters
    ----------
    billable    : Billable account code, e.g. 'T11', 'A18'
    mailer_name : Mailer/client name (max 19 chars)
    manager_po  : Manager PO number (max 9 chars)
    mailer_po   : Mailer PO number (max 15 chars)
    dry_run     : If True, allocates WO# but does not write to DB

    Returns WorkOrderResult with wo_number and wccust.
    """
    mgr    = WorkOrderManager()
    wccust = _billable_to_wccust(billable)
    worde3 = _today_mmddyy()
    wo_num = mgr.allocate_and_create(
        wccust      = wccust,
        worde3      = worde3,
        mailer_name = mailer_name,
        manager_po  = manager_po,
        mailer_po   = mailer_po,
        dry_run     = dry_run,
    )

    return WorkOrderResult(wo_number=wo_num, wccust=wccust)


if __name__ == "__main__":
    import logging, sys
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    dry = "--dry-run" in sys.argv
    result = create_work_order(
        billable    = "T11",
        mailer_name = "Test Mailer",
        manager_po  = "PO00001",
        mailer_po   = "MPO0001",
        dry_run     = dry,
    )
    print(f"WO#: {result.wo_number}  WCCUST: {result.wccust}  {'(dry run)' if dry else ''}")

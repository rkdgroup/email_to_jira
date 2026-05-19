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
"""

from __future__ import annotations
from dataclasses import dataclass
from datetime import date
from base import IBMiBase

_WO_LIBRARY = "DMIJOBS"
_WO_MAX     = 500_000


def _billable_to_wccust(code: str) -> int:
    """'T11' -> 2011  ('A69' -> 1069).  Matches PEPBOOKRPG SUB01 logic."""
    letter_pos = ord(code[0].upper()) - ord("A") + 1
    trailing   = int(code[1:].strip())
    return ((letter_pos - 1) % 9 + 1) * 1000 + trailing


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


@dataclass
class WorkOrderResult:
    wo_number: int
    wccust: int


class WorkOrderManager(IBMiBase):

    def get_next_wo_number(self) -> int:
        rows = self._query(
            f"SELECT MAX(WWORKO) AS MAXWO FROM {_WO_LIBRARY}.ARWRKSCH "
            f"WHERE WWORKO < {_WO_MAX}"
        )
        candidate = int(rows[0]["MAXWO"] or 460000) + 1
        while True:
            taken = self._query(
                f"SELECT WWORKO FROM {_WO_LIBRARY}.ARWRKSCH "
                f"WHERE WWORKO = {candidate} FETCH FIRST 1 ROW ONLY"
            )
            if not taken:
                return candidate
            candidate += 1

    def create_wo(
        self,
        wo_number: int,
        wccust: int,
        worde3: int,
        mailer_name: str,
        manager_po: str,
        mailer_po: str,
    ) -> None:
        sql = f"""
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
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(
                sql,
                ("M1", wo_number, wccust, worde3, worde3,
                 _make_acronym(mailer_name), manager_po[:9], mailer_po[:15]),
            )
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
    Allocate the next WO# and insert it into DMIJOBS.ARWRKSCH.

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
    wo_num = mgr.get_next_wo_number()

    if not dry_run:
        mgr.create_wo(
            wo_number   = wo_num,
            wccust      = wccust,
            worde3      = worde3,
            mailer_name = mailer_name,
            manager_po  = manager_po,
            mailer_po   = mailer_po,
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

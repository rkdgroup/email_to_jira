"""
Fix WCCUST for WOs created with the broken modulo formula.
Correct formula is: letter_pos * 1000 + trailing (e.g. K40 -> 11040, N71 -> 14071).
"""
import sys
sys.path.insert(0, ".")
from base import get_connection

FIXES = [
    # WO,     correct WCCUST  # billing code — Jira ticket
    (461599,  21019),  # U19 — DSLF-450  (previously mis-fixed to 3019)
    (461660,  16082),  # P82 — PAWS OF HONOR
    (461665,  14013),  # N13 — NATL POLICE & TROOPER ASSO
    (461677,  14013),  # N13 — NATL POLICE & TROOPER ASSO
    (461683,  14013),  # N13 — NATL POLICE & TROOPER ASSO
    (461787,  16032),  # P32 — PROJECT TOY DROP
    (461790,  14071),  # N71 — NCCI SWEEPS
    (461794,  16047),  # P47 — PROJECT FOSTER
    (461795,  16032),  # P32 — PROJECT TOY DROP
    (461796,  14071),  # N71 — NCCI SWEEPS
    (461886,  14091),  # N91 — NFOF
    (461888,  16089),  # P89 — PROJECT HEAL VETERANS
    (461974,  16047),  # P47 — PROJECT FOSTER
    (461977,  11040),  # K40 — KIDS WISH NETWORK (DSLF-533)
    (461981,  14071),  # N71 — NCCI SWEEPS (DSLF-537)
    (462008,  14091),  # N91 — NFOF
    (462009,  14091),  # N91 — NFOF
]

conn = get_connection()
try:
    cur = conn.cursor()
    for wo, correct_wccust in FIXES:
        cur.execute(
            "SELECT WWORKO, WCCUST FROM DMIJOBS.ARWRKSCH WHERE WWORKO = ?",
            (wo,)
        )
        row = cur.fetchone()
        if row is None:
            print(f"WO {wo}: NOT FOUND in ARWRKSCH")
            continue
        current_wccust = row[1]
        if current_wccust == correct_wccust:
            print(f"WO {wo}: WCCUST already correct ({correct_wccust}), skipping")
            continue
        cur.execute(
            "UPDATE DMIJOBS.ARWRKSCH SET WCCUST = ? WHERE WWORKO = ?",
            (correct_wccust, wo)
        )
        print(f"WO {wo}: WCCUST updated {current_wccust} -> {correct_wccust}")
    conn.commit()
    print("Done.")
finally:
    conn.close()

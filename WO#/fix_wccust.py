"""
One-off fix: correct WCCUST for WO 461597 (T11→2011) and WO 461599 (U19→3019).
These were created before the _billable_to_wccust formula was corrected.
"""
import sys
sys.path.insert(0, ".")
from base import get_connection

FIXES = [
    (461597, 2011),   # T11 — DSLF-447
    (461599, 3019),   # U19 — DSLF-450
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

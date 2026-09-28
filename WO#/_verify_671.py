"""Read-only verify: WO 463130 row in ARWRKSCH."""
import os
from pathlib import Path
import pyodbc
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")
load_dotenv(Path(__file__).parent.parent / ".env", override=False)

conn = pyodbc.connect(
    "DRIVER={IBM i Access ODBC Driver};"
    f"SYSTEM={os.environ.get('IBMI_HOST', 'SYSTEM5.DATA-MANAGEMENT.COM')};"
    f"UID={os.environ.get('IBMI_USER', 'DMISUVAM')};PWD={os.environ.get('IBMI_PASSWORD', '')}",
    timeout=30,
)
cur = conn.cursor()
cur.execute('SELECT WTYPE, WWORKO, WCCUST, WDESC, "WXCOD#", WMAILR, WORDE3, WRELES '
            "FROM DMIJOBS.ARWRKSCH WHERE WWORKO = 463130")
for row in cur.fetchall():
    print(row, flush=True)
conn.close()

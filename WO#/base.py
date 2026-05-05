"""
IBM i JDBC connection base class.
Requires: jaydebeapi, JPype1, jt400.jar
"""

import os
import logging
from pathlib import Path
from dotenv import load_dotenv
import jaydebeapi

load_dotenv(Path(__file__).parent / ".env")
load_dotenv(Path(__file__).parent.parent / ".env", override=False)

log = logging.getLogger(__name__)

_HOST     = os.environ.get("IBMI_HOST",     "SYSTEM5.DATA-MANAGEMENT.COM")
_USER     = os.environ.get("IBMI_USER",     "DMISUVAM")
_PASSWORD = os.environ.get("IBMI_PASSWORD", "")
_JT400_WINDOWS = (
    r"D:\Users\Public\Downloads\RDi_9.8_core_MP_ML\windows\IBM Rational Developer for i"
    r"\plugins\com.ibm.etools.iseries.toolbox_9.8.0.202304121327\runtime\jt400.jar"
)
_JT400_CANDIDATES = [
    "/opt/jt400/jt400.jar",
    "/var/lib/jenkins/workspace/DSLF-Email-Scanner/jt400.jar",
    str(Path(__file__).parent.parent / "jt400.jar"),  # project root (Jenkins workspace)
]

def _resolve_jt400() -> str:
    configured = os.environ.get("IBMI_JT400_JAR", "")
    if configured and Path(configured).exists():
        return configured
    for candidate in _JT400_CANDIDATES:
        if Path(candidate).exists():
            return candidate
    if Path(_JT400_WINDOWS).exists():
        return _JT400_WINDOWS
    return configured or _JT400_WINDOWS

_JT400 = _resolve_jt400()
_DRIVER   = "com.ibm.as400.access.AS400JDBCDriver"
_JDBC_URL = f"jdbc:as400://{_HOST}"


def get_connection():
    if not Path(_JT400).exists():
        raise FileNotFoundError(
            f"jt400.jar not found at: {_JT400}\n"
            "Set IBMI_JT400_JAR in .env to the correct path on this machine."
        )
    return jaydebeapi.connect(_DRIVER, _JDBC_URL, [_USER, _PASSWORD], _JT400)


class IBMiBase:
    def _connect(self):
        return get_connection()

    def _query(self, sql: str, max_rows: int = 500) -> list[dict]:
        if not sql.strip().upper().startswith("SELECT"):
            raise ValueError("Only SELECT statements allowed.")
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(sql)
            cols = [d[0] for d in cur.description]
            rows = cur.fetchmany(max_rows)
            return [dict(zip(cols, [str(v) if v is not None else None for v in r])) for r in rows]
        finally:
            conn.close()

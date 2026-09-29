"""
IBM i JDBC connection base class.
Requires: jaydebeapi, JPype1, jt400.jar
"""

import os
import sys
import logging
from pathlib import Path
from dotenv import load_dotenv, dotenv_values
import jpype
import jaydebeapi

load_dotenv(Path(__file__).parent / ".env")
load_dotenv(Path(__file__).parent.parent / ".env", override=False)

log = logging.getLogger(__name__)

_ENV_FILES = [
    Path(__file__).parent / ".env",
    Path(__file__).parent.parent / ".env",
    Path(__file__).parent.parent / "email_scanner" / ".env",
]


def _setting(key: str, default: str = "") -> str:
    """Env var, else the first .env holding a non-blank value, else default.

    load_dotenv never overrides, so a blank IBMI_PASSWORD already in the Jenkins
    environment masked the valid one in .env (DSLF-1342..1347, and 1139..1142).
    """
    if os.environ.get(key):
        return os.environ[key]
    for f in _ENV_FILES:
        if f.exists() and dotenv_values(f).get(key):
            return dotenv_values(f)[key]
    return default


_HOST     = _setting("IBMI_HOST", "SYSTEM5.DATA-MANAGEMENT.COM")
_USER     = _setting("IBMI_USER", "DMISUVAM")
_PASSWORD = _setting("IBMI_PASSWORD")
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


def _ensure_jvm() -> None:
    """Start the JVM with headless flag before jaydebeapi can start it without one.
    Headless is only needed on Linux (Jenkins); on Windows the AS400 driver uses AWT."""
    if jpype.isJVMStarted():
        return
    jvm_args = [f"-Djava.class.path={_JT400}"]
    if sys.platform != "win32":
        jvm_args.insert(0, "-Djava.awt.headless=true")
    jpype.startJVM(jpype.getDefaultJVMPath(), *jvm_args)


def _credential_problem() -> str | None:
    """Plain-language reason the credentials are unusable, never including a secret."""
    empty = [k for k, v in (("IBMI_HOST", _HOST), ("IBMI_USER", _USER),
                            ("IBMI_PASSWORD", _PASSWORD)) if not v]
    if not empty:
        return None
    raw = os.environ.get("IBMI_PASSWORD")
    env_state = "unset" if raw is None else ("blank" if raw == "" else "set")
    files = "; ".join(
        f"{f} exists={f.exists()} password="
        f"{'set' if f.exists() and dotenv_values(f).get('IBMI_PASSWORD') else 'blank'}"
        for f in _ENV_FILES
    )
    return (f"{', '.join(empty)} empty - jt400 would fail with ArrayIndexOutOfBoundsException. "
            f"process env IBMI_PASSWORD={env_state}; .env files: {files}; jt400={_JT400}")


def get_connection():
    problem = _credential_problem()
    if problem:
        raise RuntimeError(problem)
    if not Path(_JT400).exists():
        raise FileNotFoundError(
            f"jt400.jar not found at: {_JT400}\n"
            "Set IBMI_JT400_JAR in .env to the correct path on this machine."
        )
    _ensure_jvm()
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

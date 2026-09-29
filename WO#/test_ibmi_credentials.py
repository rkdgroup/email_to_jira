"""IBM i credential resolution tests. No DB, no JVM, no network.

    python WO#/test_ibmi_credentials.py      # standalone, prints PASS / ALL PASSED
    pytest WO#/test_ibmi_credentials.py      # also works

DSLF-1342 through -1347 (2026-09-25..28) and DSLF-1139..1142 (2026-08-31) got no work
order. The Jenkins-side error was JTOpen's `ArrayIndexOutOfBoundsException: Index 0 out
of bounds for length 0` from AS400JDBCDriver.initializeAS400 - which is what a BLANK
password string produces (reproduced). The uploaded .env held a valid password; every
loader uses load_dotenv(override=False), so a blank IBMI_PASSWORD already in the process
environment is the likely mask. A blank env password must now fall through to the file -
host, user and password together from ONE source, so a stale env user never pairs with
the file's password (a real invalid sign-on) - and a password that is still empty must
fail with a plain message instead of reaching jt400.
"""
import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import jpype
import base

_failures = []
_FILE = "IBMI_USER=SVCORDPROC\nIBMI_PASSWORD=FromTheFile123\n"


def check(name, got, want):
    if got != want:
        _failures.append(f"{name}\n     got  {got!r}\n     want {want!r}")
        print(f"FAIL: {name}")
    else:
        print(f"PASS: {name}")


def _env_file(text: str) -> Path:
    f = Path(tempfile.mkdtemp()) / ".env"
    f.write_text(text)
    return f


def _env(**kv):
    """Set exactly these IBMI_* process env vars; unset the rest."""
    for k in ("IBMI_HOST", "IBMI_USER", "IBMI_PASSWORD"):
        os.environ.pop(k, None)
    os.environ.update(kv)


def test_blank_env_password_does_not_mask_the_file():
    f = _env_file(_FILE)
    base._ENV_FILES = [f]
    _env(IBMI_PASSWORD="")
    _, _, pw, src = base._credentials()
    check("file password wins over a blank env var", pw, "FromTheFile123")
    check("source is the file", src, str(f))


def test_user_comes_from_the_same_source_as_the_password():
    base._ENV_FILES = [_env_file(_FILE)]
    _env(IBMI_USER="DMISUVAM", IBMI_PASSWORD="")
    _, user, _, _ = base._credentials()
    check("user taken from the file with the password", user, "SVCORDPROC")


def test_set_env_password_keeps_env_credentials():
    base._ENV_FILES = [_env_file(_FILE)]
    _env(IBMI_USER="ENVUSER", IBMI_PASSWORD="FromTheEnv456")
    _, user, pw, src = base._credentials()
    check("env password wins", pw, "FromTheEnv456")
    check("env user with it", user, "ENVUSER")
    check("source is env", src, "env")


def test_defaults_when_blank_everywhere():
    base._ENV_FILES = [_env_file("IBMI_HOST=\n")]
    _env(IBMI_HOST="", IBMI_PASSWORD="")
    host, user, pw, src = base._credentials()
    check("blank host falls back to default", host, "SYSTEM5.DATA-MANAGEMENT.COM")
    check("no password anywhere", pw, "")
    check("no source", src, "none")


def test_empty_password_fails_loudly_before_the_jvm():
    base._ENV_FILES = [_env_file("IBMI_USER=SVCORDPROC\n")]
    _env(IBMI_PASSWORD="")
    base._PASSWORD = ""
    try:
        base.get_connection()
        check("raises on empty password", "no exception", "RuntimeError")
    except RuntimeError as exc:
        msg = str(exc)
        check("names IBMI_PASSWORD", "IBMI_PASSWORD" in msg, True)
        check("reports the blank env var", "process env IBMI_PASSWORD=blank" in msg, True)
        check("reports the source", "source=" in msg, True)
    check("JVM never started", jpype.isJVMStarted(), False)


def test_message_never_carries_the_password():
    base._ENV_FILES = [_env_file("IBMI_PASSWORD=SecretValue789\n")]
    base._PASSWORD = ""
    try:
        base.get_connection()
    except RuntimeError as exc:
        check("password value absent", "SecretValue789" in str(exc), False)
        check("file reported as holding one", "password=set" in str(exc), True)


if __name__ == "__main__":
    test_blank_env_password_does_not_mask_the_file()
    test_user_comes_from_the_same_source_as_the_password()
    test_set_env_password_keeps_env_credentials()
    test_defaults_when_blank_everywhere()
    test_empty_password_fails_loudly_before_the_jvm()
    test_message_never_carries_the_password()
    if _failures:
        print("\n".join(_failures))
        sys.exit(1)
    print("ALL PASSED")

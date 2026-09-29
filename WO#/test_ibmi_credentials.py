"""IBM i credential resolution tests. No DB, no JVM, no network.

    python WO#/test_ibmi_credentials.py      # standalone, prints PASS / ALL PASSED
    pytest WO#/test_ibmi_credentials.py      # also works

DSLF-1342 through -1347 (2026-09-25..28) and DSLF-1139..1142 (2026-08-31) got no work
order. The Jenkins-side error was JTOpen's `ArrayIndexOutOfBoundsException: Index 0 out
of bounds for length 0` from AS400JDBCDriver.initializeAS400 - which is what a BLANK
password string produces. The uploaded .env held a valid password, but every loader uses
load_dotenv(override=False), so an IBMI_PASSWORD already present and blank in the process
environment masked it. A blank env var must now fall through to the file, and a password
that is still empty must fail with a plain message instead of reaching jt400.
"""
import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import jpype
import base

_failures = []


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


def test_blank_env_var_does_not_mask_the_file():
    base._ENV_FILES = [_env_file("IBMI_PASSWORD=FromTheFile123\n")]
    os.environ["IBMI_PASSWORD"] = ""
    check("file value wins over a blank env var", base._setting("IBMI_PASSWORD"), "FromTheFile123")


def test_set_env_var_still_wins():
    base._ENV_FILES = [_env_file("IBMI_PASSWORD=FromTheFile123\n")]
    os.environ["IBMI_PASSWORD"] = "FromTheEnv456"
    check("non-blank env var wins", base._setting("IBMI_PASSWORD"), "FromTheEnv456")


def test_default_when_blank_everywhere():
    base._ENV_FILES = [_env_file("IBMI_HOST=\n")]
    os.environ["IBMI_HOST"] = ""
    check("blank host falls back to default", base._setting("IBMI_HOST", "SYSTEM5"), "SYSTEM5")


def test_empty_password_fails_loudly_before_the_jvm():
    base._ENV_FILES = [_env_file("IBMI_USER=SVCORDPROC\n")]
    os.environ["IBMI_PASSWORD"] = ""
    base._PASSWORD = ""
    try:
        base.get_connection()
        check("raises on empty password", "no exception", "RuntimeError")
    except RuntimeError as exc:
        msg = str(exc)
        check("names IBMI_PASSWORD", "IBMI_PASSWORD" in msg, True)
        check("reports the blank env var", "process env IBMI_PASSWORD=blank" in msg, True)
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
    test_blank_env_var_does_not_mask_the_file()
    test_set_env_var_still_wins()
    test_default_when_blank_everywhere()
    test_empty_password_fails_loudly_before_the_jvm()
    test_message_never_carries_the_password()
    if _failures:
        print("\n".join(_failures))
        sys.exit(1)
    print("ALL PASSED")

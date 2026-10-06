"""Client-profile Select By extraction. No network, no Jira, no Word files.

    python test_profile_select_by.py    # standalone
    pytest test_profile_select_by.py

DSLF-1373: the A15R card wraps "MOST RECENT DATE &" / "LARGEST DOLLAR" over two lines, and only the first was kept.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from build_profile_yaml import _parse_lines


def test_wrapped_select_by_keeps_its_continuation():
    lines = ["SELECT BY:  MOST RECENT DATE &", "                LARGEST DOLLAR", "STANDARD SUPPRESSIONS:"]
    assert _parse_lines(lines)["select_by"] == "MOST RECENT DATE & LARGEST DOLLAR"


def test_complete_select_by_does_not_swallow_the_next_line():
    lines = ["SELECT BY:  TRANSACTION $ AND DATE", "                LARGEST DOLLAR"]
    assert _parse_lines(lines)["select_by"] == "TRANSACTION $ AND DATE"


def test_dangling_select_by_never_swallows_a_label():
    lines = ["SELECT BY:  MOST RECENT DATE &", "$ CAP:  $99.99"]
    assert _parse_lines(lines)["select_by"] == "MOST RECENT DATE &"


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_"):
            fn()
            print("PASS", name)
    print("ALL PASSED")

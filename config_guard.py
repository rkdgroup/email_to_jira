"""Startup config validation — fail fast if any config/*.yaml is malformed.

Called at the top of the Jenkins entry points (email_scanner, qc_checker) so a
bad hand-edit to a config YAML aborts the whole run LOUDLY (exit 1 -> Jenkins
build FAILED) instead of silently failing every message. This is a cheap syntax
gate only; see verify_configs.py for deeper content audits.
"""
import sys
from pathlib import Path

import yaml

_CONFIG_DIR = Path(__file__).resolve().parent / "config"


def validate_configs_or_exit(config_dir: Path = _CONFIG_DIR) -> None:
    """yaml.safe_load every config/*.yaml; on any failure print a clear error and exit(1)."""
    errors = []
    for path in sorted(config_dir.glob("*.yaml")):
        parsed, last_err = False, None
        # Match client_lookup._load_yaml: try utf-8 then cp1252. Only a STRUCTURAL
        # YAML error is fatal — an encoding-only issue is tolerated at runtime.
        for enc in ("utf-8", "cp1252"):
            try:
                with open(path, "r", encoding=enc) as fh:
                    yaml.safe_load(fh)
                parsed = True
                break
            except UnicodeDecodeError as e:
                last_err = e
                continue
            except yaml.YAMLError as e:
                last_err = e
                break  # structural error — retrying other encodings won't help
        if not parsed:
            errors.append((path.name, last_err))

    if errors:
        print("FATAL: invalid config YAML — aborting before processing:", file=sys.stderr)
        for name, err in errors:
            print(f"  - config/{name}: {err}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    validate_configs_or_exit()
    print("All config/*.yaml parse OK.")

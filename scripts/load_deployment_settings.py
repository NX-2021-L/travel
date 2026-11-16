#!/usr/bin/env python3
"""Load a deployment's settings file into the workflow environment (FLY-TSK-028-0B, A-2).

  load_deployment_settings.py deploy/settings.env      appends KEY=VALUE lines to $GITHUB_ENV

Fails (exit 2) when the file is missing, empty or malformed, so a run with no settings stops at
its first step. Prints KEYS only. Stdlib only.
"""
import os
import re
import sys

LINE = re.compile(r"^([A-Z][A-Z0-9_]*)=(.*)$")


def parse(text: str) -> dict:
    out = {}
    for n, raw in enumerate(text.splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        m = LINE.match(line)
        if not m or not m.group(2).strip():
            raise ValueError(f"line {n}: not KEY=VALUE with a value")
        out[m.group(1)] = m.group(2).strip()
    if not out:
        raise ValueError("no settings")
    return out


def main(argv) -> int:
    if len(argv) != 1:
        print("usage: load_deployment_settings.py <settings-file>", file=sys.stderr)
        return 2
    try:
        with open(argv[0]) as f:
            settings = parse(f.read())
    except (OSError, ValueError) as exc:
        print(f"deployment settings refused: {argv[0]}: {exc}", file=sys.stderr)
        return 2
    target = os.environ.get("GITHUB_ENV")
    if target:
        with open(target, "a") as f:
            for k, v in settings.items():
                f.write(f"{k}={v}\n")
    print("deployment settings loaded: " + ", ".join(sorted(settings)))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

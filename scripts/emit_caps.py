#!/usr/bin/env python3
"""Emit or check the committed travel caps/permission snapshots (DVP-TSK-889).

  emit_caps.py          rewrite backend/lambda/travel_mcp/{caps,permission_manifest}.json
  emit_caps.py --check  exit 1 if the committed snapshots are not byte-for-byte what the code builds

FLY-TSK-031-0A (A-14): the snapshots are settings-free (no surface, no timestamp, no allow-list or
write-group value), so they are the same bytes for every deployment of one build, and --check
compares BYTES, not parsed values. The output does not depend on the settings it runs under.
"""
import json
import os
import sys
from pathlib import Path

os.environ.setdefault("AWS_REGION", "emit-caps-marker")  # no AWS call is made; see the settings note below
D = Path(__file__).resolve().parents[1] / "backend" / "lambda" / "travel_mcp"
sys.path.insert(0, str(D))
import contract  # noqa: E402

# lambda_function refuses to import without every required setting (FLY-TSK-028-0B). The snapshots
# depend on none of them, so this build-time tool (never the function) stands in a marker for any that
# are absent; tests/test_parity_across_settings.py proves the output is identical under different values.
for _k, _spec in contract.SETTINGS.items():
    if _spec["required"]:
        os.environ.setdefault(_k, "emit-caps-marker")
os.environ.setdefault("DOOR_MODE", "public")  # door.load() also refuses an absent mode; the snapshots do not depend on it
import lambda_function as lf  # noqa: E402

DOCS = {"caps.json": lf.build_caps, "permission_manifest.json": lf.build_permission_manifest}


def render(doc):
    return json.dumps(doc, indent=2, sort_keys=True) + "\n"


def main(argv):
    check = "--check" in argv
    bad = 0
    for name, fn in DOCS.items():
        doc = fn()
        want = render(doc)
        path = D / name
        if check:
            if not path.exists() or path.read_text() != want:
                print(f"DRIFT: {name}")
                bad = 1
        else:
            path.write_text(want)
    return bad


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

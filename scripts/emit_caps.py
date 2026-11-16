#!/usr/bin/env python3
"""Emit or check the committed travel caps/permission snapshots (DVP-TSK-889).

  emit_caps.py          rewrite backend/lambda/travel_mcp/{caps,permission_manifest}.json
  emit_caps.py --check  exit 1 if the committed snapshots drift from the code
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
# depend on the surface name only, which MUST be supplied; the other required settings do not shape
# them, so this build-time tool (never the function) stands in a marker for any that are absent.
if not os.environ.get("SURFACE_NAME"):
    sys.exit("emit_caps: SURFACE_NAME must be supplied (the snapshots are generated for one surface)")
for _k, _spec in contract.SETTINGS.items():
    if _spec["required"]:
        os.environ.setdefault(_k, "emit-caps-marker")
import lambda_function as lf  # noqa: E402

DOCS = {"caps.json": lf.build_caps, "permission_manifest.json": lf.build_permission_manifest}


def strip(doc):
    return {k: v for k, v in doc.items() if k != "generatedAt"}


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
            if not path.exists() or strip(json.loads(path.read_text())) != strip(json.loads(want)):
                print(f"DRIFT: {name}")
                bad = 1
        else:
            path.write_text(want)
    return bad


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

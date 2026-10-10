#!/usr/bin/env python3
"""Emit or check the committed io-travel caps/permission snapshots (DVP-TSK-889).

  emit_caps.py          rewrite backend/lambda/travel_mcp/{caps,permission_manifest}.json
  emit_caps.py --check  exit 1 if the committed snapshots drift from the code
"""
import json
import os
import sys
from pathlib import Path

os.environ.setdefault("AWS_DEFAULT_REGION", "us-west-2")
D = Path(__file__).resolve().parents[1] / "backend" / "lambda" / "travel_mcp"
sys.path.insert(0, str(D))
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

#!/bin/bash
# FLY-TSK-023: associate the edge-401-header Lambda@Edge version (origin-response) with the io-travel
# distribution's default behavior. Idempotent: replaces any existing origin-response association.
# Usage: AWS_PROFILE=<admin> infra/cloudfront/associate-edge-401.sh [DISTRIBUTION_ID] [STACK_NAME]
set -euo pipefail
DIST="${1:-E2DDNP2MQ5IZCS}"; STACK="${2:-io-travel-edge-401}"
ARN=$(aws cloudformation describe-stacks --region us-east-1 --stack-name "$STACK" \
  --query "Stacks[0].Outputs[?OutputKey=='VersionArn'].OutputValue" --output text)
[ -n "$ARN" ] && [ "$ARN" != "None" ] || { echo "no VersionArn output on $STACK" >&2; exit 1; }
TMP=$(mktemp -d); trap 'rm -rf "$TMP"' EXIT
aws cloudfront get-distribution-config --id "$DIST" > "$TMP/cfg.json"
ETAG=$(python3 -c "import json;print(json.load(open('$TMP/cfg.json'))['ETag'])")
python3 - "$TMP/cfg.json" "$ARN" > "$TMP/new.json" <<'PY'
import json, sys
cfg = json.load(open(sys.argv[1]))["DistributionConfig"]; arn = sys.argv[2]
b = cfg["DefaultCacheBehavior"]
lfa = b.get("LambdaFunctionAssociations") or {"Quantity": 0}
items = [i for i in lfa.get("Items", []) if i.get("EventType") != "origin-response"]
items.append({"LambdaFunctionARN": arn, "EventType": "origin-response", "IncludeBody": False})
b["LambdaFunctionAssociations"] = {"Quantity": len(items), "Items": items}
print(json.dumps(cfg))
PY
aws cloudfront update-distribution --id "$DIST" --if-match "$ETAG" --distribution-config "file://$TMP/new.json" \
  --query 'Distribution.[Id,Status,DistributionConfig.DefaultCacheBehavior.LambdaFunctionAssociations.Items[].EventType]' --output text

#!/usr/bin/env bash
# Idempotently create io's travel records table (FLY-TSK-041), matching RecordsTable in
# infra/cloudformation/travel-deployment.yaml. Does NOT apply that template and does not touch
# io-travel-flights or the function (BRD OD-3).
# Usage: AWS_PROFILE=... infra/create_records_table.sh [table-name] [region]
# Safe to re-run: prints "already exists" and only ensures PITR + deletion protection.
set -euo pipefail
TABLE="${1:-io-travel-records}"
REGION="${2:-us-west-2}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if aws dynamodb describe-table --table-name "$TABLE" --region "$REGION" >/dev/null 2>&1; then
  echo "table $TABLE already exists"
else
  echo "creating table $TABLE in $REGION"
  aws dynamodb create-table --table-name "$TABLE" --region "$REGION" \
    --billing-mode PAY_PER_REQUEST \
    --deletion-protection-enabled \
    --attribute-definitions AttributeName=pk,AttributeType=S AttributeName=sk,AttributeType=S \
                            AttributeName=area_id,AttributeType=S AttributeName=date_iso,AttributeType=S \
    --key-schema AttributeName=pk,KeyType=HASH AttributeName=sk,KeyType=RANGE \
    --global-secondary-indexes 'IndexName=area-index,KeySchema=[{AttributeName=area_id,KeyType=HASH},{AttributeName=date_iso,KeyType=RANGE}],Projection={ProjectionType=ALL}' \
    >/dev/null
  aws dynamodb wait table-exists --table-name "$TABLE" --region "$REGION"
fi
# PITR and deletion protection (each only changed when not already on).
"$HERE/enable_table_protection.sh" "$TABLE" "$REGION"

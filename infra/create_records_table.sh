#!/usr/bin/env bash
# Idempotently create io's travel records table (FLY-TSK-041), matching RecordsTable in
# infra/cloudformation/travel-deployment.yaml. Does NOT apply that template and does not touch
# the flights table or the function (BRD OD-3).
# Usage: AWS_PROFILE=... infra/create_records_table.sh [table-name] [region]
# Safe to re-run: prints "already exists" and only ensures the stream, PITR and deletion protection.
#
# Stream (FLY-TSK-045, A-38): the table carries a stream of new images (NEW_IMAGE), as RecordsTable does in the
# template. io's production table was made by this script before the stream existed, so on 2026-11-16 (23:56Z) it
# got the stream from ONE `aws dynamodb update-table --stream-specification StreamEnabled=true,StreamViewType=NEW_IMAGE`,
# on io's approval, in place; not from a stack and not from a deploy-path plan. Nothing in travel reads the stream.
set -euo pipefail
TABLE="${1:?ABORT: pass the records table name (RECORDS_TABLE_NAME of the deployment)}"
REGION="${2:?ABORT: pass the region (AWS_REGION of the deployment)}"
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
    --stream-specification StreamEnabled=true,StreamViewType=NEW_IMAGE \
    --global-secondary-indexes 'IndexName=area-index,KeySchema=[{AttributeName=area_id,KeyType=HASH},{AttributeName=date_iso,KeyType=RANGE}],Projection={ProjectionType=ALL}' \
    >/dev/null
  aws dynamodb wait table-exists --table-name "$TABLE" --region "$REGION"
fi
# The stream, only when it is not already on (an existing table keeps whatever it has).
if [ "$(aws dynamodb describe-table --table-name "$TABLE" --region "$REGION" --query 'Table.StreamSpecification.StreamEnabled' --output text)" != "True" ]; then
  aws dynamodb update-table --table-name "$TABLE" --region "$REGION" \
    --stream-specification StreamEnabled=true,StreamViewType=NEW_IMAGE >/dev/null
  aws dynamodb wait table-exists --table-name "$TABLE" --region "$REGION"
fi
# PITR and deletion protection (each only changed when not already on).
"$HERE/enable_table_protection.sh" "$TABLE" "$REGION"

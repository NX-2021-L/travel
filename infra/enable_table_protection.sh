#!/usr/bin/env bash
# Idempotently enable point-in-time recovery and deletion protection on a DynamoDB table.
# Usage: AWS_PROFILE=... infra/enable_table_protection.sh <table-name> [region]
# Safe to re-run: each setting is only changed when it is not already on.
set -euo pipefail
TABLE="${1:?usage: $0 <table-name> [region]}"
REGION="${2:-us-west-2}"

pitr=$(aws dynamodb describe-continuous-backups --table-name "$TABLE" --region "$REGION" \
  --query 'ContinuousBackupsDescription.PointInTimeRecoveryDescription.PointInTimeRecoveryStatus' --output text)
if [ "$pitr" != "ENABLED" ]; then
  echo "enabling point-in-time recovery on $TABLE"
  aws dynamodb update-continuous-backups --table-name "$TABLE" --region "$REGION" \
    --point-in-time-recovery-specification PointInTimeRecoveryEnabled=true >/dev/null
else
  echo "point-in-time recovery already enabled on $TABLE"
fi

dp=$(aws dynamodb describe-table --table-name "$TABLE" --region "$REGION" \
  --query 'Table.DeletionProtectionEnabled' --output text)
if [ "$dp" != "True" ]; then
  echo "enabling deletion protection on $TABLE"
  aws dynamodb update-table --table-name "$TABLE" --region "$REGION" \
    --deletion-protection-enabled >/dev/null
else
  echo "deletion protection already enabled on $TABLE"
fi

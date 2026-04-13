#!/bin/bash
# Phase 4 (FLY-TSK-014): Register travel_flight_candidates Glue table
# Table: hive.devops_agentcli_db.travel_flight_candidates
# Location: s3://devops-agentcli-compute/projects/travel/flight-candidates/deduped/data/
# 422 rows, 33 columns, Parquet SerDe
# Queryable via Trino on analytics EC2, accessible at data.jreese.net Superset

aws glue create-table \
  --database-name devops_agentcli_db \
  --table-input file://glue-table-definition.json \
  --region us-west-2

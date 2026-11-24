#!/usr/bin/env bash
# FLY-TSK-089 (BRD DOC-D14184D324F0 T-2): travel's records conformance gate, read and write groups.
#   bash scripts/records_conformance.sh        (needs the CodeArtifact settings in the environment and AWS credentials)
# Installs the published @io-kit/records at tests/conformance/KIT_VERSION from CodeArtifact npm, then runs the corpus:
#   1. read groups, read-only mode, against tests/records_local_server.py          (synthetic fixtures)
#   2. every group travel declares, against tests/records_local_server.py --writable (in-process store, real handlers)
# Exits non-zero when any case fails. Nothing here reaches a live store: both servers are local and synthetic.
# Run by CI on main and by the deploy workflow's release job BEFORE plan, so a failing case stops both deployments.
set -euo pipefail
cd "$(dirname "$0")/.."
VERSION=$(tr -d '[:space:]' < tests/conformance/KIT_VERSION)
TOKEN=$(aws codeartifact get-authorization-token --domain "${CODEARTIFACT_DOMAIN}" --domain-owner "${CODEARTIFACT_OWNER}" \
          --region "${CODEARTIFACT_REGION}" --query authorizationToken --output text)
echo "::add-mask::${TOKEN}"
E="https://${CODEARTIFACT_DOMAIN}-${CODEARTIFACT_OWNER}.d.codeartifact.${CODEARTIFACT_REGION}.amazonaws.com/npm/${CODEARTIFACT_REPOSITORY}/"
(
  cd tests/conformance
  printf '@io-kit:registry=%s\n//%s:_authToken=%s\n' "$E" "${E#https://}" "$TOKEN" > .npmrc
  trap 'rm -f .npmrc' EXIT
  npm install --no-save --no-package-lock "@io-kit/records@${VERSION}" --silent
)
echo "@io-kit/records ${VERSION}"
# The servers are synthetic; they must not see the job's AWS credentials.
export AWS_ACCESS_KEY_ID=testing AWS_SECRET_ACCESS_KEY=testing AWS_SESSION_TOKEN=testing
OUT=$(mktemp -d)
trap 'kill $(jobs -p) 2>/dev/null || true' EXIT
serve() {  # serve <name> [--writable]  -> READY line in $OUT/<name>
  python3 tests/records_local_server.py 0 "${@:2}" > "$OUT/$1" 2>&1 &
  for _ in $(seq 1 100); do grep -q '^READY ' "$OUT/$1" && return 0; sleep 0.2; done
  cat "$OUT/$1"; return 1
}
serve read
read -r _ PORT DOOR < "$OUT/read"
echo "::add-mask::${DOOR}"
echo "== read groups (read-only)"
node tests/conformance/run_records_conformance.mjs "$PORT" "$DOOR"
serve write --writable
read -r _ PORT DOOR OTHER < "$OUT/write"
echo "::add-mask::${DOOR}"; echo "::add-mask::${OTHER}"
echo "== write groups (the sets travel declares)"
node tests/conformance/run_records_conformance_writes.mjs "$PORT" "$DOOR" "$OTHER"

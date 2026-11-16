#!/usr/bin/env bash
set -euo pipefail

# travel MCP Server Lambda deployment script
# Follows the Enceladus Lambda deployment pattern

# FLY-TSK-028-0B (A-2): no deployment is named here. Everything that identifies one is supplied in the
# environment (deploy/settings.env, loaded by the workflow); an absent one aborts before anything is built.
FUNCTION_NAME="${FUNCTION_NAME:?ABORT: FUNCTION_NAME is not supplied}"
REGION="${AWS_REGION:?ABORT: AWS_REGION is not supplied}"
ROLE_ARN="${LAMBDA_ROLE_ARN:-}"   # needed only when the function is created; checked there
CA_DOMAIN="${CODEARTIFACT_DOMAIN:?ABORT: CODEARTIFACT_DOMAIN is not supplied}"
CA_OWNER="${CODEARTIFACT_OWNER:?ABORT: CODEARTIFACT_OWNER is not supplied}"
CA_REGION="${CODEARTIFACT_REGION:?ABORT: CODEARTIFACT_REGION is not supplied}"
CA_REPO="${CODEARTIFACT_REPOSITORY:?ABORT: CODEARTIFACT_REPOSITORY is not supplied}"
RUNTIME="python3.12"
ARCHITECTURE="x86_64"
MEMORY=512
TIMEOUT=30
HANDLER="lambda_function.lambda_handler"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BUILD_DIR="/tmp/${FUNCTION_NAME}-build"
ZIP_FILE="/tmp/${FUNCTION_NAME}.zip"

log() { echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] $*"; }

log "Starting deployment of ${FUNCTION_NAME}"

# --- Build ---
log "Building Lambda package..."
rm -rf "${BUILD_DIR}" "${ZIP_FILE}"
mkdir -p "${BUILD_DIR}"

# Install dependencies — native x86_64 pip install matches the
# GitHub Actions runner and Lambda x86_64 architecture.
# io-kit-mcp-edge is served from the private CodeArtifact repository named by CODEARTIFACT_* (FLY-TSK-022).
# The token is never echoed; the deploy role has CodeArtifact read.
CODEARTIFACT_TOKEN=$(aws codeartifact get-authorization-token --domain "${CA_DOMAIN}" --domain-owner "${CA_OWNER}" --region "${CA_REGION}" --query authorizationToken --output text)
pip install \
  --target "${BUILD_DIR}" \
  -r "${SCRIPT_DIR}/requirements.txt" \
  --extra-index-url "https://aws:${CODEARTIFACT_TOKEN}@${CA_DOMAIN}-${CA_OWNER}.d.codeartifact.${CA_REGION}.amazonaws.com/pypi/${CA_REPO}/simple/" \
  --quiet
unset CODEARTIFACT_TOKEN

# Copy function code: EVERY *.py module in this directory ships, except the deploy-time
# helpers listed below (they run on the runner, not in the Lambda). A lane that adds a
# module (contract.py, lane_*.py) is packaged without touching this script (FLY-TSK-040).
# tests/test_deploy_packaging.py executes this block; keep the BEGIN/END markers.
# BEGIN package-modules
DEPLOY_ONLY_HELPERS="resolve_env.py"
SHIPPED_MODULES=()
for module_path in "${SCRIPT_DIR}"/*.py; do
  module_name="$(basename "${module_path}")"
  case " ${DEPLOY_ONLY_HELPERS} " in *" ${module_name} "*) continue ;; esac
  cp "${module_path}" "${BUILD_DIR}/"
  SHIPPED_MODULES+=("${module_name}")
done
[ "${#SHIPPED_MODULES[@]}" -gt 0 ] || { echo "ABORT: no modules to package"; exit 4; }
log "Packaged ${#SHIPPED_MODULES[@]} modules: ${SHIPPED_MODULES[*]}"
# END package-modules

# Create zip
cd "${BUILD_DIR}"
zip -r "${ZIP_FILE}" . -x '*.pyc' '__pycache__/*' --quiet
log "Package size: $(du -h "${ZIP_FILE}" | cut -f1)"

# --- Role ARN ---
log "Using role: ${ROLE_ARN}"

# --- Resolve environment + guard (before any code is shipped) ---
# Merge the current function env with values supplied by this shell so variables
# that are not supplied (e.g. COGNITO_ALLOWED_SUBS) are PRESERVED; refuse to
# deploy when a Cognito pool is configured with an empty allow-list (E2-R12), and when ANY
# required setting (contract.SETTINGS) is absent from the resolved environment (FLY-TSK-028-0B).
CURRENT_ENV=$(aws lambda get-function-configuration --function-name "${FUNCTION_NAME}" \
    --region "${REGION}" --query 'Environment.Variables' --output json 2>/dev/null || echo '{}')
export CURRENT_ENV
if ! RESOLVED_ENV_JSON=$(python3 "${SCRIPT_DIR}/resolve_env.py"); then
    log "ABORT: environment guard failed (see message above)"
    exit 3
fi
unset CURRENT_ENV

# --- Deploy ---
if aws lambda get-function --function-name "${FUNCTION_NAME}" --region "${REGION}" >/dev/null 2>&1; then
    log "Updating existing function..."
    aws lambda update-function-code \
        --function-name "${FUNCTION_NAME}" \
        --zip-file "fileb://${ZIP_FILE}" \
        --region "${REGION}" \
        --architectures "${ARCHITECTURE}" \
        --output text --query 'FunctionArn'

    aws lambda wait function-updated-v2 \
        --function-name "${FUNCTION_NAME}" \
        --region "${REGION}"
else
    log "Creating new function..."
    aws lambda create-function \
        --function-name "${FUNCTION_NAME}" \
        --runtime "${RUNTIME}" \
        --role "${ROLE_ARN:?ABORT: LAMBDA_ROLE_ARN is not supplied (needed to create the function)}" \
        --handler "${HANDLER}" \
        --zip-file "fileb://${ZIP_FILE}" \
        --memory-size "${MEMORY}" \
        --timeout "${TIMEOUT}" \
        --architectures "${ARCHITECTURE}" \
        --region "${REGION}" \
        --output text --query 'FunctionArn'

    aws lambda wait function-active-v2 \
        --function-name "${FUNCTION_NAME}" \
        --region "${REGION}"
fi

# --- Configure ---
log "Updating function configuration..."

# Environment is resolved by resolve_env.py (merge with the current function env).
ENV_JSON="${RESOLVED_ENV_JSON}"

aws lambda update-function-configuration \
    --function-name "${FUNCTION_NAME}" \
    --runtime "${RUNTIME}" \
    --memory-size "${MEMORY}" \
    --timeout "${TIMEOUT}" \
    --handler "${HANDLER}" \
    --environment "${ENV_JSON}" \
    --region "${REGION}" \
    --output text --query 'FunctionArn'

aws lambda wait function-updated-v2 \
    --function-name "${FUNCTION_NAME}" \
    --region "${REGION}"

# --- Function URL ---
if ! aws lambda get-function-url-config --function-name "${FUNCTION_NAME}" --region "${REGION}" >/dev/null 2>&1; then
    log "Creating Function URL..."
    aws lambda create-function-url-config \
        --function-name "${FUNCTION_NAME}" \
        --auth-type NONE \
        --cors '{"AllowOrigins":["*"],"AllowMethods":["*"],"AllowHeaders":["*"],"AllowCredentials":true,"MaxAge":3600}' \
        --region "${REGION}" \
        --output text --query 'FunctionUrl'

    aws lambda add-permission \
        --function-name "${FUNCTION_NAME}" \
        --statement-id allow-public-function-url \
        --action lambda:InvokeFunctionUrl \
        --principal "*" \
        --function-url-auth-type NONE \
        --region "${REGION}" 2>/dev/null || true
fi

FUNC_URL=$(aws lambda get-function-url-config \
    --function-name "${FUNCTION_NAME}" \
    --region "${REGION}" \
    --query 'FunctionUrl' --output text)

log "Function URL: ${FUNC_URL}"
log "Deployment complete: ${FUNCTION_NAME}"

# --- Cleanup ---
rm -rf "${BUILD_DIR}" "${ZIP_FILE}"

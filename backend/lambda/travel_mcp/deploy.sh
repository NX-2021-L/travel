#!/usr/bin/env bash
set -euo pipefail

# io-travel MCP Server Lambda deployment script
# Follows the Enceladus Lambda deployment pattern

FUNCTION_NAME="io-travel-mcp-server"
ROLE_ARN="${LAMBDA_ROLE_ARN:-arn:aws:iam::356364570033:role/io-travel-mcp-lambda-role}"
REGION="${AWS_REGION:-us-west-2}"
RUNTIME="python3.12"
ARCHITECTURE="arm64"
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

# Install dependencies — two passes:
# 1. Binary-only for compiled packages (cryptography, etc.)
# 2. Regular install for pure-Python packages (mcp, PyJWT, etc.)
pip install \
  --platform manylinux2014_aarch64 \
  --implementation cp \
  --python-version 3.12 \
  --only-binary=:all: \
  --target "${BUILD_DIR}" \
  cryptography cffi certifi \
  --quiet

pip install \
  --target "${BUILD_DIR}" \
  --no-deps \
  -r "${SCRIPT_DIR}/requirements.txt" \
  --quiet

# Second pass with deps to catch anything missed
pip install \
  --target "${BUILD_DIR}" \
  -r "${SCRIPT_DIR}/requirements.txt" \
  --quiet 2>/dev/null || true

# Copy function code
cp "${SCRIPT_DIR}/lambda_function.py" "${BUILD_DIR}/"

# Create zip
cd "${BUILD_DIR}"
zip -r "${ZIP_FILE}" . -x '*.pyc' '__pycache__/*' '*.dist-info/*' --quiet
log "Package size: $(du -h "${ZIP_FILE}" | cut -f1)"

# --- Role ARN ---
log "Using role: ${ROLE_ARN}"

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
        --role "${ROLE_ARN}" \
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

# Build environment JSON — only include non-empty values
ENV_JSON=$(python3 -c "
import json, os
env = {
    'TABLE_NAME': os.environ.get('TABLE_NAME', 'io-travel-flights'),
    'MCP_TRANSPORT': 'streamable_http',
    'COGNITO_USER_POOL_ID': os.environ.get('COGNITO_USER_POOL_ID', ''),
    'COGNITO_CLIENT_ID': os.environ.get('COGNITO_CLIENT_ID', ''),
    'COGNITO_CLIENT_SECRET': os.environ.get('COGNITO_CLIENT_SECRET', ''),
    'COGNITO_DOMAIN': os.environ.get('COGNITO_DOMAIN', ''),
    'COGNITO_REGION': os.environ.get('COGNITO_REGION', 'us-east-1'),
    'MCP_API_KEY': os.environ.get('MCP_API_KEY', ''),
    'SERVER_BASE_URL': os.environ.get('SERVER_BASE_URL', ''),
}
# Filter out empty values to avoid AWS CLI parse errors
filtered = {k: v for k, v in env.items() if v}
print(json.dumps({'Variables': filtered}))
")

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

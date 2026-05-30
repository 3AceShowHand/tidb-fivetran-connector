#!/usr/bin/env bash

set -euo pipefail

: "${AWS_PROFILE:=full-manager-service-role}"
: "${AWS_REGION:=us-east-1}"
: "${AWS_ACCOUNT_ID:=385595570414}"
: "${ECR_REPOSITORY:=tidb-fivetran-deployer}"
: "${IMAGE_TAG:=0.1.0}"
: "${PUSH:=1}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
IMAGE_URI="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${ECR_REPOSITORY}:${IMAGE_TAG}"

echo "Building ${IMAGE_URI}"
docker build \
  -f "${REPO_ROOT}/tidb-fivetran-connector/Dockerfile.deployer" \
  -t "${IMAGE_URI}" \
  "${REPO_ROOT}"

if [[ "${PUSH}" == "1" ]]; then
  aws ecr get-login-password --profile "${AWS_PROFILE}" --region "${AWS_REGION}" \
    | docker login --username AWS --password-stdin "${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"
  docker push "${IMAGE_URI}"
fi

echo "${IMAGE_URI}"

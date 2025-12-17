#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -euo pipefail

export IMPALA_HOME="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export IMPALA_BUILD_THREADS="${IMPALA_BUILD_THREADS:-12}"
OCI_IMG="${OCI_IMG:-apache/impaladev}"
OCI_TAG="${OCI_TAG:-latest}"
IMPALA_DOCKER_BUILDER="${IMPALA_DOCKER_BUILDER:-impala-builder}"
OUTPUT_TYPE="${OUTPUT_TYPE:-image}"
PUSH="${PUSH:-0}"

if [[ -z "${PLATFORM:-}" ]]; then
  if [[ "$(uname -p)" == "x86_64" ]]; then
    PLATFORM="linux/amd64"
    OCI_TAG="${OCI_TAG}-x86"
  else
    PLATFORM="linux/arm64"
    OCI_TAG="${OCI_TAG}-arm"
  fi
fi

if [[ "${PUSH}" == "1" ]]; then
  PUSH=",push=true"
else
  PUSH=""
fi

# Cleanup function to remove docker buildx builder (if it exists).
rm_docker_builder() {
  docker buildx rm -f "${IMPALA_DOCKER_BUILDER}" || true
}

# The following commands assume they are run from the parent directory of the directory
# containing this script.
pushd "$(dirname "$(readlink -f "$0")")/.."
trap 'popd || true; rm_docker_builder' EXIT

# Determine git information for labeling the image.
GIT_HASH="$(git rev-parse HEAD)"
GIT_REPO="$(git remote get-url origin | cut -d'/' -f3- | cut -d'@' -f2-)"

echo "[INFO] Building Impala devcontainer:"
echo "         Git Repo:    ${GIT_REPO}"
echo "         Git Hash:    ${GIT_HASH}"
echo "         OCI Image:   ${OCI_IMG}:${OCI_TAG}"
echo "         Platform:    ${PLATFORM}"
echo "         Output Type: ${OUTPUT_TYPE}"
echo -n "         Push:        "
if [[ -n "${PUSH}" ]]; then
  echo "true"
else
  echo "false"
fi
echo
echo "============================================================================"
echo

SKIP_JAVA_DETECTION=1
. bin/impala-config.sh
export IMPALA_TOOLCHAIN_PKGS_RELPATH="${IMPALA_TOOLCHAIN_PACKAGES_HOME#"${IMPALA_HOME}"}"

# Setup docker buildx builder
rm_docker_builder

set -x
docker buildx create \
  --name "${IMPALA_DOCKER_BUILDER}" \
  --driver docker-container \
  --bootstrap \
  --use

GIT_REPO="${GIT_REPO}" GIT_HASH="${GIT_HASH}" devcontainer \
  build \
  --workspace-folder="${IMPALA_HOME}" \
  --config=".devcontainer-build/devcontainer.json" \
  --image-name="${OCI_IMG}:${OCI_TAG}" \
  --platform="${PLATFORM}" \
  --output "type=${OUTPUT_TYPE}${PUSH}"

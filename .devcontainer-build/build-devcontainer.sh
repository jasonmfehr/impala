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

# Builds the Impala devcontainer OCI image using the devcontainer CLI and docker buildx.
#
# Inputs (via environment variables):
#   OCI_IMG:        The name of the OCI image to build. Default: "apache/impala-dev"
#   OCI_TAG:        The tag of the OCI image to build. The image platform type (either
#                   "x86" or "arm" will be appended to this value). Default: "latest"
#   DOCKER_BUILDER: The name of the docker buildx builder to create/use.
#                   Default: "impala-builder"
#   OUTPUT_TYPE:    The output type for the devcontainer build command. Default: "image"
#   PUSH:           If set to 1, the built image will be pushed to a remote registry,
#                   otherwise the resulting output will not be pushed. Default: "0"

set -euo pipefail

# Script Inputs via Environment Variables.
OCI_IMG="${OCI_IMG:-apache/impala-dev}"
OCI_TAG="${OCI_TAG:-latest}"
DOCKER_BUILDER="${DOCKER_BUILDER:-impala-builder}"
OUTPUT_TYPE="${OUTPUT_TYPE:-image}"
PUSH="${PUSH:-0}"
JAVA_VERSION="${JAVA_VERSION:-17}"

IMPALA_HOME="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export IMPALA_HOME
export IMPALA_BUILD_THREADS="${IMPALA_BUILD_THREADS:-12}"

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
  docker buildx rm -f "${DOCKER_BUILDER}" || true
}

# The following commands assume they are run from the parent directory of the directory
# containing this script.
pushd "$(dirname "$(readlink -f "$0")")/.."
trap 'popd || true; rm_docker_builder' EXIT

# Determine git information for labeling the image.
GIT_HASH="$(git rev-parse HEAD)"
GIT_BRANCH="$(git rev-parse --abbrev-ref HEAD)"
GIT_REMOTE_NAME="$(git config --get "branch.${GIT_BRANCH}.remote")"
GIT_REPO="$(git remote get-url "${GIT_REMOTE_NAME}" | cut -d'/' -f3- | cut -d'@' -f2-)"

echo "[INFO] Building Impala devcontainer:"
echo "         Git Repo:     ${GIT_REPO}"
echo "         Git Branch:   ${GIT_BRANCH}"
echo "         Git Hash:     ${GIT_HASH}"
echo "         OCI Image:    ${OCI_IMG}:${OCI_TAG}"
echo "         Platform:     ${PLATFORM}"
echo "         Output Type:  ${OUTPUT_TYPE}"
echo -n "         Push:         "
if [[ -n "${PUSH}" ]]; then
  echo "true"
else
  echo "false"
fi
echo "         Java Version: ${JAVA_VERSION}"
echo
echo "============================================================================"
echo

# Setup docker buildx builder
rm_docker_builder

set -x
docker buildx create \
    --name "${DOCKER_BUILDER}" \
    --driver docker-container \
    --bootstrap \
    --use

GIT_REPO="${GIT_REPO}" \
GIT_BRANCH="${GIT_BRANCH}" \
GIT_HASH="${GIT_HASH}" \
JAVA_VERSION="${JAVA_VERSION}" \
devcontainer build \
    --workspace-folder="${IMPALA_HOME}" \
    --config=".devcontainer-build/devcontainer.json" \
    --image-name="${OCI_IMG}:${OCI_TAG}" \
    --platform="${PLATFORM}" \
    --output "type=${OUTPUT_TYPE}${PUSH}"

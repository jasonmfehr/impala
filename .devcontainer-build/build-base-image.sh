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

# Builds the Impala devcontainer base OCI image using docker buildx.
#
# Inputs (via environment variables):
#   OCI_IMG:        The name of the base OCI image to build. 
#                   Default: "apache/impala-dev"
#   OCI_TAG:        The tag of the OCI image to build.
#                   Default: "${OS}_${OS_VERSION}-${SHORT_PLATFORM}-base"
#   DOCKER_BUILDER: The name of the docker buildx builder to create/use.
#                   Default: "impala-builder-base-${OS}_${OS_VERSION}-${SHORT_PLATFORM}"
#   PUSH:           If set to 1, the built image will be pushed to a remote registry,
#                   otherwise the resulting output will not be pushed. Default: "0"
#   OS:             The operating system family to build for.
#                   Valid values: "ubuntu", "rocky". Default: "ubuntu"
#   OS_VERSION:     The version of the OS to build for. Valid values:
#                   - For ubuntu: "20.04", "22.04", "24.04"
#                   - For rocky: "8.6", "8.8", "9.2"
#                   Default: "22.04" for ubuntu, "9.2" for rocky

set -euo pipefail

# Script Inputs via Environment Variables.
OS="${OS:-ubuntu}"
OCI_IMG="${OCI_IMG:-apache/impala-dev}"
PUSH="${PUSH:-0}"

# Set default OS_VERSION based on OS if not specified
if [[ -z "${OS_VERSION:-}" ]]; then
  if [[ "${OS}" == "ubuntu" ]]; then
    OS_VERSION="22.04"
  elif [[ "${OS}" == "rocky" ]]; then
    OS_VERSION="9.2"
  else
    echo "[ERROR] Unknown OS: ${OS}" >&2
    exit 1
  fi
fi

# Validate OS and OS_VERSION combinations
case "${OS}" in
  ubuntu)
    case "${OS_VERSION}" in
      20.04|22.04|24.04)
        ;;
      *)
        echo "[ERROR] Unsupported Ubuntu version: ${OS_VERSION}" >&2
        echo "[ERROR] Supported versions: 20.04, 22.04, 24.04" >&2
        exit 1
        ;;
    esac
    ;;
  rocky)
    case "${OS_VERSION}" in
      8.6|8.8|9.2)
        ;;
      *)
        echo "[ERROR] Unsupported Rocky Linux version: ${OS_VERSION}" >&2
        echo "[ERROR] Supported versions: 8.6, 8.8, 9.2" >&2
        exit 1
        ;;
    esac
    ;;
  *)
    echo "[ERROR] Unsupported OS: ${OS}" >&2
    echo "[ERROR] Supported OS families: ubuntu, rocky" >&2
    exit 1
    ;;
esac

# Auto-detect platform if not specified.
if [[ -z "${PLATFORM:-}" ]]; then
  if [[ "$(uname -p)" == "x86_64" ]]; then
    PLATFORM="linux/amd64"
    SHORT_PLATFORM="x86"
  else
    PLATFORM="linux/arm64"
    SHORT_PLATFORM="arm"
  fi
fi

# Set OCI tag and docker builder name now that all variables are defined.
OS_PLATFORM="${OS}_${OS_VERSION}-${SHORT_PLATFORM}"
DOCKER_BUILDER="${DOCKER_BUILDER:-impala-builder-base-${OS_PLATFORM}}"
OCI_TAG="${OCI_TAG:-${OS_PLATFORM}-base}"

if [[ "${PUSH}" == "1" ]]; then
  PUSH_FLAG="--push"
else
  PUSH_FLAG="--load"
fi

# Cleanup function to remove docker buildx builder (if it exists).
rm_docker_builder() {
  docker buildx rm -f "${DOCKER_BUILDER}" || true
}

# The following commands assume they are run from the directory containing this script.
pushd "$(dirname "$(readlink -f "$0")")"
trap 'popd || true; rm_docker_builder' EXIT

# Determine git information for labeling the image.
GIT_HASH="$(git rev-parse HEAD)"
GIT_BRANCH="$(git rev-parse --abbrev-ref HEAD)"
GIT_REMOTE_NAME="$(git config --get "branch.${GIT_BRANCH}.remote")"
GIT_REPO="$(git remote get-url "${GIT_REMOTE_NAME}" | cut -d'/' -f3- | cut -d'@' -f2-)"

# Name of the image that will be built.
BASE_IMAGE="${OCI_IMG}:${OCI_TAG}"

echo "[INFO] Building Impala base image:"
echo "         Git Repo:     ${GIT_REPO}"
echo "         Git Branch:   ${GIT_BRANCH}"
echo "         Git Hash:     ${GIT_HASH}"
echo "         OS:           ${OS}"
echo "         OS Version:   ${OS_VERSION}"
echo "         Base Image:   ${BASE_IMAGE}"
echo "         Platform:     ${PLATFORM}"
echo -n "         Push:         "
if [[ "${PUSH}" == "1" ]]; then
  echo "true"
else
  echo "false"
fi
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

# Build the base image
docker buildx build \
    --file "Dockerfile.base.${OS}" \
    --tag "${BASE_IMAGE}" \
    --platform="${PLATFORM}" \
    --progress=plain \
    --build-arg "OS_VERSION=${OS_VERSION}" \
    --build-arg "GIT_REPO=${GIT_REPO}" \
    --build-arg "GIT_BRANCH=${GIT_BRANCH}" \
    --build-arg "GIT_HASH=${GIT_HASH}" \
    ${PUSH_FLAG} \
    .

set +x

echo
echo "============================================================================"
echo "[INFO] Base image build complete: ${BASE_IMAGE}"
echo "============================================================================"

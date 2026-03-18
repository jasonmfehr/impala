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
#   OS:             The operating system family to build for. Valid values: "ubuntu", "rocky"
#                   Default: "ubuntu"
#   OS_VERSION:     The version of the OS to build for. Valid values:
#                   - For ubuntu: "20.04", "22.04", "24.04"
#                   - For rocky: "8", "9"
#                   Default: "22.04" for ubuntu, "9" for rocky

set -euo pipefail

# Script Inputs via Environment Variables.
OCI_IMG="${OCI_IMG:-apache/impala-dev}"
OCI_TAG="${OCI_TAG:-latest}"
DOCKER_BUILDER="${DOCKER_BUILDER:-impala-builder}"
OUTPUT_TYPE="${OUTPUT_TYPE:-image}"
PUSH="${PUSH:-0}"
JAVA_VERSION="${JAVA_VERSION:-17}"
OS="${OS:-ubuntu}"

# Set default OS_VERSION based on OS if not specified
if [[ -z "${OS_VERSION:-}" ]]; then
  if [[ "${OS}" == "ubuntu" ]]; then
    OS_VERSION="22.04"
  elif [[ "${OS}" == "rocky" ]]; then
    OS_VERSION="9"
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
      8|9)
        ;;
      *)
        echo "[ERROR] Unsupported Rocky Linux version: ${OS_VERSION}" >&2
        echo "[ERROR] Supported versions: 8, 9" >&2
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

# Determine base image name and tag
BASE_IMAGE_NAME="${OCI_IMG}-base-${OS}"
BASE_IMAGE_TAG="${OS_VERSION}-${PLATFORM##*/}"
BASE_IMAGE="${BASE_IMAGE_NAME}:${BASE_IMAGE_TAG}"

echo "[INFO] Building Impala devcontainer:"
echo "         Git Repo:     ${GIT_REPO}"
echo "         Git Branch:   ${GIT_BRANCH}"
echo "         Git Hash:     ${GIT_HASH}"
echo "         OS:           ${OS}"
echo "         OS Version:   ${OS_VERSION}"
echo "         Base Image:   ${BASE_IMAGE}"
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

# Build the base image first
echo "[INFO] Building base image: ${BASE_IMAGE}"
if [[ "${OS}" == "ubuntu" ]]; then
  BASE_DOCKERFILE="Dockerfile.base.ubuntu"
  OS_VERSION_ARG="UBUNTU_VERSION"
elif [[ "${OS}" == "rocky" ]]; then
  BASE_DOCKERFILE="Dockerfile.base.rocky"
  OS_VERSION_ARG="ROCKY_VERSION"
fi

docker buildx build \
    --file ".devcontainer-build/${BASE_DOCKERFILE}" \
    --tag "${BASE_IMAGE}" \
    --platform="${PLATFORM}" \
    --build-arg "${OS_VERSION_ARG}=${OS_VERSION}" \
    --build-arg "GIT_REPO=${GIT_REPO}" \
    --build-arg "GIT_BRANCH=${GIT_BRANCH}" \
    --build-arg "GIT_HASH=${GIT_HASH}" \
    --build-arg "JAVA_VERSION=${JAVA_VERSION}" \
    --build-arg "IMPALA_CMAKE_VERSION=${IMPALA_CMAKE_VERSION:-}" \
    --build-arg "IMPALA_GCC_VERSION=${IMPALA_GCC_VERSION:-}" \
    --build-arg "IMPALA_BUILD_THREADS=${IMPALA_BUILD_THREADS}" \
    --load \
    "${IMPALA_HOME}"

echo "[INFO] Building main devcontainer image: ${OCI_IMG}:${OCI_TAG}"

BASE_IMAGE="${BASE_IMAGE}" \
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

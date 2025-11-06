#!/bin/bash
set -euo pipefail

IMPALA_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"/..

if [[ -z "${PLATFORM:-}" ]]; then
  if [[ "$(uname -m)" == "x86_64" ]]; then
    PLATFORM="linux/amd64"
  else
    PLATFORM="linux/arm64"
  fi
fi

. "${IMPALA_ROOT}/bin/impala-config.sh"
devcontainer \
  build \
  --workspace-folder="${IMPALA_ROOT}" \
  --config=".devcontainer-build/devcontainer.json" \
  --image-name="jasonmfehr/impaladev:latest" \
  --platform "${PLATFORM}" \
  --build-arg "IMPALA_CMAKE_VERSION=${IMPALA_CMAKE_VERSION}" \
  --build-arg "IMPALA_GCC_VERSION=${IMPALA_GCC_VERSION}" \
  --build-arg "TOOLCHAIN_PACKAGES_RELATIVE_PATH=${IMPALA_TOOLCHAIN_PACKAGES_HOME#"${IMPALA_HOME}"}"

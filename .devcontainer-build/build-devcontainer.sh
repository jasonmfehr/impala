#!/bin/bash
set -euo pipefail

export IMPALA_HOME="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export IMPALA_BUILD_THREADS="${IMPALA_BUILD_THREADS:-12}"
OCI_IMG="${OCI_IMG:-apache/impaladev}"
OCI_TAG="${OCI_TAG:-latest}"

if [[ -z "${PLATFORM:-}" ]]; then
  if [[ "$(uname -p)" == "x86_64" ]]; then
    PLATFORM="linux/amd64"
  else
    PLATFORM="linux/arm64"
  fi
fi

SKIP_JAVA_DETECTION=1
. bin/impala-config.sh
export IMPALA_TOOLCHAIN_PACKAGES_RELPATH="${IMPALA_TOOLCHAIN_PACKAGES_HOME#"${IMPALA_HOME}"}"

devcontainer \
  build \
  --workspace-folder="${IMPALA_HOME}" \
  --config=".devcontainer-build/devcontainer.json" \
  --image-name="${OCI_IMG}:${OCI_TAG}" \
  --platform "${PLATFORM}" \

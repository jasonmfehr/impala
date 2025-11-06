#!/bin/bash
set -euo pipefail

export IMPALA_HOME="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"/..

if [[ -z "${PLATFORM:-}" ]]; then
  if [[ "$(uname -m)" == "x86_64" ]]; then
    PLATFORM="linux/amd64"
  else
    PLATFORM="linux/arm64"
  fi
fi

 . bin/impala-config.sh --skip_java_detection
 export IMPALA_TOOLCHAIN_PACKAGES_RELPATH="${IMPALA_TOOLCHAIN_PACKAGES_HOME#"${IMPALA_HOME}"}"

devcontainer \
  build \
  --workspace-folder="${IMPALA_HOME}" \
  --config=".devcontainer-build/devcontainer.json" \
  --image-name="jasonmfehr/impaladev:latest" \
  --platform "${PLATFORM}" \

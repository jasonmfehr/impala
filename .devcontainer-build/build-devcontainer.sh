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

devcontainer build --workspace-folder="${IMPALA_ROOT}" --config=".devcontainer-build/devcontainer.json"  --image-name="jasonmfehr/impaladev:latest" --platform "${PLATFORM}"

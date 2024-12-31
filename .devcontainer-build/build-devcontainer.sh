#!/bin/bash
set -euo pipefail

IMPALA_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"/..

devcontainer build --workspace-folder="${IMPALA_ROOT}" --config=".devcontainer-build/devcontainer.json"  --image-name="jasonmfehr/impaladev:latest" --platform "linux/amd64"
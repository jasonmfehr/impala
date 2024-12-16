#!/bin/bash
set -euo pipefail

IMPALA_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"/..

DOCKER_IMG="${2-impaladev}:${1-latest}"
export DOCKER_DEFAULT_PLATFORM=linux/amd64

docker build --force-rm --progress=plain --add-host="impdev:127.0.0.1" --tag="${DOCKER_IMG}" -f .devcontainer/Dockerfile "${IMPALA_ROOT}"

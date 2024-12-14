#!/bin/bash
set -euo pipefail

DOCKER_IMG="${2-impaladev}:${1-latest}"
export DOCKER_DEFAULT_PLATFORM=linux/amd64

# onexit() {
#   popd
# }

# pushd ..
# trap onexit EXIT
DOCKER_BUILDKIT=1 docker build --force-rm --progress=plain --add-host="impdev:127.0.0.1" --tag="${DOCKER_IMG}" -f .devcontainer/Dockerfile /Users/jfehr/dev/apache/impala/

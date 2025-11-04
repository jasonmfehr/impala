#!/bin/bash
set -euo pipefail

IMPALA_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"/..
export DOCKER_BUILDKIT=1

docker build \
    --add-host=ip4.impala.test:127.0.0.1 \
    --add-host=ip4:127.0.0.1 \
    --add-host=ip6.impala.test:127.0.0.1 \
    --add-host=ip6:127.0.0.1 \
    --add-host=ip46.impala.test:127.0.0.1 \
    --add-host=ip46:127.0.0.1 \
    --add-host=buildkitsandbox:127.0.0.1 \
    --file=.devcontainer-build/Dockerfile \
    --platform "linux/amd64" \
    --progress=plain \
    --tag=jasonmfehr/impaladev:latest \
    "${IMPALA_ROOT}"

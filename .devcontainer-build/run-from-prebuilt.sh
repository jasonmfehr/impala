#!/bin/bash
set -euo pipefail

docker rm impaladevctr -f || true
docker volume rm impaladevctr -f || true
docker run -d --name=impaladevctr jasonmfehr/impaladev:latest
devcontainer set-up --container-id impaladevctr --config=/home/jfehr/impala/.devcontainer/devcontainer.json

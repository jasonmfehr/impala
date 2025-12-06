#!/bin/bash
set -euo pipefail

echodate() {
  echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*"
}

echodate "Starting container initialization..."

if [[ $(grep -cE "127.0.0.1\s+$(hostname)" /etc/hosts || true) == "0" ]]; then
  echodate "Adding container hostname '$(hostname)]' to /etc/hosts..."
  echo "127.0.0.1 $(hostname) $(hostname -s)" | sudo tee -a /etc/hosts
  echo "DNS setup complete."
fi

echodate "Setting up SSH..."
/bin/bash -c 'sudo ssh-keygen -A; sudo service ssh start'
echodate "SSH setup complete."

echodate "Starting PostgreSQL..."
sudo service postgresql start
echodate "PostgreSQL started."

echodate "Starting supporting services..."
cd "${IMPALA_HOME}"
. bin/impala-config.sh
. bin/set-classpath.sh
./testdata/bin/run-all.sh
echodate "Supporting services started."

echo
echodate "Initialization finished. Container is ready"

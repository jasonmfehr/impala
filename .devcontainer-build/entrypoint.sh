#!/bin/bash
set -euo pipefail

echo "Setting up SSH..."
/bin/bash -c 'sudo ssh-keygen -A; sudo service ssh start'
echo "SSH setup complete."

echo "Starting PostgreSQL..."
sudo service postgresql start
echo "PostgreSQL started."

# echo "Starting supporting services..."
# cd "${IMPALA_HOME}"
# . bin/impala-config.sh
# . bin/set-classpath.sh
# ./testdata/bin/run-all.sh
# echo "Supporting services started."

echo
echo "Initialization finished. Container is ready"
sleep infinity

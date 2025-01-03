#!/bin/bash
set -euo pipefail

# setup ssh
sudo ssh-keygen -A
sudo /usr/sbin/sshd

# setup container hostname to 127.0.0.1 mapping
echo -e "\n127.0.0.1 $(hostname) $(hostname -s)" | sudo tee -a /etc/hosts
NEW_HOSTS=$(mktemp)
sed 's/127.0.1.1/127.0.0.1/g' /etc/hosts > "${NEW_HOSTS}"
diff -u /etc/hosts "${NEW_HOSTS}" || true
sudo cp "${NEW_HOSTS}" /etc/hosts
rm "${NEW_HOSTS}"

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

# start postgres
sudo service postgresql start
# sudo -u postgres PGDATA=/var/lib/pgsql/data bash -c 'pg_ctl start -w --timeout=120 >> /var/lib/pgsql/pg.log 2>&1'
# POSTGRES_VER="$(ls /usr/lib/postgresql | sort -nr | head -n1)"
# POSTGRES_BIN="/usr/lib/postgresql/${POSTGRES_VER}/bin"
# POSTGRES_DATA="/var/lib/postgresql/${POSTGRES_VER}/main"
# sudo -u postgres "${POSTGRES_BIN}/pg_ctl" initdb -D "${POSTGRES_DATA}"
# sudo -u postgres "${POSTGRES_BIN}/pg_ctl" start -D "${POSTGRES_DATA}"

# start supporting services
sudo -u impdev /home/impdev/testdata/bin/run-all.sh

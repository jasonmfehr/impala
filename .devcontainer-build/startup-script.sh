#!/bin/bash
set -euo pipefail

sudo ssh-keygen -A
sudo /usr/sbin/sshd

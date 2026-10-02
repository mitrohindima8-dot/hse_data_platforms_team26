#!/bin/bash

set -e

HOSTS=("team-26-nn" "team-26-00" "team-26-01")
KEY="$HOME/.ssh/team_internal"

for host in "${HOSTS[@]}"; do
    scp -i "$KEY" install_hadoop.sh team@"$host":/tmp/install_hadoop.sh
    ssh -i "$KEY" team@"$host" "chmod +x /tmp/install_hadoop.sh && /tmp/install_hadoop.sh"
done

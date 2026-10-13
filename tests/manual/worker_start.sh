#!/usr/bin/env bash
# Start Ray on a vast worker (runs on the worker as user ray, from /opt/rayvast/).
# Usage: bash worker_start.sh "<localhost port requests, e.g. 70004 or 70004 70005>" [<worker port requests>] [extra env K=V ...]
# The default worker port requests are the 57 ports 70006-70039, 70041-70063.
set -u
LOCALHOST_REQ=$1
WORKER_REQ=${2:-"$(seq -s ' ' 70006 70039) $(seq -s ' ' 70041 70063)"}
shift; shift || true
set -a
. /etc/rayvast_vast.env
. /opt/rayvast/worker.env
set +a
export PATH=/home/ray/anaconda3/bin:$PATH
export RAYVAST_PRIVATE_IP=$(hostname -I | awk '{print $1}')
export RAYVAST_LOCALHOST_PORTS=$(for r in $LOCALHOST_REQ; do eval echo \$VAST_TCP_PORT_$r; done | paste -sd,)
WORKER_PORTS=$(for r in $WORKER_REQ; do eval echo \$VAST_TCP_PORT_$r; done | paste -sd,)
for kv in "$@"; do export "$kv"; done
echo "PUBLIC=$RAYVAST_PUBLIC_IP PRIVATE=$RAYVAST_PRIVATE_IP LOCALHOST_PORTS=$RAYVAST_LOCALHOST_PORTS worker ports ($(echo $WORKER_PORTS | tr , '\n' | wc -l)): $WORKER_PORTS extra: $*"
ray start --address=$RAYVAST_HEAD_IP:6379 --node-ip-address=$PUBLIC_IPADDR --num-cpus=8 \
  --node-manager-port=$VAST_TCP_PORT_70000 --object-manager-port=$VAST_TCP_PORT_70001 \
  --metrics-export-port=$VAST_TCP_PORT_70002 --dashboard-agent-grpc-port=$VAST_TCP_PORT_70003 \
  --dashboard-agent-listen-port=$VAST_TCP_PORT_70004 --runtime-env-agent-port=$VAST_TCP_PORT_70005 \
  --worker-port-list=$WORKER_PORTS 2>&1 | grep -vE '^\s*$' | tail -5

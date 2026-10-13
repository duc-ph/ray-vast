#!/usr/bin/env bash
# C4 head heartbeat loop (in place of the provider). Runs on the test EC2 host.
# Every 30 s, for each worker: touch /opt/rayvast/heartbeat over SSH as ray, then copy /opt/rayvast/watchdog.log
# back to <out dir>/<worker name>/. Each attempt is logged to <out dir>/heartbeat.log with the time and exit codes.
# Usage: bash heartbeat.sh <out dir> <name>=<ip>:<ssh port> [...]
# With NO_TOUCH=1 the loop only copies the logs (to watch a worker after the "provider" stopped).
set -u
OUT=$1; shift
SSHOPT="-i $HOME/.ssh/rayvast_head -o UserKnownHostsFile=$HOME/.ssh/known_hosts_rayvast -o ConnectTimeout=10 -o BatchMode=yes"
while true; do
  for w in "$@"; do
    NAME=${w%%=*}; IP=${w#*=}; IP=${IP%:*}; PORT=${w##*:}
    mkdir -p $OUT/$NAME
    touch_rc=skipped
    if [ "${NO_TOUCH:-0}" != 1 ]; then
      ssh $SSHOPT -p $PORT ray@$IP touch /opt/rayvast/heartbeat
      touch_rc=$?
    fi
    t_touch=$(date -u +%FT%TZ)
    scp -q $SSHOPT -P $PORT ray@$IP:/opt/rayvast/watchdog.log $OUT/$NAME/watchdog.log
    copy_rc=$?
    echo "$t_touch $NAME touch_rc=$touch_rc copy_rc=$copy_rc" >> $OUT/heartbeat.log
  done
  sleep 30
done

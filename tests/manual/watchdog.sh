#!/usr/bin/env bash
# C3.3 worker watchdog (T10). Runs on a vast worker as user ray.
# Every 15 s it tries a TCP connection to <head ip>:6379. After <limit> s without a connection, it destroys
# its own instance: first with vast's per-instance key (CONTAINER_API_KEY in the env of PID 1), and if that
# is refused, with the scoped key in /opt/rayvast/vast_key. It logs the HTTP codes, never the keys.
# Usage: nohup setsid bash watchdog.sh <head ip> <limit seconds> <own instance id> &
HEAD=$1; LIMIT=$2; ID=$3
LOG=/opt/rayvast/watchdog.log
last_ok=$(date +%s)
while true; do
  if timeout 5 bash -c "exec 3<>/dev/tcp/$HEAD/6379" 2>/dev/null; then last_ok=$(date +%s); state=ok; else state=fail; fi
  down=$(( $(date +%s) - last_ok ))
  echo "$(date -u +%FT%TZ) $state down=${down}s" >> $LOG
  if [ $down -ge $LIMIT ]; then
    echo "$(date -u +%FT%TZ) limit reached, destroying instance $ID" >> $LOG
    CONTAINER_API_KEY=$(tr '\0' '\n' < /proc/1/environ | sed -n 's/^CONTAINER_API_KEY=//p') \
    /home/ray/anaconda3/bin/python - "$ID" >> $LOG 2>&1 <<'PY'
import os, sys, time, urllib.error, urllib.request
for name, key in (("CONTAINER_API_KEY", os.environ.get("CONTAINER_API_KEY", "")),
                  ("scoped key file", open("/opt/rayvast/vast_key").read().strip())):
    req = urllib.request.Request(f"https://console.vast.ai/api/v0/instances/{sys.argv[1]}/", method="DELETE",
                                 headers={"Authorization": f"Bearer {key}"})
    try:
        r = urllib.request.urlopen(req, timeout=30)
        code, text = r.status, r.read().decode()[:200]
    except urllib.error.HTTPError as e:
        code, text = e.code, e.read().decode()[:200]
    except OSError as e:
        code, text = None, repr(e)
    print(time.strftime("%FT%TZ", time.gmtime()), f"DELETE with {name}: HTTP {code} {text}", flush=True)
    if code == 200:
        break
PY
    sleep 60
  fi
  sleep 15
done

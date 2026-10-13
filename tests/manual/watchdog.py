# C4 worker watchdog (T13, T14). Runs on a vast worker as root, with the system Python 3 of the image.
# The start script writes this file and ends with `exec sudo -E /usr/bin/python3 /opt/rayvast/watchdog.py`,
# so PID 1 is sudo (root) and this process is its only child. If it stops, the container stops.
# Every 15 s it reads the mtime of /opt/rayvast/heartbeat (the head touches it over SSH every 30 s):
#   - no heartbeat file 10 min after the start -> destroy the instance (a candidate that the provider forgot)
#   - heartbeat older than 5 min -> destroy the instance
#   - any unexpected exception -> destroy the instance, then exit
# Destroy = DELETE /api/v0/instances/$CONTAINER_ID/ with vast's per-instance key $CONTAINER_API_KEY.
# The key is never logged. Each check and decision goes to /opt/rayvast/watchdog.log and to stdout.
import os
import sys
import time
import urllib.error
import urllib.request

HEARTBEAT = "/opt/rayvast/heartbeat"
LOG = "/opt/rayvast/watchdog.log"
CHECK_EVERY_S = 15
GRACE_S = 600
MAX_AGE_S = 300
started = time.time()


def say(msg):
    line = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()) + " " + msg
    print(line, flush=True)
    # /opt/rayvast belongs to user ray, so ray could put a symlink at LOG. O_NOFOLLOW stops root from
    # writing through it.
    try:
        fd = os.open(LOG, os.O_WRONLY | os.O_APPEND | os.O_CREAT | os.O_NOFOLLOW, 0o644)
        os.write(fd, (line + "\n").encode())
        os.close(fd)
    except OSError as e:
        print(f"log write failed: {e!r}", flush=True)


def destroy(reason):
    say(f"DESTROY reason='{reason}'")
    url = f"https://console.vast.ai/api/v0/instances/{os.environ.get('CONTAINER_ID', '')}/"
    headers = {"Authorization": f"Bearer {os.environ.get('CONTAINER_API_KEY', '')}"}
    deadline = time.time() + 120
    while True:
        try:
            r = urllib.request.urlopen(urllib.request.Request(url, method="DELETE", headers=headers), timeout=20)
            say(f"DELETE HTTP {r.status} {r.read().decode()[:200]}")
            return
        except urllib.error.HTTPError as e:
            say(f"DELETE HTTP {e.code} {e.read().decode()[:200]}")
            # The API answered. Retry only on rate limit and server errors.
            if e.code != 429 and e.code < 500:
                return
        except Exception as e:
            say(f"DELETE no answer {e!r}")
        if time.time() > deadline:
            say("DELETE gave up after 120 s")
            return
        time.sleep(10)


try:
    say(f"start pid={os.getpid()} ppid={os.getppid()} uid={os.getuid()} instance={os.environ['CONTAINER_ID']} "
        f"key_present={bool(os.environ.get('CONTAINER_API_KEY'))} check={CHECK_EVERY_S}s grace={GRACE_S}s "
        f"max_age={MAX_AGE_S}s")
    while True:
        now = time.time()
        if os.path.exists(HEARTBEAT):
            age = now - os.stat(HEARTBEAT).st_mtime
            say(f"check heartbeat_age={age:.0f}s")
            if age > MAX_AGE_S:
                destroy(f"heartbeat age {age:.0f} s > {MAX_AGE_S} s")
        else:
            say(f"check heartbeat=missing since_start={now - started:.0f}s")
            if now - started > GRACE_S:
                destroy(f"no heartbeat file {now - started:.0f} s after start")
        time.sleep(CHECK_EVERY_S)
except BaseException as e:
    say(f"unexpected exception {e!r}")
    destroy("unexpected exception")
    sys.exit(1)

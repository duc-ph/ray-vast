for p in python3 python /home/ray/anaconda3/bin/python /opt/conda/bin/python /usr/local/bin/python3 /usr/bin/python3; do
  y=$(command -v $p) && $y -c "import ray" 2>/dev/null && break; y=
done
[ "$y" ] || { echo "rayvast: no Python with ray; the head will destroy this instance" >&2; sleep 999999999; }
exec $y -c '
import hashlib as h, os, ssl, sys, time, threading as t, urllib.request as u
e = os.environ
c = ssl._create_unverified_context()
r = []
def get():
    try: r.append(u.urlopen("https://%s:%s/bootstrap" % (e["RAYVAST_HEAD_IP"], e["RAYVAST_PORT_A"]), context=c, timeout=30).read())
    except Exception: pass
n = time.time()
while time.time() - n < 600:
    x = t.Thread(target=get, daemon=True); x.start(); x.join(30)
    if r: break
    time.sleep(10)
if r and h.sha256(r[0]).hexdigest() == e["RAYVAST_BOOTSTRAP_SHA256"]:
    os.umask(63)
    open("/tmp/rayvast-bootstrap.py", "wb").write(r[0])
    os.execv(sys.executable, [sys.executable, "/tmp/rayvast-bootstrap.py"])
class N(u.HTTPRedirectHandler):
    redirect_request = lambda *a: None
while 1:
    try:
        u.build_opener(N).open(u.Request("%s/instances/%s/" % (e.get("RAYVAST_VAST_API", "https://console.vast.ai/api/v0"), e["CONTAINER_ID"]), method="DELETE", headers={"Authorization": "Bearer " + e["CONTAINER_API_KEY"]}), timeout=30)
        break
    except u.HTTPError as x:
        if x.code == 404: break
    except Exception: pass
    time.sleep(5)
time.sleep(999999999)'

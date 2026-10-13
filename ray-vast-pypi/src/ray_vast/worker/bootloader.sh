for p in python3 python /home/ray/anaconda3/bin/python /opt/conda/bin/python /usr/local/bin/python3 /usr/bin/python3; do
  y=$(command -v $p) && $y -c "import ray" 2>/dev/null && break; y=
done
[ "$y" ] || { echo "rayvast: no Python with ray; the head will destroy this instance" >&2; sleep 999999999; }
exec $y -c '
import hashlib as h, os, ssl, sys, urllib.request as u
e = os.environ
c = ssl._create_unverified_context()
b = u.urlopen("https://%s:%s/bootstrap" % (e["RAYVAST_HEAD_IP"], e["RAYVAST_PORT_A"]), context=c, timeout=30).read()
if h.sha256(b).hexdigest() != e["RAYVAST_BOOTSTRAP_SHA256"]: sys.exit("rayvast: bootstrap SHA-256 mismatch")
os.umask(63)
open("/tmp/rayvast-bootstrap.py", "wb").write(b)
os.execv(sys.executable, [sys.executable, "/tmp/rayvast-bootstrap.py"])'

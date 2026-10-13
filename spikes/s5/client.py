import hashlib, json, os, ssl, sys, tempfile, urllib.request

def call(url, ctx, data=None):
    req = urllib.request.Request(url, data=data, method="POST" if data is not None else "GET")
    with urllib.request.build_opener(urllib.request.HTTPSHandler(context=ctx)).open(req, timeout=10) as r:
        return r.read()

e = os.environ
base = f"https://{e['HEAD']}:{e['PLAIN_PORT']}"
mtls = f"https://{e['HEAD']}:{e['MTLS_PORT']}"
unverified = ssl.create_default_context()
unverified.check_hostname = False
unverified.verify_mode = ssl.CERT_NONE
ca_pem = call(base + "/ca.pem", unverified)
if hashlib.sha256(ssl.PEM_cert_to_DER_cert(ca_pem.decode())).hexdigest() != e["CA_SHA256"]:
    sys.exit("FAIL fingerprint mismatch")
ca_file = os.path.join(tempfile.mkdtemp(), "ca.pem")
open(ca_file, "wb").write(ca_pem)
ctx = ssl.create_default_context(cafile=ca_file)
print("enroll", call(base + "/enroll", ctx, b"{}"))
if e.get("NODE_CRT"):
    ctx.load_cert_chain(e["NODE_CRT"], e["NODE_KEY"])
print("heartbeat", call(mtls + "/heartbeat", ctx, b"{}"))

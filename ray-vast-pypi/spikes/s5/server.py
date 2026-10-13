# Test head: 18000 plain TLS, 18001 mTLS required, 18002 rogue-CA cert serving REAL ca.pem,
# 18003 head cert without the IP in SAN, 18004 rogue cert serving ROGUE ca.pem.
import http.server, ssl, threading, os, json
P = os.path.expanduser("~/s5/pki/")
def serve(port, crt, key, ca_file, client_ca=None):
    class H(http.server.BaseHTTPRequestHandler):
        def log_message(self, *a): pass
        def do_GET(self):
            if self.path == "/ca.pem":
                body = open(P + ca_file, "rb").read()
                self.send_response(200); self.end_headers(); self.wfile.write(body)
            else: self.send_error(404)
        def do_POST(self):
            self.rfile.read(int(self.headers.get("Content-Length", 0)))
            if self.path == "/enroll": out = {"ok": "enroll"}
            elif self.path == "/heartbeat" and client_ca:
                out = {"ok": "heartbeat", "peer": self.connection.getpeercert()["subject"][0][0][1]}
            else: return self.send_error(404)
            body = json.dumps(out).encode()
            self.send_response(200); self.end_headers(); self.wfile.write(body)
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(P + crt, P + key)
    if client_ca:
        ctx.verify_mode = ssl.CERT_REQUIRED
        ctx.load_verify_locations(P + client_ca)
    srv = http.server.ThreadingHTTPServer(("0.0.0.0", port), H)
    srv.socket = ctx.wrap_socket(srv.socket, server_side=True)
    srv.serve_forever()
for args in [(18000,"head.crt","head.key","ca.pem"), (18001,"head.crt","head.key","ca.pem","ca.pem"),
             (18002,"roguehead.crt","roguehead.key","ca.pem"), (18003,"nosan.crt","nosan.key","ca.pem"),
             (18004,"roguehead.crt","roguehead.key","rogue.pem")]:
    threading.Thread(target=serve, args=args, daemon=True).start()
threading.Event().wait()

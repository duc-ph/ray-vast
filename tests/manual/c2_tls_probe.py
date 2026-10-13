# C2.3 step 2: TLS 1.3 client-cert check. openssl s_client shows "New, TLSv1.3" even when the server
# will reject the client cert, because in TLS 1.3 the server checks the cert after the client finishes.
# So we send the HTTP/2 preface and a SETTINGS frame after the handshake and read the server's answer.
# Usage: python3 c2_tls_probe.py <ip> <port> <ca.crt> [<client.crt> <client.key>]  (label printed)
import socket
import ssl
import sys

ip, port, ca = sys.argv[1], int(sys.argv[2]), sys.argv[3]
ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE
ctx.set_alpn_protocols(["h2"])
if len(sys.argv) > 5:
    ctx.load_cert_chain(sys.argv[4], sys.argv[5])
try:
    s = ctx.wrap_socket(socket.create_connection((ip, port), timeout=10))
    s.sendall(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n" + b"\x00\x00\x00\x04\x00\x00\x00\x00\x00")
    data = s.recv(64)
    print(f"handshake ok ({s.version()}, alpn={s.selected_alpn_protocol()}); server answered {len(data)} bytes; "
          f"first frame type={data[3] if len(data) > 3 else None} (4=SETTINGS => accepted)")
except Exception as e:
    print(f"rejected: {e!r}")

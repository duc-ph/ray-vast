"""A stand-in for `ray.scripts.scripts` (put its parent dir on PYTHONPATH). `main()` plays `ray start --block`.

It writes its argv and a few env values to $FAKE_RAY_OUT, serves HTTP on the two 127.0.0.1 agent ports
(RAYVAST_LOCALHOST_PORTS) with the statuses $FAKE_RAY_AGENT_STATUS (comma list, one per request, the last one repeats; default 401), and then:
sleeps forever, or exits with $FAKE_RAY_EXIT_CODE after $FAKE_RAY_EXIT_AFTER seconds when that is set.
"""
import http.server
import json
import os
import sys
import threading
import time


def main():
    env = os.environ
    with open(env["FAKE_RAY_OUT"], "w") as f:
        json.dump({"argv": sys.argv[1:], "env": dict(env)}, f)

    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        count = 0

        def do_GET(self):
            statuses = env.get("FAKE_RAY_AGENT_STATUS", "401").split(",")
            self.send_response(int(statuses[min(Handler.count, len(statuses) - 1)]))
            Handler.count += 1
            self.send_header("Content-Length", "0")
            self.end_headers()

    for port in env["RAYVAST_LOCALHOST_PORTS"].split(","):
        server = http.server.ThreadingHTTPServer(("127.0.0.1", int(port)), Handler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
    if "FAKE_RAY_EXIT_AFTER" in env:
        time.sleep(float(env["FAKE_RAY_EXIT_AFTER"]))
        sys.exit(int(env.get("FAKE_RAY_EXIT_CODE", "1")))
    while True:
        time.sleep(3600)

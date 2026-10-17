"""A stand-in for `ray.scripts.scripts` (put its parent dir on PYTHONPATH). `main()` plays `ray start --block`.

It writes its argv and a few env values to $FAKE_RAY_OUT, serves HTTP on the two 127.0.0.1 agent ports
(RAYVAST_LOCALHOST_PORTS) with the statuses $FAKE_RAY_AGENT_STATUS (comma list, one per request, the last one repeats; default 401), listens on
the first $FAKE_RAY_LISTEN_WORKER_PORTS ports of --worker-port-list (worker processes in use; the first one on
IPv6; default 0), and then:
sleeps until its parent is gone, or exits with $FAKE_RAY_EXIT_CODE after $FAKE_RAY_EXIT_AFTER seconds when that is set.
"""
import http.server
import json
import os
import socket
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
    worker_ports = next(a for a in sys.argv if a.startswith("--worker-port-list=")).split("=")[1].split(",")
    listeners = []
    for i, port in enumerate(worker_ports[:int(env.get("FAKE_RAY_LISTEN_WORKER_PORTS", "0"))]):
        listener = socket.socket(socket.AF_INET6 if i == 0 else socket.AF_INET)  # the first one on IPv6 (tcp6)
        listener.bind(("::" if i == 0 else "0.0.0.0", int(port)))
        listener.listen(1)
        listeners.append(listener)
    # Outbound connections whose local port is a worker port (the identity ports are in the kernel's ephemeral range):
    # $FAKE_RAY_CONNECT_FROM_WORKER_PORTS stay open (ESTABLISHED), then $FAKE_RAY_TIME_WAIT_WORKER_PORTS are closed at
    # once by this side (TIME_WAIT). Both go to the first agent port. They take the ports after the listeners.
    agent = ("127.0.0.1", int(env["RAYVAST_LOCALHOST_PORTS"].split(",")[0]))
    rest = worker_ports[len(listeners):]
    connected = int(env.get("FAKE_RAY_CONNECT_FROM_WORKER_PORTS", "0"))
    for i, port in enumerate(rest[:connected + int(env.get("FAKE_RAY_TIME_WAIT_WORKER_PORTS", "0"))]):
        client = socket.socket()
        client.bind(("127.0.0.1", int(port)))
        client.connect(agent)
        if i < connected:
            listeners.append(client)
        else:
            client.close()
    if "FAKE_RAY_EXIT_AFTER" in env:
        time.sleep(float(env["FAKE_RAY_EXIT_AFTER"]))
        sys.exit(int(env.get("FAKE_RAY_EXIT_CODE", "1")))
    parent = os.getppid()
    while os.getppid() == parent:  # stand-in for a real `ray start --block`: never outlive the bootstrap
        time.sleep(1)

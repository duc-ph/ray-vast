"""A fake vast API for the worker tests: GET/DELETE /instances/<id>/ with a bearer key. Records every call."""
import http.server
import threading
import time


class FakeVast:
    def __init__(self, port, expected_api_key, listen_host="0.0.0.0"):
        self.expected_api_key = expected_api_key
        self.destroys = []  # (monotonic time, instance id)
        self.delete_statuses = []  # statuses to answer to the next deletes before 200 (for retry tests)
        self.bad_auth = 0
        vast = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def _answer(self, deleting):
                if self.headers.get("Authorization") != f"Bearer {vast.expected_api_key}":
                    vast.bad_auth += 1
                    status = 401
                elif deleting and vast.delete_statuses:
                    status = vast.delete_statuses.pop(0)
                else:
                    status = 200
                    if deleting:
                        vast.destroys.append((time.monotonic(), int(self.path.strip("/").split("/")[-1])))
                self.send_response(status)
                self.send_header("Content-Length", "2")
                self.end_headers()
                self.wfile.write(b"{}")

            def do_GET(self):
                self._answer(deleting=False)

            def do_DELETE(self):
                self._answer(deleting=True)

        self._server = http.server.ThreadingHTTPServer((listen_host, port), Handler)
        self._server.daemon_threads = True

    def start(self):
        threading.Thread(target=self._server.serve_forever, daemon=True).start()

    def stop(self):
        self._server.shutdown()
        self._server.server_close()

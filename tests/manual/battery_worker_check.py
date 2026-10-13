# C2.3 step 5 (worker side): the job port on 127.0.0.1 without a token must give 401.
# Usage: python battery_worker_check.py <port> [<port> ...]
import sys
import urllib.error
import urllib.request

for port in sys.argv[1:]:
    for label, headers in (("no token", {}), ("wrong token", {"Authorization": "Bearer " + "0" * 64})):
        try:
            code = urllib.request.urlopen(urllib.request.Request(f"http://127.0.0.1:{port}/api/jobs/", headers=headers), timeout=5).status
        except urllib.error.HTTPError as e:
            code = e.code
        except OSError as e:
            code = repr(e)
        print(f"127.0.0.1:{port} {label}: {code}")

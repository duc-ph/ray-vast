"""Thin client for the vast.ai REST API (the calls that ray-vast needs)."""
import logging
import random
import re
import time

import requests

log = logging.getLogger(__name__)

BASE_URL = "https://console.vast.ai/api/v0"
IDENTITY_PORTS = range(70000, 70064)
TIMEOUT_S = 30
TRIES = 5
BACKOFF_BASE_S = 1.0
BACKOFF_MAX_S = 8.0
_SECRET_RES = (
    re.compile(r'("[^"]*(?:api_key|token)[^"]*"\s*:\s*)"[^"]*"', re.IGNORECASE),
    re.compile(r"Bearer\s+\S+", re.IGNORECASE),
)


class VastError(Exception):
    """A vast API call failed. Holds no auth header and no instance key."""

    def __init__(self, message, status=None):
        super().__init__(message)
        self.status = status


class VastClient:
    def __init__(self, api_key, base_url=BASE_URL, sleep=time.sleep, jitter=random.random):
        self.base_url = base_url.rstrip("/")
        self._api_key = api_key
        self._sleep = sleep
        self._jitter = jitter
        self._session = requests.Session()

    def _request(self, method, path, json=None, retry_unsafe=True, secrets=()):
        """Return the response, or None for HTTP 404. Retries 429, 5xx and connection errors with backoff.

        `secrets`: literal values to remove from an error body.
        """
        headers = {"Authorization": f"Bearer {self._api_key}"}
        for attempt in range(1, TRIES + 1):
            error = None
            try:
                resp = self._session.request(method, self.base_url + path, headers=headers, json=json, timeout=TIMEOUT_S)
                status = resp.status_code
            except requests.RequestException as e:
                status, error = None, type(e).__name__  # the exception text may hold the URL or headers
            if error and (not retry_unsafe or attempt == TRIES):
                raise VastError(f"{method} {path}: {error}")
            if status is not None:
                if status == 404:
                    return None
                if status < 400:
                    return resp
                if not (status == 429 or (status >= 500 and retry_unsafe)) or attempt == TRIES:
                    body = resp.text
                    for pattern in _SECRET_RES:
                        body = pattern.sub(lambda m: (m.group(1) + '"<redacted>"') if m.lastindex else "Bearer <redacted>", body)
                    for secret in secrets:
                        body = body.replace(secret, "<redacted>")
                    body = body[:300]
                    raise VastError(f"{method} {path}: HTTP {status}: {body}", status=status)
            delay = min(BACKOFF_BASE_S * 2 ** (attempt - 1), BACKOFF_MAX_S) * (1 + self._jitter())
            log.warning("%s %s: HTTP %s, retry %d/%d in %.1f s", method, path, status or "connection error",
                        attempt, TRIES - 1, delay)
            self._sleep(delay)

    def search_offers(self, query, order_by="dph_total", limit=100):
        """Return the offers matching `query` (vast filter dict), cheapest first by default."""
        body = {**query, "order": [[order_by, "asc"]], "type": "on-demand", "limit": limit}
        resp = self._request("POST", "/bundles/", json=body)
        if resp is None:
            raise VastError("POST /bundles/: HTTP 404", status=404)
        return resp.json()["offers"]

    def create_instance(self, offer_id, image, env, args, label, disk_gb):
        """Rent `offer_id` and return the new instance ID. The per-instance key in the response is dropped."""
        body = {
            "client_id": "me",
            "image": image,
            "env": {**{f"-p {p}:{p}": "1" for p in IDENTITY_PORTS}, **env},
            "runtype": "args",
            "args": args,
            "label": label,
            "disk": disk_gb,
        }
        # vast may echo the env in an error, in any form: remove the literal secret values (the join token).
        secrets = [v for k, v in env.items() if v and re.search("token|key", k, re.IGNORECASE)]
        # A 5xx or a lost connection may still have rented the offer, so only 429 retries (no duplicate rentals).
        resp = self._request("PUT", f"/asks/{offer_id}/", json=body, retry_unsafe=False, secrets=secrets)
        if resp is None:
            raise VastError(f"PUT /asks/{offer_id}/: offer not found", status=404)
        result = resp.json()
        result.pop("instance_api_key", None)
        if not result.get("success", True) or "new_contract" not in result:
            reason = {k: result.get(k) for k in ("success", "error", "msg") if k in result}
            message = f"PUT /asks/{offer_id}/: {reason}"
            for secret in secrets:
                message = message.replace(secret, "<redacted>")
            raise VastError(message[:300])
        log.info("rented offer %s as instance %s", offer_id, result["new_contract"])
        return int(result["new_contract"])

    def list_instances(self):
        """Return all instances of the account."""
        resp = self._request("GET", "/instances/")
        if resp is None:
            raise VastError("GET /instances/: HTTP 404", status=404)
        return resp.json()["instances"]

    def destroy_instance(self, instance_id):
        """Destroy the instance. An instance that is already gone counts as success."""
        resp = self._request("DELETE", f"/instances/{instance_id}/")
        if resp is not None and resp.json().get("success") is False:
            raise VastError(f"DELETE /instances/{instance_id}/: success false")

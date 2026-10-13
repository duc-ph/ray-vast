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
_SECRET_RE = re.compile(r'("instance_api_key"\s*:\s*)"[^"]*"')


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

    def _request(self, method, path, key=None, json=None, retry_5xx=True):
        """Return the response, or None for HTTP 404. Retries 429, 5xx and connection errors with backoff."""
        headers = {"Authorization": f"Bearer {key or self._api_key}"}
        for attempt in range(1, TRIES + 1):
            error = None
            try:
                resp = self._session.request(method, self.base_url + path, headers=headers, json=json, timeout=TIMEOUT_S)
                status = resp.status_code
            except requests.RequestException as e:
                status, error = None, type(e).__name__  # the exception text may hold the URL or headers
            if error and (not retry_5xx or attempt == TRIES):
                raise VastError(f"{method} {path}: {error}")
            if status is not None:
                if status == 404:
                    return None
                if status < 400:
                    return resp
                if not (status == 429 or (status >= 500 and retry_5xx)) or attempt == TRIES:
                    body = _SECRET_RE.sub(r'\1"<redacted>"', resp.text[:300])
                    raise VastError(f"{method} {path}: HTTP {status}: {body}", status=status)
            delay = min(BACKOFF_BASE_S * 2 ** (attempt - 1), BACKOFF_MAX_S) * (1 + self._jitter())
            log.warning("%s %s: HTTP %s, retry %d/%d in %.1f s", method, path, status or "connection error",
                        attempt, TRIES - 1, delay)
            self._sleep(delay)

    def search_offers(self, query, order_by="dph_total", limit=20):
        """Return the offers matching `query` (vast filter dict), cheapest first by default."""
        body = {**query, "order": [[order_by, "asc"]], "type": "on-demand", "limit": limit}
        return self._request("POST", "/bundles/", json=body).json()["offers"]

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
        # A 5xx or a lost connection may still have rented the offer, so only 429 retries (no duplicate rentals).
        resp = self._request("PUT", f"/asks/{offer_id}/", json=body, retry_5xx=False)
        if resp is None:
            raise VastError(f"PUT /asks/{offer_id}/: offer not found", status=404)
        result = resp.json()
        result.pop("instance_api_key", None)
        if not result.get("success", True) or "new_contract" not in result:
            raise VastError(f"PUT /asks/{offer_id}/: {_SECRET_RE.sub('', str(result))[:300]}")
        log.info("rented offer %s as instance %s", offer_id, result["new_contract"])
        return int(result["new_contract"])

    def get_instance(self, instance_id, key=None):
        """Return the instance dict, or None when it is gone. `key` is a per-instance key (CONTAINER_API_KEY)."""
        resp = self._request("GET", f"/instances/{instance_id}/", key=key)
        if resp is None:
            return None
        instance = resp.json().get("instances")
        return instance or None

    def list_instances(self):
        """Return all instances of the account."""
        return self._request("GET", "/instances/").json()["instances"]

    def destroy_instance(self, instance_id, key=None):
        """Destroy the instance. An instance that is already gone counts as success."""
        self._request("DELETE", f"/instances/{instance_id}/", key=key)

import json
import logging
import os
import pathlib

import pytest
import requests
import responses

from ray_vast.vast_api import BASE_URL, VastClient, VastError

SECRET = "SECRET123"
CREATE_OK = {"success": True, "new_contract": 48743418, "instance_api_key": SECRET}


@pytest.fixture
def sleeps():
    return []


@pytest.fixture
def client(sleeps):
    return VastClient("ACCOUNTKEY", sleep=sleeps.append, jitter=lambda: 0.0)


@responses.activate
def test_search_offers_sends_query_and_returns_offers(client):
    responses.post(f"{BASE_URL}/bundles/", json={"offers": [{"id": 1}, {"id": 2}]})
    offers = client.search_offers({"num_gpus": {"eq": 0}}, order_by="dph_total", limit=5)
    sent = json.loads(responses.calls[0].request.body)
    assert offers == [{"id": 1}, {"id": 2}]
    assert sent == {"num_gpus": {"eq": 0}, "order": [["dph_total", "asc"]], "type": "on-demand", "limit": 5}
    assert responses.calls[0].request.headers["Authorization"] == "Bearer ACCOUNTKEY"


@responses.activate
def test_create_instance_returns_id_and_adds_identity_ports(client):
    responses.put(f"{BASE_URL}/asks/48743418/", json=CREATE_OK)
    iid = client.create_instance(48743418, "img:1", {"A": "b"}, ["bash", "-c", "x"], "lbl", 20)
    sent = json.loads(responses.calls[0].request.body)
    assert iid == 48743418
    assert sent["client_id"] == "me" and sent["runtype"] == "args" and sent["disk"] == 20
    assert sent["image"] == "img:1" and sent["label"] == "lbl" and sent["args"] == ["bash", "-c", "x"]
    assert sent["env"]["A"] == "b"
    assert [k for k in sent["env"] if k.startswith("-p ")] == [f"-p {p}:{p}" for p in range(70000, 70064)]
    assert all(v == "1" for k, v in sent["env"].items() if k.startswith("-p "))


@responses.activate
def test_get_instance_returns_dict_with_optional_key_and_none_when_gone(client):
    responses.get(f"{BASE_URL}/instances/7/", json={"instances": {"id": 7, "actual_status": "running"}})
    responses.get(f"{BASE_URL}/instances/8/", status=404, json={"error": "not found"})
    responses.get(f"{BASE_URL}/instances/9/", json={"instances": None})
    assert client.get_instance(7, key="CONTAINERKEY")["actual_status"] == "running"
    assert responses.calls[0].request.headers["Authorization"] == "Bearer CONTAINERKEY"
    assert client.get_instance(8) is None
    assert client.get_instance(9) is None


@responses.activate
def test_list_instances_returns_all(client):
    responses.get(f"{BASE_URL}/instances/", json={"instances": [{"id": 1, "label": "a"}, {"id": 2, "label": "b"}]})
    assert [i["id"] for i in client.list_instances()] == [1, 2]


@responses.activate
def test_destroy_instance_success_and_already_gone(client):
    responses.delete(f"{BASE_URL}/instances/1/", json={"success": True})
    responses.delete(f"{BASE_URL}/instances/2/", status=404, json={"error": "gone"})
    client.destroy_instance(1, key="CONTAINERKEY")
    client.destroy_instance(2)
    assert responses.calls[0].request.headers["Authorization"] == "Bearer CONTAINERKEY"


@responses.activate
def test_429_and_5xx_retry_with_growing_backoff(client, sleeps):
    responses.get(f"{BASE_URL}/instances/", status=429)
    responses.get(f"{BASE_URL}/instances/", status=503)
    responses.get(f"{BASE_URL}/instances/", body=requests.ConnectionError("boom"))
    responses.get(f"{BASE_URL}/instances/", json={"instances": []})
    assert client.list_instances() == []
    assert sleeps == [1.0, 2.0, 4.0]


@responses.activate
def test_retries_are_bounded_and_error_has_status(client, sleeps):
    responses.get(f"{BASE_URL}/instances/", status=500, body="down")
    with pytest.raises(VastError) as e:
        client.list_instances()
    assert e.value.status == 500
    assert len(responses.calls) == 5
    assert sum(sleeps) <= 30


@responses.activate
def test_4xx_does_not_retry(client, sleeps):
    responses.get(f"{BASE_URL}/instances/", status=401, json={"error": "bad key"})
    with pytest.raises(VastError) as e:
        client.list_instances()
    assert e.value.status == 401 and sleeps == []


@responses.activate
def test_create_retries_429_but_not_5xx(client, sleeps):
    responses.put(f"{BASE_URL}/asks/1/", status=429)
    responses.put(f"{BASE_URL}/asks/1/", json=CREATE_OK)
    assert client.create_instance(1, "img", {}, [], "l", 20) == 48743418
    assert len(sleeps) == 1
    responses.put(f"{BASE_URL}/asks/2/", status=502)
    with pytest.raises(VastError):
        client.create_instance(2, "img", {}, [], "l", 20)
    assert len(responses.calls) == 3


@responses.activate
def test_secrets_never_leak(client, caplog):
    caplog.set_level(logging.DEBUG)
    texts = []
    responses.put(f"{BASE_URL}/asks/1/", json=CREATE_OK)
    responses.put(f"{BASE_URL}/asks/2/", status=400, json={"success": False, "instance_api_key": SECRET, "msg": "x"})
    responses.put(f"{BASE_URL}/asks/3/", json={"success": False, "instance_api_key": SECRET, "error": "gone"})
    responses.put(f"{BASE_URL}/asks/4/", status=500, body=f'{{"instance_api_key": "{SECRET}"}}')
    responses.put(f"{BASE_URL}/asks/5/", body=requests.ConnectionError(f"Bearer ACCOUNTKEY {SECRET}"))
    texts.append(str(client.create_instance(1, "img", {}, [], "l", 20)))
    for offer in (2, 3, 4, 5):
        with pytest.raises(VastError) as e:
            client.create_instance(offer, "img", {}, [], "l", 20)
        texts += [str(e.value), repr(e.value), str(e.value.__cause__), str(e.value.__context__)]
    everything = "\n".join(texts) + caplog.text
    assert SECRET not in everything
    assert "ACCOUNTKEY" not in everything


LIVE_KEY = pathlib.Path("~/.config/vastai/vast_api_key").expanduser()


@pytest.mark.live
def test_live_search_offers_returns_offers():
    key = os.environ.get("VAST_API_KEY") or LIVE_KEY.read_text().strip()
    offers = VastClient(key).search_offers({"num_gpus": {"eq": 0}, "rentable": {"eq": True}}, limit=3)
    assert offers and "dph_total" in offers[0]

import json
import logging
import os
import pathlib

import pytest
import requests
import responses

import ray_vast

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
    responses.get(f"{BASE_URL}/instances/10/", json={"instances": {}})
    assert client.get_instance(10) is None


@responses.activate
@pytest.mark.parametrize("bad_key", ["", "  ", "\n", 5])
def test_empty_or_blank_instance_key_raises_without_http_call(client, bad_key):
    with pytest.raises(ValueError):
        client.get_instance(7, key=bad_key)
    with pytest.raises(ValueError):
        client.destroy_instance(7, key=bad_key)
    assert len(responses.calls) == 0


@responses.activate
def test_instance_key_is_sent_never_the_account_key(client):
    responses.get(f"{BASE_URL}/instances/7/", json={"instances": {"id": 7}})
    responses.delete(f"{BASE_URL}/instances/7/", json={"success": True})
    client.get_instance(7, key="abc")
    client.destroy_instance(7, key="abc")
    assert [c.request.headers["Authorization"] for c in responses.calls] == ["Bearer abc", "Bearer abc"]


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


@responses.activate
def test_error_body_is_redacted_before_truncation(client):
    body = json.dumps({"pad": "x" * 265, "instance_api_key": SECRET, "Auth_Token": "TOK999", "note": "Bearer ACCOUNTKEY"})
    assert body.index(SECRET) < 300 < body.index(SECRET) + len(SECRET)  # the secret straddles char 300
    responses.get(f"{BASE_URL}/instances/", status=400, body=body)
    with pytest.raises(VastError) as e:
        client.list_instances()
    assert SECRET not in str(e.value) and "TOK999" not in str(e.value) and "ACCOUNTKEY" not in str(e.value)
    # truncating first would leave the front part of the secret in the message
    assert body[body.index(SECRET):300] not in str(e.value)
    assert "xxxx" in str(e.value)


@responses.activate
def test_create_failure_message_has_no_secret_fields(client):
    responses.put(f"{BASE_URL}/asks/1/", json={"success": False, "error": "no_such_ask", "instance_api_key": SECRET})
    with pytest.raises(VastError) as e:
        client.create_instance(1, "img", {}, [], "l", 20)
    assert "no_such_ask" in str(e.value) and SECRET not in str(e.value)


@responses.activate
def test_search_and_list_raise_on_404(client):
    responses.post(f"{BASE_URL}/bundles/", status=404)
    responses.get(f"{BASE_URL}/instances/", status=404)
    with pytest.raises(VastError):
        client.search_offers({})
    with pytest.raises(VastError):
        client.list_instances()


@responses.activate
def test_destroy_with_success_false_raises(client):
    responses.delete(f"{BASE_URL}/instances/1/", json={"success": False, "msg": "busy"})
    with pytest.raises(VastError):
        client.destroy_instance(1)


@responses.activate
def test_every_call_has_a_timeout(client):
    responses.post(f"{BASE_URL}/bundles/", json={"offers": []})
    responses.put(f"{BASE_URL}/asks/1/", json=CREATE_OK)
    responses.get(f"{BASE_URL}/instances/", json={"instances": []})
    responses.get(f"{BASE_URL}/instances/1/", json={"instances": {"id": 1}})
    responses.delete(f"{BASE_URL}/instances/1/", json={"success": True})
    client.search_offers({})
    client.create_instance(1, "img", {}, [], "l", 20)
    client.list_instances()
    client.get_instance(1)
    client.destroy_instance(1)
    assert len(responses.calls) == 5
    assert [c.request.req_kwargs["timeout"] for c in responses.calls] == [30] * 5


FIXTURES = pathlib.Path(__file__).resolve().parents[1] / "fixtures"


@responses.activate
def test_real_archived_shapes(client):
    offer = json.loads((FIXTURES / "vast_offer.json").read_text())
    poll = json.loads((FIXTURES / "vast_instance_poll.json").read_text())
    # The poll output keeps only some fields; public_ipaddr and ports (keyed "<port>/tcp") follow tests/manual/c4_rent.py.
    instance = {"id": 55448911, "machine_id": offer["machine_id"], "public_ipaddr": "85.10.218.46\n",
                "ports": {"70040/tcp": [{"HostIp": "0.0.0.0", "HostPort": "70040"}]}, **poll}
    responses.post(f"{BASE_URL}/bundles/", json={"offers": [offer]})
    responses.get(f"{BASE_URL}/instances/55448911/", json={"instances": instance})
    got_offer = client.search_offers({})[0]
    got = client.get_instance(55448911)
    assert (got_offer["id"], got_offer["machine_id"], got_offer["dph_total"]) == (48743418, 3969, 0.09703703703703705)
    assert got["actual_status"] == "running" and got["dph_total"] == pytest.approx(0.1025925925925926)
    assert got["machine_id"] == 3969 and got["public_ipaddr"].strip() == "85.10.218.46" and "70040/tcp" in got["ports"]


LIVE_KEY = pathlib.Path("~/.config/vastai/vast_api_key").expanduser()


@pytest.mark.live
def test_live_search_offers_returns_offers():
    key = os.environ.get("VAST_API_KEY") or LIVE_KEY.read_text().strip()
    offers = VastClient(key).search_offers({"num_gpus": {"eq": 0}, "rentable": {"eq": True}}, limit=3)
    assert offers and "dph_total" in offers[0]


@pytest.mark.live
def test_live_search_with_gpu_24gb_example_query():
    import yaml

    from ray_vast.config import build_offer_query, load_and_bootstrap

    config, _ = load_and_bootstrap(pathlib.Path(ray_vast.__file__).parent / "examples" / "gpu-24gb.yaml")
    query = build_offer_query(config["available_node_types"]["gpu24"], config["provider"]["allow_non_datacenter"], "gpu24")
    key = os.environ.get("VAST_API_KEY") or LIVE_KEY.read_text().strip()
    offers = VastClient(key).search_offers(query)  # raises on HTTP 400 if the body (type: on-demand) is not accepted
    print(len(offers), "offers for", query)
    assert isinstance(offers, list)


@responses.activate
def test_backoff_delay_includes_jitter(sleeps):
    jittery = VastClient("K", sleep=sleeps.append, jitter=lambda: 0.5)
    responses.get(f"{BASE_URL}/instances/", status=429)
    responses.get(f"{BASE_URL}/instances/", status=429)
    responses.get(f"{BASE_URL}/instances/", json={"instances": []})
    jittery.list_instances()
    assert sleeps == [1.5, 3.0]


@responses.activate
def test_create_does_not_retry_a_lost_connection(client, sleeps):
    responses.put(f"{BASE_URL}/asks/1/", body=requests.ConnectionError("boom"))
    responses.put(f"{BASE_URL}/asks/1/", json=CREATE_OK)
    with pytest.raises(VastError):
        client.create_instance(1, "img", {}, [], "l", 20)
    assert len(responses.calls) == 1 and sleeps == []


@responses.activate
def test_create_with_success_false_raises_even_when_it_returns_a_contract(client):
    responses.put(f"{BASE_URL}/asks/1/", json={"success": False, "new_contract": 5})
    with pytest.raises(VastError):
        client.create_instance(1, "img", {}, [], "l", 20)


@responses.activate
def test_create_on_missing_offer_raises_404(client):
    responses.put(f"{BASE_URL}/asks/1/", status=404)
    with pytest.raises(VastError) as e:
        client.create_instance(1, "img", {}, [], "l", 20)
    assert e.value.status == 404


@responses.activate
def test_bearer_token_in_a_short_error_body_is_redacted(client):
    responses.get(f"{BASE_URL}/instances/", status=400, body='{"detail": "bad Bearer TOPSECRET99 sent"}')
    with pytest.raises(VastError) as e:
        client.list_instances()
    assert "TOPSECRET99" not in str(e.value)


@responses.activate
def test_base_url_with_trailing_slash_is_accepted(sleeps):
    slashed = VastClient("K", base_url=BASE_URL + "/", sleep=sleeps.append)
    responses.get(f"{BASE_URL}/instances/", json={"instances": []})
    assert slashed.list_instances() == []

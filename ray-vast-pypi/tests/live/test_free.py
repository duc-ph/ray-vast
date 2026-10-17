"""Group `free` (docs/test-plan.md, claim matrix F01-F20): no cluster and no rental. Unit, integration and local Docker
runs, the package build, and the public vast offer search. Each scenario runs the existing tests that prove its claims
in a subprocess and keeps their output (JUnit XML and the text) as evidence. Run on the dev box (Docker): about 80 min."""
import hashlib
import json
import os
import re
import subprocess
import sys
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path

import pytest
import yaml

from livekit import check
from cluster import BIN, MANUAL, vast

PKG = Path(__file__).resolve().parents[2]
PY = sys.executable
SHIM = PKG / "src" / "ray_vast" / "shim" / "librayvast_shim.so"
SHIM_SHA256 = "79055dea"  # first hex of the committed shim (test-results.md, step 10c)
RAY_IMAGE = "rayproject/ray:2.59.0-py311-cpu"
FULL = ["-o", "addopts=", "-p", "no:cacheprovider", "-m", "not live and not local_e2e"]
pytestmark = pytest.mark.free


def run_pytest(record, name, args, timeout=3600, extra_env=None):
    """Run pytest in the package; keep the output and the JUnit XML. Returns (exit code, {test name: outcome})."""
    xml = record.dir / f"{name}.xml"
    env = {k: v for k, v in os.environ.items() if not k.startswith(("PYTEST_XDIST", "RAYVAST_LIVE_"))}  # not a worker of this run
    done = subprocess.run([PY, "-m", "pytest", *args, f"--junitxml={xml}", "-q"], cwd=PKG, capture_output=True, text=True,
                          timeout=timeout, env={**env, "PYTHONPATH": str(PKG / "src"), **(extra_env or {})})
    record.evidence(f"{name}.txt", done.stdout + done.stderr)
    outcomes = {}
    if xml.exists():
        for case in ET.parse(xml).iter("testcase"):
            kind = next((c.tag for c in case if c.tag in ("failure", "error", "skipped")), "passed")
            outcomes[case.get("name")] = kind
    return done.returncode, outcomes


def summary(outcomes):
    counts = {}
    for kind in outcomes.values():
        counts[kind] = counts.get(kind, 0) + 1
    return counts


def all_pass(record, label, code, outcomes, must_include=()):
    check(record, f"{label}: exit 0", code == 0, summary(outcomes))
    check(record, f"{label}: no failure or error", not [n for n, k in outcomes.items() if k in ("failure", "error")], summary(outcomes))
    for part in must_include:
        names = [n for n, k in outcomes.items() if part in n and k == "passed"]
        check(record, f"{label}: a passing test {part!r}", names, names[:3])


def docker(*args, timeout=1800):
    return subprocess.run(["docker", *args], capture_output=True, text=True, timeout=timeout)


# --- F01-F07: unit and integration ----------------------------------------------------------------------------------


def test_F01_shim_rules_and_mutants(record):
    digest = hashlib.sha256(SHIM.read_bytes()).hexdigest()
    record.data("shim_sha256", digest)
    check(record, "committed shim is the step 10c build", digest.startswith(SHIM_SHA256), digest)
    code, outcomes = run_pytest(record, "test_shim", ["-o", "addopts=", "-p", "no:cacheprovider", "tests/integration/test_shim.py"])
    all_pass(record, "shim tests", code, outcomes)
    check(record, "14 shim tests", len(outcomes) == 14, len(outcomes))
    done = subprocess.run([PY, "scripts/shim_mutants.py"], cwd=PKG, capture_output=True, text=True, timeout=1800)
    record.evidence("shim_mutants.txt", done.stdout + done.stderr)
    survivors = set(re.findall(r"^SURVIVED (\S+)", done.stdout, re.M))
    check(record, "mutation run: the unchanged source builds the committed .so", done.returncode == 0, done.stdout[-300:])
    check(record, "mutation run: only the 2 equivalent mutants survive",
          survivors <= {"errno-save-removed-in-bind", "errno-save-removed-in-init"}, done.stdout.strip().splitlines()[-1])


def test_F02_ray_node_with_a_fake_public_ip_and_the_shim(record):
    """C0.2: a Ray head in a container advertises 203.0.113.10, an IP that it does not own. With the shim: no Errno 99,
    the node is ALIVE, and a job through the shimmed job port succeeds. Without it (control): Errno 99 or no node."""
    ports = ("--port=6379 --dashboard-host=127.0.0.1 --node-manager-port=6380 --object-manager-port=6381 --metrics-export-port=6382 "
             "--dashboard-agent-grpc-port=6383 --dashboard-agent-listen-port=6384 --runtime-env-agent-port=6385 "
             "--min-worker-port=10002 --max-worker-port=10100")
    script = f"""
set -u
PRIV=$(hostname -i | awk '{{print $1}}')
export RAY_enable_autoscaler_v2=1
if [ "$MODE" = shim ]; then
  export LD_PRELOAD=/shim/librayvast_shim.so RAYVAST_PUBLIC_IP=203.0.113.10 RAYVAST_PRIVATE_IP=$PRIV RAYVAST_LOCALHOST_PORTS=6384,6385
fi
timeout 120 ray start --head --node-ip-address=203.0.113.10 --num-cpus=1 {ports} > /tmp/start.log 2>&1; echo "ray start exit $?"
sleep 30
ray list nodes --format json 2>/dev/null | python -c 'import json,sys; print("NODES", json.dumps([n["state"] for n in json.load(sys.stdin)]))' || echo "NODES none"
timeout 180 ray job submit --address http://127.0.0.1:8265 -- python -c 'import ray; ray.init(); print("RESULT", ray.get(ray.remote(lambda: 7).remote()))' 2>&1 | grep -E "RESULT|succeeded|failed" 
echo "ERRNO99 $(grep -l 'Errno 99' /tmp/ray/session_latest/logs/* 2>/dev/null | wc -l)"
"""
    out = {}
    for mode in ("shim", "control"):
        done = docker("run", "--rm", "-e", f"MODE={mode}", "-v", f"{SHIM.parent}:/shim:ro", RAY_IMAGE, "bash", "-c", script, timeout=600)
        out[mode] = done.stdout + done.stderr
        record.evidence(f"{mode}.txt", out[mode])
    check(record, "shim: node ALIVE", 'NODES ["ALIVE"]' in out["shim"], re.findall(r"NODES .*", out["shim"]))
    check(record, "shim: job ran a task", "RESULT 7" in out["shim"], "")
    check(record, "shim: no Errno 99", "ERRNO99 0" in out["shim"], re.findall(r"ERRNO99 \d+", out["shim"]))
    control_broken = "ERRNO99 0" not in out["control"] or "RESULT 7" not in out["control"]
    check(record, "control without the shim: Errno 99 or no job", control_broken, re.findall(r"(ray start exit \d+|ERRNO99 \d+|NODES .*)", out["control"]))


def test_F03_config_and_cli_checks(record, tmp_path):
    code, outcomes = run_pytest(record, "unit_config_cli", [*FULL, "tests/unit/test_config.py", "tests/unit/test_cli.py"])
    all_pass(record, "config and CLI unit tests", code, outcomes,
             must_include=["0_1_0", "old_provider_region", "without_the_generated_key", "existing_cluster", "unknown_profile"])
    base = yaml.safe_load((PKG / "src/ray_vast/examples/gpu-24gb.yaml").read_text())
    env = {**os.environ, "HOME": str(tmp_path), "AWS_PROFILE": "none-such", "VAST_API_KEY": "x"}
    for field, value in {"region": "us-west-2", "head_instance_type": "t3.large", "port_a": 1, "port_b": 2, "cert_max_age_days": 30,
                         "image_size_gb": 20, "key_name": "k", "key_file": "f", "order_by": "dph_total"}.items():
        config = json.loads(json.dumps(base))
        config["provider"][field] = value
        path = tmp_path / f"{field}.yaml"
        path.write_text(yaml.safe_dump(config))
        done = subprocess.run([str(BIN / "ray-vast"), "up", str(path)], env=env, capture_output=True, text=True, timeout=120)
        record.evidence(f"old_field_{field}.txt", done.stdout + done.stderr)
        check(record, f"up rejects provider.{field} with what to do", done.returncode == 1 and
              f"provider.{field} is no longer a field (" in done.stderr, done.stderr.strip()[-200:])
    for flag in ("--index-url", "--key-name", "--key-file"):
        done = subprocess.run([str(BIN / "ray-vast"), "up", "x.yaml", flag, "v"], env=env, capture_output=True, text=True, timeout=60)
        check(record, f"up {flag}: exit 2, unrecognized", done.returncode == 2 and "unrecognized arguments" in done.stderr, done.stderr[-200:])
    for command in ([], ["up"], ["down"], ["attach"], ["dashboard"]):
        done = subprocess.run([str(BIN / "ray-vast"), *command, "--help"], capture_output=True, text=True, timeout=60)
        record.evidence(f"help_{'_'.join(command) or 'top'}.txt", done.stdout)
        check(record, f"ray-vast {' '.join(command)} --help", done.returncode == 0 and "usage: ray-vast" in done.stdout, "")


def test_F04_provider_state_logic(record):
    code, outcomes = run_pytest(record, "unit_provider", [*FULL, "tests/unit/test_provider.py", "tests/unit/test_state.py",
                                                         "tests/unit/test_aws_sg.py", "tests/unit/test_vast_api.py",
                                                         "tests/unit/test_provider_ray_adapter.py"])
    all_pass(record, "provider unit tests", code, outcomes, must_include=[
        "missing_from_vast_counts_only_after_the_grace", "one_listing_without_an_old_worker", "heartbeat", "breaker",
        "blocklist", "budget", "all_five_groups_full", "start_up_sweep" if any("start_up_sweep" in n for n in outcomes) else "sweep"])


@pytest.mark.limits
def test_F05_bootstrap_logic(record):
    code, outcomes = run_pytest(record, "unit_bootstrap", [*FULL, "tests/unit/test_bootstrap.py"])
    all_pass(record, "bootstrap tests", code, outcomes, must_include=[
        "cgroup_v1_cpu_quota", "at_most_one_thread_for_each_url", "cannot_start_its_thread", "vast_instance_variables_pid_1_logs_and_waits"])


@pytest.mark.security
def test_F06_head_server_and_pki(record):
    code, outcomes = run_pytest(record, "unit_head_pki", [*FULL, "tests/unit/test_head_server.py", "tests/unit/test_pki.py"])
    all_pass(record, "head server and PKI tests", code, outcomes)
    text = (PKG / "tests/unit/test_head_server.py").read_text()
    keys = re.search(r"\{\s*\"slot_id\".*?\}", text, re.S)
    check(record, "a test asserts the exact enroll answer keys", keys is not None and "ca_pem" not in keys.group(0),
          keys.group(0)[:200] if keys else "no such assertion")


def test_F07_bootloader_in_docker(record):
    code, outcomes = run_pytest(record, "worker_container", ["-o", "addopts=", "-p", "no:cacheprovider", "-m", "not live",
                                                            "tests/integration/test_worker_container.py"], timeout=5400)
    all_pass(record, "bootloader and bootstrap container tests", code, outcomes,
             must_include=["one_changed_byte", "without_ray_python", "exec_entrypoint", "signal_to_pid_1", "zombies"])
    code, outcomes = run_pytest(record, "missing_env", [*FULL, "tests/unit/test_bootstrap.py", "-k", "vast_instance_variables"])
    all_pass(record, "missing CONTAINER_ID or CONTAINER_API_KEY: log and wait", code, outcomes)
    check(record, "both variables tested", len(outcomes) == 2, sorted(outcomes))


# --- F08-F15: the local end-to-end scenarios (step 11), each in its own run (they can run at the same time) ----------

LOCAL_E2E = {
    "F08": "test_scale_up_three_workers_with_k_2_runs_the_battery_then_idle_scale_down_to_zero",
    "F09": "test_two_workers_with_the_same_public_ip_both_join_and_the_idle_one_is_removed",
    "F10": "test_host_with_more_cores_than_declared_runs_with_the_real_core_count",
    "F11": "test_a_candidate_that_never_enrolls_is_ended_and_its_slot_is_asked_for_again",
    "F12": "test_head_silence_makes_each_worker_report_and_destroy_itself",
    "F13": "test_container_restart_is_refused_a_second_enrollment_and_destroys_itself",
    "F14": "test_monitor_crash_stops_the_head_and_every_worker_destroys_itself",
    "F15": "test_three_enrollment_failures_in_a_row_pause_the_node_type_then_it_launches_again",
}


def _local_e2e_test(scenario):
    def test(record):
        case = LOCAL_E2E[scenario]
        code, outcomes = run_pytest(record, "local_e2e", ["-o", "addopts=", "-p", "no:cacheprovider", "-m", "local_e2e",
                                                         f"tests/local_e2e/test_local_e2e.py::{case}"])
        record.data("local_e2e_case", case)
        check(record, f"{case} passed", code == 0 and outcomes.get(case) == "passed", (code, outcomes))
    return test


test_F08_local_e2e_scale_up_battery_scale_down = _local_e2e_test("F08")
test_F09_local_e2e_two_workers_one_public_ip = _local_e2e_test("F09")
test_F10_local_e2e_more_cores_than_declared = _local_e2e_test("F10")
test_F11_local_e2e_candidate_never_enrolls = _local_e2e_test("F11")
test_F12_local_e2e_head_silent = _local_e2e_test("F12")
test_F13_local_e2e_container_restart = _local_e2e_test("F13")
test_F14_local_e2e_monitor_stops = _local_e2e_test("F14")
test_F15_local_e2e_rent_loop_breaker = _local_e2e_test("F15")


@pytest.mark.limits
def test_F16_local_c10_pairs_with_the_real_head_and_bootstrap(record):
    """The C10 local check (tests/manual/c10/test_c10_local_e2e.py): one worker with --pids-limit 1280, 14 ports, B = 10.
    The 5 pair runs run at the same time (xdist; each has its own network namespace)."""
    selected = "core.py-tuned or core.py-many or c10_data.py-many-560 or serve.py-full or core.py-full"
    code, outcomes = run_pytest(record, "c10_local_e2e", ["-o", "addopts=", "-p", "no:cacheprovider", "-s", "-n", "5",
                                                         str(MANUAL / "c10" / "test_c10_local_e2e.py"), "-k", selected], timeout=7200,
                                extra_env={"C10E_OUT": str(record.dir / "c10e")})
    all_pass(record, "C10 local pairs", code, outcomes)
    check(record, "5 pair runs", len(outcomes) == 5, sorted(outcomes))
    text = "".join(p.read_text() for p in sorted((record.dir / "c10e").glob("*")))  # each run's result line, job log and head log
    for line in re.findall(r"C10E_RESULT .*", text):
        record.data(line.split(" ", 4)[1] + " " + line.split(" ", 4)[2] + " " + line.split(" ", 5)[4], line[:400])
    warning = re.search(r"is at its process limit: \d+ refused forks/threads \(\+\d+\), pids \d+ of 1280, worker ports in use \d+ of \d+\. "
                        r"Ray work on this worker can queue, hang or fail\. See https://duc-ph\.github\.io/ray-vast/how-to/troubleshoot/"
                        r"#a-worker-is-at-its-process-limit", text)
    check(record, "the WARNING line has the documented format", warning is not None, warning.group(0)[:200] if warning else "")


# --- F17-F20: package, Python 3.10, offers, doc snippets -----------------------------------------------------------


@pytest.fixture(scope="module")
def dist(tmp_path_factory):
    out = tmp_path_factory.mktemp("dist")
    done = subprocess.run([PY, "-m", "build", "--outdir", str(out), str(PKG)], capture_output=True, text=True, timeout=900)
    assert done.returncode == 0, done.stderr[-1000:]
    return out


@pytest.mark.box_disk
@pytest.mark.release
def test_F17_wheel_and_sdist(record, dist):
    wheel, sdist = next(dist.glob("*.whl")), next(dist.glob("*.tar.gz"))
    done = subprocess.run([str(BIN / "twine"), "check", "--strict", str(wheel), str(sdist)], capture_output=True, text=True, timeout=300)
    record.evidence("twine_check.txt", done.stdout + done.stderr)
    check(record, "twine check --strict", done.returncode == 0, done.stdout.strip().splitlines()[-1:])
    names = zipfile.ZipFile(wheel).namelist()
    record.evidence("wheel_files.txt", "\n".join(names) + "\n")
    for need in ("ray_vast/shim/librayvast_shim.so", "ray_vast/worker/bootstrap.py", "ray_vast/worker/bootloader.sh",
                 "ray_vast/examples/gpu-24gb.yaml", "ray_vast/examples/gpu-80gb.yaml", "ray_vast/examples/cpu-heavy.yaml",
                 "ray_vast/iam/ray-vast-head-boundary.json", "ray_vast/iam/ray-vast-user.json"):
        check(record, f"wheel has {need}", need in names, "")
    shim = hashlib.sha256(zipfile.ZipFile(wheel).read("ray_vast/shim/librayvast_shim.so")).hexdigest()
    check(record, "wheel shim = committed shim", shim == hashlib.sha256(SHIM.read_bytes()).hexdigest(), shim[:16])
    metadata = zipfile.ZipFile(wheel).read(next(n for n in names if n.endswith("METADATA"))).decode()
    check(record, "Requires-Dist ray[default]>=2.59", re.search(r"Requires-Dist: ray\[default\]>=2\.59", metadata) is not None, "")
    check(record, "Requires-Python >=3.10", "Requires-Python: >=3.10" in metadata, "")
    from livekit import scan_secrets
    from cluster import vast_key
    unpacked = dist / "unpacked"
    zipfile.ZipFile(wheel).extractall(unpacked)
    subprocess.run(["tar", "-xzf", str(sdist), "-C", str(unpacked)], check=True)
    found = scan_secrets(unpacked, [vast_key()])
    # The test files hold fake keys and `Bearer x` headers on purpose (they test the redaction); the real vast key is never there.
    real = [p for p in found if "/tests/" not in p.as_posix() or vast_key().encode() in p.read_bytes()]
    record.data("pattern_hits_in_test_files", [str(p.relative_to(unpacked)) for p in found if p not in real])
    check(record, "no secret in the wheel or the sdist", not real, [str(p.relative_to(unpacked)) for p in real][:5])


@pytest.mark.box_disk
@pytest.mark.release
def test_F18_python_3_10_install(record, dist):
    """The wheel installs and runs on 3.10. Then the unit tests run on 3.10 in the layout they need: the package source
    tree (an editable install: some CLI tests build a wheel from it) next to the repository's docs/test-plan.md (the
    claim matrix test)."""
    wheel = next(dist.glob("*.whl"))
    script = f"""set -e
pip install -q /dist/{wheel.name} > /tmp/pip.log 2>&1 || (tail -20 /tmp/pip.log; exit 1)
python -V
ray-vast --help | head -3
mkdir -p /tmp/repo/docs && cp /plan/test-plan.md /tmp/repo/docs/ && cp -r /pkg /tmp/repo/ray-vast-pypi && cd /tmp/repo/ray-vast-pypi && rm -rf .venv
pip install -q -e '.[test]' > /tmp/pip.log 2>&1 || (tail -20 /tmp/pip.log; exit 1)
python -m pytest -q -p no:cacheprovider tests/unit 2>&1 | tail -3
"""
    done = docker("run", "--rm", "-v", f"{dist}:/dist:ro", "-v", f"{PKG}:/pkg:ro", "-v", f"{PKG.parent / 'docs'}:/plan:ro", "python:3.10",
                  "bash", "-c", script, timeout=3600)
    record.evidence("py310.txt", done.stdout + done.stderr)
    check(record, "Python 3.10", "Python 3.10" in done.stdout, re.findall(r"Python 3\.\S+", done.stdout))
    check(record, "ray-vast --help works", "usage: ray-vast" in done.stdout, "")
    passed = re.search(r"(\d+) passed", done.stdout)
    check(record, "fast unit run passes on 3.10", done.returncode == 0 and passed and "failed" not in done.stdout.splitlines()[-1],
          done.stdout.strip().splitlines()[-1:])


def _doc_queries():
    """Every YAML block with an `offer_query` in node-types.md."""
    text = (PKG / "docs/how-to/node-types.md").read_text()
    found = []
    for n, block in enumerate(re.findall(r"```yaml\n(.*?)```", text, re.S)):
        try:
            data = yaml.safe_load(block)
        except yaml.YAMLError:
            continue
        stack = [data]
        while stack:
            item = stack.pop()
            if isinstance(item, dict):
                if "offer_query" in item and "resources" in item:
                    found.append((f"node-types block {n}", item))
                stack.extend(item.values())
    return found


def test_F19_offer_queries_of_the_examples_and_the_docs(record):
    from ray_vast.config import bootstrap_config, build_offer_query
    client, rows = vast(), []
    for path in sorted((PKG / "src/ray_vast/examples").glob("*.yaml")):
        config, _ = bootstrap_config(yaml.safe_load(path.read_text()))
        provider = config["provider"]
        for name, node_type in config["available_node_types"].items():
            if name == config["head_node_type"]:
                continue
            offers = client.search_offers(build_offer_query(node_type, provider["allow_non_datacenter"], name))
            machines = {o["machine_id"] for o in offers}
            first = sorted(o["dph_total"] for o in offers)[:provider["candidate_factor"]]
            rows.append({"source": path.name, "type": name, "offers": len(offers), "machines": len(machines),
                         "cheapest": [round(x, 3) for x in first], "budget": provider["max_dollars_per_hour"]})
            check(record, f"{path.name} {name}: >= 4 offers on >= 3 machines", len(offers) >= 4 and len(machines) >= 3, rows[-1])
            check(record, f"{path.name} {name}: the first candidates fit the budget", sum(first) <= provider["max_dollars_per_hour"], rows[-1])
    for source, node_type in _doc_queries():
        offers = client.search_offers(build_offer_query(node_type, False, source))
        rows.append({"source": source, "query": node_type["node_config"]["offer_query"], "offers": len(offers)})
        check(record, f"{source}: vast accepts the query", isinstance(offers, list), len(offers))
    record.evidence("offers.json", json.dumps(rows, indent=1))


@pytest.mark.box_disk
def test_F20_doc_snippets_run_in_ray_2_59(record, tmp_path):
    """Every Python block of the library pages compiles, and its imports work in the Ray 2.59 image. The core snippet runs
    on a local single node (the snippets on a cluster run verbatim in the libraries group)."""
    blocks = []
    for page in sorted((PKG / "docs/libraries").glob("*.md")):
        for n, block in enumerate(re.findall(r"```python\n(.*?)```", page.read_text(), re.S)):
            blocks.append((f"{page.stem}_{n}", block))
    for name, block in blocks:
        (tmp_path / f"{name}.py").write_text(block)
        try:
            compile(block, name, "exec")
            ok, detail = True, ""
        except SyntaxError as e:
            ok, detail = False, str(e)
        check(record, f"{name} compiles", ok, detail)
    imports = sorted({line.strip() for _, b in blocks for line in b.splitlines() if re.match(r"\s*(import|from) \S+", line)})
    (tmp_path / "imports.py").write_text("\n".join(imports) + "\nprint('IMPORTS OK')\n")
    record.evidence("imports.py", (tmp_path / "imports.py").read_text())
    done = docker("run", "--rm", "-v", f"{tmp_path}:/s:ro", RAY_IMAGE.replace("-cpu", "-gpu"), "bash", "-c",
                  "pip install -q torch==2.7.1 --index-url https://download.pytorch.org/whl/cpu >/tmp/pip.log 2>&1 || tail -5 /tmp/pip.log; "
                  "python /s/imports.py", timeout=3600)
    record.evidence("imports.txt", done.stdout + done.stderr)
    check(record, "all snippet imports work in the Ray 2.59 image (torch from pip)", "IMPORTS OK" in done.stdout, done.stderr[-300:])
    # The page's two blocks, on a local node with the page's CPUS = 24, then one call through the pool.
    core = "\n".join(b for n, b in blocks if n.startswith("core_") and ("ray.remote" in b or "CPUS =" in b))
    (tmp_path / "core_run.py").write_text("import ray\nray.init(num_cpus=24)\n" + core +
                                          "\nprint('CPUS', CPUS, 'CALL', ray.get(pool[0].work.remote(1)))\n")
    done = docker("run", "--rm", "-v", f"{tmp_path}:/s:ro", RAY_IMAGE, "bash", "-c", "cd /tmp && timeout 600 python /s/core_run.py", timeout=900)
    record.evidence("core_run.txt", done.stdout + done.stderr)
    check(record, "the core snippets run on a local node", done.returncode == 0 and "CPUS 24.0 CALL None" in done.stdout,
          (done.stdout + done.stderr)[-300:])

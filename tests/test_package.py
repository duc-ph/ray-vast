"""The built package: the wheel and the sdist, their contents and metadata, and an install on the oldest Python."""
import hashlib
import ipaddress
import re
import shutil
import subprocess
import sys
import tarfile
import venv
import zipfile
from pathlib import Path

import pytest

PROJECT = Path(__file__).resolve().parents[1]
COMMITTED_SHIM_SHA256 = "79055dea6e913180c362c81e2b0c9903dd7a39bac5e3d5bab0e491bb9b3817fd"


@pytest.fixture(scope="module")
def installed(tmp_path_factory):
    """Build the wheel, install it without dependencies into a fresh venv, and return the venv bin dir and the wheel."""
    work = tmp_path_factory.mktemp("wheel")
    subprocess.run(
        [sys.executable, "-m", "build", "--wheel", "--no-isolation", "--outdir", str(work), str(PROJECT)],
        check=True, capture_output=True,
    )
    wheel = next(work.glob("ray_vast-*.whl"))
    venv.create(work / "venv", with_pip=True)
    bin_dir = work / "venv" / "bin"
    subprocess.run([str(bin_dir / "pip"), "install", "--no-deps", str(wheel)], check=True, capture_output=True)
    return bin_dir, wheel


@pytest.mark.slow  # builds a wheel or an sdist
def test_wheel_is_pure_python(installed):
    _, wheel = installed
    assert wheel.name.endswith("-py3-none-any.whl")


@pytest.mark.slow  # builds a wheel or an sdist
def test_console_script_prints_help(installed):
    bin_dir, _ = installed
    result = subprocess.run([str(bin_dir / "ray-vast"), "--help"], capture_output=True, text=True)
    assert result.returncode == 0
    for command in ("up", "down", "attach", "dashboard"):
        assert command in result.stdout


@pytest.mark.slow  # builds a wheel or an sdist
def test_package_data_is_found_after_install(installed):
    bin_dir, _ = installed
    code = (
        "from importlib.resources import files\n"
        "r = files('ray_vast')\n"
        "print((r/'shim'/'librayvast_shim.so').is_file(), (r/'worker'/'bootstrap.py').is_file(),"
        " (r/'worker'/'bootloader.sh').is_file(), any((r/'examples').iterdir()),"
        " (r/'iam'/'ray-vast-head-boundary.json').is_file(), (r/'iam'/'ray-vast-user.json').is_file())\n"
    )
    result = subprocess.run([str(bin_dir / "python"), "-c", code], capture_output=True, text=True, check=True)
    assert result.stdout.split() == ["True"] * 6


@pytest.mark.slow  # builds a wheel or an sdist
def test_installed_shim_has_committed_sha256(installed):
    bin_dir, _ = installed
    code = "from importlib.resources import files; print(files('ray_vast').joinpath('shim/librayvast_shim.so').read_bytes().hex())"
    result = subprocess.run([str(bin_dir / "python"), "-c", code], capture_output=True, text=True, check=True)
    assert hashlib.sha256(bytes.fromhex(result.stdout.strip())).hexdigest() == COMMITTED_SHIM_SHA256


@pytest.fixture(scope="module")
def sdist(tmp_path_factory):
    """Build the sdist and return the tar file."""
    work = tmp_path_factory.mktemp("sdist")
    subprocess.run(
        [sys.executable, "-m", "build", "--sdist", "--no-isolation", "--outdir", str(work), str(PROJECT)],
        check=True, capture_output=True,
    )
    return next(work.glob("ray_vast-*.tar.gz"))


@pytest.mark.slow  # builds a wheel or an sdist
def test_sdist_has_sources_and_no_docs(sdist):
    with tarfile.open(sdist) as tar:
        # Names look like "ray_vast-<version>/<path>"; drop the first part.
        names = {name.split("/", 1)[1] for name in tar.getnames() if "/" in name}
    for expected in ("src/ray_vast/shim/librayvast_shim.so", "src/ray_vast/worker/bootstrap.py", "pyproject.toml"):
        assert expected in names
    assert not [n for n in names if n.startswith("docs/")]


# Addresses that are not a real host: loopback, private ranges, "any" and the documentation ranges (RFC 5737).
NOT_REAL = [ipaddress.ip_network(n) for n in ("0.0.0.0/32", "127.0.0.0/8", "10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16",
                                                "192.0.2.0/24", "198.51.100.0/24", "203.0.113.0/24")]
# Four dotted numbers that are not part of a longer dotted string (an OID such as 1.3.6.1.5.5.7.3.1 is not an address).
IPV4 = re.compile(rb"(?<![\d.])(?:\d{1,3}\.){3}\d{1,3}(?!\.?\d)")


@pytest.mark.slow  # builds a wheel or an sdist
def test_sdist_holds_no_real_ip_address(sdist):
    """The tests and fixtures ship in the sdist: they use documentation addresses, never a real host's address."""
    found = []
    with tarfile.open(sdist) as tar:
        for member in tar.getmembers():
            if member.isfile():
                for match in IPV4.findall(tar.extractfile(member).read()):
                    try:
                        address = ipaddress.ip_address(match.decode())
                    except ValueError:  # not an address, for example 300.1.2.3
                        continue
                    if not any(address in network for network in NOT_REAL):
                        found.append(f"{member.name}: {address}")
    assert found == []


def test_source_shim_has_committed_sha256():
    shim = PROJECT / "src" / "ray_vast" / "shim" / "librayvast_shim.so"
    assert hashlib.sha256(shim.read_bytes()).hexdigest() == COMMITTED_SHIM_SHA256


@pytest.mark.slow  # builds a wheel or an sdist
def test_twine_accepts_the_wheel_and_the_sdist_and_the_metadata_names_ray_and_python(installed, sdist):
    _, wheel = installed
    done = subprocess.run([sys.executable, "-m", "twine", "check", "--strict", str(wheel), str(sdist)], capture_output=True, text=True,
                          timeout=300)
    assert done.returncode == 0, done.stdout + done.stderr
    with zipfile.ZipFile(wheel) as z:
        names = z.namelist()
        metadata = z.read(next(n for n in names if n.endswith("METADATA"))).decode()
    for need in ("ray_vast/examples/gpu-24gb.yaml", "ray_vast/examples/gpu-80gb.yaml", "ray_vast/examples/cpu-heavy.yaml"):
        assert need in names
    assert re.search(r"Requires-Dist: ray\[default\]>=2\.59", metadata) and "Requires-Python: >=3.10" in metadata


@pytest.mark.slow
@pytest.mark.timeout(3600)
@pytest.mark.skipif(shutil.which("docker") is None, reason="needs Docker")
def test_the_wheel_installs_and_the_fast_tests_pass_on_python_3_10(installed, tmp_path):
    """The wheel installs and runs on 3.10. Then the fast tests run on 3.10 in an editable install of a copy of the source
    tree (some CLI tests build a wheel from it)."""
    _, wheel = installed
    script = f"""set -e
pip install -q /dist/{wheel.name} > /tmp/pip.log 2>&1 || (tail -20 /tmp/pip.log; exit 1)
python -V
ray-vast --help | head -3
cp -r /pkg /tmp/pkg && cd /tmp/pkg && rm -rf .venv test-results
pip install -q -e '.[test]' > /tmp/pip.log 2>&1 || (tail -20 /tmp/pip.log; exit 1)
python -m pytest -q -rfE -p no:cacheprovider 2>&1
"""
    done = subprocess.run(["docker", "run", "--rm", "-v", f"{wheel.parent}:/dist:ro", "-v", f"{PROJECT}:/pkg:ro", "python:3.10",
                           "bash", "-c", script], capture_output=True, text=True, timeout=3500)
    lines = done.stdout.strip().splitlines()
    assert "Python 3.10" in done.stdout and "usage: ray-vast" in done.stdout, done.stdout[-2000:] + done.stderr[-2000:]
    assert done.returncode == 0 and re.search(r"\d+ passed", lines[-1]) and "failed" not in lines[-1], "\n".join(lines[-30:])

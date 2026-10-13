import hashlib
import subprocess
import sys
import venv
from pathlib import Path

import pytest

PROJECT = Path(__file__).resolve().parents[2]
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


def test_wheel_is_pure_python(installed):
    _, wheel = installed
    assert wheel.name.endswith("-py3-none-any.whl")


def test_console_script_prints_help(installed):
    bin_dir, _ = installed
    result = subprocess.run([str(bin_dir / "ray-vast"), "--help"], capture_output=True, text=True)
    assert result.returncode == 0
    for command in ("up", "down", "attach", "dashboard"):
        assert command in result.stdout


def test_package_data_is_found_after_install(installed):
    bin_dir, _ = installed
    code = (
        "from importlib.resources import files\n"
        "r = files('ray_vast')\n"
        "print((r/'shim'/'librayvast_shim.so').is_file(), (r/'worker'/'bootstrap.py').is_file(),"
        " (r/'worker'/'bootloader.sh').is_file(), any((r/'examples').iterdir()))\n"
    )
    result = subprocess.run([str(bin_dir / "python"), "-c", code], capture_output=True, text=True, check=True)
    assert result.stdout.split() == ["True"] * 4


def test_installed_shim_has_committed_sha256(installed):
    bin_dir, _ = installed
    code = "from importlib.resources import files; print(files('ray_vast').joinpath('shim/librayvast_shim.so').read_bytes().hex())"
    result = subprocess.run([str(bin_dir / "python"), "-c", code], capture_output=True, text=True, check=True)
    assert hashlib.sha256(bytes.fromhex(result.stdout.strip())).hexdigest() == COMMITTED_SHIM_SHA256

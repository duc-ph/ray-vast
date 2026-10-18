"""The Python blocks of the library pages (docs/libraries/*.md): each compiles, and their imports work in the Ray 2.59
image. The core snippet runs on a local single node. The live library tests run the snippets on a real cluster."""
import re
import shutil
import subprocess
from pathlib import Path

import pytest

DOCS = Path(__file__).resolve().parents[1] / "docs"
RAY_IMAGE = "rayproject/ray:2.59.0-py311-cpu"


def blocks():
    found = []
    for page in sorted((DOCS / "libraries").glob("*.md")):
        for n, block in enumerate(re.findall(r"```python\n(.*?)```", page.read_text(), re.S)):
            found.append((f"{page.stem}_{n}", block))
    return found


def test_every_python_block_of_the_library_pages_compiles():
    found = blocks()
    assert found
    for name, block in found:
        compile(block, name, "exec")


def docker(*args, timeout=1800):
    return subprocess.run(["docker", *args], capture_output=True, text=True, timeout=timeout)


@pytest.mark.slow
@pytest.mark.timeout(3600)
@pytest.mark.skipif(shutil.which("docker") is None, reason="needs Docker")
def test_the_snippet_imports_work_in_the_ray_image_and_the_core_snippet_runs(tmp_path):
    found = blocks()
    imports = sorted({line.strip() for _, b in found for line in b.splitlines() if re.match(r"\s*(import|from) \S+", line)})
    (tmp_path / "imports.py").write_text("\n".join(imports) + "\nprint('IMPORTS OK')\n")
    done = docker("run", "--rm", "-v", f"{tmp_path}:/s:ro", RAY_IMAGE.replace("-cpu", "-gpu"), "bash", "-c",
                  "pip install -q torch==2.7.1 --index-url https://download.pytorch.org/whl/cpu >/tmp/pip.log 2>&1 || tail -5 /tmp/pip.log; "
                  "python /s/imports.py", timeout=3000)
    assert "IMPORTS OK" in done.stdout, done.stdout[-1000:] + done.stderr[-2000:]
    # The page's two blocks, on a local node with CPU = 24, then one call through the pool.
    core = "\n".join(b for n, b in found if n.startswith("core_") and ("ray.remote" in b or "CPUS =" in b))
    (tmp_path / "core_run.py").write_text("import ray\nray.init(num_cpus=24)\n" + core +
                                          "\nprint('CPUS', CPUS, 'CALL', ray.get(pool[0].work.remote(1)))\n")
    done = docker("run", "--rm", "-v", f"{tmp_path}:/s:ro", RAY_IMAGE, "bash", "-c", "cd /tmp && timeout 600 python /s/core_run.py",
                  timeout=900)
    assert done.returncode == 0 and "CPUS 24.0 CALL None" in done.stdout, (done.stdout + done.stderr)[-2000:]

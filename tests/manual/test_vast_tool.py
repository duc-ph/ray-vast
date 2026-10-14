# Unit tests for the logic in vast_tool.py that decides what to destroy or what to write as a secret.
# Run: ray-vast-pypi/.venv/bin/pytest tests/manual/test_vast_tool.py (no network).
import vast_tool


def test_test_labels_match():
    assert vast_tool.is_test_label("ray-vast-test-20261014")
    assert vast_tool.is_test_label("ray-vast:rvtest-c5")
    assert vast_tool.is_test_label("ray-vast:rvtest-c8")


def test_other_labels_do_not_match():
    for label in (None, "", "ray-vast:c5a-c7b4", "ray-vast:prod", "rayvast-test-c4-20261013", "badminton",
                  "my-ray-vast-test-1", "x ray-vast:rvtest-c5", "ray-vast:rvtest"):
        assert not vast_tool.is_test_label(label), label


def test_join_token_from_extra_env_list():
    instance = {"extra_env": [["-p 70000:70000", "1"], ["RAYVAST_JOIN_TOKEN", "abc123"], ["RAYVAST_CLUSTER", "rvtest-c5"]]}
    assert vast_tool.join_token(instance) == "abc123"


def test_join_token_from_extra_env_dict():
    assert vast_tool.join_token({"extra_env": {"RAYVAST_JOIN_TOKEN": "abc123"}}) == "abc123"


def test_join_token_missing():
    assert vast_tool.join_token({"extra_env": [["RAYVAST_CLUSTER", "x"]]}) is None
    assert vast_tool.join_token({}) is None


def test_scan_secrets_prints_only_the_names_of_files_with_a_secret(tmp_path):
    import subprocess
    from pathlib import Path
    token = "f00dfeed" * 8
    token_file = tmp_path / "token"
    token_file.write_text(token + "\n")
    data = tmp_path / "data"
    (data / "sub").mkdir(parents=True)
    # Built at run time, so a scan of tests/manual does not find these fake secrets.
    private, bearer, iak, cak = "PRIVATE" + " KEY", "Bearer" + " 0123abcd", "instance_api" + "_key", "CONTAINER_API" + "_KEY"
    files = {
        "clean.log": "bootstrap: enrolled as slot slot-1-abc123\nAuthorization: Bearer <redacted>\nCONTAINER_API_KEY=$KEY\n",
        "sub/token.log": f"x {token} y\n",
        "pem.txt": f"-----BEGIN {private}-----\n",
        "create.json": f'{{"{iak}": "0123abcd"}}\n',
        "env.txt": f"{cak}=0123abcd\n",
        "curl.txt": f"Authorization: {bearer}\n",
    }
    for name, text in files.items():
        (data / name).write_text(text)
    script = Path(__file__).with_name("scan_secrets.sh")
    result = subprocess.run(["bash", str(script), str(data), str(token_file)], capture_output=True, text=True,
                            env={"PATH": "/usr/bin:/bin", "HOME": str(tmp_path)})
    assert token not in result.stdout + result.stderr
    assert sorted(result.stdout.split()) == sorted(str(data / n) for n in files if n != "clean.log")
    assert result.returncode == 1


def test_scan_secrets_is_silent_on_clean_data(tmp_path):
    import subprocess
    from pathlib import Path
    (tmp_path / "token").write_text("f00dfeed" * 8)
    (tmp_path / "data").mkdir()
    (tmp_path / "data" / "a.log").write_text("nothing here\n")
    script = Path(__file__).with_name("scan_secrets.sh")
    result = subprocess.run(["bash", str(script), str(tmp_path / "data"), str(tmp_path / "token")], capture_output=True,
                            text=True, env={"PATH": "/usr/bin:/bin", "HOME": str(tmp_path)})
    assert (result.stdout, result.returncode) == ("", 0)

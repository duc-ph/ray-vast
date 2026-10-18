"""Which tests run where and together (unit-tested in tests/test_live_suite_kit.py): the record name of a test, the xdist
group of each test and the libraries host of a test."""

CLUSTER_FIXTURES = ("aws_only", "lifecycle", "libraries", "release")
# The fixture that declares the one need of a test that is not a cluster: the test needs the AWS account with no ray-vast
# role, and it may change an account-wide resource (the boundary policy). It runs before every cluster `up` of the run.
EXCLUSIVE_ACCOUNT = "exclusive_account"


def record_name(name):
    """The record name of a test from its pytest name: `test_some_name[gpu]` gives `some_name[gpu]`."""
    return name.removeprefix("test_")


def xdist_group(fixturenames, host=None):
    """The xdist group of one test, for `pytest -n N --dist loadgroup` (conftest forces loadgroup), from its fixtures only.
    xdist runs all tests of a group in one worker process, so a session fixture of that group runs `up` once and `down`
    once. Each cluster fixture is a group; a test with the restricted IAM user joins lifecycle (one user for each run);
    each libraries host is its own cluster and group. A test with the `exclusive_account` fixture is in the group of that
    name. None: any worker. A test with two groups is a ValueError (an `exclusive_account` test with a cluster fixture
    would wait for itself)."""
    wanted = {EXCLUSIVE_ACCOUNT: EXCLUSIVE_ACCOUNT in fixturenames,
              "aws_only": "aws_only" in fixturenames,
              "lifecycle": "lifecycle" in fixturenames or "restricted_user" in fixturenames,
              f"libraries-{host}": "libraries" in fixturenames,
              "release": "release" in fixturenames}
    groups = [group for group, want in wanted.items() if want]
    if len(groups) > 1:
        raise ValueError(f"a test in two xdist groups: {', '.join(groups)}")
    return groups[0] if groups else None


def host_of(item):
    """The libraries host of a test item: its `where` parameter (None for other tests)."""
    params = getattr(item, "callspec", None)
    return params.params.get("where") if params is not None else None


def cluster_fixture(fixturenames):
    """The cluster fixture of a test from its fixture closure (`item.fixturenames`: also a fixture that another fixture
    uses), or "none"."""
    return next((f for f in CLUSTER_FIXTURES if f in fixturenames), "none")

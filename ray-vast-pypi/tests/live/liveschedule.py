"""Which tests run where and together (unit-tested in tests/unit/test_live_kit.py): the group and need markers, the xdist
group of each test, the libraries host of a test, and the scenarios of the claim matrix and of the pytest collection."""
import re
import signal

GROUP_MARKERS = ("free", "aws", "lifecycle", "failure", "security", "limits", "libraries", "release")  # the claim matrix's groups
CLUSTER_FIXTURES = ("aws_only", "lifecycle", "libraries", "release")
ROW = re.compile(r"^\|\s*([A-Z]+\d\d)\s*\|[^|]*\|([^|]*)\|([^|]*)\|")

NEEDS = {  # the markers that declare a need of a test; conftest registers them and xdist_group maps them
    "exclusive_account": "the test needs the AWS account with no ray-vast role, and it may change an account-wide resource "
                         "(the boundary policy): it runs before every cluster `up` of the run (no cluster fixture)",
    "box_disk": "the test builds or installs the package on the dev box (up to about 1.5 GB of disk): the tests with this "
                "marker run in one process, one after the other, and share one wheel build",
}


def xdist_group(markers, fixturenames, host=None):
    """The xdist group of one test, for `pytest -n N --dist loadgroup` (conftest forces loadgroup), from its markers and
    fixtures only. xdist runs all tests of a group in one worker process, so a session fixture of that group runs `up` once
    and `down` once. Each cluster fixture is a group; a test with the restricted IAM user joins lifecycle (one user for
    each run); each libraries host (H1, H2, H3, gpu2, H4) is its own cluster and group. A need of NEEDS is a group of the
    same name. None: any worker. A test with two groups is a ValueError (an `exclusive_account` test with a cluster
    fixture would wait for itself)."""
    wanted = {"exclusive_account": "exclusive_account" in markers,
              "aws_only": "aws_only" in fixturenames,
              "lifecycle": "lifecycle" in fixturenames or "restricted_user" in fixturenames,
              f"libraries-{(host or '').lower()}": "libraries" in fixturenames,
              "box_disk": "box_disk" in markers,
              "release": "release" in fixturenames}
    groups = [group for group, want in wanted.items() if want]
    if len(groups) > 1:
        raise ValueError(f"a test in two xdist groups: {', '.join(groups)}")
    return groups[0] if groups else None


def host_of(item):
    """The libraries host of a test item: its `where` parameter or its `host` marker (None for other tests)."""
    params = getattr(item, "callspec", None)
    if params is not None and "where" in params.params:
        return params.params["where"]
    marker = item.get_closest_marker("host")
    return marker.args[0] if marker else None


def cluster_fixture(fixturenames):
    """The cluster fixture of a test from its fixture closure (`item.fixturenames`: also a fixture that another fixture
    uses), or "none"."""
    return next((f for f in CLUSTER_FIXTURES if f in fixturenames), "none")


def matrix_rows(text):
    """The scenarios of the claim matrix (docs/test-plan.md): [{id, markers, fixture}] in the order of the tables. Only the
    tables with the matrix header (`| ID | Name | Markers | Fixture |`) count; another table ends at its first non-table line."""
    rows, in_matrix = [], False
    for line in text.splitlines():
        if not line.startswith("|"):
            in_matrix = False
        elif line.startswith("| ID | Name | Markers | Fixture |"):
            in_matrix = True
        m = ROW.match(line)
        if in_matrix and m and m.group(3).strip():
            rows.append({"id": m.group(1), "markers": sorted(x.strip() for x in m.group(2).split(",") if x.strip()),
                         "fixture": m.group(3).strip().split()[0]})
    return rows


def suite_rows(directory):
    """The scenarios of the live suite as pytest collects them (in this process, `--collect-only`, with the conftest of
    `directory`): one row for each `test_<ID>_...` item, so also for each parameter. A row has the group and need markers
    of the item (also a marker that the conftest or `pytest.param` adds) and its cluster fixture (`cluster_fixture` of
    its fixture closure). A collection error raises RuntimeError. The conftest changes os.environ and the SIGTERM handler
    of the process; both are restored."""
    import os
    import pytest
    rows, wanted = [], set(GROUP_MARKERS) | set(NEEDS)

    class Collect:
        @staticmethod
        def pytest_collection_finish(session):
            for item in session.items:
                m = re.match(r"test_([A-Z]+\d\d)_", item.name)
                if m:
                    rows.append({"id": m.group(1), "markers": sorted({x.name for x in item.iter_markers()} & wanted),
                                 "fixture": cluster_fixture(item.fixturenames)})

    environ, sigterm = dict(os.environ), signal.getsignal(signal.SIGTERM)
    try:
        code = pytest.main([str(directory), "--collect-only", "-qq", "-o", "addopts=", "-p", "no:cacheprovider"], plugins=[Collect()])
    finally:
        os.environ.clear()
        os.environ.update(environ)
        signal.signal(signal.SIGTERM, sigterm)
    if code != 0:
        raise RuntimeError(f"the collection of {directory} failed (pytest exit {code})")
    return rows

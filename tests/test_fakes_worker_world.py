"""The port blocks of the fake worker world (tests/fakes/worker_world.py): test processes that run at the same time (the
parallel live suite runs the bootstrap and the container tests together) must never get the same ports."""
import subprocess
import sys

from fakes.worker_world import PORT_LOCKS, World


def test_worlds_that_exist_at_the_same_time_get_port_blocks_that_do_not_overlap(tmp_path):
    worlds = [World(tmp_path / f"w{n}") for n in range(20)]
    try:
        blocks = sorted(w.port_base for w in worlds)
        assert all(b - a >= 67 for a, b in zip(blocks, blocks[1:])), blocks
        # another process sees the blocks as taken, also before any world listens on them
        code = ("import sys; sys.path.insert(0, 'tests'); from fakes.worker_world import PORT_LOCKS, World; "
                f"w = World({str(tmp_path / 'other')!r}); print(w.port_base)")
        other = int(subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True).stdout)
        assert all(abs(other - b) >= 67 for b in blocks), (other, blocks)
    finally:
        for w in worlds:
            w.stop()


def test_a_stopped_world_gives_its_block_back(tmp_path):
    first = World(tmp_path / "a")
    first.start()
    lock = PORT_LOCKS / f"{first.port_base}.lock"
    taken = subprocess.run([sys.executable, "-c", LOCK_FREE, str(lock)], capture_output=True, text=True).stdout.strip()
    first.stop()
    free = subprocess.run([sys.executable, "-c", LOCK_FREE, str(lock)], capture_output=True, text=True).stdout.strip()
    assert (taken, free) == ("False", "True")


LOCK_FREE = """import fcntl, sys
with open(sys.argv[1], "w") as f:
    try:
        fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        print(True)
    except BlockingIOError:
        print(False)
"""

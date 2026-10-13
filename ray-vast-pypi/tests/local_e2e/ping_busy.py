# Step 11: ask the detached actor that hold_nodes.py started which container it runs in. Prints PING <container id>.
import ray

ray.init(address="auto", namespace="st11")
print("PING", ray.get(ray.get_actor("busy").container.remote(), timeout=120), flush=True)

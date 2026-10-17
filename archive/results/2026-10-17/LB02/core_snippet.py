import ray

@ray.remote(num_cpus=0.5)          # CPUS / B
def child(i):
    ...

@ray.remote(num_cpus=0.5)          # CPUS / B
class Runner:
    def work(self, i):
        return ray.get(child.remote(i))   # the actor waits for a task: 2 processes

pool = [Runner.remote() for _ in range(5)]   # B // 2, because each call also needs a task

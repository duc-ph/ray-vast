from ray import serve

@serve.deployment(
    num_replicas=20,                        # at most B for each worker
    ray_actor_options={"num_cpus": 0.4},    # CPUS / B
)
class Model:
    def __call__(self, request):
        ...

serve.run(Model.bind(), route_prefix="/")

import ray
from ray.data import DataContext, ExecutionResources, TaskPoolStrategy

ctx = DataContext.get_current()
ctx.max_hash_shuffle_aggregators = 2                                # B // 4
ctx.execution_options.resource_limits = ExecutionResources(cpu=2)   # B // 4

ds = ray.data.read_parquet("s3://ray-vast-rvtest-20261017-089e/lb03/in",
                           concurrency=2,             # B // 4
                           override_num_blocks=5)     # B // 2
ds = ds.map_batches(my_fn, compute=TaskPoolStrategy(size=2))   # B // 4
ds.groupby("key").count().write_parquet("s3://ray-vast-rvtest-20261017-089e/lb03/out-H3")

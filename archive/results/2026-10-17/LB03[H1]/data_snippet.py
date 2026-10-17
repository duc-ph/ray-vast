import ray
from ray.data import DataContext, ExecutionResources, TaskPoolStrategy

ctx = DataContext.get_current()
ctx.max_hash_shuffle_aggregators = 13                                # B // 4
ctx.execution_options.resource_limits = ExecutionResources(cpu=13)   # B // 4

ds = ray.data.read_parquet("s3://ray-vast-rvtest-20261017-6f9e/lb03/in",
                           concurrency=13,             # B // 4
                           override_num_blocks=27)     # B // 2
ds = ds.map_batches(my_fn, compute=TaskPoolStrategy(size=13))   # B // 4
ds.groupby("key").count().write_parquet("s3://ray-vast-rvtest-20261017-6f9e/lb03/out-H1")

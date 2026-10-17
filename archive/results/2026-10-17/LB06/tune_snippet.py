from ray import tune

tuner = tune.Tuner(
    tune.with_resources(trainable, {"cpu": 0.5}),                 # CPUS / B
    param_space={"x": tune.uniform(0, 1)},
    tune_config=tune.TuneConfig(num_samples=60, metric="score", mode="max",
                                max_concurrent_trials=5),        # B // 2
    run_config=tune.RunConfig(storage_path="s3://ray-vast-rvtest-20261017-fcde/lb06/experiments"),
)
tuner.fit()

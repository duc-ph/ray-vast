from ray import tune

config = (PPOConfig().environment("CartPole-v1")
          .env_runners(num_env_runners=0)
          .learners(num_learners=2, num_gpus_per_learner=1, num_cpus_per_learner=0.3)   # CPUS / B
          .resources(placement_strategy="STRICT_PACK"))   # all learners on one worker
grid = tune.Tuner(config.algo_class, param_space=config,
           run_config=tune.RunConfig(
               stop={"training_iteration": 2},
               checkpoint_config=tune.CheckpointConfig(checkpoint_at_end=False))).fit()

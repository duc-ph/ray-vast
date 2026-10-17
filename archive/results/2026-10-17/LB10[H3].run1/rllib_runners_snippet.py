from ray.rllib.algorithms.ppo import PPOConfig

config = (PPOConfig().environment("CartPole-v1")
          .env_runners(num_env_runners=2,             # 2 (tested); B // 2 or fewer for each worker
                       num_envs_per_env_runner=20,     # more environments here, not more runners
                       num_cpus_per_env_runner=0.4))   # CPUS / B
algo = config.build_algo()
algo.train()

import torch
from ray.train import RunConfig, ScalingConfig
from ray.train.torch import TorchTrainer

def train_func(config):
    loader = torch.utils.data.DataLoader(dataset, batch_size=128, num_workers=2)  # small num_workers
    train_body(loader)

trainer = TorchTrainer(
    train_func,
    scaling_config=ScalingConfig(
        num_workers=2,                  # at most B // 2 = 5; for GPUs, the GPUs of one worker
        use_gpu=True,
        resources_per_worker={"CPU": 0.211, "GPU": 1},   # CPUS / B
        placement_strategy="STRICT_PACK",   # all Train worker processes on one worker
    ),
    run_config=RunConfig(storage_path="s3://ray-vast-rvtest-20261017-fa72/lb07/experiments"),
)
trainer.fit()

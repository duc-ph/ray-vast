# Ray library support

You use the Ray libraries with the normal Ray workflow: `ray job submit` with `--working-dir` and `runtime_env`.
Read [Run your Ray workload](../how-to/run-your-workload.md).

This table shows the libraries that we tested on a live cluster (EC2 head, vast GPU workers). It also shows the result, and what you must do.

Test: a live test on 2026-10-14, Ray 2.59, image `rayproject/ray:2.59.0-py311-gpu`. The workers were 1-GPU and 2-GPU vast hosts.

| Library | Tested | Result | What you must do |
|---|---|---|---|
| Jobs | `ray job submit` with `--working-dir`, `list`, `logs --follow`, `stop`. `runtime_env` `pip` with `torch==2.7.1` (it brings its own CUDA runtime). | Works. The first torch install took about 4–5 minutes on a worker. | Keep the `dashboard` tunnel open. Build large packages into the image for a faster start. |
| Ray Core | Tasks, actors, actor restarts, placement groups (`PACK`, `STRICT_SPREAD`), a 2 GB object from one host to another. | Works. Objects move between hosts at about 30 MB/s. | Do not run many busy worker processes on one worker. Read [The process limit of vast hosts](limits.md#the-process-limit-of-vast-hosts). |
| Ray Data | Map and filter, `random_shuffle`, `groupby`, GPU batch inference, `read_parquet` from S3, `read_csv`, `write_parquet`. | Works with a CPU limit. A job does not crash the worker. A hash shuffle (`groupby`) can hang on a worker with few ports unless you set `max_hash_shuffle_aggregators` and `resource_limits`. Read [A worker is at its process limit](../how-to/troubleshoot.md#a-worker-is-at-its-process-limit). | Limit the CPUs of Ray Data. Write output to a cloud bucket. Read [Run your Ray workload](../how-to/run-your-workload.md#ray-data-limit-the-cpus). |
| Ray Tune | Trials on many nodes, ASHA, GPU trials, checkpoints. | Works. Checkpoints need cloud storage. | Set `storage_path` to a cloud bucket. Read [Checkpoints](../how-to/run-your-workload.md#ray-tune-and-ray-train-checkpoints). |
| Ray Train | PyTorch on one worker with 1 GPU, and with 2 GPUs (NCCL). PyTorch DDP on 2 workers (NCCL and Gloo). | Works on one worker. Does not work on two or more workers. | Use one worker with many GPUs. Set `storage_path` to a cloud bucket. Read [Limits](limits.md#no-multi-node-ray-train). |
| Ray Serve | Replicas on many nodes, GPU replicas, `DeploymentHandle` calls, HTTP on the head. | Works. | Use an SSH tunnel for HTTP from your machine. Read [Ray Serve: HTTP access](../how-to/run-your-workload.md#ray-serve-http-access). |

## What is true for all libraries

- The head has `CPU: 0`. Every task and actor with resources runs on a vast worker.
- A worker's disk is lost when the worker ends. Write results and checkpoints to cloud storage.
- A local output path (for example `write_parquet("/tmp/out")`) writes files to the disk of each worker. The head and your machine do not see these files. Use a cloud bucket for shared output.
- Each worker has a fixed number of ports for Ray worker processes (tasks, actors and drivers). Read [Limits](limits.md#58-worker-ports-for-each-worker).
- Ray traffic between nodes goes over the public internet. Large object transfers between workers are slower than in one data center.
- Only Ray's own ports are open between the nodes (the 64 identity ports of each worker). A library that opens its own connections between nodes on other ports cannot reach the other node.

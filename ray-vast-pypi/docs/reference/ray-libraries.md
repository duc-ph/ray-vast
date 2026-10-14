# Ray library support

You use the Ray libraries with the normal Ray workflow: `ray job submit` with `--working-dir` and `runtime_env`.
Read [Run your Ray workload](../how-to/run-your-workload.md).

This table shows which libraries we tested on a live cluster (EC2 head, vast GPU workers), the result, and what you must do.

<!-- C9 RESULTS PENDING -->
!!! note
    Results of the live test C9 go here.

| Library | Tested | Result | What you must do |
|---|---|---|---|
| Ray Core (tasks, actors, placement groups, objects) | | | |
| Ray Data | | | |
| Ray Tune | | | |
| Ray Train | | | |
| Ray Serve | | | |

## What is true for all libraries

These facts come from the design. They do not depend on the C9 results.

- The head has `CPU: 0`. Every task and actor with resources runs on a vast worker.
- A worker's disk is lost when the worker ends. Write results and checkpoints to cloud storage.
- Each worker has 58 ports for Ray worker processes (tasks, actors and drivers). Read [Limits](limits.md).
- Ray traffic between nodes goes over the public internet. Large object transfers between workers are slower than in one data center.
- Only Ray's own ports are open between the nodes (the 64 identity ports of each worker). A library that opens its own connections between nodes on other ports cannot reach the other node.

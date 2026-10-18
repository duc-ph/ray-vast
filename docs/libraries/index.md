# Ray libraries

All Ray libraries run on a ray-vast cluster. Some need an extra change on vast.
Each library page tells what works, what is different on vast, and which settings to use.

Send your work with `ray job submit`. Read [Submit jobs](jobs.md).

| Library | One worker | Many workers | What to know |
|---|---|---|---|
| [Ray Core](core.md) | ✅ | ✅ | |
| [Ray Data](data.md) | 🟨 | 🟨 | Write the output to a cloud bucket. A local output path stays on each worker. |
| [Ray Train](train.md) | 🟨 | ❌ | Write checkpoints to a cloud bucket. Training across workers fails behind the NAT of vast. Use one worker with many GPUs. |
| [Ray Tune](tune.md) | 🟨 | 🟨 | Write checkpoints to a cloud bucket. |
| [Ray Serve](serve.md) | ✅ | ✅ | |
| [RLlib](rllib.md) | ✅ | 🟨 | Put all learners on one worker. Learners across workers fail behind the NAT of vast. |

✅ works. 🟨 works with an extra change. ❌ the main use fails.

## Processes at once

Each worker runs a limited number of Ray processes (tasks and actors) at the same time. We call this number *N*.
With the defaults of each library, your work fits. You need the settings of a library page only in special cases, for example many small tasks.
Read [Many tasks or actors](many-tasks.md).

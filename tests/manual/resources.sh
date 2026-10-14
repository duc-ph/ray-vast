#!/bin/bash
# Safety rule 5 of docs/test-plan.md: append one line to archive/results/<UTC date>/resources.csv with the time, the
# cycle, the step, the CPU and memory of the container ray-vast-head, the number of the cycle's vast instances
# (label ray-vast:rvtest-<cycle>) and the sum of their dph_total. Runs on the operator machine.
# The head IP comes from ~/.ray-vast/rvtest-<cycle>.json (written by `ray-vast up`). Key: RAYVAST_KEY_FILE or ~/ray-vast-devbox.pem.
# Usage: bash resources.sh CYCLE STEP      (for example: bash resources.sh c5 C5.2)
set -euo pipefail
cycle=$1 step=$2
here=$(cd "$(dirname "$0")" && pwd)
cluster=rvtest-$cycle
out=$here/../../archive/results/$(date -u +%F)/resources.csv
mkdir -p "$(dirname "$out")"
[ -s "$out" ] || echo "utc,cycle,step,head_cpu,head_mem,instances,dph_total_sum" > "$out"
head_ip=$(python3 -c "import json, sys; print(json.load(open(sys.argv[1]))['head_ip'])" "$HOME/.ray-vast/$cluster.json" 2>/dev/null || true)
head_stats=","
if [ -n "$head_ip" ]; then
  head_stats=$(ssh -i "${RAYVAST_KEY_FILE:-$HOME/ray-vast-devbox.pem}" -o BatchMode=yes -o ConnectTimeout=10 \
    -o StrictHostKeyChecking=accept-new -o UserKnownHostsFile="$HOME/.ray-vast/$cluster.known_hosts" "ubuntu@$head_ip" \
    "sudo docker stats --no-stream --format '{{.CPUPerc}},{{.MemUsage}}' ray-vast-head" 2>/dev/null | tr -d ' ' || echo ",")
fi
vast=$(cd "$here" && python3 -c "
import vast_tool
found = [i for i in vast_tool.instances() if i.get('label') == 'ray-vast:$cluster']
print(f'{len(found)},{sum(i.get(\"dph_total\") or 0 for i in found):.4f}')")
line="$(date -u +%FT%TZ),$cycle,$step,${head_stats:-,},$vast"
echo "$line" >> "$out"
echo "$line"

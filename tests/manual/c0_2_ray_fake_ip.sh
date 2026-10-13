#!/usr/bin/env bash
# C0.2: Ray single node with a fake public IP (203.0.113.10), in the Ray container.
# Run on the test EC2 host. Usage: bash c0_2_ray_fake_ip.sh <step: 1|2|3|4>
set -u
STEP=$1
IMAGE=rayproject/ray:2.59.0-py311-cpu
OUT=/opt/rayvast-test/results/c0_2
PRIVATE_IP=$(curl -s -H "X-aws-ec2-metadata-token: $(curl -s -X PUT http://169.254.169.254/latest/api/token -H 'X-aws-ec2-metadata-token-ttl-seconds: 60')" http://169.254.169.254/latest/meta-data/local-ipv4)
SHIM_ENV="-e LD_PRELOAD=/opt/rayvast/librayvast_shim.so -e RAYVAST_PUBLIC_IP=203.0.113.10 -e RAYVAST_PRIVATE_IP=$PRIVATE_IP -e RAYVAST_LOCALHOST_PORTS=6384 -e RAYVAST_SHIM_LOG=/results/c0_2_shim.log"
RAY_FLAGS="--head --node-ip-address=203.0.113.10 --port=6379 --dashboard-host=127.0.0.1 --node-manager-port=6380 --object-manager-port=6381 --metrics-export-port=6382 --dashboard-agent-grpc-port=6383 --dashboard-agent-listen-port=6384 --runtime-env-agent-port=6385 --min-worker-port=10002 --max-worker-port=10100"
mkdir -p $OUT

if [ "$STEP" = 1 ]; then
  docker rm -f rayvast-c0 >/dev/null 2>&1
  docker run -d --network host --name rayvast-c0 -v /opt/rayvast-test/shim:/opt/rayvast:ro -v /opt/rayvast-test/results:/results -v /opt/rayvast-test/tests:/tests:ro $IMAGE sleep infinity
  echo "== step 1: ray start without the shim"
  docker exec rayvast-c0 ray start $RAY_FLAGS 2>&1 | tail -20
  echo "== wait 60 s, then look for Errno 99 and the raylet"
  sleep 60
  docker exec rayvast-c0 bash -c 'ps -eo pid,comm,args | grep -E "raylet|gcs_server|agent" | grep -v grep; cd /tmp/ray/session_latest/logs && grep -l "Errno 99" * ; grep -h -m3 -B2 "Errno 99" runtime_env_agent.log dashboard_agent.log 2>/dev/null | tail -20; echo "-- raylet.out tail"; tail -15 raylet.out'
  docker exec rayvast-c0 bash -c 'rm -rf /results/c0_2/step1_logs; cp -r /tmp/ray/session_latest/logs /results/c0_2/step1_logs'
fi

if [ "$STEP" = 2 ]; then
  docker exec rayvast-c0 ray stop --force 2>&1 | tail -2
  docker exec rayvast-c0 rm -rf /tmp/ray
  rm -f /opt/rayvast-test/results/c0_2_shim.log
  echo "== step 2: ray start with the shim (PRIVATE_IP=$PRIVATE_IP)"
  docker exec $SHIM_ENV rayvast-c0 ray start $RAY_FLAGS 2>&1 | tail -20
  sleep 15
  docker exec $SHIM_ENV rayvast-c0 ray status
  docker exec rayvast-c0 bash -c 'ps -eo pid,comm,args | grep -E "raylet|gcs_server|agent" | grep -v grep'
  echo "== LD_PRELOAD in /proc/<pid>/environ of every process in the container"
  docker exec rayvast-c0 bash -c 'for p in $(ls /proc | grep -E "^[0-9]+$"); do [ -r /proc/$p/environ ] || continue; c=$(cat /proc/$p/comm); e=$(tr "\0" "\n" < /proc/$p/environ | grep -c "^LD_PRELOAD=/opt/rayvast/librayvast_shim.so"); m=$(grep -c librayvast_shim /proc/$p/maps); echo "$p $c env_LD_PRELOAD=$e shim_mapped=$m"; done'
fi

if [ "$STEP" = 3 ]; then
  echo "== step 3: driver (20 tasks, 1 actor x 10 calls, 1 pip task)"
  vmstat -t 5 > $OUT/step3_vmstat.log &
  VM=$!
  date -u +%FT%TZ
  docker exec $SHIM_ENV rayvast-c0 python /tests/ray_driver.py 1 2>&1 | grep -vE "^\s*$" | tail -20
  date -u +%FT%TZ
  kill $VM
fi

if [ "$STEP" = 4 ]; then
  echo "== step 4: shim log counts (comm call af) and ss -ltnp"
  awk '{print $3, $4, $NF}' /opt/rayvast-test/results/c0_2_shim.log | sort | uniq -c | sort -rn
  echo "-- distinct rewrites"
  awk '{print $3, $4, $5, $6, $7}' /opt/rayvast-test/results/c0_2_shim.log | sort | uniq -c | sort -rn
  sudo ss -ltnp | grep -vE "sshd|systemd-resolve|:53 "
fi

if [ "$STEP" = 5 ]; then
  echo "== extra: job port and ray job submit through the dashboard (C2.2.6 path)"
  grep -h -E "Failed to bind|failed to bind|HTTP service will be disabled" $(docker exec rayvast-c0 readlink -f /tmp/ray/session_latest/logs/dashboard_agent.log | sed 's#^#/proc/'$(docker inspect -f '{{.State.Pid}}' rayvast-c0)'/root#') 2>/dev/null | head -3
  docker exec rayvast-c0 grep -c -E "Failed to bind|HTTP service will be disabled" /tmp/ray/session_latest/logs/dashboard_agent.log
  curl -s -o /dev/null -w '6384 code %{http_code}\n' http://127.0.0.1:6384/api/jobs/
  timeout 300 docker exec $SHIM_ENV rayvast-c0 ray job submit --address http://127.0.0.1:8265 -- python -c "import ray; ray.init(); print(ray.cluster_resources())" 2>&1 | tail -6
fi

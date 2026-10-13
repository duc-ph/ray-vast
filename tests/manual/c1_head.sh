#!/usr/bin/env bash
# C1: EC2 head with its real public IP, TLS, token auth and the shim.
# Run on the test EC2 host. Usage: bash c1_head.sh <step: setup|1|2|3|4|5>
set -u
STEP=$1
IMAGE=rayproject/ray:2.59.0-py311-cpu
BASE=/opt/rayvast-test
SEC=$BASE/secrets
RES=$BASE/results
IMDS_TOKEN=$(curl -s -X PUT http://169.254.169.254/latest/api/token -H 'X-aws-ec2-metadata-token-ttl-seconds: 60')
HEAD_IP=$(curl -s -H "X-aws-ec2-metadata-token: $IMDS_TOKEN" http://169.254.169.254/latest/meta-data/public-ipv4)
HEAD_PRIVATE_IP=$(curl -s -H "X-aws-ec2-metadata-token: $IMDS_TOKEN" http://169.254.169.254/latest/meta-data/local-ipv4)
echo "HEAD_IP=$HEAD_IP HEAD_PRIVATE_IP=$HEAD_PRIVATE_IP"

if [ "$STEP" = setup ]; then
  echo "== setup 1: CA, token, head cert (SAN IP:$HEAD_IP, IP:127.0.0.1)"
  umask 077
  openssl req -x509 -newkey rsa:2048 -days 2 -nodes -subj "/CN=rayvast-test-ca-20261012" -keyout $SEC/ca.key -out $SEC/ca.crt 2>/dev/null
  openssl rand -hex 32 > $SEC/token
  openssl req -newkey rsa:2048 -nodes -subj "/CN=rayvast-head" -keyout $SEC/head.key -out $SEC/head.csr 2>/dev/null
  printf "subjectAltName=IP:%s,IP:127.0.0.1\n" "$HEAD_IP" > $SEC/head.ext
  openssl x509 -req -in $SEC/head.csr -CA $SEC/ca.crt -CAkey $SEC/ca.key -CAcreateserial -days 2 -extfile $SEC/head.ext -out $SEC/head.crt 2>/dev/null
  chmod 600 $SEC/*
  ls -l $SEC
  openssl x509 -in $SEC/head.crt -noout -subject -issuer -enddate -ext subjectAltName
  openssl verify -CAfile $SEC/ca.crt $SEC/head.crt

  echo "== setup 2: head.env (paths only, no secret values)"
  cat > $BASE/head.env <<EOF
RAY_USE_TLS=1
RAY_TLS_SERVER_CERT=$SEC/head.crt
RAY_TLS_SERVER_KEY=$SEC/head.key
RAY_TLS_CA_CERT=$SEC/ca.crt
RAY_AUTH_MODE=token
RAY_AUTH_TOKEN_PATH=$SEC/token
LD_PRELOAD=$BASE/shim/librayvast_shim.so
RAYVAST_PUBLIC_IP=$HEAD_IP
RAYVAST_PRIVATE_IP=$HEAD_PRIVATE_IP
RAYVAST_LOCALHOST_PORTS=6384
RAYVAST_SHIM_LOG=$RES/c1_shim.log
EOF
  cat $BASE/head.env
fi

if [ "$STEP" = 1 ]; then
  rm -f $RES/c1_shim.log
  docker rm -f rayvast-head >/dev/null 2>&1
  docker run -d --network host --name rayvast-head --env-file $BASE/head.env -v $BASE:$BASE $IMAGE sleep infinity
  docker ps --filter name=rayvast-head --format '{{.Names}} {{.Image}} {{.Status}}'
  docker exec rayvast-head python -c "import ray,sys; print(ray.__version__, sys.version)"
fi

if [ "$STEP" = 2 ]; then
  docker exec rayvast-head ray start --head --node-ip-address=$HEAD_IP --port=6379 --dashboard-host=127.0.0.1 --num-cpus=0 --node-manager-port=6380 --object-manager-port=6381 --metrics-export-port=6382 --dashboard-agent-grpc-port=6383 --dashboard-agent-listen-port=6384 --runtime-env-agent-port=6385 --min-worker-port=10002 --max-worker-port=10100 2>&1 | grep -vE "^\s*$" | tail -6
  sleep 15
  docker exec rayvast-head ray status
  echo "== shim mapped in each process"
  docker exec rayvast-head bash -c 'for p in $(ls /proc | grep -E "^[0-9]+$"); do [ -r /proc/$p/maps ] || continue; echo "$(cat /proc/$p/comm) shim_mapped=$(grep -c librayvast_shim /proc/$p/maps)"; done | sort | uniq -c'
fi

if [ "$STEP" = 3 ]; then
  sudo ss -ltnp | grep -vE "sshd|systemd-resolve|containerd|127.0.0.5[34]"
  echo "== Errno 99 / bind failures in Ray logs"
  docker exec rayvast-head bash -c 'cd /tmp/ray/session_latest/logs; grep -l -E "Errno 99|Cannot assign requested|Failed to bind|HTTP service will be disabled" *.log *.err *.out 2>/dev/null; echo "(end of list)"; ps -eo comm,stat | grep -c defunct'
fi

if [ "$STEP" = 4 ]; then
  echo "== no token (host curl, same network namespace as the container; the image has no curl)"
  curl -s -o /dev/null -w 'no token: %{http_code}\n' http://127.0.0.1:6384/api/jobs/
  curl -s -o /dev/null -w 'wrong token: %{http_code}\n' -H "Authorization: Bearer $(openssl rand -hex 32)" http://127.0.0.1:6384/api/jobs/
  curl -s -o /dev/null -w 'right token: %{http_code}\n' -H "Authorization: Bearer $(cat $SEC/token)" http://127.0.0.1:6384/api/jobs/
  echo "== no token, from inside the container (python urllib)"
  docker exec rayvast-head python -c "
import urllib.request, urllib.error
try: print('container no token:', urllib.request.urlopen('http://127.0.0.1:6384/api/jobs/').status)
except urllib.error.HTTPError as e: print('container no token:', e.code)"
  echo "== the job port is not reachable on the public or private IP"
  curl -s -m 5 -o /dev/null -w 'private ip: %{http_code}\n' http://$HEAD_PRIVATE_IP:6384/api/jobs/ || echo "private ip: connect failed (rc=$?)"
fi

if [ "$STEP" = 5 ]; then
  echo "== driver with num_cpus=0; tcpdump on ens5 for packets to or from HEAD_IP"
  sudo timeout 900 tcpdump -i ens5 -nn -w $RES/c1_step5_headip.pcap "host $HEAD_IP" 2>$RES/c1_step5_tcpdump.err &
  sleep 2
  vmstat -t 5 > $RES/c1_step5_vmstat.log &
  VM=$!
  date -u +%FT%TZ
  docker exec rayvast-head python $BASE/tests/ray_driver.py 0 2>&1 | grep -vE "^\s*$" | tail -8
  date -u +%FT%TZ
  kill $VM
  echo "== ray job submit through the dashboard"
  timeout 300 docker exec rayvast-head ray job submit --address http://127.0.0.1:8265 -- python -c "import ray; ray.init(); print(ray.cluster_resources())" 2>&1 | tail -4
  sleep 2
  sudo pkill -INT -f "tcpdump -i ens5 -nn -w $RES/c1_step5_headip.pcap"
  sleep 1
  echo "== packets on ens5 with HEAD_IP: $(sudo tcpdump -nn -r $RES/c1_step5_headip.pcap 2>/dev/null | wc -l)"
  echo "== established TCP with peer HEAD_IP: $(sudo ss -tn | grep -c "$HEAD_IP")"
  echo "== shim log: connect lines to HEAD_IP, by new address"
  grep " connect " $RES/c1_shim.log | grep -E "(^| )(::ffff:)?$HEAD_IP:" | awk '{n=$7; sub(/:[0-9]+$/,"",n); print n}' | sort | uniq -c
  echo "== shim log counts by comm, call, af"
  awk '{print $3, $4, $NF}' $RES/c1_shim.log | sort | uniq -c | sort -rn
  echo "== shim log counts by comm, call, port"
  awk '{n=split($5,a,":"); print $3, $4, a[n]}' $RES/c1_shim.log | sort | uniq -c | sort -rn
fi

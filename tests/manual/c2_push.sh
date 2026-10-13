#!/usr/bin/env bash
# C2.1 steps 1-2: sign a node cert for one worker and push the secrets, shim and scripts over SSH.
# Run on the test EC2 host. Usage: bash c2_push.sh <name> <public_ip> <ssh_port>
set -u
NAME=$1; IP=$2; SSHP=$3
BASE=/opt/rayvast-test
SEC=$BASE/secrets
STAGE=$SEC/$NAME
umask 077
mkdir -p $STAGE
openssl req -newkey rsa:2048 -nodes -subj "/CN=rayvast-$NAME" -keyout $STAGE/node.key -out $STAGE/node.csr 2>/dev/null
printf "subjectAltName=IP:%s,IP:127.0.0.1\n" "$IP" > $STAGE/node.ext
openssl x509 -req -in $STAGE/node.csr -CA $SEC/ca.crt -CAkey $SEC/ca.key -CAcreateserial -days 2 -extfile $STAGE/node.ext -out $STAGE/node.crt 2>/dev/null
openssl verify -CAfile $SEC/ca.crt $STAGE/node.crt
openssl x509 -in $STAGE/node.crt -noout -ext subjectAltName | tail -1
cp $SEC/ca.crt $SEC/token $STAGE/
cat > $STAGE/worker.env <<ENV
RAY_USE_TLS=1
RAY_TLS_SERVER_CERT=/opt/rayvast/node.crt
RAY_TLS_SERVER_KEY=/opt/rayvast/node.key
RAY_TLS_CA_CERT=/opt/rayvast/ca.crt
RAY_AUTH_MODE=token
RAY_AUTH_TOKEN_PATH=/opt/rayvast/token
LD_PRELOAD=/opt/rayvast/librayvast_shim.so
RAYVAST_PUBLIC_IP=$IP
RAYVAST_HEAD_IP=$(curl -s -X PUT http://169.254.169.254/latest/api/token -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' | xargs -I{} curl -s -H 'X-aws-ec2-metadata-token: {}' http://169.254.169.254/latest/meta-data/public-ipv4)
RAYVAST_SHIM_LOG=/opt/rayvast/shim.log
ENV
SSHOPT="-i $HOME/.ssh/rayvast_head -o UserKnownHostsFile=$HOME/.ssh/known_hosts_rayvast -o ConnectTimeout=10"
scp -q $SSHOPT -P $SSHP $STAGE/ca.crt $STAGE/node.crt $STAGE/node.key $STAGE/token $STAGE/worker.env \
  $BASE/shim/librayvast_shim.so $BASE/tests/worker_start.sh $BASE/tests/battery_worker_check.py ray@$IP:/opt/rayvast/ 2>&1
ssh $SSHOPT -p $SSHP ray@$IP 'cd /opt/rayvast && chmod 600 ca.crt node.crt node.key token worker.env && chmod 644 librayvast_shim.so && ls -l /opt/rayvast && sha256sum librayvast_shim.so'

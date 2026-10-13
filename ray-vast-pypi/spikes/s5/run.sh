cd ~/s5 && bash gen_certs.sh 35.91.242.90 >/dev/null && chmod 644 pki/*
pkill -f "[s]5/server.py"; nohup python3 ~/s5/server.py >server.log 2>&1 &
sleep 1
CA=$(cat pki/ca.sha256); ROG=$(cat pki/rogue.sha256)
docker pull -q python:3.9-slim >/dev/null; 
run() { # label image python env...
  lbl=$1; img=$2; py=$3; shift 3
  out=$(docker run --rm --name s5-$RANDOM --network host -v ~/s5:/s5:ro -e HEAD=127.0.0.1 -e PLAIN_PORT=18000 -e MTLS_PORT=18001 -e CA_SHA256=$CA -e NODE_CRT=/s5/pki/node.crt -e NODE_KEY=/s5/pki/node.key "$@" $img $py -I /s5/client.py 2>&1 | tail -3 | tr '\n' ' ')
  echo "$lbl | $out"
}
for t in "3.11 rayproject/ray:2.59.0-py311-cpu /home/ray/anaconda3/bin/python" "3.9 python:3.9-slim python"; do
 set -- $t; v=$1; img=$2; py=$3
 echo "=== $v"
 docker run --rm --name s5-v --network host $img $py -c "import ssl,sys;print(sys.version.split()[0], ssl.OPENSSL_VERSION)"
 run happy $img $py
 run wrong-fp $img $py -e CA_SHA256=$ROG
 run rogue-cert-real-ca $img $py -e PLAIN_PORT=18002
 run rogue-cert-rogue-ca $img $py -e PLAIN_PORT=18004
 run no-ip-SAN $img $py -e PLAIN_PORT=18003
 run hb-no-client-cert $img $py -e NODE_CRT= -e NODE_KEY=
 run hb-rogue-client-cert $img $py -e NODE_CRT=/s5/pki/rogenode.crt -e NODE_KEY=/s5/pki/rogenode.key
done
pkill -f "[s]5/server.py"; docker ps -a --format '{{.Names}}' | grep ^s5 

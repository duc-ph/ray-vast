#!/bin/bash
# Host side: make test CA, head cert (SAN IP), node cert, rogue CA + rogue certs, no-SAN-IP head cert.
set -e
IP=${1:?public ip}
mkdir -p ~/s5/pki && cd ~/s5/pki && rm -f *
mkca() { openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -keyout $1.key -out $1.pem -days 2 -subj "/CN=$1" -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign" 2>/dev/null; }
mkleaf() { # name ca san eku
  openssl req -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -keyout $1.key -out $1.csr -subj "/CN=$1" 2>/dev/null
  printf "subjectAltName=$3\nextendedKeyUsage=$4\nbasicConstraints=CA:FALSE\n" > $1.ext
  openssl x509 -req -in $1.csr -CA $2.pem -CAkey $2.key -CAcreateserial -out $1.crt -days 2 -extfile $1.ext 2>/dev/null
}
mkca ca; mkca rogue
mkleaf head ca "IP:$IP,IP:127.0.0.1" serverAuth
mkleaf node ca "DNS:node" clientAuth
mkleaf roguehead rogue "IP:$IP,IP:127.0.0.1" serverAuth
mkleaf rogenode rogue "DNS:node" clientAuth
mkleaf nosan ca "IP:10.9.9.9,DNS:head.test" serverAuth
openssl x509 -in ca.pem -outform DER | sha256sum | cut -d' ' -f1 > ca.sha256
openssl x509 -in rogue.pem -outform DER | sha256sum | cut -d' ' -f1 > rogue.sha256
ls

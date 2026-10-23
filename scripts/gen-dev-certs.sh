#!/usr/bin/env bash
# Generates a throwaway CA and one mTLS certificate per component for local
# runs and the Docker demo. Never use these outside development.
#
#   scripts/gen-dev-certs.sh [out-dir]   (default: certs/)
set -euo pipefail

out="${1:-certs}"
mkdir -p "$out"
cd "$out"

openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 30 \
  -subj "/CN=valve-dev-ca" -keyout ca.key -out ca.crt 2>/dev/null

for name in configurator controller global-controller aggregator agent; do
  openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
    -subj "/CN=$name" -keyout "$name.key" -out "$name.csr" 2>/dev/null
  openssl x509 -req -in "$name.csr" -CA ca.crt -CAkey ca.key -CAcreateserial -days 30 \
    -extfile <(printf "subjectAltName=DNS:%s,DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth,clientAuth\nkeyUsage=digitalSignature" "$name") \
    -out "$name.crt" 2>/dev/null
  rm "$name.csr"
done
rm -f ca.srl
chmod 600 ./*.key
echo "wrote CA and certificates for configurator, controller, global-controller, aggregator, agent to $out/"

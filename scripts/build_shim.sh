#!/bin/sh
# Build the shim in manylinux2014 (glibc 2.17) and check it. Run where Docker works.
# The build goes to a temp dir; compare the printed sha256 with the committed .so.
set -eu
dir=$(cd "$(dirname "$0")/../src/ray_vast/shim" && pwd)
out=$(mktemp -d)
docker run --rm -v "$dir":/src:ro -v "$out":/out quay.io/pypa/manylinux2014_x86_64 sh -eu -c '
  gcc -shared -fPIC -O2 -o /out/librayvast_shim.so /src/rayvast_shim.c -ldl
  sha256sum /out/librayvast_shim.so
  symbols=$(nm -D /out/librayvast_shim.so)
  echo "$symbols"
  echo "$symbols" | grep -q " T bind$" || { echo "missing T bind" >&2; exit 1; }
  echo "$symbols" | grep -q " T connect$" || { echo "missing T connect" >&2; exit 1; }
  newest=$(echo "$symbols" | grep -o "GLIBC_[0-9.]*" | sed "s/GLIBC_//" | sort -V | tail -1)
  [ -z "$newest" ] || [ "$(printf "%s\n2.17\n" "$newest" | sort -V | tail -1)" = "2.17" ] || { echo "GLIBC_$newest is above 2.17" >&2; exit 1; }
  echo "ok: symbols and glibc version"
'
echo "built: $out/librayvast_shim.so"

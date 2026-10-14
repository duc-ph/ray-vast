#!/bin/bash
# Safety rule 7 of docs/test-plan.md: print the names (never the text) of the files under DIR that hold a secret.
# Secrets: the values in the TOKENFILEs (the cluster token, join tokens; one value per line), the vast key
# (VAST_API_KEY or ~/.config/vastai/vast_api_key), the PEM private key header, and values after instance_api_key,
# CONTAINER_API_KEY= and "Bearer ". Expected: no output, exit code 0. Exit code 1 when a file matches.
# Usage: bash scan_secrets.sh DIR TOKENFILE...
set -euo pipefail
dir=$1
shift
umask 077
patterns=$(mktemp)
trap 'rm -f "$patterns"' EXIT
# grep -f treats an empty line as "match everything", so drop empty lines.
cat "$@" /dev/null | tr -d '\r' | grep -v '^[[:space:]]*$' >> "$patterns" || true
key_file=$HOME/.config/vastai/vast_api_key
if [ -n "${VAST_API_KEY:-}" ]; then
  printf '%s\n' "$VAST_API_KEY" >> "$patterns"
elif [ -s "$key_file" ]; then
  tr -d '[:space:]' < "$key_file" >> "$patterns"
  echo >> "$patterns"
else
  echo "scan_secrets: warning: no vast key found, so it is not in the scan" >&2
fi
{
  [ -s "$patterns" ] && grep -rlF -f "$patterns" -- "$dir" || true
  grep -rlE -e 'PRIVATE[ ]KEY' \
    -e 'instance_api_key"?[[:space:]]*[:=][[:space:]]*"?[A-Za-z0-9]' \
    -e 'CONTAINER_API_KEY=[A-Za-z0-9]' \
    -e 'Bearer [A-Za-z0-9]' -- "$dir" || true
} | sort -u | { found=0; while IFS= read -r f; do echo "$f"; found=1; done; exit $found; }

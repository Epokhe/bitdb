#!/usr/bin/env bash
set -euo pipefail

DIR="$(pwd)"           # assumed to be bitdb working directory
FILE="../segment.dat"  # adjust if needed
RANDRATE=10000
SEQBS_SMALL=32768      # 32 KiB
RANDBS_SMALL=4096      # 4 KiB

echo "=== DEFAULT BLOCK SIZES (1 MiB seq, 4 KiB rand) ==="
for i in $(seq 1 10); do
  echo
  echo "--- Run #$i (shared-FD) ---"
  go run iotest.go -mode=mix-shared -file="${FILE}" -randrate="${RANDRATE}"

  echo "--- Run #$i (split-FD) ---"
  go run iotest.go -mode=mix-split  -file="${FILE}" -randrate="${RANDRATE}"
done

echo
echo "=== SMALL BLOCKS (32 KiB seq, 4 KiB rand) ==="
for i in $(seq 1 10); do
  echo
  echo "--- Run #$i (shared-FD) ---"
  go run iotest.go -mode=mix-shared -file="${FILE}" \
      -seqbs="${SEQBS_SMALL}" -randbs="${RANDBS_SMALL}" -randrate="${RANDRATE}"

  echo "--- Run #$i (split-FD) ---"
  go run iotest.go -mode=mix-split  -file="${FILE}" \
      -seqbs="${SEQBS_SMALL}" -randbs="${RANDBS_SMALL}" -randrate="${RANDRATE}"
done

echo
echo "All tests complete."


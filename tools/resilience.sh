#!/usr/bin/env bash
# Runs the failure-model experiments used in REPORT.md.
# usage: tools/resilience.sh [numNodes] [numRequests]
set -u
N=${1:-1000}
R=${2:-10}
cd "$(dirname "$0")/.."
OUT=results
mkdir -p "$OUT"

run() {
  local name=$1; shift
  echo "=== $name: ./project3 $N $R --verify $* ==="
  ./project3 "$N" "$R" --verify "$@" | tee "$OUT/$name.txt" | \
    grep -E "Delivery success|Average number|Data lost|Corrupt values|Routing loops|Re-routes|Repairs|Nodes killed|Links failed|Misdelivered"
}

run baseline
for k in 5 10 25 50; do
  run "kill_$k"     --kill "$k"
done
for l in 10 25 50; do
  run "link_temp_$l" --link-fail "$l" --link-mode temp
  run "link_perm_$l" --link-fail "$l" --link-mode perm
done
run "kill10_link25"  --kill 10 --link-fail 25 --link-mode perm
run "kill25_norepl"  --kill 25 --replicas 0

#!/usr/bin/env bash
# Runs the scaling benchmarks and saves raw logs in results/scale_<N>.txt
set -eu
cd "$(dirname "$0")/.."
mkdir -p results

run_scale() {
  local n=$1 r=$2; shift 2
  echo "=== scale_${n}: ./project3 $n $r $* ==="
  /usr/bin/time -l ./project3 "$n" "$r" "$@" > "results/scale_${n}.txt" 2>&1
  grep -E "Network built|Requests completed|Request phase|log16|Max hops|Hop distribution|Misdelivered|Average number|real|maximum resident" "results/scale_${n}.txt"
}

run_scale 100   3 --verify
run_scale 1000  8 --verify
run_scale 10000 3 --verify
run_scale 25000 2
run_scale 50000 2


#!/usr/bin/env bash
set -Eeuo pipefail

usage() {
    echo "usage: $0 WRITABLE_ZAKURA_CACHE ZAKURA_CONFIG [RUN_ROOT]" >&2
    echo "       PERF=1 FETCH_WORKERS=8 INDEX_MAP_SIZE=68719476736 $0 WRITABLE_ZAKURA_CACHE ZAKURA_CONFIG [RUN_ROOT]" >&2
    exit 2
}

[[ $# -ge 2 && $# -le 3 ]] || usage

repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
snapshot=$(realpath "$1")
config=$(realpath "$2")
run_root=${3:-"$repo/benchmark-runs"}
mkdir -p "$run_root"
run_root=$(realpath "$run_root")
run="$run_root/$(date -u +%Y%m%dT%H%M%SZ)-$$"
index="$run/ztreamer-index"
metrics=${METRICS_LISTEN:-127.0.0.1:9999}
fetch_workers=${FETCH_WORKERS:-4}
index_map_size=${INDEX_MAP_SIZE:-17179869184}

[[ -d "$snapshot" ]] || { echo "snapshot cache is not a directory: $snapshot" >&2; exit 1; }
[[ -f "$config" ]] || { echo "Zakura config does not exist: $config" >&2; exit 1; }
command -v curl >/dev/null || { echo "curl is required" >&2; exit 1; }
command -v setsid >/dev/null || { echo "setsid is required" >&2; exit 1; }
if [[ ${PERF:-0} == 1 ]]; then
    command -v perf >/dev/null || { echo "perf is required when PERF=1" >&2; exit 1; }
fi

mkdir -p "$run"
echo "Using Zakura state in place at $snapshot (Zakura will update it)"

echo "Building ztreamerd"
CARGO_PROFILE_RELEASE_DEBUG=1 cargo build --manifest-path "$repo/Cargo.toml" --release -p ztreamerd

command=(
    "$repo/target/release/ztreamerd"
    --zakura-config "$config"
    --index-dir "$index"
    --index-map-size "$index_map_size"
    --fetch-workers "$fetch_workers"
    --metrics-listen "$metrics"
    --index-only
)
if [[ ${PERF:-0} == 1 ]]; then
    command=(perf record -F 999 -g --call-graph dwarf -o "$run/perf.data" -- "${command[@]}")
fi
timer=()
if [[ -x /usr/bin/time ]]; then
    timer=(/usr/bin/time -v -o "$run/time.txt")
fi

{
    echo "started_utc=$(date -u --iso-8601=seconds)"
    echo "snapshot=$snapshot"
    echo "config=$config"
    echo "ztreamer_commit=$(git -C "$repo" rev-parse HEAD)"
    echo "ztreamer_dirty_files=$(git -C "$repo" status --porcelain | wc -l)"
    echo "zakura_commit=$(git -C "$repo/../zakura" rev-parse HEAD)"
    echo "zakura_dirty_files=$(git -C "$repo/../zakura" status --porcelain | wc -l)"
    echo "logical_cpus=$(nproc)"
    echo "kernel=$(uname -srmo)"
    printf 'command='; printf '%q ' "${command[@]}"; echo
} > "$run/metadata.txt"

echo "Running benchmark; artifacts: $run"
started_seconds=$SECONDS
set +e
ZAKURA_STATE__CACHE_DIR="$snapshot" \
ZAKURA_STATE__EPHEMERAL=false \
ZAKURA_STATE__STORAGE_MODE=archive \
setsid "${timer[@]}" "${command[@]}" > >(tee "$run/ztreamerd.log") 2>&1 &
pid=$!
set -e

cleanup() {
    kill -INT -- "-$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
}
trap cleanup INT TERM EXIT

echo "unix_time,metric,value" > "$run/metrics.csv"
while kill -0 "$pid" 2>/dev/null; do
    now=$(date +%s.%N)
    curl -fsS "http://$metrics/metrics" 2>/dev/null |
        awk -v now="$now" '
            /^(ztreamer_index_|state_finalized_block_height|sync_estimated_)/ && $1 !~ /^#/ {
                print now "," $1 "," $2
            }
        ' >> "$run/metrics.csv" || true
    sleep 1
done

set +e
wait "$pid"
status=$?
set -e
trap - INT TERM EXIT
echo "finished_utc=$(date -u --iso-8601=seconds)" >> "$run/metadata.txt"
echo "exit_status=$status" >> "$run/metadata.txt"
echo "wall_seconds=$((SECONDS - started_seconds))" >> "$run/time.txt"

if (( status != 0 )); then
    echo "Benchmark failed with status $status; see $run/ztreamerd.log" >&2
    exit "$status"
fi

echo "Benchmark complete: $run"

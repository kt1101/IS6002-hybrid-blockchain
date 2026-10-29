#!/usr/bin/env bash
# Generate YCSB data for ycsbbench with YCSB 0.17.0 (BasicDB) inside an
# eclipse-temurin:11-jre container; the local YCSB copy in
# scripts/temp/ycsb-0.17.0 is mounted, nothing is downloaded.
#
# Output (default OUT_DIR=results/ycsb, gitignored), for each workload w:
#   <w>.load.dat   RECORDS INSERT lines   (ycsbbench --load-path)
#   <w>.run.dat    OPS READ/UPDATE lines  (ycsbbench --run-path)
# e.g. a.load.dat / a.run.dat for workloads/uniform/workloada. Only operation
# lines (INSERT/READ/UPDATE) are kept; the Java command line and the property
# dump that ycsb.sh prints to stdout are stripped.
#
# Values: fieldcount=1, fieldlength=1000, so every INSERT and every UPDATE
# carries one 1 KB field (the original data used 10 x 100 B fields, which makes
# workload-A updates write only 100 B).
# The load phase does not depend on the workload mix (same recordcount, field
# layout and key order), so one load file is generated and hard-linked as
# <w>.load.dat for every workload (saves ~10 MB per extra workload).
#
# Records: 10k by default for every backend (~10 MB of values per node). The
# original kafka-orig server keeps an unpruned in-memory Merkle tree, and a
# 100k-record load OOMs any per-node memory cap that still lets N=16 nodes fit
# in the Docker VM. OPS stays 100k (the run file is replayed in a loop).
#
# Env: RECORDS=10000 OPS=100000 WORKLOADS="a b c" OUT_DIR=results/ycsb
#      YCSB_HOME=scripts/temp/ycsb-0.17.0 IMAGE=eclipse-temurin:11-jre
# Example (small data set for a quick tm-orig smoke run):
#   RECORDS=2000 OPS=2000 WORKLOADS=a OUT_DIR=results/ycsb/smoke experiments/ycsb/gen.sh
set -euo pipefail
. "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/lib.sh"

RECORDS="${RECORDS:-10000}"
OPS="${OPS:-100000}"
WORKLOADS="${WORKLOADS:-a b c}"
OUT_DIR="${OUT_DIR:-$HBDB_ROOT/results/ycsb}"
YCSB_HOME="${YCSB_HOME:-$HBDB_ROOT/scripts/temp/ycsb-0.17.0}"
IMAGE="${IMAGE:-eclipse-temurin:11-jre}"
FIELDCOUNT=1
FIELDLENGTH=1000

hbdb_require_uint RECORDS "$RECORDS"
hbdb_require_uint OPS "$OPS"
[ "$RECORDS" -ge 1 ] && [ "$OPS" -ge 1 ] || die "RECORDS and OPS must be >= 1"
[ -x "$YCSB_HOME/bin/ycsb.sh" ] || die "YCSB not found at $YCSB_HOME (expected bin/ycsb.sh)"
for w in $WORKLOADS; do
  [ -f "$HBDB_ROOT/workloads/uniform/workload$w" ] || die "no workload file workloads/uniform/workload$w"
done
mkdir -p "$OUT_DIR"
OUT_DIR="$(cd "$OUT_DIR" && pwd)"

# ycsb PHASE WORKLOAD OUTFILE: run one YCSB phase, keep only operation lines,
# write OUTFILE atomically.
ycsb() {
  local phase="$1" w="$2" out="$3"
  log "ycsb $phase workload$w (records=$RECORDS ops=$OPS) -> $out"
  docker run --rm \
    -v "$YCSB_HOME:/ycsb:ro" \
    -v "$HBDB_ROOT/workloads:/workloads:ro" \
    -v "$OUT_DIR:/out" \
    "$IMAGE" bash -c '
      set -euo pipefail
      /ycsb/bin/ycsb.sh "$0" basic -P "/workloads/uniform/workload$1" \
        -p recordcount="$2" -p operationcount="$3" \
        -p fieldcount="$4" -p fieldlength="$5" \
        | grep -E "^(INSERT|READ|UPDATE) " > "/out/.$6.tmp"
    ' "$phase" "$w" "$RECORDS" "$OPS" "$FIELDCOUNT" "$FIELDLENGTH" "$(basename "$out")"
  mv "$OUT_DIR/.$(basename "$out").tmp" "$out"
}

# count FILE PATTERN
count() {
  grep -cE "$2" "$1" || true
}

first=""
for w in $WORKLOADS; do
  if [ -z "$first" ]; then
    first="$w"
    ycsb load "$w" "$OUT_DIR/$w.load.dat"
    got="$(count "$OUT_DIR/$w.load.dat" '^INSERT ')"
    [ "$got" -eq "$RECORDS" ] || die "$w.load.dat has $got INSERT lines, expected $RECORDS"
  else
    log "linking $w.load.dat -> $first.load.dat"
    rm -f "$OUT_DIR/$w.load.dat"
    ln "$OUT_DIR/$first.load.dat" "$OUT_DIR/$w.load.dat"
  fi
  ycsb run "$w" "$OUT_DIR/$w.run.dat"
  got="$(count "$OUT_DIR/$w.run.dat" '^(READ|UPDATE|INSERT) ')"
  [ "$got" -eq "$OPS" ] || die "$w.run.dat has $got operation lines, expected $OPS"
done

ls -l "$OUT_DIR" >&2
log "YCSB data ready in $OUT_DIR"

#!/usr/bin/env bash
# P0 baseline: the paper's original Veritas backends (kafka-orig, tm-orig) on
# the Docker cluster. Appends rows to results/raw/<EXP>.csv through run_one.sh.
#
#   experiments/exp/p0_baseline.sh                       # parts (a) and (b)
#   PART=a REPS=1 DRY_RUN=1 experiments/exp/p0_baseline.sh   # only print the plan
#
# Parts (for each backend in kafka-orig, tm-orig; REPS reps each):
#   (a) N=4, CLIENTS in {4,16,64,256}.
#   (b) CLIENTS = the backend's best CLIENTS, N in {4,8,16}.
#       Best CLIENTS = argmax of the mean tps over the reps at N=4
#       (experiments/exp/best_clients.py on results/raw/<EXP>.csv, i.e. the
#       part (a) rows; ties -> smaller CLIENTS), unless overridden by
#       BEST_CLIENTS_KAFKA_ORIG / BEST_CLIENTS_TM_ORIG. Part (b) needs the part
#       (a) rows of that backend (or the override). Completeness guard:
#       without an override, part (b) of a backend runs only if every
#       (CLIENTS in {4,16,64,256}, rep 1..REPS) row exists at N=4; otherwise
#       the missing rows are logged, part (b) of that backend is skipped and
#       counted as missing (exit 4). Best CLIENTS is never picked from partial
#       data, so a later fill-in cannot change it and orphan N=8/16 rows.
#
# Env (defaults):
#   PART=all (a | b | all)   REPS=3   EXP=p0   DRY_RUN=0 (1: print the
#   run_one.sh invocations that would run, run nothing, write nothing)
#   EXTRA_REPS=2 (max extra reps for a noisy configuration, 0 = never)
#   BEST_CLIENTS_KAFKA_ORIG, BEST_CLIENTS_TM_ORIG (optional, positive ints)
#   Everything else (DURATION=60s, WARMUP=10s, CPUS, MEM, LOAD_*, ...) is
#   inherited by run_one.sh. COMMIT_MODE is unset here: each backend runs in
#   its native mode (kafka-orig async, tm-orig sync).
#
# Noise rule (plan: std/mean > 10% -> rerun): once a configuration has all its
# REPS rows, CV = sample std / mean of tps over its rows. If CV > 10%, rep
# REPS+1, then REPS+2 (at most EXTRA_REPS), are run, stopping as soon as CV <=
# 10%. Extra reps are ordinary rows with higher rep numbers, so resuming
# works unchanged, and best_clients.py averages over every rep present. The
# summary prints the CV of each configuration and flags those still above 10%
# (informational, not an error). Extra reps add rows beyond the 36 below.
#
# Resumable: a (backend, n, clients, rep) that already has a row in
# results/raw/<EXP>.csv is skipped. The CSV has no "part" column, so part (b)'s
# N=4 rows are told apart from part (a)'s only if their CLIENTS differ (only
# possible with a BEST_CLIENTS_* override outside {4,16,64,256}). Normally they
# coincide: the existing part (a) row counts, N=4 is not run a second time, and
# the distinct configurations are 2 x (4 + 2) x REPS (36 for REPS=3), not
# the 42 of the plan. A run that failed leaves no row, so it is tried again on
# the next invocation. Never reuse an EXP with a different DURATION, WARMUP or
# workload: existing rows would be taken as done.
#
# Failures: run_one.sh exit 2 (cluster unhealthy) or 3 (bench failed or a
# container died) is retried once on a fresh cluster. If it still fails,
# "<utc> backend n clients rep rc" is appended to results/raw/<EXP>.failures
# and the batch continues (the .failures file is append-only). Exit 1 (harness or input error), or any other exit
# code (e.g. 130 after Ctrl-C), aborts the batch with that code.
#
# At the end a summary lists rows present vs expected and this run's failures.
# Exit: 0 all expected rows present (or DRY_RUN), 4 finished with missing rows,
#       failures or a skipped part (b), 1 bad input, aborted by run_one.sh, or
#       other abort.
set -euo pipefail
. "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/lib.sh"

PART="${PART:-all}"
REPS="${REPS:-3}"
EXP="${EXP:-p0}"
DRY_RUN="${DRY_RUN:-0}"
BACKENDS="kafka-orig tm-orig"
CLIENTS_A="4 16 64 256"
NS_B="4 8 16"
CV_MAX=0.10
EXTRA_REPS="${EXTRA_REPS:-2}"
# The brief: COMMIT_MODE is never passed; run_one.sh picks the native mode.
unset COMMIT_MODE

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
run_one="$HBDB_ROOT/experiments/run_one.sh"
best_py="$here/best_clients.py"
cv_py="$here/cv.py"
csv="$HBDB_RESULTS_RAW/$EXP.csv"
failures_file="$HBDB_RESULTS_RAW/$EXP.failures"

case "$PART" in a | b | all) ;; *) die "PART must be a, b or all, got '$PART'" ;; esac
hbdb_require_uint REPS "$REPS"
[ "$REPS" -ge 1 ] || die "REPS must be >= 1"
case "$EXP" in
  '' | *[!A-Za-z0-9._-]*) die "EXP must match [A-Za-z0-9._-]+, got '$EXP'" ;;
esac
case "$DRY_RUN" in 0 | 1) ;; *) die "DRY_RUN must be 0 or 1, got '$DRY_RUN'" ;; esac
hbdb_require_uint EXTRA_REPS "$EXTRA_REPS"
[ -x "$run_one" ] || die "missing or not executable: $run_one"
if [ -n "${BEST_CLIENTS_KAFKA_ORIG:-}" ]; then
  hbdb_require_uint BEST_CLIENTS_KAFKA_ORIG "$BEST_CLIENTS_KAFKA_ORIG"
  [ "$BEST_CLIENTS_KAFKA_ORIG" -ge 1 ] || die "BEST_CLIENTS_KAFKA_ORIG must be >= 1"
fi
if [ -n "${BEST_CLIENTS_TM_ORIG:-}" ]; then
  hbdb_require_uint BEST_CLIENTS_TM_ORIG "$BEST_CLIENTS_TM_ORIG"
  [ "$BEST_CLIENTS_TM_ORIG" -ge 1 ] || die "BEST_CLIENTS_TM_ORIG must be >= 1"
fi
if [ -s "$csv" ]; then
  [ "$(head -n 1 "$csv")" = "$HBDB_CSV_HEADER" ] || die "$csv has a different header"
fi

PLAN=""        # distinct "backend n clients rep" lines, in plan order
FAILED_NOW=""  # failures of this invocation, same format as the .failures file
SKIPPED_B=""   # part (b) skipped (incomplete part (a)): one line per backend
CFGS=""        # distinct "backend n clients" configurations, in plan order

# has_row BACKEND N CLIENTS REP: 0 if the CSV already has such a row.
has_row() {
  [ -s "$csv" ] || return 1
  awk -F, -v b="$1" -v n="$2" -v c="$3" -v r="$4" \
    'NR > 1 && $4 == b && $5 == n && $7 == c && $12 == r { f = 1 } END { exit !f }' "$csv"
}

add_plan() {
  if ! printf '%s\n' "$PLAN" | grep -Fxq "$1"; then
    PLAN="${PLAN:+$PLAN
}$1"
  fi
}

# override_for BACKEND: the BEST_CLIENTS_* override of the backend (maybe empty).
override_for() {
  case "$1" in
    kafka-orig) echo "${BEST_CLIENTS_KAFKA_ORIG:-}" ;;
    tm-orig) echo "${BEST_CLIENTS_TM_ORIG:-}" ;;
  esac
}

# part_a_missing BACKEND: set MISSING_A to the "clients/rep" of every part (a)
# row (N=4, CLIENTS_A, rep 1..REPS) that is not in the CSV, space-separated;
# those rows are also added to the plan so the summary lists them as missing.
# (Sets a global, not a subshell result, because add_plan changes PLAN.)
part_a_missing() {
  local c r
  MISSING_A=""
  for c in $CLIENTS_A; do
    r=1
    while [ "$r" -le "$REPS" ]; do
      if ! has_row "$1" 4 "$c" "$r"; then
        add_plan "$1 4 $c $r"
        MISSING_A="$MISSING_A$c/$r "
      fi
      r=$((r + 1))
    done
  done
}

# cfg_cv BACKEND N CLIENTS: print "<cv|NA> <rows>" (see cv.py).
cfg_cv() {
  if [ -s "$csv" ]; then
    python3 "$cv_py" "$csv" "$1" "$2" "$3" || echo "NA 0"
  else
    echo "NA 0"
  fi
}

# cv_high CV: 0 if CV is a number above CV_MAX.
cv_high() {
  [ "$1" != NA ] && awk -v c="$1" -v m="$CV_MAX" 'BEGIN { exit !(c + 0 > m + 0) }'
}

# extra_reps BACKEND N CLIENTS: noise rule, run after the REPS reps of a
# configuration (see header). Needs all REPS rows; stops once CV <= CV_MAX.
extra_reps() {
  local b="$1" n="$2" c="$3" r=1 k=1 cv rows
  case " $CFGS " in *" $b:$n:$c "*) ;; *) CFGS="${CFGS:+$CFGS }$b:$n:$c" ;; esac
  while [ "$r" -le "$REPS" ]; do
    has_row "$b" "$n" "$c" "$r" || return 0
    r=$((r + 1))
  done
  while [ "$k" -le "$EXTRA_REPS" ]; do
    read -r cv rows <<EOT
$(cfg_cv "$b" "$n" "$c")
EOT
    cv_high "$cv" || return 0
    log "CV $cv > $CV_MAX over $rows rows of $b n=$n clients=$c: running extra rep $k/$EXTRA_REPS (rep $((REPS + k)))"
    run_cfg "$b" "$n" "$c" "$((REPS + k))"
    k=$((k + 1))
  done
}

summary() {
  local present=0 expected=0 missing="" b n c r cfg cv rows flag high=""
  while IFS=' ' read -r b n c r; do
    [ -n "$b" ] || continue
    expected=$((expected + 1))
    if has_row "$b" "$n" "$c" "$r"; then
      present=$((present + 1))
    else
      missing="${missing}  $b n=$n clients=$c rep=$r"$'\n'
    fi
  done <<EOT
$PLAN
EOT
  {
    echo "---- summary ($EXP, PART=$PART, REPS=$REPS) ----"
    echo "rows present: $present / $expected expected (distinct configurations in $csv)"
    if [ -n "$SKIPPED_B" ]; then
      echo "part (b) SKIPPED (counts as missing):"
      printf '%s\n' "$SKIPPED_B" | sed 's/^/  /'
    fi
    if [ -n "$missing" ]; then
      echo "missing:"
      printf '%s' "$missing"
    fi
    if [ -n "$CFGS" ]; then
      echo "CV of tps (sample std / mean over all reps; threshold $CV_MAX):"
      for cfg in $CFGS; do
        IFS=: read -r b n c <<EOT
$cfg
EOT
        read -r cv rows <<EOT
$(cfg_cv "$b" "$n" "$c")
EOT
        flag=""
        if cv_high "$cv"; then
          flag="  ABOVE 10%"
          high="$high $b/n$n/c$c"
        fi
        echo "  $b n=$n clients=$c: cv=$cv ($rows rows)$flag"
      done
      if [ -n "$high" ]; then
        echo "still above 10% after the extra reps:$high"
      fi
    fi
    if [ -n "$FAILED_NOW" ]; then
      echo "failures in this run (also in $failures_file):"
      printf '%s\n' "$FAILED_NOW" | sed 's/^/  /'
    else
      echo "failures in this run: none"
    fi
  } >&2
  SUMMARY_EXPECTED=$expected
  SUMMARY_PRESENT=$present
}

# run_cfg BACKEND N CLIENTS REP
run_cfg() {
  local b="$1" n="$2" c="$3" r="$4" rc attempt
  add_plan "$b $n $c $r"
  if has_row "$b" "$n" "$c" "$r"; then
    log "skip (row exists): $b n=$n clients=$c rep=$r"
    return 0
  fi
  if [ "$DRY_RUN" = 1 ]; then
    echo "DRY_RUN: EXP=$EXP BACKEND=$b N=$n CLIENTS=$c REP=$r $run_one"
    return 0
  fi
  rc=0
  for attempt in 1 2; do
    log "run $b n=$n clients=$c rep=$r (attempt $attempt/2)"
    rc=0
    EXP="$EXP" BACKEND="$b" N="$n" CLIENTS="$c" REP="$r" "$run_one" || rc=$?
    case "$rc" in
      0)
        has_row "$b" "$n" "$c" "$r" || { log "run_one.sh exited 0 but $csv has no row for $b n=$n clients=$c rep=$r"; summary; exit 1; }
        return 0
        ;;
      2 | 3)
        log "run_one.sh exit $rc for $b n=$n clients=$c rep=$r"
        ;;
      *)
        log "run_one.sh exit $rc: aborting the batch"
        summary
        exit "$rc"
        ;;
    esac
  done
  local entry
  entry="$(date -u +%Y-%m-%dT%H:%M:%SZ) $b $n $c $r $rc"
  echo "$entry" >>"$failures_file"
  FAILED_NOW="${FAILED_NOW:+$FAILED_NOW
}$entry"
  log "giving up on $b n=$n clients=$c rep=$r (rc=$rc), continuing; recorded in $failures_file"
  return 0
}

[ "$DRY_RUN" = 1 ] || mkdir -p "$HBDB_RESULTS_RAW"

if [ "$PART" = a ] || [ "$PART" = all ]; then
  log "part (a): N=4, CLIENTS in {$CLIENTS_A}, $REPS rep(s)"
  for b in $BACKENDS; do
    for c in $CLIENTS_A; do
      r=1
      while [ "$r" -le "$REPS" ]; do
        run_cfg "$b" 4 "$c" "$r"
        r=$((r + 1))
      done
      extra_reps "$b" 4 "$c"
    done
  done
fi

if [ "$PART" = b ] || [ "$PART" = all ]; then
  for b in $BACKENDS; do
    best="$(override_for "$b")"
    if [ -z "$best" ]; then
      part_a_missing "$b"
      if [ -n "$MISSING_A" ]; then
        log "part (b) $b skipped: part (a) rows missing at N=4 (clients/rep): $MISSING_A"
        SKIPPED_B="${SKIPPED_B:+$SKIPPED_B
}$b: part (a) rows missing (clients/rep): $MISSING_A"
        continue
      fi
      if ! best="$(python3 "$best_py" "$csv" "$b")"; then
        log "part (b) $b skipped: best_clients.py failed"
        SKIPPED_B="${SKIPPED_B:+$SKIPPED_B
}$b: best_clients.py failed on $csv"
        continue
      fi
    fi
    log "part (b) $b: CLIENTS=$best, N in {$NS_B}, $REPS rep(s)"
    for n in $NS_B; do
      r=1
      while [ "$r" -le "$REPS" ]; do
        run_cfg "$b" "$n" "$best" "$r"
        r=$((r + 1))
      done
      extra_reps "$b" "$n" "$best"
    done
  done
fi

SUMMARY_EXPECTED=0
SUMMARY_PRESENT=0
summary
if [ "$DRY_RUN" = 1 ]; then
  exit 0
fi
if [ -n "$FAILED_NOW" ] || [ -n "$SKIPPED_B" ] || [ "$SUMMARY_PRESENT" -ne "$SUMMARY_EXPECTED" ]; then
  exit 4
fi
exit 0

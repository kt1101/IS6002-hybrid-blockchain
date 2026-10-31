# shellcheck shell=bash
# Shared machinery of the E1-E6 drivers (experiments/exp/e1_clients.sh ...
# e6_faults.sh). Source it, do not execute it. A driver is:
#
#   . "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/exp_lib.sh"
#   exp_begin e1 "$@"                      # args, env, backends
#   exp_plan() {                           # the matrix: called twice by exp_main
#     for b in $EXP_BACKEND_LIST; do
#       exp_cfg "$b" 4 0 64 100 a 0 none   # BACKEND N F CLIENTS BLOCK WORKLOAD RTT FAULT
#     done
#   }
#   exp_main
#
# exp_main calls exp_plan twice: first with EXP_VALIDATE_ONLY=1 (exp_cfg only
# validates its configuration: fault string, tendermint limits, YCSB files),
# so a bad plan dies before the first run and before any --dry-run output;
# then for real, followed by exp_finish.
#
# exp_cfg runs one configuration for REPS=1..3 (plus the noise-rule reps), each
# through experiments/run_one.sh with COMMIT_MODE=sync, appending to
# results/raw/<EXP>.csv.
#
# Common env (every driver):
#   REPS=3        base repetitions per configuration
#   EXTRA_REPS=2  max extra reps of a noisy configuration (0 = never)
#   EXP           CSV name (default: the driver's, e1 ... e6)
#   BACKENDS      subset/order of "kafka raft tendermint bft2" (backends.sh)
#   DRY_RUN=1     same as --dry-run
#   DURATION=60s WARMUP=10s (E6: DURATION=180s) and everything else run_one.sh
#                 reads (CPUS, MEM, LOAD_*, ...) are inherited by run_one.sh.
#   COMMIT_MODE   is always sync (the plan: E1-E6 compare sync commit); any
#                 other value in the environment is an error.
#   BATCH_FLUSH   unset = veritas default 1 s ("as in the original code") for
#                 E1-E6. A different value (run_one.sh BATCH_FLUSH) needs a
#                 new EXP name, because the CSV has no column for it.
#
# --dry-run: touches nothing (no Docker, no files). stdout has one line per
# planned run: "DRY_RUN: EXP=... BACKEND=... REP=..." if it would run, or
# "DRY_SKIP: ..." if its row already exists; everything else (notes, summary
# with the counts) goes to stderr.
#
# Noise rule (plan: std/mean > 10% -> rerun), as in p0_baseline.sh: once a
# configuration has its REPS rows, CV = sample std / mean of tps over its
# rows (cv.py, restricted to the configuration's own block size, workload,
# RTT, fault and f); if CV > 10% rep REPS+1, then REPS+2 (at most EXTRA_REPS),
# run, stopping as soon as CV <= 10%. Extra reps are ordinary rows with higher
# rep numbers, so resuming works unchanged. The summary prints every CV and
# flags those still above 10% (informational, not an error). An extra rep that
# runs joins the planned runs, so the summary's planned count includes it.
#
# Resumable: a run whose row (backend, n, f, clients, block_size, workload,
# rtt_ms, fault, rep) is already in results/raw/<EXP>.csv is skipped; a run
# that failed leaves no row, so the next invocation tries it again. Never
# reuse an EXP with a different DURATION, WARMUP, BATCH_FLUSH or CPUS/MEM: the
# existing rows would be taken as done.
#
# Failures: run_one.sh exit 2 (cluster unhealthy) or 3 (bench failed, a
# container died, a fault was not injected) is retried once on a fresh
# cluster; if it fails again, "<utc> backend n f clients block workload rtt
# fault rep rc" is appended to results/raw/<EXP>.failures (append-only) and
# the batch continues. Exit 1 (harness or input error) or any other code
# (130 after Ctrl-C) aborts the batch with that code.
#
# CLIENTS of E2-E6 (exp_resolve_clients): the backend's best CLIENTS from E1,
# i.e. best_clients.py on results/raw/$E1_EXP.csv (E1_EXP=e1; N=4 rows; ties
# -> the smaller CLIENTS), used only if E1 has every (CLIENTS in
# {4,16,64,256}, rep 1..REPS) row of the backend (the same completeness guard
# as p0_baseline.sh: never pick the best from partial data). Otherwise that
# backend is skipped, counted as missing (exit 4). Overrides:
# FIXED_CLIENTS=<n> for every backend, or BEST_CLIENTS_<BACKEND>=<n>
# (BEST_CLIENTS_KAFKA, _RAFT, _TENDERMINT, _BFT2) for one.
#
# Exit: 0 all planned rows present (or --dry-run), 4 finished with missing
# rows, failures or a skipped backend, 1 bad input or aborted by run_one.sh.
# Bash 3.2 compatible (the host's bash): no associative arrays, no mapfile.

EXP_LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$EXP_LIB_DIR/../lib.sh"
. "$EXP_LIB_DIR/../faults.sh"
. "$EXP_LIB_DIR/backends.sh"

EXP_CV_MAX=0.10
EXP_CLIENTS_E1="4 16 64 256" # E1's CLIENTS; also what exp_resolve_clients needs
EXP_PLAN=""                  # distinct "backend n f clients block workload rtt fault rep" lines
EXP_CFGS=""                  # distinct "backend n f clients block workload rtt fault" lines
EXP_FAILED_NOW=""            # this invocation's failures (same format as the .failures file)
EXP_SKIPPED=""               # backends skipped (no usable E1 best), one line each
EXP_DRY=0
EXP_RESOLVED_CLIENTS=""
EXP_VALIDATE_ONLY=0
EXP_BEST_PLACEHOLDER="best-of-e1"

# exp_dur_seconds D: whole seconds of a duration like 60s, 500ms, 3m, 1h.
exp_dur_seconds() {
  case "$1" in
    *ms) echo $(((10#${1%ms} + 999) / 1000)) ;;
    *s) echo $((10#${1%s})) ;;
    *m) echo $((10#${1%m} * 60)) ;;
    *h) echo $((10#${1%h} * 3600)) ;;
    *) die "bad duration '$1'" ;;
  esac
}

# exp_usage: print the leading comment block of the driver ($0).
exp_usage() {
  awk 'NR > 1 && /^#/ { sub(/^# ?/, ""); print; next } NR > 1 { exit }' "$0"
}

# exp_hook_for BACKEND: the PRE_DOWN_HOOK run_one.sh gets for this backend
# (print nothing = no hook). A driver redefines it (E2: kafka_ops.sh).
exp_hook_for() {
  return 0
}

# exp_begin DEFAULT_EXP ARGS...: parse the arguments, validate the environment
# and set EXP, REPS, EXTRA_REPS, EXP_CSV, EXP_BACKEND_LIST, ... A driver that
# needs a duration other than 60s sets DURATION before calling it.
exp_begin() {
  local default_exp="$1" a
  shift
  for a in "$@"; do
    case "$a" in
      --dry-run) EXP_DRY=1 ;;
      -h | --help)
        exp_usage
        exit 0
        ;;
      *) die "unknown argument '$a' (only --dry-run and --help)" ;;
    esac
  done
  case "${DRY_RUN:-0}" in
    0) ;;
    1) EXP_DRY=1 ;;
    *) die "DRY_RUN must be 0 or 1, got '$DRY_RUN'" ;;
  esac
  EXP="${EXP:-$default_exp}"
  EXP_DEFAULT="$default_exp"
  case "$EXP" in
    '' | *[!A-Za-z0-9._-]*) die "EXP must match [A-Za-z0-9._-]+, got '$EXP'" ;;
  esac
  REPS="${REPS:-3}"
  EXTRA_REPS="${EXTRA_REPS:-2}"
  hbdb_require_uint REPS "$REPS"
  [ "$REPS" -ge 1 ] || die "REPS must be >= 1"
  hbdb_require_uint EXTRA_REPS "$EXTRA_REPS"
  E1_EXP="${E1_EXP:-e1}"
  case "$E1_EXP" in
    '' | *[!A-Za-z0-9._-]*) die "E1_EXP must match [A-Za-z0-9._-]+, got '$E1_EXP'" ;;
  esac
  # The plan: every comparison runs in sync commit.
  if [ -n "${COMMIT_MODE:-}" ] && [ "$COMMIT_MODE" != sync ]; then
    die "COMMIT_MODE=$COMMIT_MODE: E1-E6 always run COMMIT_MODE=sync"
  fi
  COMMIT_MODE=sync
  DURATION="${DURATION:-60s}"
  WARMUP="${WARMUP:-10s}"
  export COMMIT_MODE DURATION WARMUP
  echo "$DURATION" | grep -Eq '^[0-9]+(ms|s|m|h)$' || die "DURATION must look like 60s, 3m; got '$DURATION'"
  echo "$WARMUP" | grep -Eq '^[0-9]+(ms|s|m|h)$' || die "WARMUP must look like 10s, 500ms; got '$WARMUP'"
  EXP_DURATION_S="$(exp_dur_seconds "$DURATION")"
  # The CSV records no BATCH_FLUSH: a non-default value must not land in the
  # driver's own EXP next to 1 s rows.
  if [ -n "${BATCH_FLUSH:-}" ] && [ "$BATCH_FLUSH" != 1s ] && [ "$EXP" = "$default_exp" ]; then
    die "BATCH_FLUSH=$BATCH_FLUSH is not the 1 s default of E1-E6: set EXP to a new name (e.g. EXP=${default_exp}_bf$BATCH_FLUSH), the CSV has no BATCH_FLUSH column"
  fi
  EXP_RUN_ONE="$HBDB_ROOT/experiments/run_one.sh"
  EXP_CV_PY="$EXP_LIB_DIR/cv.py"
  EXP_BEST_PY="$EXP_LIB_DIR/best_clients.py"
  EXP_CSV="$HBDB_RESULTS_RAW/$EXP.csv"
  EXP_E1_CSV="$HBDB_RESULTS_RAW/$E1_EXP.csv"
  EXP_FAILURES="$HBDB_RESULTS_RAW/$EXP.failures"
  [ -x "$EXP_RUN_ONE" ] || die "missing or not executable: $EXP_RUN_ONE"
  EXP_BACKEND_LIST="$(exp_backends)"
  [ -n "$EXP_BACKEND_LIST" ] || die "no backend to run (BACKENDS='${BACKENDS:-}')"
  if [ -s "$EXP_CSV" ]; then
    [ "$(head -n 1 "$EXP_CSV")" = "$HBDB_CSV_HEADER" ] || die "$EXP_CSV has a different header"
  fi
  [ "$EXP_DRY" = 1 ] || mkdir -p "$HBDB_RESULTS_RAW"
  log "$EXP: backends {$EXP_BACKEND_LIST}, REPS=$REPS, EXTRA_REPS=$EXTRA_REPS, COMMIT_MODE=sync, DURATION=$DURATION, WARMUP=$WARMUP${BATCH_FLUSH:+, BATCH_FLUSH=$BATCH_FLUSH}$([ "$EXP_DRY" = 1 ] && echo ', DRY RUN')"
}

# exp_fault_rec FAULT: the CSV form of FAULT (',' -> '+'), as run_one.sh records it.
exp_fault_rec() {
  echo "$1" | tr ',' '+'
}

# exp_has_row CSV BACKEND N F CLIENTS BLOCK WORKLOAD RTT FAULT REP: 0 if the
# CSV has a row of that configuration and rep.
exp_has_row() {
  local csv="$1" fr
  [ -s "$csv" ] || return 1
  fr="$(exp_fault_rec "$9")"
  awk -F, -v b="$2" -v n="$3" -v f="$4" -v c="$5" -v blk="$6" -v w="$7" -v rtt="$8" -v fl="$fr" -v r="${10}" \
    'NR > 1 && $4 == b && $5 == n && $6 == f && $7 == c && $8 == blk && $9 == w && $10 == rtt && $11 == fl && $12 == r { found = 1 }
     END { exit !found }' "$csv"
}

exp_add_plan() {
  if ! printf '%s\n' "$EXP_PLAN" | grep -Fxq "$1"; then
    EXP_PLAN="${EXP_PLAN:+$EXP_PLAN
}$1"
  fi
}

# exp_cfg_cv BACKEND N F CLIENTS BLOCK WORKLOAD RTT FAULT: "<cv|NA> <rows>".
exp_cfg_cv() {
  if [ -s "$EXP_CSV" ]; then
    python3 "$EXP_CV_PY" "$EXP_CSV" "$1" "$2" "$4" "f=$3" "block_size=$5" "workload=$6" "rtt_ms=$7" "fault=$(exp_fault_rec "$8")" || echo "NA 0"
  else
    echo "NA 0"
  fi
}

exp_cv_high() {
  [ "$1" != NA ] && awk -v c="$1" -v m="$EXP_CV_MAX" 'BEGIN { exit !(c + 0 > m + 0) }'
}

# exp_validate_cfg BACKEND N F CLIENTS BLOCK WORKLOAD RTT FAULT: refuse, before
# anything runs, a configuration run_one.sh would reject (so a bad plan dies
# at once, in --dry-run too, instead of aborting a night's batch half way).
exp_validate_cfg() {
  local b="$1" n="$2" f="$3" c="$4" blk="$5" w="$6" rtt="$7" fault="$8"
  hbdb_in_list "$b" "$EXP_BACKENDS_ALL" || die "unknown backend '$b'"
  hbdb_check_n "$n"
  hbdb_require_uint F "$f"
  hbdb_require_uint BLOCK_SIZE "$blk"
  [ "$blk" -ge 1 ] || die "BLOCK_SIZE must be >= 1"
  hbdb_require_uint RTT_MS "$rtt"
  case "$w" in
    '' | *[!A-Za-z0-9_-]*) die "WORKLOAD must match [A-Za-z0-9_-]+, got '$w'" ;;
  esac
  if [ "$c" = "$EXP_BEST_PLACEHOLDER" ]; then
    [ "$EXP_DRY" = 1 ] || die "internal error: unresolved CLIENTS outside --dry-run"
  else
    hbdb_require_uint CLIENTS "$c"
    [ "$c" -ge 1 ] || die "CLIENTS must be >= 1"
    # Tendermint: sync commit, CLIENTS and LOAD_THREADS <= 256 (lib.sh).
    hbdb_check_tendermint_limits "$b" "$COMMIT_MODE" "$c" "${LOAD_THREADS:-$(hbdb_default_load_threads "$b")}"
  fi
  fault_parse "$fault" "$b" "$n" "$EXP_DURATION_S"
  exp_require_workload "$w"
}

# exp_require_workload W: unless --dry-run, die unless the YCSB files of
# WORKLOAD=W exist (run_one.sh would exit 1 and abort the batch). E4 calls it
# for every workload up front, so a missing file stops the driver before any run.
exp_require_workload() {
  [ "$EXP_DRY" != 1 ] || return 0
  [ -s "$HBDB_YCSB_DIR/$1.load.dat" ] && [ -s "$HBDB_YCSB_DIR/$1.run.dat" ] ||
    die "missing YCSB data for WORKLOAD=$1 in $HBDB_YCSB_DIR (run experiments/ycsb/gen.sh)"
}

# exp_run BACKEND N F CLIENTS BLOCK WORKLOAD RTT FAULT REP: one run, with the
# retry and failure logging of the header. Returns 0 (done, skipped, failed
# and logged); exits for an abort.
exp_run() {
  local b="$1" n="$2" f="$3" c="$4" blk="$5" w="$6" rtt="$7" fault="$8" r="$9" rc attempt entry desc
  desc="BACKEND=$b N=$n F=$f CLIENTS=$c BLOCK_SIZE=$blk WORKLOAD=$w RTT_MS=$rtt FAULT=$fault REP=$r"
  exp_add_plan "$b $n $f $c $blk $w $rtt $fault $r"
  if exp_has_row "$EXP_CSV" "$b" "$n" "$f" "$c" "$blk" "$w" "$rtt" "$fault" "$r"; then
    if [ "$EXP_DRY" = 1 ]; then
      echo "DRY_SKIP: EXP=$EXP $desc COMMIT_MODE=sync (row exists)"
    else
      log "skip (row exists): $desc"
    fi
    return 0
  fi
  if [ "$EXP_DRY" = 1 ]; then
    echo "DRY_RUN: EXP=$EXP $desc COMMIT_MODE=sync experiments/run_one.sh"
    return 0
  fi
  rc=0
  for attempt in 1 2; do
    log "run $desc (attempt $attempt/2)"
    rc=0
    EXP="$EXP" BACKEND="$b" N="$n" F="$f" CLIENTS="$c" BLOCK_SIZE="$blk" WORKLOAD="$w" RTT_MS="$rtt" FAULT="$fault" REP="$r" \
      COMMIT_MODE=sync PRE_DOWN_HOOK="$(exp_hook_for "$b")" "$EXP_RUN_ONE" || rc=$?
    case "$rc" in
      0)
        exp_has_row "$EXP_CSV" "$b" "$n" "$f" "$c" "$blk" "$w" "$rtt" "$fault" "$r" ||
          { log "run_one.sh exited 0 but $EXP_CSV has no row for: $desc"; exp_summary; exit 1; }
        return 0
        ;;
      2 | 3)
        log "run_one.sh exit $rc for: $desc"
        ;;
      *)
        log "run_one.sh exit $rc: aborting the batch"
        exp_summary
        exit "$rc"
        ;;
    esac
  done
  entry="$(date -u +%Y-%m-%dT%H:%M:%SZ) $b $n $f $c $blk $w $rtt $fault $r $rc"
  echo "$entry" >>"$EXP_FAILURES"
  EXP_FAILED_NOW="${EXP_FAILED_NOW:+$EXP_FAILED_NOW
}$entry"
  log "giving up on: $desc (rc=$rc), continuing; recorded in $EXP_FAILURES"
  return 0
}

# exp_cfg BACKEND N F CLIENTS BLOCK WORKLOAD RTT FAULT: REPS reps, then the
# noise rule's extra reps.
exp_cfg() {
  local r=1
  exp_validate_cfg "$@"
  [ "$EXP_VALIDATE_ONLY" != 1 ] || return 0
  if ! printf '%s\n' "$EXP_CFGS" | grep -Fxq "$*"; then
    EXP_CFGS="${EXP_CFGS:+$EXP_CFGS
}$*"
  fi
  while [ "$r" -le "$REPS" ]; do
    exp_run "$@" "$r"
    r=$((r + 1))
  done
  exp_extra_reps "$@"
}

# exp_extra_reps BACKEND N F CLIENTS BLOCK WORKLOAD RTT FAULT: the noise rule.
exp_extra_reps() {
  local r=1 k=1 cv rows
  while [ "$r" -le "$REPS" ]; do
    exp_has_row "$EXP_CSV" "$1" "$2" "$3" "$4" "$5" "$6" "$7" "$8" "$r" || return 0
    r=$((r + 1))
  done
  while [ "$k" -le "$EXTRA_REPS" ]; do
    read -r cv rows <<EOT
$(exp_cfg_cv "$@")
EOT
    exp_cv_high "$cv" || return 0
    log "CV $cv > $EXP_CV_MAX over $rows rows of $1 n=$2 f=$3 clients=$4 block=$5 workload=$6 rtt=$7 fault=$8: extra rep $k/$EXTRA_REPS (rep $((REPS + k)))"
    exp_run "$@" "$((REPS + k))"
    k=$((k + 1))
  done
}

# exp_resolve_clients BACKEND: set EXP_RESOLVED_CLIENTS to the CLIENTS of the
# backend for E2-E6 (see the header). Return 1 (backend skipped, recorded in
# EXP_SKIPPED) when E1 cannot tell. In --dry-run an unresolvable value is the
# placeholder $EXP_BEST_PLACEHOLDER (return 0), so the plan still lists the runs.
exp_resolve_clients() {
  local b="$1" up ov c r missing=""
  EXP_RESOLVED_CLIENTS=""
  if [ -n "${FIXED_CLIENTS:-}" ]; then
    hbdb_require_uint FIXED_CLIENTS "$FIXED_CLIENTS"
    [ "$FIXED_CLIENTS" -ge 1 ] || die "FIXED_CLIENTS must be >= 1"
    EXP_RESOLVED_CLIENTS="$FIXED_CLIENTS"
    return 0
  fi
  up="$(echo "$b" | tr 'a-z' 'A-Z')"
  eval "ov=\"\${BEST_CLIENTS_$up:-}\""
  if [ -n "$ov" ]; then
    hbdb_require_uint "BEST_CLIENTS_$up" "$ov"
    [ "$ov" -ge 1 ] || die "BEST_CLIENTS_$up must be >= 1"
    EXP_RESOLVED_CLIENTS="$ov"
    return 0
  fi
  for c in $EXP_CLIENTS_E1; do
    r=1
    while [ "$r" -le "$REPS" ]; do
      exp_has_row "$EXP_E1_CSV" "$b" 4 0 "$c" 100 a 0 none "$r" || missing="$missing$c/$r "
      r=$((r + 1))
    done
  done
  if [ -z "$missing" ] && EXP_RESOLVED_CLIENTS="$(python3 "$EXP_BEST_PY" "$EXP_E1_CSV" "$b")"; then
    [ "$EXP_VALIDATE_ONLY" = 1 ] || log "$b: best CLIENTS from $EXP_E1_CSV = $EXP_RESOLVED_CLIENTS"
    return 0
  fi
  EXP_RESOLVED_CLIENTS=""
  if [ "$EXP_DRY" = 1 ]; then
    if [ "$EXP_VALIDATE_ONLY" != 1 ]; then
      if [ -n "$missing" ]; then
        log "$b: best CLIENTS of E1 not known yet (E1 rows missing, clients/rep: $missing); the dry run lists CLIENTS=$EXP_BEST_PLACEHOLDER"
      else
        log "$b: best CLIENTS of E1 not known (best_clients.py failed on $EXP_E1_CSV); the dry run lists CLIENTS=$EXP_BEST_PLACEHOLDER"
      fi
    fi
    EXP_RESOLVED_CLIENTS="$EXP_BEST_PLACEHOLDER"
    return 0
  fi
  if [ "$EXP_VALIDATE_ONLY" != 1 ]; then
    if [ -n "$missing" ]; then
      log "$b skipped: $EXP_E1_CSV lacks E1 rows at N=4 (clients/rep): $missing(run E1 first, or set BEST_CLIENTS_$up / FIXED_CLIENTS)"
      EXP_SKIPPED="${EXP_SKIPPED:+$EXP_SKIPPED
}$b: E1 rows missing (clients/rep): $missing- to run it anyway set BEST_CLIENTS_$up=<n> (or FIXED_CLIENTS=<n> for all backends)"
    else
      log "$b skipped: best_clients.py failed on $EXP_E1_CSV"
      EXP_SKIPPED="${EXP_SKIPPED:+$EXP_SKIPPED
}$b: best_clients.py failed on $EXP_E1_CSV - to run it anyway set BEST_CLIENTS_$up=<n> (or FIXED_CLIENTS=<n> for all backends)"
    fi
  fi
  return 1
}

# exp_summary: rows present vs planned, skipped backends, missing rows, every
# configuration's CV, this run's failures (all on stderr). Sets
# EXP_SUMMARY_PLANNED / EXP_SUMMARY_PRESENT. A driver may define
# exp_summary_extra to add lines.
exp_summary() {
  local present=0 planned=0 missing="" b n f c blk w rtt fault r cfg cv rows flag high=""
  while IFS=' ' read -r b n f c blk w rtt fault r; do
    [ -n "$b" ] || continue
    planned=$((planned + 1))
    if exp_has_row "$EXP_CSV" "$b" "$n" "$f" "$c" "$blk" "$w" "$rtt" "$fault" "$r"; then
      present=$((present + 1))
    else
      missing="${missing}  $b n=$n f=$f clients=$c block=$blk workload=$w rtt=$rtt fault=$fault rep=$r"$'\n'
    fi
  done <<EOT
$EXP_PLAN
EOT
  {
    echo "---- summary ($EXP, REPS=$REPS) ----"
    echo "rows present: $present / $planned planned (distinct runs in $EXP_CSV)"
    if [ -n "$EXP_SKIPPED" ]; then
      echo "backends SKIPPED (their runs are not in the plan; count as missing):"
      printf '%s\n' "$EXP_SKIPPED" | sed 's/^/  /'
    fi
    if [ "$EXP_DRY" != 1 ] && [ -n "$missing" ]; then
      echo "missing:"
      printf '%s' "$missing"
    fi
    if [ "$EXP_DRY" != 1 ] && [ -n "$EXP_CFGS" ]; then
      echo "CV of tps (sample std / mean over all reps; threshold $EXP_CV_MAX):"
      while IFS=' ' read -r b n f c blk w rtt fault; do
        [ -n "$b" ] || continue
        read -r cv rows <<EOT2
$(exp_cfg_cv "$b" "$n" "$f" "$c" "$blk" "$w" "$rtt" "$fault")
EOT2
        flag=""
        if exp_cv_high "$cv"; then
          flag="  ABOVE 10%"
          high="$high $b/n$n/f$f/c$c/b$blk/w$w/rtt$rtt/$fault"
        fi
        echo "  $b n=$n f=$f clients=$c block=$blk workload=$w rtt=$rtt fault=$fault: cv=$cv ($rows rows)$flag"
      done <<EOT3
$EXP_CFGS
EOT3
      if [ -n "$high" ]; then
        echo "still above 10% after the extra reps:$high"
      fi
    fi
    if [ "$EXP_DRY" != 1 ]; then
      if [ -n "$EXP_FAILED_NOW" ]; then
        echo "failures in this run (also in $EXP_FAILURES):"
        printf '%s\n' "$EXP_FAILED_NOW" | sed 's/^/  /'
      else
        echo "failures in this run: none"
      fi
    fi
    if command -v exp_summary_extra >/dev/null 2>&1; then
      exp_summary_extra
    fi
  } >&2
  EXP_SUMMARY_PLANNED=$planned
  EXP_SUMMARY_PRESENT=$present
}

# exp_main: validate the whole plan, then run it (see the header).
exp_main() {
  EXP_VALIDATE_ONLY=1
  exp_plan
  EXP_VALIDATE_ONLY=0
  exp_plan
  exp_finish
}

# exp_finish: the final summary and the exit code (see the header).
exp_finish() {
  EXP_SUMMARY_PLANNED=0
  EXP_SUMMARY_PRESENT=0
  exp_summary
  if [ "$EXP_DRY" = 1 ]; then
    log "dry run: $EXP_SUMMARY_PLANNED planned runs ($EXP_SUMMARY_PRESENT already have a row)"
    exit 0
  fi
  if [ -n "$EXP_FAILED_NOW" ] || [ -n "$EXP_SKIPPED" ] || [ "$EXP_SUMMARY_PRESENT" -ne "$EXP_SUMMARY_PLANNED" ]; then
    exit 4
  fi
  exit 0
}

# shellcheck shell=bash
# Interleaved run order for drivers that split their matrix over several
# EXPs (E9-E12: one EXP per attestation / read / fault mode). Sourced by the
# parent process of such a driver, after lib.sh.
#
# Without it the parent ran every rep of the first EXP, then every rep of the
# next, so a slow drift of the host (or of anything else over a long batch)
# landed on the EXPs planned last and was confounded with their
# configuration (E9, 2026-11-10). exp_interleave makes the rep (pass) the
# OUTERMOST loop across EXPs: pass 1 runs rep 1 of every EXP, pass 2 rep 2,
# ..., then the noise rule's extra passes x1..x<EXTRA_REPS>; and the EXP that
# starts a pass rotates (pass i starts at child i mod n). Each child runs one
# pass of its EXP (EXP_ONLY_PASS, exp_lib.sh), so within an EXP the order is
# exp_lib's (rep outermost over its configurations) and the resume key is
# unchanged: a rerun skips every run that has a row.
#
#   EXP_CHILDREN="ENV=v ENV=v ...
#   ENV=v ..."                      one line per child EXP (no spaces in values)
#   exp_interleave "$0" "$@"        returns the worst exit (0, 4 = rows missing
#                                   after the last pass; any other code of a
#                                   child stops at once with that code)

# exp_interleave_passes: the passes, in order (1..REPS, x1..xEXTRA_REPS).
exp_interleave_passes() {
  local r=1 k=1
  while [ "$r" -le "$1" ]; do
    echo "$r"
    r=$((r + 1))
  done
  while [ "$k" -le "$2" ]; do
    echo "x$k"
    k=$((k + 1))
  done
}

exp_interleave() {
  local self="$1" reps="${REPS:-3}" extra="${EXTRA_REPS:-2}" n i=0 j k p last line rc worst=0 passes
  shift
  hbdb_require_uint REPS "$reps"
  [ "$reps" -ge 1 ] || die "REPS must be >= 1"
  hbdb_require_uint EXTRA_REPS "$extra"
  n="$(printf '%s\n' "$EXP_CHILDREN" | grep -c .)"
  [ "$n" -ge 1 ] || die "exp_interleave: no child EXP"
  passes="$(exp_interleave_passes "$reps" "$extra")"
  last="$(printf '%s\n' "$passes" | tail -n 1)"
  for p in $passes; do
    k=0
    while [ "$k" -lt "$n" ]; do
      j=$(((i + k) % n + 1))
      line="$(printf '%s\n' "$EXP_CHILDREN" | grep . | sed -n "${j}p")"
      rc=0
      # shellcheck disable=SC2086 # one VAR=value word per setting
      env $line EXP_ONLY_PASS="$p" "$self" "$@" || rc=$?
      case "$rc" in
        0) ;;
        4) if [ "$p" = "$last" ] && [ "$worst" -eq 0 ]; then worst=4; fi ;;
        *) return "$rc" ;;
      esac
      k=$((k + 1))
    done
    i=$((i + 1))
  done
  return "$worst"
}

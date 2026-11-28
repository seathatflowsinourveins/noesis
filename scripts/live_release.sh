#!/usr/bin/env bash
# Tier-0 live release: freeze the gate-verified src/ tree for the live supervisor unit
# (docker/systemd/tier0-live-supervisor.service), so an edit in the working tree cannot change the
# code that protects a live position.
#
#   live_release.sh [TEST_PATH_OR_NODE_ID...]   run the gates, then cut a stamped release
#   live_release.sh --verify [RELEASE_DIR]      verify a release (default: the current one)
#
# A release is $HOME/tier0-live-releases/<UTC stamp>-<manifest digest prefix>/ and holds exactly
# src/ and RELEASE_MANIFEST.sha256: one `sha256sum` line per file (checked with `sha256sum -c`)
# under a comment header that records the risk.py hash and the gates. "current" is a symlink to
# the newest release. Releases are read-only; run `chmod -R u+w` on one before deleting it.
#
# The gates are the `just` ones (fmt-check, lint, typecheck, ast) plus the given pytest selection,
# or the whole suite when none is given. No option skips a gate. The release holds the bytes that
# were in src/ before the gates started: if src/ changes while they run, nothing is released.
set -euo pipefail

RISK_PY_SHA256="ad8071dcd4196c4c529c02107114ebfdda6692bc9d7c220031f1b19ea3e4d847"
MANIFEST_NAME="RELEASE_MANIFEST.sha256"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
RELEASES_ROOT="$HOME/tier0-live-releases"
STAGE=""

fail() {
  echo "live-release: REFUSED: $*" >&2
  exit 1
}

cleanup() {
  if [[ -n "$STAGE" && "$STAGE" == "$RELEASES_ROOT"/.staging-* && -d "$STAGE" ]]; then
    chmod -R u+w "$STAGE" 2>/dev/null || true
    rm -rf "$STAGE"
  fi
}

# Sorted relative paths of the source files under ./src (bytecode caches are not source).
list_source_files() {
  find src -type f ! -name '*.pyc' ! -path '*/__pycache__/*' -print | LC_ALL=C sort
}

# sha256sum lines for those files, in the same order.
hash_source_files() {
  list_source_files | xargs -r -d '\n' sha256sum
}

# Fails unless the directory holds exactly src/ and a manifest that matches every file under src/,
# with no file added, removed or changed, and the pinned risk kernel.
verify_release() (
  target="$1"
  real="$(readlink -f -- "$target")" || fail "no release at $target"
  [[ -d "$real/src" && -f "$real/$MANIFEST_NAME" ]] \
    || fail "not a release (needs src/ and $MANIFEST_NAME): $target"
  cd "$real"
  entries="$(find . -mindepth 1 -maxdepth 1 -printf '%f\n' | LC_ALL=C sort | tr '\n' ' ')"
  [[ "$entries" == "$MANIFEST_NAME src " ]] \
    || fail "unexpected entries beside src/ and the manifest in $real: $entries"
  [[ -z "$(find src ! -type f ! -type d -print -quit)" ]] \
    || fail "an entry under src/ is neither a file nor a directory in $real"
  sha256sum --quiet --strict -c "$MANIFEST_NAME" >&2 || fail "hash manifest mismatch in $real"
  listed="$(grep -v '^#' "$MANIFEST_NAME" | cut -c67- | LC_ALL=C sort)"
  present="$(find src -type f -print | LC_ALL=C sort)"
  [[ "$listed" == "$present" ]] || fail "the files under src/ are not the manifest's files in $real"
  [[ "$(sha256sum src/tier0_sota/risk.py | cut -d' ' -f1)" == "$RISK_PY_SHA256" ]] \
    || fail "risk.py is not the pinned risk kernel in $real"
  echo "LIVE_RELEASE_VERIFIED release=$real files=$(wc -l <<<"$present")"
)

gate() {
  local name="$1"
  shift
  echo "live-release: gate $name: $*" >&2
  "$@" || fail "gate $name failed; no release was cut"
}

cut_release() {
  local selection_re='^tests/[][A-Za-z0-9_./:-]+$'
  local arg before staged digest stamp name selection="FULL_SUITE"
  for arg in "$@"; do
    [[ "$arg" =~ $selection_re ]] \
      || fail "the pytest selection takes test paths or node ids under tests/ only (got: $arg)"
    [[ -e "$REPO_ROOT/${arg%%::*}" ]] || fail "no such test path: ${arg%%::*}"
  done
  if [[ "$#" -gt 0 ]]; then
    selection="$*"
  fi

  cd "$REPO_ROOT"
  [[ -f src/tier0_sota/risk.py ]] || fail "no src/tier0_sota/risk.py under $REPO_ROOT"
  [[ "$(sha256sum src/tier0_sota/risk.py | cut -d' ' -f1)" == "$RISK_PY_SHA256" ]] \
    || fail "src/tier0_sota/risk.py is not the pinned risk kernel"
  [[ -z "$(find src ! -type f ! -type d -print -quit)" ]] \
    || fail "an entry under src/ is neither a file nor a directory"
  [[ -z "$(list_source_files | grep -vE '^[A-Za-z0-9_./-]+$' || true)" ]] \
    || fail "a file name under src/ has characters outside A-Z a-z 0-9 _ . / -"
  [[ ! -e "$RELEASES_ROOT/current" || -L "$RELEASES_ROOT/current" ]] \
    || fail "$RELEASES_ROOT/current exists and is not a symlink"

  # The gates must exercise this tree, not another checkout the shared environment points at.
  export PYTHONPATH="$REPO_ROOT/src:$REPO_ROOT" TIER0_REPO_ROOT="$REPO_ROOT"
  if [[ -z "${UV_PROJECT_ENVIRONMENT:-}" && ! -x .venv/bin/python && -x "$HOME/noesis/.venv/bin/python" ]]; then
    export UV_PROJECT_ENVIRONMENT="$HOME/noesis/.venv"
  fi

  before="$(hash_source_files)"
  gate fmt-check uv run --no-sync ruff format --check src tests scripts evals
  gate lint uv run --no-sync ruff check .
  gate typecheck uv run --no-sync ty check
  gate ast uv run --no-sync ast-grep scan --config sgconfig.yml
  gate pytest uv run --no-sync pytest -q -p no:cacheprovider "$@"
  [[ "$(hash_source_files)" == "$before" ]] \
    || fail "src/ changed while the gates ran; no release was cut"

  mkdir -p "$RELEASES_ROOT"
  STAGE="$(mktemp -d "$RELEASES_ROOT/.staging-XXXXXXXX")"
  list_source_files | xargs -r -d '\n' cp --parents -t "$STAGE"
  staged="$(cd "$STAGE" && hash_source_files)"
  [[ "$staged" == "$before" ]] || fail "the copied files are not the gate-verified files"

  stamp="$(date -u +%Y%m%dT%H%M%SZ)"
  digest="$(printf '%s\n' "$staged" | sha256sum | cut -d' ' -f1)"
  {
    echo "# tier0-live-release v1"
    echo "# created_utc=$stamp"
    echo "# source_root=$REPO_ROOT"
    echo "# files_sha256=$digest"
    echo "# risk_py_sha256=$RISK_PY_SHA256"
    if [[ -f uv.lock ]]; then
      echo "# uv_lock_sha256=$(sha256sum uv.lock | cut -d' ' -f1)"
    fi
    echo "# gates=fmt-check,lint,typecheck,ast,pytest"
    echo "# pytest_selection=$selection"
    printf '%s\n' "$staged"
  } >"$STAGE/$MANIFEST_NAME"
  verify_release "$STAGE" >/dev/null

  name="$stamp-${digest:0:12}"
  [[ ! -e "$RELEASES_ROOT/$name" ]] || fail "release $name already exists"
  mv -T "$STAGE" "$RELEASES_ROOT/$name"
  STAGE=""
  chmod -R a-w "$RELEASES_ROOT/$name"
  verify_release "$RELEASES_ROOT/$name"

  # Atomic switch: a supervisor that is starting sees the old release or the new one, never a mix.
  ln -sfn "$name" "$RELEASES_ROOT/.current-next"
  mv -T "$RELEASES_ROOT/.current-next" "$RELEASES_ROOT/current"
  echo "LIVE_RELEASE_OK release=$RELEASES_ROOT/$name current=$name files_sha256=$digest pytest_selection=$selection"
}

trap cleanup EXIT
if [[ "${1:-}" == "--verify" ]]; then
  [[ "$#" -le 2 ]] || fail "usage: live_release.sh --verify [RELEASE_DIR]"
  verify_release "${2:-$RELEASES_ROOT/current}"
else
  cut_release "$@"
fi

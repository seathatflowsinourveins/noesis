#!/usr/bin/env bash
set -euo pipefail

# Idempotent Noesis WSL2 dual-machine remote host bootstrap (ADR-0034).
# Installs loopback-only OpenSSH (127.0.0.1:2222), enables systemd lingering,
# verifies & installs SHA-256-pinned stablyai/orca v1.4.281 Linux server binary
# into $HOME/.local/opt/orca-server, and configures user-systemd `orca-ide serve`
# on 127.0.0.1:6768 with cgroup v2 per-terminal memory limits (16G).
#
# Never binds 0.0.0.0, never opens live broker ports (4001/7496/8798/8796/9223),
# never modifies /mnt/z/noesis, and never prints secrets or private keys.

ORCA_VERSION="1.4.281"
ORCA_ASSET="orca-server-linux-x64.tar.gz"
ORCA_URL="https://github.com/stablyai/orca/releases/download/v${ORCA_VERSION}/${ORCA_ASSET}"
ORCA_SHA256="5076e46d55d4221e55d2b3eec307be37c1f55a645b6535bb4b2fef378a759620"

SSH_PORT="2222"
ORCA_SERVE_PORT="6768"
ORCA_TERMINAL_MEMORY_MAX_VALUE="${ORCA_TERMINAL_MEMORY_MAX:-16G}"
ORCA_TERMINAL_MEMORY_SWAP_MAX_VALUE="${ORCA_TERMINAL_MEMORY_SWAP_MAX:-0}"
TAILSCALE_HOST="${TIER0_TAILSCALE_HOST:-noesis.tail3224ad.ts.net}"
FORBIDDEN_PORTS=(4001 7496 8798 8796 9223)

DRY_RUN=false
PUBKEY_FILE=""

usage() {
  cat <<'EOF'
Usage: scripts/remote/setup_noesis_wsl_remote_host.sh [--dry-run] [--pubkey-file <path>] [--tailscale-host <fqdn>]

Idempotently configures Noesis WSL2 for Tailscale-proxied SSH (127.0.0.1:2222)
and headless Linux Orca Server (127.0.0.1:6768, stablyai/orca v1.4.281).
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run)
      DRY_RUN=true
      shift
      ;;
    --pubkey-file)
      if [[ $# -lt 2 ]]; then
        echo "Missing path for --pubkey-file" >&2
        exit 2
      fi
      PUBKEY_FILE="$2"
      shift 2
      ;;
    --tailscale-host)
      if [[ $# -lt 2 ]]; then
        echo "Missing hostname for --tailscale-host" >&2
        exit 2
      fi
      TAILSCALE_HOST="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

TARGET_USER="${NOESIS_REMOTE_USER:-${SUDO_USER:-${USER:-$(id -un)}}}"
if [[ -z "$TARGET_USER" || ! "$TARGET_USER" =~ ^[a-z_][a-z0-9_-]*[$]?$ ]]; then
  echo "FAIL-CLOSED: invalid target user '${TARGET_USER}'" >&2
  exit 2
fi

TARGET_HOME="${NOESIS_REMOTE_HOME:-}"
if [[ -z "$TARGET_HOME" ]]; then
  TARGET_HOME="$(getent passwd "$TARGET_USER" | cut -d: -f6 || true)"
fi
if [[ -z "$TARGET_HOME" ]]; then
  TARGET_HOME="${HOME}"
fi

if [[ ! "$TAILSCALE_HOST" =~ ^[A-Za-z0-9.-]+$ ]]; then
  echo "FAIL-CLOSED: invalid tailscale host '${TAILSCALE_HOST}'" >&2
  exit 2
fi

# 1. Fail-closed check: zero forbidden live broker or CDP ports listening.
if command -v ss >/dev/null 2>&1; then
  SS_OUT="$(ss -lntH 2>/dev/null || true)"
  for port in "${FORBIDDEN_PORTS[@]}"; do
    if grep -Eq "[:.]${port}[[:space:]]" <<<"$SS_OUT"; then
      echo "FAIL-CLOSED: forbidden port ${port} is listening; refusing remote host setup" >&2
      exit 2
    fi
  done
fi

ORCA_INSTALL_DIR="${TARGET_HOME}/.local/opt/orca-server"
ORCA_BIN="${ORCA_INSTALL_DIR}/bin/orca-ide"
ORCA_SYMLINK="${TARGET_HOME}/.local/bin/orca-ide"
SSH_DROPIN="/etc/ssh/sshd_config.d/99-noesis-tailnet.conf"
SSH_DIR="${TARGET_HOME}/.ssh"
AUTH_KEYS="${SSH_DIR}/authorized_keys"

run_root() {
  if [[ "$(id -u)" -eq 0 ]]; then
    "$@"
  else
    sudo "$@"
  fi
}

run_as_target() {
  if [[ "$(id -un)" == "$TARGET_USER" ]]; then
    "$@"
  else
    sudo -u "$TARGET_USER" -H "$@"
  fi
}

if [[ "$DRY_RUN" == "true" ]]; then
  cat <<EOF
[DRY-RUN] Noesis WSL2 Remote Host Plan (ADR-0034)
  target_user       = ${TARGET_USER}
  target_home       = ${TARGET_HOME}
  sshd_dropin       = ${SSH_DROPIN} (ListenAddress 127.0.0.1, Port ${SSH_PORT}, key-only)
  linger            = loginctl enable-linger ${TARGET_USER}
  ssh_dir           = ${SSH_DIR} (0700) / ${AUTH_KEYS} (0600)
  orca_version      = v${ORCA_VERSION} (${ORCA_ASSET})
  orca_sha256       = ${ORCA_SHA256}
  orca_install_dir  = ${ORCA_INSTALL_DIR}
  orca_serve        = --port ${ORCA_SERVE_PORT} --bind 127.0.0.1 --pairing-address ${TAILSCALE_HOST}:${ORCA_SERVE_PORT} --refuse-public
  cgroup_v2_limits  = ORCA_TERMINAL_MEMORY_MAX=${ORCA_TERMINAL_MEMORY_MAX_VALUE} ORCA_TERMINAL_MEMORY_SWAP_MAX=${ORCA_TERMINAL_MEMORY_SWAP_MAX_VALUE}
[DRY-RUN-OK]
EOF
  exit 0
fi

# 2. Idempotently install openssh-server if absent.
if [[ ! -x /usr/sbin/sshd ]]; then
  echo "[STEP] Installing openssh-server..."
  run_root apt-get update -qq
  run_root env DEBIAN_FRONTEND=noninteractive apt-get install -y -qq openssh-server
fi

# 3. Write loopback-only hardened sshd drop-in (127.0.0.1:2222, key-only, POSIX SFTP).
echo "[STEP] Writing hardened loopback-only ${SSH_DROPIN}..."
SSHD_TMP="$(mktemp)"
trap 'rm -f "$SSHD_TMP"' EXIT
cat >"$SSHD_TMP" <<EOF
# Managed by noesis scripts/remote/setup_noesis_wsl_remote_host.sh (ADR-0034)
ListenAddress 127.0.0.1
Port ${SSH_PORT}
PermitRootLogin no
PasswordAuthentication no
KbdInteractiveAuthentication no
PubkeyAuthentication yes
AllowUsers ${TARGET_USER}
AllowAgentForwarding no
Subsystem sftp /usr/lib/openssh/sftp-server
EOF
run_root install -d -m 0755 /etc/ssh/sshd_config.d
run_root install -m 0644 "$SSHD_TMP" "$SSH_DROPIN"
run_root mkdir -p /run/sshd
run_root /usr/sbin/sshd -t
run_root systemctl enable --now ssh
run_root systemctl reload-or-restart ssh

# 4. Enable systemd lingering so user units & cgroup v2 limits survive SSH disconnect.
echo "[STEP] Enabling systemd lingering for ${TARGET_USER}..."
run_root loginctl enable-linger "${TARGET_USER}"

# 5. Ensure ~/.ssh (0700) and authorized_keys (0600); optionally append a public key.
echo "[STEP] Ensuring ${SSH_DIR} (0700) and ${AUTH_KEYS} (0600)..."
run_as_target mkdir -p "$SSH_DIR"
run_as_target chmod 0700 "$SSH_DIR"
run_as_target touch "$AUTH_KEYS"
run_as_target chmod 0600 "$AUTH_KEYS"

if [[ -n "$PUBKEY_FILE" ]]; then
  if [[ ! -f "$PUBKEY_FILE" ]]; then
    echo "FAIL-CLOSED: --pubkey-file not found: ${PUBKEY_FILE}" >&2
    exit 2
  fi
  PUBKEY_LINE="$(head -n 1 "$PUBKEY_FILE" | tr -d '\r')"
  if [[ ! "$PUBKEY_LINE" =~ ^(ssh-ed25519|sk-ssh-ed25519@openssh\.com|ecdsa-sha2-nistp256)[[:space:]] ]]; then
    echo "FAIL-CLOSED: --pubkey-file must contain a valid Ed25519/ECDSA public key (never a private key)" >&2
    exit 2
  fi
  if ! grep -Fqx "$PUBKEY_LINE" "$AUTH_KEYS"; then
    printf '%s\n' "$PUBKEY_LINE" | run_as_target tee -a "$AUTH_KEYS" >/dev/null
    run_as_target chmod 0600 "$AUTH_KEYS"
  fi
fi

# 6. Verify/install SHA-256-pinned stablyai/orca v1.4.281 Linux server binary.
NEED_ORCA_INSTALL=true
if [[ -x "$ORCA_BIN" ]]; then
  INSTALLED_VER="$("$ORCA_BIN" --version 2>/dev/null || true)"
  if grep -Fq "$ORCA_VERSION" <<<"$INSTALLED_VER"; then
    NEED_ORCA_INSTALL=false
  fi
fi

if [[ "$NEED_ORCA_INSTALL" == "true" ]]; then
  echo "[STEP] Downloading and SHA-256 verifying ${ORCA_ASSET} (v${ORCA_VERSION})..."
  ORCA_TMP_DIR="$(mktemp -d)"
  trap 'rm -f "$SSHD_TMP"; rm -rf "$ORCA_TMP_DIR"' EXIT
  curl -fsSL -o "${ORCA_TMP_DIR}/${ORCA_ASSET}" "$ORCA_URL"
  printf '%s  %s\n' "$ORCA_SHA256" "${ORCA_TMP_DIR}/${ORCA_ASSET}" | sha256sum -c -
  run_as_target mkdir -p "${ORCA_INSTALL_DIR}" "${TARGET_HOME}/.local/bin"
  tar -xzf "${ORCA_TMP_DIR}/${ORCA_ASSET}" -C "${ORCA_TMP_DIR}"
  EXTRACTED_ROOT="${ORCA_TMP_DIR}"
  if [[ -d "${ORCA_TMP_DIR}/orca-server-linux-x64" ]]; then
    EXTRACTED_ROOT="${ORCA_TMP_DIR}/orca-server-linux-x64"
  fi
  rm -rf "${ORCA_INSTALL_DIR}.new"
  run_as_target mkdir -p "${ORCA_INSTALL_DIR}.new"
  cp -a "${EXTRACTED_ROOT}/." "${ORCA_INSTALL_DIR}.new/"
  rm -rf "${ORCA_INSTALL_DIR}"
  mv "${ORCA_INSTALL_DIR}.new" "${ORCA_INSTALL_DIR}"
fi

if [[ ! -x "$ORCA_BIN" ]]; then
  echo "FAIL-CLOSED: expected Orca binary missing after extraction: ${ORCA_BIN}" >&2
  exit 1
fi
RESOLVED_ORCA="$(readlink -f "$ORCA_BIN")"
if [[ "$RESOLVED_ORCA" == "/usr/bin/orca" ]]; then
  echo "FAIL-CLOSED: refused Linux GNOME screen reader /usr/bin/orca" >&2
  exit 1
fi
run_as_target mkdir -p "${TARGET_HOME}/.local/bin"
run_as_target ln -sfn "$ORCA_BIN" "$ORCA_SYMLINK"

# 7. Install user-systemd `orca-ide serve` with loopback bind and cgroup v2 memory cap.
echo "[STEP] Configuring user-systemd orca-ide serve on 127.0.0.1:${ORCA_SERVE_PORT}..."
run_as_target "$ORCA_BIN" serve service install \
  --command "$ORCA_BIN" \
  --port "$ORCA_SERVE_PORT" \
  --bind 127.0.0.1 \
  --pairing-address "${TAILSCALE_HOST}:${ORCA_SERVE_PORT}" \
  --refuse-public

USER_SYSTEMD_DIR="${TARGET_HOME}/.config/systemd/user"
ORCA_UNIT_NAME=""
for candidate in "${USER_SYSTEMD_DIR}"/*orca*.service; do
  if [[ -f "$candidate" ]]; then
    ORCA_UNIT_NAME="$(basename "$candidate")"
    break
  fi
done
ORCA_UNIT_NAME="${ORCA_UNIT_NAME:-orca-serve.service}"

DROPIN_DIR="${USER_SYSTEMD_DIR}/${ORCA_UNIT_NAME}.d"
run_as_target mkdir -p "$DROPIN_DIR"
cat <<EOF | run_as_target tee "${DROPIN_DIR}/10-cgroup-memory.conf" >/dev/null
[Service]
Environment=ORCA_TERMINAL_MEMORY_MAX=${ORCA_TERMINAL_MEMORY_MAX_VALUE}
Environment=ORCA_TERMINAL_MEMORY_SWAP_MAX=${ORCA_TERMINAL_MEMORY_SWAP_MAX_VALUE}
EOF

run_as_target systemctl --user daemon-reload
run_as_target systemctl --user enable --now "${ORCA_UNIT_NAME}"

# 8. Print SSH host-key fingerprint and pairing command (zero private key material).
echo ""
echo "=== NOESIS WSL2 REMOTE HOST READY (ADR-0034) ==="
if [[ -f /etc/ssh/ssh_host_ed25519_key.pub ]]; then
  echo "SSH Ed25519 Host Key Fingerprint (verify on Mac before trusting):"
  ssh-keygen -lf /etc/ssh/ssh_host_ed25519_key.pub
fi
echo "Orca Server Unit   : ${ORCA_UNIT_NAME} (127.0.0.1:${ORCA_SERVE_PORT}, ORCA_TERMINAL_MEMORY_MAX=${ORCA_TERMINAL_MEMORY_MAX_VALUE})"
echo "Next Step (Windows): Run scripts/remote/setup_noesis_windows_tailscale_serve.ps1 in Admin pwsh"
echo "Next Step (Pairing): ${ORCA_SYMLINK} pair --address ${TAILSCALE_HOST}:${ORCA_SERVE_PORT}"

#!/usr/bin/env python3
"""Zero-window Windows Task Scheduler launcher for the 24/5 Paper Farm loop.

Runs via `.venv/Scripts/pythonw.exe` (`/SUBSYSTEM:WINDOWS`) with
`CREATE_NO_WINDOW = 0x08000000` so scheduled 1-minute observation/paper ticks
never flash console windows on Windows desktops.
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import os
import pathlib
import re
import shutil
import socket
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from typing import Any

CREATE_NO_WINDOW = 0x08000000 if sys.platform == "win32" else 0
EXPECTED_RISK_SHA256 = "ad8071dcd4196c4c529c02107114ebfdda6692bc9d7c220031f1b19ea3e4d847"
FORBIDDEN_LIVE_PORTS: tuple[int, ...] = (4001, 7496, 8798, 8796)
IBKR_PAPER_PORT = 4002
WSL_DISTRO = "Noesis-SOTA"
OBSERVE_FRESH_WINDOW_SECS = 65.0
_PIN_TOKEN_RE = re.compile(r"^[A-Z][A-Z0-9.-]{0,6}$")
_WIN_DRIVE_RE = re.compile(r"^([A-Za-z]):(?:[\\/](.*))?$")
_DUCKDB_LOCK_PATTERNS: tuple[str, ...] = (
    "could not set lock on file",
    "process cannot access the file",
    "file is already open",
    "conflicting lock is held",
    "used by another process",
    "being used by another process",
)


def _sanitize(text: str) -> str:
    home_str = str(pathlib.Path.home())
    cleaned = text.replace(home_str, "%USERPROFILE%").replace(
        home_str.replace("\\", "/"), "%USERPROFILE%"
    )
    return re.sub(r"/home/[A-Za-z0-9._-]+", "~", cleaned).strip()


def _atomic_write_json(dest: pathlib.Path, payload: dict[str, Any]) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(f".{dest.name}.{os.getpid()}.tmp")
    tmp.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    os.replace(tmp, dest)


def _port_open(port: int, timeout: float = 0.6) -> bool:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=timeout):
            return True
    except OSError:
        return False


def _extract_tcp_port(endpoint: str) -> int | None:
    cleaned = endpoint.strip()
    if not cleaned or ":" not in cleaned:
        return None
    port_part = cleaned.rsplit(":", 1)[-1]
    if not port_part.isdigit():
        return None
    return int(port_part)


def _parse_windows_netstat_ports(output: str) -> set[int]:
    ports: set[int] = set()
    for line in output.splitlines():
        parts = line.split()
        if len(parts) < 4 or parts[0].upper() != "TCP":
            continue
        for endpoint in (parts[1], parts[2]):
            port = _extract_tcp_port(endpoint)
            if port is not None:
                ports.add(port)
    return ports


def _parse_proc_net_tcp_ports(content: str) -> set[int]:
    ports: set[int] = set()
    for line in content.splitlines()[1:]:
        parts = line.split()
        if len(parts) < 3:
            continue
        for endpoint in (parts[1], parts[2]):
            if ":" in endpoint:
                with contextlib.suppress(ValueError):
                    ports.add(int(endpoint.rsplit(":", 1)[-1], 16))
    return ports


def _probe_forbidden_port(port: int) -> bool:
    return _port_open(port, timeout=0.35)


def _active_forbidden_tcp_ports() -> tuple[list[int], str | None]:
    """Return active forbidden local OR remote TCP ports (fail-closed on probe error)."""
    observed: set[int] = set()
    if sys.platform == "win32":
        netstat_bin = shutil.which("netstat")
        if not netstat_bin:
            return [], "netstat executable not found for TCP port guard"
        try:
            proc = subprocess.run(  # noqa: S603
                [netstat_bin, "-ano", "-p", "TCP"],
                capture_output=True,
                text=True,
                timeout=5.0,
                check=False,
                creationflags=CREATE_NO_WINDOW,
                stdin=subprocess.DEVNULL,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            return [], f"TCP port guard failed: {_sanitize(str(exc))}"
        if proc.returncode != 0:
            return [], f"TCP port guard exited {proc.returncode}"
        observed |= _parse_windows_netstat_ports(proc.stdout)
    else:
        for proc_path in (pathlib.Path("/proc/net/tcp"), pathlib.Path("/proc/net/tcp6")):
            if proc_path.is_file():
                with contextlib.suppress(OSError):
                    observed |= _parse_proc_net_tcp_ports(proc_path.read_text(encoding="utf-8"))
    with ThreadPoolExecutor(max_workers=len(FORBIDDEN_LIVE_PORTS)) as pool:
        for port, is_open in zip(
            FORBIDDEN_LIVE_PORTS,
            pool.map(_probe_forbidden_port, FORBIDDEN_LIVE_PORTS),
            strict=True,
        ):
            if is_open:
                observed.add(port)
    hits = sorted(port for port in FORBIDDEN_LIVE_PORTS if port in observed)
    return hits, None


def _resolve_fnox_bin() -> str | None:
    found = shutil.which("fnox")
    if found:
        return found
    if sys.platform == "win32":
        local_app = pathlib.Path(
            os.environ.get("LOCALAPPDATA") or (pathlib.Path.home() / "AppData" / "Local")
        )
        candidates = (
            local_app / "mise" / "shims" / "fnox.exe",
            local_app / "mise" / "installs" / "fnox" / "latest" / "fnox.exe",
        )
    else:
        home = pathlib.Path.home()
        candidates = (
            home / ".local" / "share" / "mise" / "shims" / "fnox",
            home / ".local" / "bin" / "fnox",
        )
    for cand in candidates:
        if cand.is_file():
            return str(cand)
    return None


def _resolve_wsl_bin() -> str | None:
    found = shutil.which("wsl.exe") or shutil.which("wsl")
    if found:
        return found
    sys_root = pathlib.Path(os.environ.get("SYSTEMROOT") or r"C:\Windows")
    cand = sys_root / "System32" / "wsl.exe"
    return str(cand) if cand.is_file() else None


def _win_to_wsl_path(raw: pathlib.Path | str) -> str:
    text = str(raw).strip()
    match = _WIN_DRIVE_RE.match(text)
    if match:
        drive = match.group(1).lower()
        rest = (match.group(2) or "").replace("\\", "/").strip("/")
        return f"/mnt/{drive}/{rest}" if rest else f"/mnt/{drive}"
    return text.replace("\\", "/")


def _windows_venv_has_nautilus(repo_root: pathlib.Path) -> bool:
    sp = repo_root / ".venv" / "Lib" / "site-packages"
    if not sp.is_dir():
        return False
    return (sp / "nautilus_trader").is_dir() or any(sp.glob("nautilus_trader*.pyd"))


def _resolve_posix_python(repo_root: pathlib.Path) -> pathlib.Path | None:
    for cand in (
        pathlib.Path.home() / "noesis" / ".venv" / "bin" / "python",
        repo_root / ".venv" / "bin" / "python",
    ):
        if cand.is_file() and os.access(cand, os.X_OK):
            return cand
    return None


def _is_duckdb_lock_error(text: str) -> bool:
    lowered = text.lower()
    if any(pat in lowered for pat in _DUCKDB_LOCK_PATTERNS):
        return True
    return ("ioexception" in lowered or "io error" in lowered) and (
        "lock" in lowered or "permission denied" in lowered
    )


def _extract_error_tail(text: str, limit: int = 400) -> str:
    cleaned = _sanitize(text)
    if len(cleaned) <= limit:
        return cleaned
    return cleaned[-limit:].lstrip()


def _sanitize_pin_symbols(raw: str) -> str:
    cleaned: list[str] = []
    seen: set[str] = set()
    for token in raw.split(","):
        sym = token.strip().upper()
        if sym and _PIN_TOKEN_RE.match(sym) and sym not in seen:
            seen.add(sym)
            cleaned.append(sym)
    return ",".join(cleaned) if cleaned else "SOXL,SOXS,TQQQ,SQQQ"


def _observe_manifests_fresh(
    manifest_dir: pathlib.Path,
    *,
    max_age_secs: float = OBSERVE_FRESH_WINDOW_SECS,
    now_ts: float | None = None,
) -> bool:
    now = time.time() if now_ts is None else now_ts
    required = (
        manifest_dir / "live_whole_market_movers_manifest.json",
        manifest_dir / "paper_farm_live_auto_entry_status.json",
        manifest_dir / "tv_sota_pack" / "tv_compounding_manifest.json",
    )
    for path in required:
        try:
            age = now - path.stat().st_mtime
        except OSError:
            return False
        if age < 0.0 or age >= max_age_secs:
            return False
    return True


def _build_wsl_delegate_command(
    wsl_bin: str,
    *,
    script_path: pathlib.Path,
    manifest_dir: pathlib.Path,
    intent: str,
    top_k: int,
    pin_symbols: str,
) -> list[str]:
    wsl_script = _win_to_wsl_path(script_path)
    wsl_manifest = _win_to_wsl_path(manifest_dir)
    pins_clean = _sanitize_pin_symbols(pin_symbols)
    return [
        wsl_bin,
        "-d",
        WSL_DISTRO,
        "--exec",
        "/usr/bin/env",
        "python3",
        wsl_script,
        intent,
        "--manifest-dir",
        wsl_manifest,
        "--top-k",
        str(int(top_k)),
        "--pin-symbols",
        pins_clean,
        "--from-wsl-delegate",
    ]


def _resolve_age_key_file() -> pathlib.Path:
    env_val = os.environ.get("FNOX_AGE_KEY_FILE", "").strip()
    if env_val:
        return pathlib.Path(env_val)
    if sys.platform == "win32":
        with contextlib.suppress(OSError, ImportError):
            import winreg

            with winreg.OpenKey(winreg.HKEY_CURRENT_USER, "Environment") as key:
                reg_val, _ = winreg.QueryValueEx(key, "FNOX_AGE_KEY_FILE")
                if isinstance(reg_val, str) and reg_val.strip():
                    return pathlib.Path(os.path.expandvars(reg_val.strip()))
    return pathlib.Path.home() / ".config" / "fnox" / "age.txt"


def _acquire_single_instance_lock(lock_path: pathlib.Path) -> Any | None:
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    handle = lock_path.open("a+b")
    if sys.platform == "win32":
        import msvcrt

        try:
            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        except OSError:
            handle.close()
            return None
    else:
        import fcntl

        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            handle.close()
            return None
    return handle


def _release_lock(handle: Any | None) -> None:
    if handle is None:
        return
    with contextlib.suppress(OSError):
        if sys.platform == "win32":
            import msvcrt

            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    with contextlib.suppress(OSError):
        handle.close()


def _verify_safety_invariants(repo_root: pathlib.Path) -> str | None:
    risk_path = repo_root / "src" / "tier0_sota" / "risk.py"
    if not risk_path.is_file():
        return "missing src/tier0_sota/risk.py"
    digest = hashlib.sha256(risk_path.read_bytes()).hexdigest().lower()
    if digest != EXPECTED_RISK_SHA256:
        return f"risk.py sha256 mismatch: {digest}"
    hits, probe_err = _active_forbidden_tcp_ports()
    if probe_err:
        return probe_err
    if hits:
        return f"forbidden live broker port active (local or remote): {hits[0]}"
    return None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Zero-window 24/5 Paper Farm loop tick.")
    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument("--observe", action="store_true")
    mode_group.add_argument("--confirm-paper", action="store_true")
    parser.add_argument("--manifest-dir", required=True)
    parser.add_argument("--top-k", type=int, default=15)
    parser.add_argument("--pin-symbols", default="SOXL,SOXS,TQQQ,SQQQ")
    parser.add_argument("--from-wsl-delegate", action="store_true")
    args = parser.parse_args(argv)

    script_path = pathlib.Path(__file__).resolve()
    repo_root = script_path.parents[1]
    manifest_dir = pathlib.Path(args.manifest_dir)
    logs_dir = manifest_dir.parent / "logs"
    intent = "--observe" if args.observe else "--confirm-paper"
    health_path = logs_dir / (
        "paper-farm-observer-health.json" if args.observe else "paper-farm-execution-health.json"
    )
    if args.from_wsl_delegate and sys.platform != "win32":
        lock_path = pathlib.Path("/tmp") / (  # noqa: S108
            "tier0-paper-farm-observe-delegate.lock"
            if args.observe
            else "tier0-paper-farm-execution-delegate.lock"
        )
    else:
        lock_path = logs_dir / (
            "paper-farm-observe.lock" if args.observe else "paper-farm-execution.lock"
        )

    lock_handle = _acquire_single_instance_lock(lock_path)
    if lock_handle is None:
        return 0

    started = time.monotonic()
    rc = 1
    err_msg = ""
    ibkr_reachable = False
    runtime_mode = "unknown"
    try:
        safety_err = _verify_safety_invariants(repo_root)
        if safety_err:
            err_msg = safety_err
            return 2

        ibkr_reachable = _port_open(IBKR_PAPER_PORT, timeout=0.5)
        pins_clean = _sanitize_pin_symbols(args.pin_symbols)
        env = os.environ.copy()

        if args.observe and _observe_manifests_fresh(manifest_dir):
            if sys.platform == "win32":
                has_win = (
                    repo_root / ".venv" / "Scripts" / "python.exe"
                ).is_file() and _windows_venv_has_nautilus(repo_root)
                runtime_mode = "windows:.venv:fresh" if has_win else f"wsl2:{WSL_DISTRO}:fresh"
            else:
                runtime_mode = "linux:.venv:fresh"
            rc = 0
            return 0

        if sys.platform == "win32":
            py_exe = repo_root / ".venv" / "Scripts" / "python.exe"
            if py_exe.is_file() and _windows_venv_has_nautilus(repo_root):
                fnox_bin = _resolve_fnox_bin()
                if not fnox_bin:
                    err_msg = "fnox executable not found"
                    return 2
                age_key = _resolve_age_key_file()
                if not age_key.is_file():
                    err_msg = "missing fnox age identity file"
                    return 2
                runtime_mode = "windows:.venv"
                env["PYTHONDONTWRITEBYTECODE"] = "1"
                env["PYTHONPATH"] = str(repo_root / "src")
                env["FNOX_AGE_KEY_FILE"] = str(age_key)
                env["IBKR_PAPER_TCP_REACHABLE"] = "1" if ibkr_reachable else "0"
                cmd = [
                    fnox_bin,
                    "exec",
                    "--profile",
                    "paper",
                    "--",
                    str(py_exe),
                    "-m",
                    "tier0_sota.live.runner",
                    intent,
                    "--manifest-dir",
                    str(manifest_dir),
                    "--top-k",
                    str(args.top_k),
                    "--pin-symbols",
                    pins_clean,
                ]
            else:
                wsl_bin = _resolve_wsl_bin()
                if not wsl_bin:
                    err_msg = "windows .venv lacks nautilus_trader and wsl.exe not found"
                    return 2
                runtime_mode = f"wsl2:{WSL_DISTRO}"
                cmd = _build_wsl_delegate_command(
                    wsl_bin,
                    script_path=script_path,
                    manifest_dir=manifest_dir,
                    intent=intent,
                    top_k=args.top_k,
                    pin_symbols=pins_clean,
                )
        else:
            py_exe_posix = _resolve_posix_python(repo_root)
            if py_exe_posix is None:
                err_msg = "missing WSL2/Linux venv python"
                return 2
            fnox_bin = _resolve_fnox_bin()
            if not fnox_bin:
                err_msg = "fnox executable not found"
                return 2
            age_key = _resolve_age_key_file()
            if not age_key.is_file():
                err_msg = "missing fnox age identity file"
                return 2
            runtime_mode = "linux:.venv"
            env["PYTHONDONTWRITEBYTECODE"] = "1"
            env["PYTHONPATH"] = str(repo_root / "src")
            env["TIER0_DATA_ROOT"] = str(manifest_dir.parent)
            env["FNOX_AGE_KEY_FILE"] = str(age_key)
            env["IBKR_PAPER_TCP_REACHABLE"] = "1" if ibkr_reachable else "0"
            cmd = [
                fnox_bin,
                "exec",
                "--profile",
                "paper",
                "--",
                str(py_exe_posix),
                "-m",
                "tier0_sota.live.runner",
                intent,
                "--manifest-dir",
                str(manifest_dir),
                "--top-k",
                str(args.top_k),
                "--pin-symbols",
                pins_clean,
            ]

        proc = subprocess.run(  # noqa: S603
            cmd,
            cwd=str(repo_root),
            env=env,
            capture_output=True,
            text=True,
            timeout=55.0 if args.observe else None,
            check=False,
            creationflags=CREATE_NO_WINDOW,
            stdin=subprocess.DEVNULL,
        )
        rc = int(proc.returncode)
        if rc != 0 and args.observe and _is_duckdb_lock_error(proc.stderr or proc.stdout or ""):
            for _ in range(2):
                time.sleep(4.0)
                if _observe_manifests_fresh(manifest_dir):
                    runtime_mode = f"{runtime_mode}:fresh"
                    rc = 0
                    return 0
                proc = subprocess.run(  # noqa: S603
                    cmd,
                    cwd=str(repo_root),
                    env=env,
                    capture_output=True,
                    text=True,
                    timeout=30.0,
                    check=False,
                    creationflags=CREATE_NO_WINDOW,
                    stdin=subprocess.DEVNULL,
                )
                rc = int(proc.returncode)
                if rc == 0 or not _is_duckdb_lock_error(proc.stderr or proc.stdout or ""):
                    break
        if rc != 0:
            err_msg = _extract_error_tail(proc.stderr or proc.stdout or f"rc={rc}")
            if args.from_wsl_delegate:
                sys.stderr.write(err_msg + "\n")
        return rc
    except subprocess.TimeoutExpired:
        rc = 124
        err_msg = "paper farm tick timed out"
        return rc
    except OSError as exc:
        rc = 127
        err_msg = _extract_error_tail(str(exc))
        return rc
    finally:
        duration_ms = int((time.monotonic() - started) * 1000)
        if not args.from_wsl_delegate:
            with contextlib.suppress(OSError):
                _atomic_write_json(
                    health_path,
                    {
                        "schemaVersion": "1.0.0-tier0",
                        "updatedAtUtc": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "zeroWindowMode": True,
                        "runtime": runtime_mode,
                        "intent": intent.lstrip("-"),
                        "exitCode": rc,
                        "durationMs": duration_ms,
                        "ibkrPaperReachable": ibkr_reachable,
                        "error": err_msg,
                    },
                )
        _release_lock(lock_handle)


if __name__ == "__main__":
    raise SystemExit(main())

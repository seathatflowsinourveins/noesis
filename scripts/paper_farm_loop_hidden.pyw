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
from datetime import UTC, datetime
from typing import Any

CREATE_NO_WINDOW = 0x08000000 if sys.platform == "win32" else 0
EXPECTED_RISK_SHA256 = "ad8071dcd4196c4c529c02107114ebfdda6692bc9d7c220031f1b19ea3e4d847"
FORBIDDEN_LIVE_PORTS: tuple[int, ...] = (4001, 7496, 8798, 8796)
IBKR_PAPER_PORT = 4002


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


def _resolve_fnox_bin() -> str | None:
    found = shutil.which("fnox")
    if found:
        return found
    local_app = pathlib.Path(
        os.environ.get("LOCALAPPDATA") or (pathlib.Path.home() / "AppData" / "Local")
    )
    for cand in (
        local_app / "mise" / "shims" / "fnox.exe",
        local_app / "mise" / "installs" / "fnox" / "latest" / "fnox.exe",
    ):
        if cand.is_file():
            return str(cand)
    return None


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
    return handle


def _release_lock(handle: Any | None) -> None:
    if handle is None:
        return
    with contextlib.suppress(OSError):
        if sys.platform == "win32":
            import msvcrt

            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
    with contextlib.suppress(OSError):
        handle.close()


def _verify_safety_invariants(repo_root: pathlib.Path) -> str | None:
    risk_path = repo_root / "src" / "tier0_sota" / "risk.py"
    if not risk_path.is_file():
        return "missing src/tier0_sota/risk.py"
    digest = hashlib.sha256(risk_path.read_bytes()).hexdigest().lower()
    if digest != EXPECTED_RISK_SHA256:
        return f"risk.py sha256 mismatch: {digest}"
    for port in FORBIDDEN_LIVE_PORTS:
        if _port_open(port, timeout=0.35):
            return f"forbidden live broker port active: {port}"
    return None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Zero-window 24/5 Paper Farm loop tick.")
    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument("--observe", action="store_true")
    mode_group.add_argument("--confirm-paper", action="store_true")
    parser.add_argument("--manifest-dir", required=True)
    parser.add_argument("--top-k", type=int, default=15)
    parser.add_argument("--pin-symbols", default="SOXL,SOXS")
    args = parser.parse_args(argv)

    repo_root = pathlib.Path(__file__).resolve().parents[1]
    manifest_dir = pathlib.Path(args.manifest_dir)
    logs_dir = manifest_dir.parent / "logs"
    health_path = logs_dir / "paper-farm-observer-health.json"
    lock_path = logs_dir / "paper-farm-loop.lock"
    intent = "--observe" if args.observe else "--confirm-paper"

    lock_handle = _acquire_single_instance_lock(lock_path)
    if lock_handle is None:
        return 0

    started = time.monotonic()
    rc = 1
    err_msg = ""
    ibkr_reachable = False
    try:
        safety_err = _verify_safety_invariants(repo_root)
        if safety_err:
            err_msg = safety_err
            return 2

        py_exe = repo_root / ".venv" / "Scripts" / "python.exe"
        if not py_exe.is_file():
            err_msg = "missing worktree python.exe"
            return 2

        fnox_bin = _resolve_fnox_bin()
        if not fnox_bin:
            err_msg = "fnox executable not found"
            return 2

        age_key = _resolve_age_key_file()
        if not age_key.is_file():
            err_msg = "missing fnox age identity file"
            return 2

        ibkr_reachable = _port_open(IBKR_PAPER_PORT, timeout=1.5)
        env = os.environ.copy()
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
            args.pin_symbols,
        ]
        proc = subprocess.run(  # noqa: S603
            cmd,
            cwd=str(repo_root),
            env=env,
            capture_output=True,
            text=True,
            timeout=55.0,
            check=False,
            creationflags=CREATE_NO_WINDOW,
            stdin=subprocess.DEVNULL,
        )
        rc = int(proc.returncode)
        if rc != 0:
            err_msg = _sanitize((proc.stderr or proc.stdout or f"rc={rc}")[:300])
        return rc
    except subprocess.TimeoutExpired:
        rc = 124
        err_msg = "paper farm tick timed out"
        return rc
    except OSError as exc:
        rc = 127
        err_msg = _sanitize(str(exc))[:300]
        return rc
    finally:
        duration_ms = int((time.monotonic() - started) * 1000)
        with contextlib.suppress(OSError):
            _atomic_write_json(
                health_path,
                {
                    "schemaVersion": "1.0.0-tier0",
                    "updatedAtUtc": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "zeroWindowMode": True,
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

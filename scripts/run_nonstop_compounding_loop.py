#!/usr/bin/env python3
"""Single-tick non-stop compounding orchestrator & 24/5 paper-only TV/engine heartbeat.

Scheduling is strictly external via native upstream `systemd` (`tier0-compounding-loop.timer`)
and upstream `Dagster` (`tier0_sota.definitions.nonstop_compounding_loop_job`) — zero ad-hoc
Python infinite sleep loops.

Fail-closed invariants enforced before every tick:
- Immutable `src/tier0_sota/risk.py` SHA-256 verification.
- Paper-only environment (`LIVE_EXECUTION_ENABLED != true`, zero `AK*` live keys).
- Passive kernel socket inspection (`/proc/net/tcp`, `/proc/net/tcp6`, fallback `ss -lntH`)
  confirming zero listening/established live broker ports (`4001`, `7496`, `8798`, `8796`)
  without initiating TCP handshakes.
- Atomic exclusive lockfile with stale-PID detection.
- Zero live orders, zero CDP `:9223`, zero custom HTTP servers.
"""

from __future__ import annotations

import argparse
import hashlib
import http.client
import json
import os
import re
import socket
import subprocess
import sys
import time
import urllib.parse
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

SCHEMA_VERSION = "tier0.nonstop-compounding-loop.v1"
EXPECTED_RISK_KERNEL_SHA256 = "ad8071dcd4196c4c529c02107114ebfdda6692bc9d7c220031f1b19ea3e4d847"
PREWALL_CUTOFF_MAX_ISO = "2026-10-15"
FORBIDDEN_LIVE_PORTS: tuple[int, ...] = (4001, 7496, 8798, 8796)
REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DATA_ROOT = Path(os.environ.get("TIER0_DATA_ROOT", "/mnt/z/sota-data"))
DEFAULT_MANIFEST_DIR = DEFAULT_DATA_ROOT / "manifests"
STATUS_FILENAME = "nonstop_compounding_loop_status.json"
LOCK_REL_PATH = Path("state") / "nonstop_compounding_loop.lock"
HISTORICAL_CALIBRATION_FILENAME = "historical_extreme_mover_calibration_manifest.json"
PREWALL_CALIBRATION_FILENAME = "prewall_early_mover_calibration_manifest.json"
DEFAULT_INTERVAL_SECS = 7200
DEFAULT_CALIBRATION_TRIALS = 24
DEFAULT_SUBPROCESS_TIMEOUT_SECS = 1800
DEFAULT_HEARTBEAT_MAX_AGE_SECS = 600.0
STALE_CORRUPT_LOCK_AGE_SECS = 60.0
DEFAULT_PROC_NET_PATHS: tuple[Path, ...] = (Path("/proc/net/tcp"), Path("/proc/net/tcp6"))
LOCAL_SERVICE_ENDPOINTS: dict[str, str] = {
    "openbb_6900": "http://127.0.0.1:6900/widgets.json",
    "mlflow_5000": "http://127.0.0.1:5000/health",
    "prometheus_9090": "http://127.0.0.1:9090/-/healthy",
    "grafana_3001": "http://127.0.0.1:3001/api/health",
    "otel_13133": "http://127.0.0.1:13133/",
    "dagster_3000": "http://127.0.0.1:3000/server_info",
}


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _read_json_optional(path: Path) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        return raw if isinstance(raw, dict) else None
    except (OSError, json.JSONDecodeError):
        return None


def _as_dict(val: Any) -> dict[str, Any]:
    return dict(val) if isinstance(val, Mapping) else {}


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    encoded = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    try:
        tmp_path.write_text(encoded, encoding="utf-8", newline="\n")
        os.replace(tmp_path, path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)


def verify_risk_kernel_sha256(risk_path: Path | None = None) -> str:
    """Verify `src/tier0_sota/risk.py` matches the immutable SHA-256 digest."""
    target = risk_path or (REPO_ROOT / "src" / "tier0_sota" / "risk.py")
    if not target.is_file():
        raise RuntimeError(f"Immutable risk kernel missing: {target}")
    actual = hashlib.sha256(target.read_bytes()).hexdigest()
    if actual != EXPECTED_RISK_KERNEL_SHA256:
        raise RuntimeError(
            f"Immutable risk kernel SHA-256 mismatch: expected {EXPECTED_RISK_KERNEL_SHA256}, "
            f"got {actual}"
        )
    return actual


def verify_paper_only_env(env: Mapping[str, str] | None = None) -> None:
    """Fail closed if live execution flag, `AK*` live key, or non-paper URL is present."""
    source = os.environ if env is None else env
    live_flag = str(source.get("LIVE_EXECUTION_ENABLED", "false")).strip().lower()
    if live_flag in {"1", "true", "yes", "on"}:
        raise PermissionError(
            f"Refused compounding loop: LIVE_EXECUTION_ENABLED={live_flag!r} is prohibited."
        )
    for var_name, raw_val in source.items():
        upper_name = var_name.upper()
        val = str(raw_val).strip()
        if not val:
            continue
        if upper_name in {"ALPACA_LIVE_API_KEY", "ALPACA_LIVE_SECRET_KEY"}:
            raise PermissionError(f"Refused compounding loop: live secret env {var_name} is set.")
        if (
            ("ALPACA" in upper_name or "APCA" in upper_name)
            and "KEY" in upper_name
            and val.upper().startswith("AK")
        ):
            raise PermissionError(
                f"Refused compounding loop: live AK* Alpaca key detected in {var_name}."
            )
        if (
            upper_name in {"APCA_API_BASE_URL", "ALPACA_BASE_URL"}
            and "paper-api" not in val.lower()
        ):
            raise PermissionError(f"Refused compounding loop: non-paper Alpaca URL in {var_name}.")


def _parse_proc_net_tcp_active_ports(text: str, target_ports: set[int]) -> set[int]:
    """Passively parse `/proc/net/tcp*` for LISTEN (0A) or ESTABLISHED (01) target ports."""
    found: set[int] = set()
    for line in text.splitlines()[1:]:
        parts = line.split()
        if len(parts) < 4:
            continue
        local_addr, rem_addr, state_hex = parts[1], parts[2], parts[3].upper()
        if state_hex not in {"0A", "01"}:
            continue
        if ":" in local_addr:
            try:
                local_port = int(local_addr.rsplit(":", 1)[1], 16)
                if local_port in target_ports:
                    found.add(local_port)
            except ValueError:
                pass
        if state_hex == "01" and ":" in rem_addr:
            try:
                rem_port = int(rem_addr.rsplit(":", 1)[1], 16)
                if rem_port in target_ports:
                    found.add(rem_port)
            except ValueError:
                pass
    return found


def _scan_ss_listening_ports(target_ports: set[int]) -> set[int]:
    """Passive fallback via `ss -lntH` if `/proc/net/tcp*` is unavailable."""
    found: set[int] = set()
    try:
        proc = subprocess.run(  # noqa: S603
            ["/usr/bin/ss", "-lntH"],
            capture_output=True,
            text=True,
            timeout=3,
            check=False,
        )
    except OSError:
        return found
    for line in (proc.stdout or "").splitlines():
        for match in re.finditer(r":(\d+)\b", line):
            port_val = int(match.group(1))
            if port_val in target_ports:
                found.add(port_val)
    return found


def verify_zero_live_broker_ports(
    ports: Sequence[int] = FORBIDDEN_LIVE_PORTS,
    *,
    proc_net_paths: Sequence[Path] | None = None,
) -> list[int]:
    """Passively verify zero live broker ports are listening or established (no TCP connect)."""
    target_ports = {int(p) for p in ports}
    paths = tuple(DEFAULT_PROC_NET_PATHS if proc_net_paths is None else proc_net_paths)
    found: set[int] = set()
    any_proc_read = False
    for proc_path in paths:
        if proc_path.is_file():
            try:
                found |= _parse_proc_net_tcp_active_ports(
                    proc_path.read_text(encoding="utf-8", errors="ignore"),
                    target_ports,
                )
                any_proc_read = True
            except OSError:
                continue
    if not any_proc_read and proc_net_paths is None:
        found |= _scan_ss_listening_ports(target_ports)
    open_ports = sorted(found)
    if open_ports:
        raise PermissionError(
            f"Refused compounding loop: forbidden live broker port(s) active: {open_ports}"
        )
    return open_ports


def _pid_is_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


class CompoundingLoopLock:
    """Atomic exclusive lockfile with stale-PID detection and safe release."""

    def __init__(self, lock_path: Path) -> None:
        self.lock_path = lock_path
        self._acquired = False

    def _try_create_exclusive(self) -> bool:
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
        try:
            fd = os.open(str(self.lock_path), flags, 0o600)
        except FileExistsError:
            return False
        payload = json.dumps(
            {
                "pid": os.getpid(),
                "acquired_at_utc": _utc_now().isoformat(),
                "host": socket.gethostname(),
            },
            sort_keys=True,
        )
        try:
            os.write(fd, (payload + "\n").encode("utf-8"))
        finally:
            os.close(fd)
        self._acquired = True
        return True

    def _is_stale_lock(self) -> tuple[bool, int | None]:
        data = _read_json_optional(self.lock_path)
        if data is not None and isinstance(data.get("pid"), int):
            existing_pid = int(data["pid"])
            return (not _pid_is_alive(existing_pid)), existing_pid
        try:
            age_secs = time.time() - self.lock_path.stat().st_mtime
            return age_secs >= STALE_CORRUPT_LOCK_AGE_SECS, None
        except OSError:
            return True, None

    def acquire(self) -> None:
        """Acquire exclusive lockfile or raise BlockingIOError if an active run holds it."""
        if self._try_create_exclusive():
            return
        stale, existing_pid = self._is_stale_lock()
        if not stale:
            raise BlockingIOError(
                f"Compounding loop lock already held by active PID {existing_pid}: {self.lock_path}"
            )
        self.lock_path.unlink(missing_ok=True)
        if not self._try_create_exclusive():
            raise BlockingIOError(f"Compounding loop lock contested concurrently: {self.lock_path}")

    def release(self) -> None:
        """Release lockfile only if still owned by this process PID."""
        if not self._acquired:
            return
        try:
            data = _read_json_optional(self.lock_path)
            if data is None or data.get("pid") == os.getpid():
                self.lock_path.unlink(missing_ok=True)
        finally:
            self._acquired = False

    def __enter__(self) -> CompoundingLoopLock:
        self.acquire()
        return self

    def __exit__(self, *_exc: object) -> None:
        self.release()


def _parse_iso_age_secs(ts_raw: Any, now: datetime) -> float | None:
    if not isinstance(ts_raw, str) or not ts_raw.strip():
        return None
    try:
        parsed = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return round(max(0.0, (now - parsed.astimezone(UTC)).total_seconds()), 2)


def _probe_local_http_service(url: str, timeout_secs: float = 1.5) -> dict[str, Any]:
    """Read-only localhost observability probe (never touches broker endpoints)."""
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme != "http" or parsed.hostname not in {"127.0.0.1", "localhost"}:
        raise ValueError(f"Refused non-loopback observability probe: {url}")
    if parsed.port in FORBIDDEN_LIVE_PORTS:
        raise PermissionError(f"Refused forbidden live port probe: {parsed.port}")
    conn = http.client.HTTPConnection(parsed.netloc, timeout=timeout_secs)
    try:
        path = parsed.path or "/"
        if parsed.query:
            path = f"{path}?{parsed.query}"
        conn.request("GET", path)
        resp = conn.getresponse()
        resp.read(256)
        status_code = int(resp.status)
        return {"ok": 200 <= status_code < 400, "http_status": status_code, "url": url}
    except (http.client.HTTPException, TimeoutError, OSError) as exc:
        return {"ok": False, "http_status": None, "error": type(exc).__name__, "url": url}
    finally:
        conn.close()


def _run_stage_subprocess(
    cmd: list[str],
    *,
    cwd: Path,
    timeout_secs: int,
) -> dict[str, Any]:
    t0 = time.monotonic()
    env = dict(os.environ)
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    env["PYTHONPATH"] = str(cwd / "src")
    try:
        proc = subprocess.run(  # noqa: S603
            cmd,
            cwd=str(cwd),
            env=env,
            capture_output=True,
            text=True,
            timeout=timeout_secs,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return {
            "status": "timeout",
            "returncode": -1,
            "duration_secs": round(time.monotonic() - t0, 3),
            "command": cmd,
            "stderr_tail": f"Subprocess timed out after {timeout_secs}s",
        }
    return {
        "status": "ok" if proc.returncode == 0 else "failed",
        "returncode": proc.returncode,
        "duration_secs": round(time.monotonic() - t0, 3),
        "command": cmd,
        "stdout_tail": proc.stdout[-600:].strip() if proc.stdout else "",
        "stderr_tail": proc.stderr[-600:].strip() if proc.stderr else "",
    }


def _extract_calibration_summary(doc: Mapping[str, Any] | None) -> dict[str, Any]:
    if doc is None:
        return {"present": False}
    optuna_doc = _as_dict(doc.get("optuna"))
    mlflow_doc = _as_dict(doc.get("mlflow"))
    taxonomy = (
        doc.get("categorized_mover_taxonomy")
        or doc.get("mover_taxonomy")
        or doc.get("taxonomy")
        or doc.get("categories")
    )
    categories_count = len(taxonomy) if isinstance(taxonomy, (dict, list)) else 0
    nvda_present = any(
        doc.get(k) is not None
        for k in (
            "nvda_monday_premarket_e2e_replay",
            "nvda_monday_premarket_replay",
            "nvda_monday_premarket",
            "nvda_premarket_replay",
            "postwall_observational_case_study",
        )
    )
    per_sym = doc.get("per_symbol_min_trailing_pct")
    calibrated_symbols_count = len(per_sym) if isinstance(per_sym, Mapping) else 0
    trials = (
        optuna_doc.get("n_trials")
        or optuna_doc.get("trials_total")
        or optuna_doc.get("trials_completed")
        or doc.get("optuna_trials")
    )
    return {
        "present": True,
        "schema_version": doc.get("schema_version"),
        "generated_at_utc": doc.get("generated_at_utc"),
        "prewall_cutoff_iso": (
            doc.get("prewall_cutoff_iso")
            or doc.get("prewall_wall_date")
            or doc.get("discovery_wall_iso")
        ),
        "paper_only": doc.get("paper_only", True),
        "live_execution_enabled": doc.get("live_execution_enabled", False),
        "categories_count": categories_count,
        "calibrated_symbols_count": calibrated_symbols_count,
        "nvda_premarket_present": nvda_present,
        "trials_completed": int(trials) if isinstance(trials, (int, float)) else None,
        "mlflow_run_id": mlflow_doc.get("run_id") or doc.get("mlflow_run_id"),
    }


def _run_stage_1_calibration(
    *,
    repo_root: Path,
    manifest_dir: Path,
    trials: int,
    skip_calibration: bool,
    dry_run: bool,
    timeout_secs: int,
) -> dict[str, Any]:
    t0 = time.monotonic()
    hist_path = manifest_dir / HISTORICAL_CALIBRATION_FILENAME
    prewall_path = manifest_dir / PREWALL_CALIBRATION_FILENAME

    violations: list[str] = []
    hist_doc = _read_json_optional(hist_path)
    prewall_doc = _read_json_optional(prewall_path)

    for label, doc in (("historical_calibration", hist_doc), ("prewall_calibration", prewall_doc)):
        if doc is None:
            continue
        if doc.get("paper_only") is False:
            violations.append(f"paper_only_false:{label}")
        if doc.get("live_execution_enabled") is True:
            violations.append(f"live_execution_enabled_true:{label}")
        cutoff = str(
            doc.get("prewall_cutoff_iso")
            or doc.get("prewall_wall_date")
            or doc.get("discovery_wall_iso")
            or ""
        ).strip()
        if cutoff and cutoff[:10] > PREWALL_CUTOFF_MAX_ISO:
            violations.append(f"prewall_cutoff_exceeded:{label}:{cutoff}")

    if violations:
        return {
            "status": "failed",
            "duration_secs": round(time.monotonic() - t0, 3),
            "violations": violations,
            "historical_calibration": _extract_calibration_summary(hist_doc),
            "prewall_calibration": _extract_calibration_summary(prewall_doc),
        }

    cmd = [
        sys.executable,
        "-m",
        "tier0_sota.research",
        "--calibrate-extreme-movers",
        "--trials",
        str(trials),
        "--manifest-dir",
        str(manifest_dir),
    ]
    if skip_calibration:
        return {
            "status": "skipped",
            "reason": "skip_calibration_flag",
            "duration_secs": round(time.monotonic() - t0, 3),
            "historical_calibration": _extract_calibration_summary(hist_doc),
            "prewall_calibration": _extract_calibration_summary(prewall_doc),
        }
    if dry_run:
        return {
            "status": "dry_run",
            "upstream_module": "tier0_sota.research",
            "planned_command": cmd,
            "historical_calibration": _extract_calibration_summary(hist_doc),
            "prewall_calibration": _extract_calibration_summary(prewall_doc),
            "duration_secs": round(time.monotonic() - t0, 3),
        }

    res = _run_stage_subprocess(cmd, cwd=repo_root, timeout_secs=timeout_secs)
    hist_doc = _read_json_optional(hist_path)
    prewall_doc = _read_json_optional(prewall_path)
    res["historical_calibration"] = _extract_calibration_summary(hist_doc)
    res["prewall_calibration"] = _extract_calibration_summary(prewall_doc)
    res["manifest_present"] = bool(hist_doc is not None or prewall_doc is not None)
    res["trials_completed"] = res["historical_calibration"].get("trials_completed") or res[
        "prewall_calibration"
    ].get("trials_completed")
    return res


def _run_stage_2_reflection(
    *,
    repo_root: Path,
    manifest_dir: Path,
    skip_reflection: bool,
    dry_run: bool,
    timeout_secs: int,
) -> dict[str, Any]:
    script = repo_root / "scripts" / "run_compounding_reflection.py"
    refl_path = manifest_dir / "compounding_self_reflection_manifest.json"
    cmd = [
        sys.executable,
        str(script),
        "--manifest-dir",
        str(manifest_dir),
    ]
    if skip_reflection:
        return {
            "status": "skipped",
            "reason": "skip_reflection_flag",
            "existing_manifest_present": refl_path.is_file(),
            "duration_secs": 0.0,
        }
    if not script.is_file():
        return {
            "status": "failed",
            "reason": "reflection_script_missing",
            "script": str(script),
            "duration_secs": 0.0,
        }
    if dry_run:
        return {
            "status": "dry_run",
            "script_present": True,
            "planned_command": cmd,
            "existing_manifest_present": refl_path.is_file(),
            "duration_secs": 0.0,
        }
    res = _run_stage_subprocess(cmd, cwd=repo_root, timeout_secs=timeout_secs)
    refl_doc = _read_json_optional(refl_path)
    if refl_doc is not None:
        optuna_doc = _as_dict(refl_doc.get("optuna"))
        mlflow_doc = _as_dict(refl_doc.get("mlflow"))
        xerj_doc = _as_dict(refl_doc.get("xerj"))
        inspect_doc = _as_dict(refl_doc.get("inspect"))
        dspy_doc = _as_dict(refl_doc.get("dspy"))
        res["manifest_present"] = True
        res["optuna_trials_total"] = optuna_doc.get("trials_total")
        res["mlflow_logged"] = bool(mlflow_doc.get("logged"))
        res["mlflow_run_id"] = mlflow_doc.get("run_id")
        res["xerj_pushed"] = bool(xerj_doc.get("pushed"))
        res["inspect_accuracy"] = inspect_doc.get("accuracy")
        res["dspy_ok"] = bool(dspy_doc.get("ok"))
    else:
        res["manifest_present"] = False
    return res


def _check_systemd_timer_active(unit_name: str) -> str:
    try:
        proc = subprocess.run(  # noqa: S603
            ["/usr/bin/systemctl", "is-active", unit_name],
            capture_output=True,
            text=True,
            timeout=3,
            check=False,
        )
        out = (proc.stdout or proc.stderr or "").strip()
        return out or f"exit_{proc.returncode}"
    except OSError:
        return "unavailable"


def _summarize_mover_categories(movers: Mapping[str, Any] | None) -> dict[str, int]:
    if not isinstance(movers, Mapping):
        return {}
    entries = movers.get("entries")
    if not isinstance(entries, list):
        return {}
    counts: dict[str, int] = {}
    for entry in entries:
        if not isinstance(entry, Mapping):
            continue
        cat = str(
            entry.get("calibrated_category")
            or entry.get("potential_category")
            or entry.get("mover_category")
            or entry.get("category")
            or entry.get("setup_archetype")
            or "UNCATEGORIZED"
        ).strip()
        counts[cat] = counts.get(cat, 0) + 1
    return dict(sorted(counts.items()))


def _run_stage_3_paper_tv_heartbeat(
    *,
    manifest_dir: Path,
    now: datetime,
    max_age_secs: float = DEFAULT_HEARTBEAT_MAX_AGE_SECS,
    probe_services: bool = False,
) -> dict[str, Any]:
    t0 = time.monotonic()
    movers_path = manifest_dir / "live_whole_market_movers_manifest.json"
    bundle_path = manifest_dir / "live_lane_order_preset_bundle.json"
    exec_path = manifest_dir / "paper_farm_execution_status.json"
    coord_path = manifest_dir / "tv_sota_pack" / "tv_engine_coordination_manifest.json"
    perf_path = manifest_dir / "paper_farm_live_performance_manifest.json"
    tca_path = manifest_dir / "tca_calibration_manifest.json"
    pack_dir = manifest_dir / "tv_sota_pack"

    movers = _read_json_optional(movers_path)
    bundle = _read_json_optional(bundle_path)
    exec_doc = _read_json_optional(exec_path)
    coord = _read_json_optional(coord_path)
    perf = _read_json_optional(perf_path)
    tca = _read_json_optional(tca_path)

    violations: list[str] = []
    warnings: list[str] = []

    for label, doc, required in (
        ("tv_engine_coordination", coord, True),
        ("paper_farm_performance", perf, True),
        ("tca_calibration", tca, True),
        ("paper_farm_execution", exec_doc, False),
        ("live_lane_order_preset_bundle", bundle, False),
    ):
        if doc is None:
            if required:
                warnings.append(f"missing_manifest:{label}")
            continue
        if doc.get("paper_only") is False:
            violations.append(f"paper_only_false:{label}")
        if doc.get("live_execution_enabled") is True:
            violations.append(f"live_execution_enabled_true:{label}")

    if coord is not None:
        if coord.get("semi_auto_confirmed") is not False:
            violations.append("tv_coordination_semi_auto_confirmed_not_false")
        if coord.get("auto_submit_enabled") is True:
            violations.append("tv_coordination_auto_submit_enabled_true")

    if exec_doc is not None and exec_doc.get("entry_fault") is True:
        warnings.append("paper_farm_execution_entry_fault_true")

    movers_age = _parse_iso_age_secs(movers.get("generated_at_utc") if movers else None, now)
    coord_age = _parse_iso_age_secs(coord.get("generated_at_utc") if coord else None, now)
    perf_age = _parse_iso_age_secs(perf.get("generated_at_utc") if perf else None, now)
    exec_age = _parse_iso_age_secs(exec_doc.get("generated_at_utc") if exec_doc else None, now)

    for label, age in (
        ("movers", movers_age),
        ("tv_coordination", coord_age),
        ("perf", perf_age),
        ("paper_execution", exec_age),
    ):
        if age is not None and age > max_age_secs:
            warnings.append(f"stale_manifest:{label}:{age}s")

    pack_files = (
        sorted(p.name for p in pack_dir.iterdir() if p.is_file()) if pack_dir.is_dir() else []
    )
    pack_latest_age: float | None = None
    if pack_dir.is_dir() and pack_files:
        newest_mtime = max((pack_dir / name).stat().st_mtime for name in pack_files)
        pack_latest_age = round(max(0.0, now.timestamp() - newest_mtime), 2)

    handles = _as_dict(coord.get("venue_order_handles")) if isinstance(coord, dict) else {}
    if not handles and isinstance(exec_doc, dict):
        handles = _as_dict(exec_doc.get("venue_order_handles"))
    alpaca_profile = str(handles.get("alpaca_venue_profile") or "UNKNOWN")
    combined_max_rpm = int(handles.get("combined_max_rpm") or 0)
    combined_exit_reserve_rpm = int(handles.get("combined_exit_reserve_rpm") or 0)
    alpaca_1000_active = alpaca_profile == "ALPACA_PAPER_1000" and combined_max_rpm == 6000

    observe_timer_state = _check_systemd_timer_active("tier0-paper-farm-observe.timer")
    compounding_timer_state = _check_systemd_timer_active("tier0-compounding-loop.timer")
    if not pack_files:
        warnings.append("tv_sota_pack_empty_or_missing")

    service_probes: dict[str, Any] = {}
    if probe_services:
        for svc_name, svc_url in LOCAL_SERVICE_ENDPOINTS.items():
            service_probes[svc_name] = _probe_local_http_service(svc_url)

    status = "failed" if violations else ("warn" if warnings else "ok")
    return {
        "status": status,
        "duration_secs": round(time.monotonic() - t0, 3),
        "paper_farm_observe_timer": observe_timer_state,
        "compounding_loop_timer": compounding_timer_state,
        "violations": violations,
        "warnings": warnings,
        "paper_only_verified": not any("paper_only" in v for v in violations),
        "live_execution_disabled_verified": not any("live_execution" in v for v in violations),
        "alpaca_venue_profile": alpaca_profile,
        "combined_paper_max_rpm": combined_max_rpm,
        "combined_exit_reserve_rpm": combined_exit_reserve_rpm,
        "alpaca_paper_1000_6000rpm_verified": alpaca_1000_active,
        "tv_sota_pack_files_count": len(pack_files),
        "tv_sota_pack_latest_age_secs": pack_latest_age,
        "tv_mcp_sync_receipt_present": (pack_dir / "tv_mcp_sync_receipt.json").is_file(),
        "service_probes": service_probes,
        "manifests": {
            "live_whole_market_movers": {
                "present": movers is not None,
                "age_secs": movers_age,
                "ranked_count": movers.get("ranked_count") if movers else None,
                "category_counts": _summarize_mover_categories(movers),
            },
            "live_lane_order_preset_bundle": {
                "present": bundle is not None,
                "venue_profile": bundle.get("venue_profile") if bundle else None,
                "alpaca_per_lane_max_rpm": (
                    bundle.get("alpaca_per_lane_max_rpm") if bundle else None
                ),
                "preset_count": (
                    len(bundle.get("top_potential_symbols") or bundle.get("presets") or ())
                    if isinstance(bundle, dict)
                    and isinstance(
                        bundle.get("top_potential_symbols") or bundle.get("presets"), list
                    )
                    else None
                ),
            },
            "paper_farm_execution": {
                "present": exec_doc is not None,
                "age_secs": exec_age,
                "status": exec_doc.get("status") if exec_doc else None,
                "active_session": exec_doc.get("active_session") if exec_doc else None,
                "managed_symbols": exec_doc.get("managed_symbols") if exec_doc else None,
                "active_positions_count": (
                    exec_doc.get("active_positions_count") if exec_doc else None
                ),
                "streams": exec_doc.get("streams") if exec_doc else None,
                "entry_fault": exec_doc.get("entry_fault") if exec_doc else None,
            },
            "tv_engine_coordination": {
                "present": coord is not None,
                "age_secs": coord_age,
                "active_session": coord.get("active_session") if coord else None,
                "tca_calibration_ok": coord.get("tca_calibration_ok") if coord else None,
                "staged_alerts_evaluated": coord.get("staged_alerts_evaluated") if coord else None,
                "ready_for_operator_confirm_count": (
                    coord.get("ready_for_operator_confirm_count") if coord else None
                ),
            },
            "paper_farm_live_performance": {
                "present": perf is not None,
                "age_secs": perf_age,
                "tca_fills_count": (
                    perf.get("summary", {}).get("tca_fills_count")
                    if isinstance(perf, dict) and isinstance(perf.get("summary"), dict)
                    else None
                ),
            },
            "tca_calibration": {
                "present": tca is not None,
                "duckdb_tca_rows": tca.get("duckdb_tca_rows") if tca else None,
                "liquid_ge10_within_2x_envelope": (
                    tca.get("liquid_ge10_within_2x_envelope") if tca else None
                ),
                "realized_mean_is_bps": tca.get("realized_mean_is_bps") if tca else None,
            },
        },
    }


def _compute_compounding_totals(
    *,
    manifest_dir: Path,
    prev_status: Mapping[str, Any] | None,
    stage_1: Mapping[str, Any],
    stage_2: Mapping[str, Any],
    stage_3: Mapping[str, Any],
) -> dict[str, Any]:
    prev_totals = (
        _as_dict(prev_status.get("compounding_totals")) if isinstance(prev_status, Mapping) else {}
    )
    refl_doc = _read_json_optional(manifest_dir / "compounding_self_reflection_manifest.json")
    hist_doc = _read_json_optional(manifest_dir / HISTORICAL_CALIBRATION_FILENAME)
    prewall_doc = _read_json_optional(manifest_dir / PREWALL_CALIBRATION_FILENAME)

    optuna_trials = 0
    if isinstance(refl_doc, dict) and isinstance(refl_doc.get("optuna"), dict):
        optuna_trials += int(refl_doc["optuna"].get("trials_total") or 0)
    primary_cal_doc = hist_doc if hist_doc is not None else prewall_doc
    if isinstance(primary_cal_doc, dict) and isinstance(primary_cal_doc.get("optuna"), dict):
        optuna_trials += int(
            primary_cal_doc["optuna"].get("n_trials")
            or primary_cal_doc["optuna"].get("trials_total")
            or primary_cal_doc["optuna"].get("trials_completed")
            or 0
        )
    optuna_trials = max(optuna_trials, int(prev_totals.get("optuna_trials_total") or 0))

    mlflow_logged_now = stage_2.get("status") == "ok" and bool(stage_2.get("mlflow_logged"))
    mlflow_doc = _as_dict(refl_doc.get("mlflow")) if isinstance(refl_doc, dict) else {}
    prev_mlflow_runs = int(prev_totals.get("mlflow_runs_logged") or 0)
    mlflow_runs = (
        prev_mlflow_runs + 1
        if mlflow_logged_now
        else max(prev_mlflow_runs, 1 if mlflow_doc.get("logged") else 0)
    )

    xerj_pushed_now = stage_2.get("status") == "ok" and bool(stage_2.get("xerj_pushed"))
    xerj_doc = _as_dict(refl_doc.get("xerj")) if isinstance(refl_doc, dict) else {}
    prev_xerj = int(prev_totals.get("xerj_lessons_pushed") or 0)
    xerj_pushed = (
        prev_xerj + 1 if xerj_pushed_now else max(prev_xerj, 1 if xerj_doc.get("pushed") else 0)
    )

    prev_cal_runs = int(prev_totals.get("early_mover_calibration_runs") or 0)
    cal_runs = prev_cal_runs + (1 if stage_1.get("status") == "ok" else 0)
    if cal_runs == 0 and (hist_doc is not None or prewall_doc is not None):
        cal_runs = 1

    tca_rows = 0
    tca_info = stage_3.get("manifests", {}).get("tca_calibration", {})
    if isinstance(tca_info, Mapping) and tca_info.get("duckdb_tca_rows") is not None:
        tca_rows = int(tca_info["duckdb_tca_rows"])

    prev_cycles = int(prev_totals.get("cycles_completed") or 0)
    return {
        "cycles_completed": prev_cycles + 1,
        "optuna_trials_total": optuna_trials,
        "mlflow_runs_logged": mlflow_runs,
        "mlflow_latest_run_id": mlflow_doc.get("run_id") or prev_totals.get("mlflow_latest_run_id"),
        "xerj_lessons_pushed": xerj_pushed,
        "xerj_latest_id": xerj_doc.get("id") or prev_totals.get("xerj_latest_id"),
        "early_mover_calibration_runs": cal_runs,
        "tca_rows_calibrated": tca_rows,
    }


def run_compounding_iteration(
    *,
    repo_root: Path = REPO_ROOT,
    manifest_dir: Path = DEFAULT_MANIFEST_DIR,
    interval_secs: int = DEFAULT_INTERVAL_SECS,
    calibration_trials: int = DEFAULT_CALIBRATION_TRIALS,
    skip_calibration: bool = False,
    skip_reflection: bool = False,
    dry_run: bool = False,
    probe_services: bool = False,
    subprocess_timeout_secs: int = DEFAULT_SUBPROCESS_TIMEOUT_SECS,
    risk_path: Path | None = None,
    env: Mapping[str, str] | None = None,
    live_ports: Sequence[int] = FORBIDDEN_LIVE_PORTS,
    proc_net_paths: Sequence[Path] | None = None,
) -> dict[str, Any]:
    """Execute one fail-closed compounding cycle and write atomic status manifest."""
    started = _utc_now()
    t0 = time.monotonic()

    risk_sha = verify_risk_kernel_sha256(
        risk_path or (repo_root / "src" / "tier0_sota" / "risk.py")
    )
    verify_paper_only_env(env)
    open_live_ports = verify_zero_live_broker_ports(live_ports, proc_net_paths=proc_net_paths)

    status_path = manifest_dir / STATUS_FILENAME
    lock_path = manifest_dir / LOCK_REL_PATH

    with CompoundingLoopLock(lock_path):
        prev_status = _read_json_optional(status_path)
        iteration = int(prev_status.get("iteration", 0)) + 1 if isinstance(prev_status, dict) else 1

        stage_1 = _run_stage_1_calibration(
            repo_root=repo_root,
            manifest_dir=manifest_dir,
            trials=calibration_trials,
            skip_calibration=skip_calibration,
            dry_run=dry_run,
            timeout_secs=subprocess_timeout_secs,
        )
        stage_2 = _run_stage_2_reflection(
            repo_root=repo_root,
            manifest_dir=manifest_dir,
            skip_reflection=skip_reflection,
            dry_run=dry_run,
            timeout_secs=subprocess_timeout_secs,
        )
        stage_3 = _run_stage_3_paper_tv_heartbeat(
            manifest_dir=manifest_dir,
            now=_utc_now(),
            probe_services=probe_services,
        )

        completed = _utc_now()
        stage_statuses = (stage_1["status"], stage_2["status"], stage_3["status"])
        if any(s in {"failed", "timeout"} for s in stage_statuses):
            verdict = "FAIL"
        elif any(s == "warn" for s in stage_statuses):
            verdict = "WARN"
        else:
            verdict = "PASS"

        totals = _compute_compounding_totals(
            manifest_dir=manifest_dir,
            prev_status=prev_status,
            stage_1=stage_1,
            stage_2=stage_2,
            stage_3=stage_3,
        )
        next_run = (completed + timedelta(seconds=interval_secs)).isoformat()
        heartbeat: dict[str, Any] = {
            "schema_version": SCHEMA_VERSION,
            "iteration": iteration,
            "mode": "once",
            "scheduler_authority": "systemd_timer_and_dagster",
            "dry_run": dry_run,
            "started_at_utc": started.isoformat(),
            "completed_at_utc": completed.isoformat(),
            "duration_secs": round(time.monotonic() - t0, 3),
            "next_scheduled_utc": next_run,
            "interval_secs": interval_secs,
            "overall_verdict": verdict,
            "risk_kernel_sha256": risk_sha,
            "risk_kernel_sha256_ok": risk_sha == EXPECTED_RISK_KERNEL_SHA256,
            "paper_only": True,
            "live_execution_enabled": False,
            "live_ports_open": open_live_ports,
            "venue_posture": {
                "alpaca_venue_profile": stage_3["alpaca_venue_profile"],
                "combined_paper_max_rpm": stage_3["combined_paper_max_rpm"],
                "combined_exit_reserve_rpm": stage_3["combined_exit_reserve_rpm"],
                "alpaca_paper_1000_6000rpm_verified": stage_3["alpaca_paper_1000_6000rpm_verified"],
                "ibkr_venue_profile": "IBKR_PAPER_4002",
                "paper_only": True,
                "live_execution_enabled": False,
            },
            "stages": {
                "stage_1_early_mover_pit_calibration": stage_1,
                "stage_2_dual_flywheel_reflection": stage_2,
                "stage_3_live_paper_tv_heartbeat": stage_3,
            },
            "compounding_totals": totals,
        }
        _atomic_write_json(status_path, heartbeat)
        return heartbeat


def build_arg_parser() -> argparse.ArgumentParser:
    """Build CLI parser for single-tick upstream-scheduled compounding runs."""
    parser = argparse.ArgumentParser(
        description=(
            "Tier-0 SOTA Single-Tick Compounding Orchestrator (strictly paper-only; "
            "scheduled by native systemd timer + Dagster, zero Python sleep loops)."
        )
    )
    parser.add_argument(
        "--once",
        action="store_true",
        default=True,
        help="Run exactly one fail-closed compounding iteration and exit (only supported mode).",
    )
    parser.add_argument(
        "--interval-secs",
        type=int,
        default=DEFAULT_INTERVAL_SECS,
        help=(
            "Nominal upstream timer cadence in seconds for next_scheduled_utc metadata "
            f"(default: {DEFAULT_INTERVAL_SECS})."
        ),
    )
    parser.add_argument(
        "--calibration-trials",
        type=int,
        default=DEFAULT_CALIBRATION_TRIALS,
        help=(
            "Optuna trial count for Stage 1 PIT calibration "
            f"(default: {DEFAULT_CALIBRATION_TRIALS})."
        ),
    )
    parser.add_argument(
        "--skip-calibration",
        action="store_true",
        help="Skip Stage 1 subprocess execution (still validates existing calibration manifests).",
    )
    parser.add_argument(
        "--skip-reflection",
        action="store_true",
        help="Skip Stage 2 reflection subprocess (still emits Stage 3 heartbeat & status JSON).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate safety gates & Stage 3 heartbeat without running heavy Stage 1/2 jobs.",
    )
    parser.add_argument(
        "--probe-services",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Probe local OpenBB/MLflow/Prometheus/Grafana/OTel/Dagster health endpoints.",
    )
    parser.add_argument(
        "--manifest-dir",
        type=Path,
        default=DEFAULT_MANIFEST_DIR,
        help=f"Manifest directory (default: {DEFAULT_MANIFEST_DIR}).",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint executing exactly one fail-closed compounding tick."""
    args = build_arg_parser().parse_args(argv)
    if args.interval_secs < 1:
        raise SystemExit("--interval-secs must be >= 1")
    if args.calibration_trials < 1:
        raise SystemExit("--calibration-trials must be >= 1")

    receipt = run_compounding_iteration(
        manifest_dir=args.manifest_dir,
        interval_secs=args.interval_secs,
        calibration_trials=args.calibration_trials,
        skip_calibration=bool(args.skip_calibration),
        skip_reflection=bool(args.skip_reflection),
        dry_run=bool(args.dry_run),
        probe_services=bool(args.probe_services) and not bool(args.dry_run),
    )
    print(json.dumps(receipt, indent=2, sort_keys=True))
    return 0 if str(receipt["overall_verdict"]) != "FAIL" else 1


if __name__ == "__main__":
    raise SystemExit(main())

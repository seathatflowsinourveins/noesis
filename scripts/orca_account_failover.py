#!/usr/bin/env python3
"""Official-CLI wrapper for Orca native multi-account inspection & rate-limit failover."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

DEFAULT_MANIFEST_PATH = Path(r"Z:\sota-data\manifests\orca_account_failover_status.json")
SUPPORTED_AGENTS = ("claude", "codex")


def _mask_email(raw: str | None) -> str:
    value = str(raw or "").strip()
    if not value or "@" not in value:
        return value
    local, domain = value.split("@", 1)
    masked_local = "*" * len(local) if len(local) <= 2 else f"{local[:2]}***"
    return f"{masked_local}@{domain}"


def _extract_pct(bucket: dict[str, Any] | None) -> float | None:
    if not isinstance(bucket, dict):
        return None
    raw = bucket.get("usedPercent")
    if isinstance(raw, (int, float)):
        return float(raw)
    return None


def _account_dedup_key(account: dict[str, Any]) -> str:
    for field in ("organizationUuid", "providerAccountId", "email", "id"):
        val = str(account.get(field) or "").strip()
        if val:
            return val
    return ""


def _as_dict(val: Any) -> dict[str, Any]:
    return val if isinstance(val, dict) else {}


def _as_list(val: Any) -> list[Any]:
    return val if isinstance(val, list) else []


def _parse_candidate_health(
    account_id: str,
    *,
    label: str,
    plan_type: str | None,
    rate_limits: dict[str, Any] | None,
    is_active: bool,
    threshold_pct: float,
    include_fable: bool,
) -> dict[str, Any]:
    rl = _as_dict(rate_limits)
    status = str(rl.get("status") or "unknown")
    usage_meta = _as_dict(rl.get("usageMetadata"))
    failure_kind = str(usage_meta.get("failureKind") or "")
    session_pct = _extract_pct(rl.get("session"))
    weekly_pct = _extract_pct(rl.get("weekly"))
    fable_pct = _extract_pct(rl.get("fableWeekly"))

    is_rate_limited = status == "error" and failure_kind == "rate-limited"
    over_threshold = any(
        pct is not None and pct >= threshold_pct
        for pct in (
            session_pct,
            weekly_pct,
            fable_pct if include_fable else None,
        )
    )
    needs_switch = bool(is_rate_limited or over_threshold)
    healthy = bool(status == "ok" and not needs_switch)

    return {
        "account_id": account_id,
        "label": label,
        "plan_type": str(plan_type or rl.get("planType") or ""),
        "status": status,
        "failure_kind": failure_kind,
        "session_used_pct": session_pct,
        "weekly_used_pct": weekly_pct,
        "fable_weekly_used_pct": fable_pct,
        "is_active": is_active,
        "needs_switch": needs_switch,
        "healthy": healthy,
    }


def _candidate_sort_key(cand: dict[str, Any]) -> tuple[int, float, float, int]:
    healthy_rank = 0 if cand["healthy"] else 1
    weekly_rank = cand["weekly_used_pct"] if cand["weekly_used_pct"] is not None else 999.0
    session_rank = cand["session_used_pct"] if cand["session_used_pct"] is not None else 999.0
    default_rank = 1 if cand["account_id"] == "system" else 0
    return (healthy_rank, weekly_rank, session_rank, default_rank)


def evaluate_agent_failover(
    orca_payload: dict[str, Any],
    agent: str,
    *,
    threshold_pct: float = 95.0,
    include_fable: bool = False,
) -> dict[str, Any]:
    """Evaluate active account health and pick the healthiest failover target if needed."""
    result = _as_dict(orca_payload.get("result"))
    section = _as_dict(result.get(agent))
    rate_root = _as_dict(result.get("rateLimits"))
    active_id = str(section.get("activeAccountId") or "").strip() or None
    accounts = _as_list(section.get("accounts"))

    inactive_key = "inactiveClaudeAccounts" if agent == "claude" else "inactiveCodexAccounts"
    inactive_map: dict[str, dict[str, Any]] = {}
    for item in _as_list(rate_root.get(inactive_key)):
        if isinstance(item, dict) and item.get("accountId"):
            inactive_map[str(item["accountId"])] = _as_dict(item.get("rateLimits"))

    seen_dedup_keys: set[str] = set()
    active_entry: dict[str, Any] | None = None
    for acc in accounts:
        if isinstance(acc, dict) and str(acc.get("id") or "") == active_id:
            active_entry = acc
            break

    if active_entry is not None:
        dedup_k = _account_dedup_key(active_entry)
        if dedup_k:
            seen_dedup_keys.add(dedup_k)

    candidates: list[dict[str, Any]] = []
    if active_entry is not None and active_id is not None:
        candidates.append(
            _parse_candidate_health(
                active_id,
                label=_mask_email(active_entry.get("email")),
                plan_type=None,
                rate_limits=_as_dict(rate_root.get(agent)),
                is_active=True,
                threshold_pct=threshold_pct,
                include_fable=include_fable,
            )
        )

    for acc in accounts:
        if not isinstance(acc, dict):
            continue
        acc_id = str(acc.get("id") or "").strip()
        if not acc_id or acc_id == active_id:
            continue
        dedup_k = _account_dedup_key(acc)
        if dedup_k and dedup_k in seen_dedup_keys:
            continue
        if dedup_k:
            seen_dedup_keys.add(dedup_k)
        candidates.append(
            _parse_candidate_health(
                acc_id,
                label=_mask_email(acc.get("email")),
                plan_type=None,
                rate_limits=inactive_map.get(acc_id),
                is_active=False,
                threshold_pct=threshold_pct,
                include_fable=include_fable,
            )
        )

    sys_rl_key = f"{agent}-system-default"
    sys_default_email: str | None = None
    has_system = False
    sys_default_doc = _as_dict(section.get("systemDefault"))
    if agent == "claude" and section.get("systemDefaultEmail"):
        sys_default_email = str(section.get("systemDefaultEmail"))
        has_system = True
    elif agent == "codex" and sys_default_doc.get("hasAuth"):
        sys_default_email = str(sys_default_doc.get("email") or "")
        has_system = True

    if has_system:
        candidates.append(
            _parse_candidate_health(
                "system",
                label=_mask_email(sys_default_email) or "system-default",
                plan_type=None,
                rate_limits=inactive_map.get(sys_rl_key),
                is_active=False,
                threshold_pct=threshold_pct,
                include_fable=include_fable,
            )
        )

    active_health = next((c for c in candidates if c["is_active"]), None)
    needs_switch = bool(active_health["needs_switch"]) if active_health else False
    alternates = sorted(
        [c for c in candidates if not c["is_active"]],
        key=_candidate_sort_key,
    )
    healthy_alternates = [c for c in alternates if c["healthy"]]
    recommended_target = healthy_alternates[0]["account_id"] if healthy_alternates else None

    return {
        "agent": agent,
        "active_account_id": active_id,
        "active_label": active_health["label"] if active_health else "",
        "active_status": active_health["status"] if active_health else "unavailable",
        "active_session_used_pct": active_health["session_used_pct"] if active_health else None,
        "active_weekly_used_pct": active_health["weekly_used_pct"] if active_health else None,
        "active_fable_weekly_used_pct": (
            active_health["fable_weekly_used_pct"] if active_health else None
        ),
        "needs_switch": needs_switch,
        "recommended_target": recommended_target if needs_switch else None,
        "healthy_alternates_count": len(healthy_alternates),
        "candidates": candidates,
        "switched": False,
        "switch_result": None,
    }


def _run_orca_json(args: list[str]) -> dict[str, Any]:
    orca_exe = shutil.which("orca")
    if orca_exe is None:
        raise FileNotFoundError("orca CLI executable not found on PATH")
    proc = subprocess.run(  # noqa: S603
        [orca_exe, *args, "--json"],
        capture_output=True,
        text=True,
        check=False,
        timeout=15,
    )
    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout or f"exit={proc.returncode}").strip()
        raise RuntimeError(f"orca {' '.join(args)} failed: {detail}")
    data = json.loads(proc.stdout)
    if not isinstance(data, dict) or not data.get("ok"):
        raise RuntimeError(f"orca {' '.join(args)} returned not ok: {proc.stdout.strip()}")
    return data


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.tmp.{os.getpid()}.{uuid.uuid4().hex}")
    try:
        tmp.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8", newline="\n")
        os.replace(tmp, path)
    finally:
        if tmp.exists():
            tmp.unlink(missing_ok=True)


def run_failover_check(
    *,
    agents: list[str],
    threshold_pct: float,
    include_fable: bool,
    apply: bool,
    manifest_path: Path | None,
) -> dict[str, Any]:
    """Query `orca account list --json`, evaluate headroom, and optionally fail over."""
    orca_data = _run_orca_json(["account", "list"])
    decisions: dict[str, dict[str, Any]] = {}
    for agent in agents:
        decision = evaluate_agent_failover(
            orca_data,
            agent,
            threshold_pct=threshold_pct,
            include_fable=include_fable,
        )
        if apply and decision["needs_switch"] and decision["recommended_target"]:
            target = str(decision["recommended_target"])
            select_res = _run_orca_json(
                ["account", "select", "--agent", agent, "--account", target]
            )
            decision["switched"] = True
            decision["switch_result"] = {
                "target_account_id": target,
                "ok": bool(select_res.get("ok")),
            }
        decisions[agent] = decision

    report: dict[str, Any] = {
        "schema_version": "tier0.orca-account-failover.v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "apply_requested": apply,
        "threshold_pct": threshold_pct,
        "include_fable": include_fable,
        "agents": decisions,
    }
    if manifest_path is not None:
        _atomic_write_json(manifest_path, report)
    return report


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint for Orca native multi-account health & seamless rate-limit failover."""
    parser = argparse.ArgumentParser(
        description="Inspect Orca native OAuth headroom and fail over on rate-limit."
    )
    parser.add_argument(
        "--agent",
        choices=("claude", "codex", "all"),
        default="all",
        help="Target agent provider (default: all)",
    )
    parser.add_argument(
        "--threshold-pct",
        type=float,
        default=95.0,
        help="Used-percentage threshold to trigger failover (default: 95.0)",
    )
    parser.add_argument(
        "--include-fable",
        action="store_true",
        help="Also trigger failover on Claude Fable-specific weekly bucket exhaustion",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Call `orca account select` when active account needs failover (default: dry-run)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit machine-readable JSON output",
    )
    parser.add_argument(
        "--manifest",
        default=str(DEFAULT_MANIFEST_PATH),
        help="Manifest path to update atomically (empty string disables writing)",
    )
    args = parser.parse_args(argv)

    agents = list(SUPPORTED_AGENTS) if args.agent == "all" else [args.agent]
    manifest_path = Path(args.manifest) if args.manifest else None

    try:
        report = run_failover_check(
            agents=agents,
            threshold_pct=args.threshold_pct,
            include_fable=args.include_fable,
            apply=args.apply,
            manifest_path=manifest_path,
        )
    except Exception as exc:
        print(f"ORCA_ACCOUNT_FAILOVER_ERR: {exc}", file=sys.stderr)
        return 1

    if args.json:
        print(json.dumps(report, indent=2))
    else:
        for agent_name, dec in report["agents"].items():
            action = (
                f"SWITCHED->{dec['switch_result']['target_account_id']}"
                if dec["switched"]
                else (
                    f"RECOMMEND->{dec['recommended_target']}"
                    if dec["recommended_target"]
                    else "KEEP"
                )
            )
            print(
                f"[{agent_name}] active={dec['active_account_id']} ({dec['active_label']}) "
                f"status={dec['active_status']} session={dec['active_session_used_pct']}% "
                f"weekly={dec['active_weekly_used_pct']}% "
                f"fable={dec['active_fable_weekly_used_pct']}% "
                f"alternates_ok={dec['healthy_alternates_count']} action={action}"
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

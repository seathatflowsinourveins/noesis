"""DSPy 3.5.0 + native dspy.GEPA self-reflection aligned with Core Trading Philosophy."""

from __future__ import annotations

import http.client
import json
import os
import re
import urllib.parse
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as pkg_version
from typing import Any

import dspy
from dspy.teleprompt.gepa.gepa_utils import ScoreWithFeedback

DEFAULT_OMNIROUTE_MODEL = "openai/auto/best-coding"
DEFAULT_OMNIROUTE_API_BASE = "http://127.0.0.1:20128/v1"

_BANNED_OR_NON_OSI_TOKENS = (
    "yfinance",
    "vectorbt",
    "quantstats",
    "osv-scanner",
    "pre-commit",
    "semgrep",
    "tabpfn",
    "lobs5",
    "lakefs",
    "restate",
    "phoenix",
    "tirex",
)
_UNSAFE_LIVE_RE = re.compile(
    r"(?i)(?:live_execution_enabled\s*[:=]\s*true|paper_only\s*[:=]\s*false|"
    r"api\.alpaca\.markets|\b(?:4001|7496|8798|8796)\b)"
)
_NUM_RE = re.compile(r"-?\d+(?:\.\d+)?")
_QUANT_KEYWORDS = (
    "pbo",
    "dsr",
    "cpcv",
    "spa",
    "hac",
    "bps",
    "sharpe",
    "markout",
    "ratchet",
    "trailing",
    "cooldown",
    "reentry",
    "reverse",
    "long",
    "short",
    "clarabel",
    "skfolio",
    "inspect",
    "equity",
    "drawdown",
    "24/5",
)


class CompoundingReflectionSignature(dspy.Signature):
    """Reflect on backtest/paper telemetry and propose philosophy-aligned strategy refinements."""

    philosophy_charter: str = dspy.InputField(
        desc="Core Trading Philosophy invariants and upstream-only constraints."
    )
    optuna_trial_summary: str = dspy.InputField(
        desc=(
            "Optuna 5.1.0 multi-profile trailing/reentry study metrics, cross-leg LONG/SHORT "
            "6-gate falsification, and empirical PBO."
        )
    )
    paper_farm_snapshot: str = dspy.InputField(
        desc="Live Paper Farm telemetry and Inspect safety/philosophy evaluation status."
    )
    lesson_summary: str = dspy.OutputField(
        desc=(
            "Concise quantitative empirical lesson citing exact observed metrics and linking "
            "behavior to Core Trading Philosophy & causal regime alignment."
        )
    )
    recommended_action: str = dspy.OutputField(
        desc="Concrete upstream parameter, test, or ADR update across verified OSI SOTA packages."
    )
    philosophy_aligned: bool = dspy.OutputField(
        desc=(
            "True iff recommendation preserves near-lossless 24/5 trailing, regime alignment, "
            "immutable risk.py, and fail-closed paper-only safety invariants."
        )
    )


class Tier0SelfReflectionProgram(dspy.Module):
    """Upstream DSPy 3.5.0 ChainOfThought module for compounding strategy reflection."""

    def __init__(self) -> None:
        super().__init__()
        self.predict = dspy.ChainOfThought(CompoundingReflectionSignature)

    def forward(
        self,
        *,
        philosophy_charter: str,
        optuna_trial_summary: str,
        paper_farm_snapshot: str,
    ) -> dspy.Prediction:
        return self.predict(
            philosophy_charter=philosophy_charter,
            optuna_trial_summary=optuna_trial_summary,
            paper_farm_snapshot=paper_farm_snapshot,
        )


def build_omniroute_dspy_lm(
    model: str = DEFAULT_OMNIROUTE_MODEL,
    api_base: str = DEFAULT_OMNIROUTE_API_BASE,
    api_key: str | None = None,
) -> dspy.LM:
    """Construct a DSPy LM routed through local OmniRoute :20128 OpenAI-compatible gateway."""
    resolved_auth = (
        api_key
        if api_key is not None
        else (
            os.environ.get("OMNIROUTE_API_KEY") or os.environ.get("OPENAI_API_KEY") or "omniroute"
        )
    )
    return dspy.LM(model=model, api_base=api_base, api_key=resolved_auth, temperature=0.1)


def philosophy_convergence_metric(
    example: dspy.Example,
    pred: dspy.Prediction,
    trace: Any = None,
    pred_name: str | None = None,
    pred_trace: Any = None,
) -> tuple[float, str]:
    """Return `(score, feedback)` evaluating quantitative grounding & safety alignment."""
    del trace, pred_name, pred_trace
    lesson = str(getattr(pred, "lesson_summary", "")).strip()
    action = str(getattr(pred, "recommended_action", "")).strip()
    aligned = bool(getattr(pred, "philosophy_aligned", False))
    expected_aligned = bool(getattr(example, "philosophy_aligned", True))

    combined = f"{lesson} {action}"
    combined_lower = combined.lower()

    if banned_hits := [tok for tok in _BANNED_OR_NON_OSI_TOKENS if tok in combined_lower]:
        return 0.0, f"REJECTED: references banned/non-OSI tool(s): {', '.join(banned_hits)}."
    if _UNSAFE_LIVE_RE.search(combined):
        return 0.0, "REJECTED: violates fail-closed paper-only or live-broker port boundary."

    notes: list[str] = []
    score = 0.0

    # 1. Substantive lesson & concrete upstream action (0.20 + 0.20)
    if len(lesson) >= 25:
        score += 0.20
    else:
        notes.append("lesson_summary is too short (<25 chars)")
    if len(action) >= 20:
        score += 0.20
    else:
        notes.append("recommended_action is too short (<20 chars)")

    # 2. Quantitative metric grounding against Optuna/Paper/Inspect inputs (0.35)
    source_text = (
        f"{getattr(example, 'optuna_trial_summary', '')} "
        f"{getattr(example, 'paper_farm_snapshot', '')}"
    )
    source_nums = set(_NUM_RE.findall(source_text))
    pred_nums = set(_NUM_RE.findall(combined))
    shared_nums = source_nums & pred_nums
    kw_hits = [kw for kw in _QUANT_KEYWORDS if kw in combined_lower]

    if not source_nums:
        if len(kw_hits) >= 2:
            score += 0.35
        elif kw_hits:
            score += 0.15
            notes.append("include concrete domain/risk metrics in lesson and action")
        else:
            notes.append("missing quantitative domain metrics")
    elif shared_nums and len(kw_hits) >= 2:
        score += 0.35
    elif pred_nums and len(kw_hits) >= 2:
        score += 0.20
        notes.append("cite exact numeric telemetry values observed in the input manifests")
    elif kw_hits:
        score += 0.10
        notes.append("missing numeric grounding from Optuna/Paper/Inspect receipts")
    else:
        notes.append("missing both numeric figures and quantitative domain metrics")

    # 3. Philosophy alignment boolean accuracy (0.25)
    if aligned == expected_aligned:
        score += 0.25
    else:
        notes.append(f"philosophy_aligned={aligned} disagrees with expected={expected_aligned}")

    final_score = round(min(1.0, score), 4)
    if not notes:
        feedback = (
            f"PASS ({final_score}): quantitatively grounded ({len(shared_nums)} exact metric "
            f"matches, {len(kw_hits)} domain terms) and philosophy-aligned."
        )
    else:
        feedback = f"IMPROVE ({final_score}): " + "; ".join(notes) + "."
    return final_score, feedback


def philosophy_gepa_feedback_metric(
    gold: dspy.Example,
    pred: dspy.Prediction,
    trace: Any = None,
    pred_name: str | None = None,
    pred_trace: Any = None,
    program_trace: Any = None,
) -> ScoreWithFeedback:
    """Adapter returning `ScoreWithFeedback(score, feedback)` for native `dspy.GEPA`."""
    del program_trace
    score, feedback = philosophy_convergence_metric(
        gold, pred, trace=trace, pred_name=pred_name, pred_trace=pred_trace
    )
    return ScoreWithFeedback(score=score, feedback=feedback)


gepa_philosophy_feedback_metric = philosophy_gepa_feedback_metric


def build_gepa_reflection_optimizer(
    *,
    reflection_lm: dspy.LM | None = None,
    max_metric_calls: int = 16,
    candidate_selection_strategy: str = "pareto",
    seed: int = 0,
) -> dspy.GEPA:
    """Construct upstream `dspy.GEPA` reflective prompt optimizer (strictly off execution path)."""
    lm = reflection_lm if reflection_lm is not None else build_omniroute_dspy_lm()
    strategy = "pareto" if candidate_selection_strategy != "current_best" else "current_best"
    return dspy.GEPA(
        metric=philosophy_gepa_feedback_metric,
        max_metric_calls=max(1, int(max_metric_calls)),
        candidate_selection_strategy=strategy,
        reflection_lm=lm,
        use_merge=True,
        seed=seed,
    )


def _endpoint_reachable(url: str, timeout: float = 2.0) -> bool:
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return False
    conn_cls = (
        http.client.HTTPSConnection if parsed.scheme == "https" else http.client.HTTPConnection
    )
    path = (parsed.path.rstrip("/") or "") + "/models"
    conn = conn_cls(parsed.netloc, timeout=timeout)
    try:
        conn.request("GET", path)
        resp = conn.getresponse()
        resp.read()
        return 200 <= int(resp.status) < 400
    except (http.client.HTTPException, TimeoutError, ValueError, OSError):
        return False
    finally:
        conn.close()


def run_live_dspy_reflection(
    manifest_snapshot: dict[str, Any],
    *,
    omniroute_url: str = DEFAULT_OMNIROUTE_API_BASE,
    model: str = DEFAULT_OMNIROUTE_MODEL,
    offline: bool = False,
) -> dict[str, Any]:
    """Execute live DSPy reflection via OmniRoute :20128; never fabricates a synthetic score."""
    if offline:
        return {
            "executed_live": False,
            "status": "skipped_offline",
            "omniroute_api_base": omniroute_url,
            "model": model,
            "lesson_summary": None,
            "recommended_action": None,
            "philosophy_aligned": None,
            "metric_score": None,
            "feedback": "Live LLM reflection skipped in offline mode.",
        }
    if not _endpoint_reachable(omniroute_url):
        return {
            "executed_live": False,
            "status": "omniroute_unreachable",
            "omniroute_api_base": omniroute_url,
            "model": model,
            "lesson_summary": None,
            "recommended_action": None,
            "philosophy_aligned": None,
            "metric_score": None,
            "feedback": (
                f"OmniRoute unreachable at {omniroute_url}; no synthetic score substituted."
            ),
        }

    charter = (
        "Capture extreme movers early, align direction with causal market regime, "
        "protect 24/5 with BE+fee progressive profit-lock trailing, preserve immutable "
        "Decimal risk.py and paper_only=True / live_execution_enabled=False."
    )
    optuna_summary = json.dumps(manifest_snapshot.get("optuna", {}), sort_keys=True)[:2500]
    paper_summary = json.dumps(
        {
            "paper_farm": manifest_snapshot.get("paper_farm", {}),
            "inspect": manifest_snapshot.get("inspect", {}),
        },
        sort_keys=True,
    )[:1500]
    ref_example = dspy.Example(
        philosophy_charter=charter,
        optuna_trial_summary=optuna_summary,
        paper_farm_snapshot=paper_summary,
        philosophy_aligned=True,
    ).with_inputs("philosophy_charter", "optuna_trial_summary", "paper_farm_snapshot")

    try:
        lm = build_omniroute_dspy_lm(model=model, api_base=omniroute_url)
        program = Tier0SelfReflectionProgram()
        with dspy.context(lm=lm):
            pred = program(
                philosophy_charter=charter,
                optuna_trial_summary=optuna_summary,
                paper_farm_snapshot=paper_summary,
            )
        score, feedback = philosophy_convergence_metric(ref_example, pred)
        return {
            "executed_live": True,
            "status": "ok",
            "omniroute_api_base": omniroute_url,
            "model": model,
            "lesson_summary": str(getattr(pred, "lesson_summary", "")).strip(),
            "recommended_action": str(getattr(pred, "recommended_action", "")).strip(),
            "philosophy_aligned": bool(getattr(pred, "philosophy_aligned", False)),
            "metric_score": score,
            "feedback": feedback,
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "executed_live": False,
            "status": "live_error",
            "error": type(exc).__name__,
            "omniroute_api_base": omniroute_url,
            "model": model,
            "lesson_summary": None,
            "recommended_action": None,
            "philosophy_aligned": None,
            "metric_score": None,
            "feedback": f"Live OmniRoute reflection raised {type(exc).__name__}.",
        }


def verify_dspy_reflection_module() -> dict[str, Any]:
    """Offline structural check of DSPy module, GEPA optimizer, and quantitative metric."""
    program = Tier0SelfReflectionProgram()
    lm = build_omniroute_dspy_lm()
    gepa_opt = build_gepa_reflection_optimizer(reflection_lm=lm, max_metric_calls=4)

    example = dspy.Example(
        philosophy_charter=(
            "Extreme movers early, causal regime alignment, 24/5 near-lossless trailing."
        ),
        optuna_trial_summary=(
            "fast_reentry_cooldown_0 mean_bps=12.4 dsr=0.96 pbo=0.12 promoted=True"
        ),
        paper_farm_snapshot="paper_only=True live_execution_enabled=False inspect_accuracy=1.0",
        philosophy_aligned=True,
    ).with_inputs("philosophy_charter", "optuna_trial_summary", "paper_farm_snapshot")

    grounded_pred = dspy.Prediction(
        lesson_summary=(
            "Short cooldown reentry achieved mean_bps=12.4 with dsr=0.96 and empirical pbo=0.12 "
            "while preserving 24/5 trailing profit lock."
        ),
        recommended_action=(
            "Keep tight_near_lossless trailing with skfolio Clarabel sleeve weights in Nautilus v2."
        ),
        philosophy_aligned=True,
    )
    ungrounded_pred = dspy.Prediction(
        lesson_summary="Everything looks nice and smooth today.",
        recommended_action="Keep running without changing anything.",
        philosophy_aligned=True,
    )
    banned_pred = dspy.Prediction(
        lesson_summary="Use yfinance and set live_execution_enabled=True.",
        recommended_action="Switch to port 4001 immediately.",
        philosophy_aligned=True,
    )

    good_score, good_fb = philosophy_convergence_metric(example, grounded_pred)
    weak_score, _ = philosophy_convergence_metric(example, ungrounded_pred)
    banned_score, _ = philosophy_convergence_metric(example, banned_pred)

    try:
        gepa_ver = pkg_version("gepa")
    except PackageNotFoundError:
        gepa_ver = "unknown"

    discriminates = good_score == 1.0 and weak_score < good_score and banned_score == 0.0
    return {
        "ok": (
            discriminates
            and isinstance(program, dspy.Module)
            and isinstance(lm, dspy.LM)
            and isinstance(gepa_opt, dspy.GEPA)
        ),
        "mode": "offline_wiring_check",
        "dspy_version": getattr(dspy, "__version__", "unknown"),
        "gepa_available": hasattr(dspy, "GEPA"),
        "gepa_version": gepa_ver,
        "gepa_optimizer": type(gepa_opt).__name__,
        "program": type(program).__name__,
        "metric_score": good_score,
        "ungrounded_control_score": weak_score,
        "banned_control_score": banned_score,
        "feedback": good_fb,
        "omniroute_model": lm.model,
        "omniroute_api_base": DEFAULT_OMNIROUTE_API_BASE,
    }

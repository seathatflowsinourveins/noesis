"""DSPy 3.5.0 self-reflection program aligned with Tier-0 Core Trading Philosophy."""

from __future__ import annotations

import os
from typing import Any

import dspy
from dspy.teleprompt.gepa.gepa_utils import ScoreWithFeedback

DEFAULT_OMNIROUTE_MODEL = "openai/auto/best-coding"
DEFAULT_OMNIROUTE_API_BASE = "http://127.0.0.1:20128/v1"


class CompoundingReflectionSignature(dspy.Signature):
    """Reflect on backtest/paper telemetry and propose philosophy-aligned strategy refinements."""

    philosophy_charter: str = dspy.InputField(
        desc="Core Trading Philosophy invariants and upstream-only constraints."
    )
    optuna_trial_summary: str = dspy.InputField(
        desc="Optuna 5.1.0 multi-profile trailing/reentry study metrics and 6-gate falsification."
    )
    paper_farm_snapshot: str = dspy.InputField(
        desc="Live Paper Farm telemetry and Inspect safety/philosophy evaluation status."
    )
    lesson_summary: str = dspy.OutputField(
        desc="Concise empirical lesson linking observed behavior to philosophy & regime alignment."
    )
    recommended_action: str = dspy.OutputField(
        desc="Concrete upstream parameter or ADR update across verified SOTA packages."
    )
    philosophy_aligned: bool = dspy.OutputField(
        desc=(
            "True iff recommendation preserves near-lossless 24/5 trailing, regime, "
            "and safety invariants."
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
) -> float:
    """Score reflection outputs for empirical grounding and Core Philosophy alignment."""
    del trace
    lesson = str(getattr(pred, "lesson_summary", "")).strip()
    action = str(getattr(pred, "recommended_action", "")).strip()
    aligned = bool(getattr(pred, "philosophy_aligned", False))
    expected_aligned = bool(getattr(example, "philosophy_aligned", True))

    banned_tokens = ("yfinance", "vectorbt", "quantstats", "osv-scanner")
    combined_lower = f"{lesson} {action}".lower()
    if any(tok in combined_lower for tok in banned_tokens):
        return 0.0

    score = 0.0
    if len(lesson) >= 20:
        score += 0.35
    if len(action) >= 15:
        score += 0.35
    if aligned == expected_aligned:
        score += 0.30
    return round(score, 4)


def philosophy_gepa_feedback_metric(
    gold: dspy.Example,
    pred: dspy.Prediction,
    trace: Any = None,
    pred_name: str | None = None,
    pred_trace: Any = None,
    program_trace: Any = None,
) -> ScoreWithFeedback:
    """GEPA feedback metric returning score and textual reflection guidance."""
    del trace, pred_trace, program_trace
    score = philosophy_convergence_metric(gold, pred)
    target = pred_name or "Tier0SelfReflectionProgram"
    if score >= 1.0:
        feedback = f"{target}: aligned with Core Trading Philosophy and upstream-only charter."
    else:
        feedback = (
            f"{target}: score={score:.2f}; ensure >=20-char lesson, >=15-char upstream action, "
            "correct philosophy_aligned flag, and zero banned legacy libraries."
        )
    return ScoreWithFeedback(score=score, feedback=feedback)


def build_gepa_reflection_optimizer(
    reflection_lm: dspy.LM | None = None,
    max_metric_calls: int = 4,
) -> dspy.GEPA:
    """Construct an upstream DSPy 3.5.0 GEPA evolutionary reflection optimizer."""
    resolved_lm = reflection_lm if reflection_lm is not None else build_omniroute_dspy_lm()
    return dspy.GEPA(
        metric=philosophy_gepa_feedback_metric,
        reflection_lm=resolved_lm,
        max_metric_calls=max_metric_calls,
    )


def verify_dspy_reflection_module() -> dict[str, Any]:
    """Verify DSPy reflection signature, module, metric, and OmniRoute LM wiring offline."""
    program = Tier0SelfReflectionProgram()
    lm = build_omniroute_dspy_lm()
    example = dspy.Example(
        philosophy_charter=(
            "Extreme movers early, causal regime alignment, 24/5 near-lossless trailing."
        ),
        optuna_trial_summary="fast_reentry_cooldown_0 mean_bps=12.4 dsr=0.92 promoted=True",
        paper_farm_snapshot="paper_only=True live_execution_enabled=False inspect_accuracy=1.0",
        lesson_summary=(
            "Short cooldown reentry preserves profit lock across 24/5 RTH/ETH transitions."
        ),
        recommended_action=(
            "Promote tight_near_lossless schedule under causal regime gates in Nautilus v2."
        ),
        philosophy_aligned=True,
    ).with_inputs("philosophy_charter", "optuna_trial_summary", "paper_farm_snapshot")

    synthetic_pred = dspy.Prediction(
        lesson_summary=example.lesson_summary,
        recommended_action=example.recommended_action,
        philosophy_aligned=True,
    )
    score = philosophy_convergence_metric(example, synthetic_pred)
    return {
        "ok": score == 1.0 and isinstance(program, dspy.Module) and isinstance(lm, dspy.LM),
        "dspy_version": getattr(dspy, "__version__", "unknown"),
        "program": type(program).__name__,
        "metric_score": score,
        "gepa_available": hasattr(dspy, "GEPA"),
        "omniroute_model": lm.model,
        "omniroute_api_base": DEFAULT_OMNIROUTE_API_BASE,
    }

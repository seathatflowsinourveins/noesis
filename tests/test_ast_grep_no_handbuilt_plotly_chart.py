"""ast-grep rule `no-handbuilt-plotly-chart`: every hand-built figure/page pattern is flagged.

The rule (rules/zero_self_invention.yml, owner rule 2026-11-27) is copied into a throw-away
ast-grep project next to one fixture file per prohibited pattern; each fixture must be flagged,
a clean upstream-call fixture must not be, and the repository's src/ and scripts/ (including the
report generator and the engine tearsheet suite) must have zero matches.
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess
from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
RULES_PATH = REPO_ROOT / "rules" / "zero_self_invention.yml"
RULE_ID = "no-handbuilt-plotly-chart"

FLAGGED_FIXTURES: dict[str, str] = {
    "import_plotly.py": "import plotly\n",
    "import_graph_objects.py": "import plotly.graph_objects as go\n",
    "import_express.py": "import plotly.express as px\n",
    "import_subplots.py": "from plotly.subplots import make_subplots\n",
    "from_plotly_import.py": "from plotly import graph_objects\n",
    "import_pyplot.py": "import matplotlib.pyplot as plt\n",
    "from_pyplot_import.py": "from matplotlib.pyplot import subplots\n",
    "from_matplotlib_import.py": "from matplotlib import pyplot\n",
    "ml4t_core_go.py": "from ml4t.diagnostic.visualization.core import go\n",
    "ml4t_combine_import.py": (
        "from ml4t.diagnostic.visualization.report_generation import combine_figures_to_html\n"
    ),
    "dynamic_import.py": 'import importlib\n\nplt = importlib.import_module("matplotlib.pyplot")\n',
    "dynamic_plotly_import.py": (
        'import importlib\n\ngo = importlib.import_module("plotly.graph_objects")\n'
    ),
    "go_figure.py": "fig = go.Figure(data=[])\n",
    "make_subplots_call.py": "fig = make_subplots(rows=2, cols=1)\n",
    "module_make_subplots.py": "fig = sp.make_subplots(rows=2, cols=1)\n",
    "combine_figures.py": 'combine_figures_to_html(figures=[], title="t", output_file="x.html")\n',
    "module_combine_figures.py": 'rg.combine_figures_to_html(figures=[], title="t")\n',
    "add_trace.py": "fig.add_trace(trace)\n",
    "dataframe_to_html.py": "html = frame.to_html()\n",
    "styler_to_html.py": "html = frame.style.to_html()\n",
}

CLEAN_FIXTURE = (
    "from ml4t.diagnostic.visualization import FeatureImportanceDashboard\n"
    "from ml4t.diagnostic.visualization.backtest import generate_backtest_tearsheet\n\n"
    'generate_backtest_tearsheet(returns=returns, output_path="tearsheet.html")\n'
    'FeatureImportanceDashboard(title="t").save("importance.html", results)\n'
    'cpcv.plot_train_test_folds().write_html("folds.html")\n'
)


def _ast_grep() -> str:
    exe = shutil.which("ast-grep")
    assert exe, "ast-grep CLI (ast-grep-cli in the dev dependency group) must be on PATH"
    return exe


def _rule_document() -> str:
    documents = RULES_PATH.read_text(encoding="utf-8").split("\n---\n")
    matching = [doc for doc in documents if re.search(rf"(?m)^id:\s*{RULE_ID}\s*$", doc)]
    assert len(matching) == 1
    return matching[0]


def _scan(project: Path, *paths: str) -> list[dict[str, Any]]:
    proc = subprocess.run(  # noqa: S603 - fixed argv, no shell
        [
            _ast_grep(),
            "scan",
            "--config",
            str(project / "sgconfig.yml"),
            "--filter",
            f"^{RULE_ID}$",
            "--json=compact",
            *paths,
        ],
        cwd=project,
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode in (0, 1), proc.stderr
    return list(json.loads(proc.stdout or "[]"))


@pytest.fixture
def rule_project(tmp_path: Path) -> Path:
    (tmp_path / "sgconfig.yml").write_text("ruleDirs:\n  - rules\n", encoding="utf-8")
    (tmp_path / "rules").mkdir()
    (tmp_path / "rules" / f"{RULE_ID}.yml").write_text(_rule_document(), encoding="utf-8")
    scripts_dir = tmp_path / "scripts"
    scripts_dir.mkdir()
    for name, source in FLAGGED_FIXTURES.items():
        (scripts_dir / name).write_text(source, encoding="utf-8")
    (scripts_dir / "clean_upstream_calls.py").write_text(CLEAN_FIXTURE, encoding="utf-8")
    return tmp_path


def test_rule_is_the_only_figure_rule_with_error_severity_on_src_and_scripts() -> None:
    doc = _rule_document()
    assert re.search(r"(?m)^severity:\s*error\s*$", doc)
    assert re.search(r"(?m)^files:\s*\[src/\*\*/\*\.py, scripts/\*\*/\*\.py\]\s*$", doc)
    rule_ids = re.findall(r"(?m)^id:\s*(\S+)", RULES_PATH.read_text(encoding="utf-8"))
    assert "no-handbuilt-figure" not in rule_ids  # one rule, extended in place


def test_every_handbuilt_figure_pattern_is_flagged(rule_project: Path) -> None:
    matches = _scan(rule_project, "scripts")
    flagged = {Path(m["file"]).name for m in matches if m.get("ruleId") == RULE_ID}
    assert flagged == set(FLAGGED_FIXTURES)
    assert all(m.get("severity") == "error" for m in matches)


def test_repository_sources_generator_and_suite_are_clean() -> None:
    for script in ("generate_ml4t_and_shap_tearsheets.py", "run_sim_backtest_tearsheet_suite.py"):
        path = REPO_ROOT / "scripts" / script
        assert path.is_file()
        assert _scan(REPO_ROOT, f"scripts/{script}") == []
    assert _scan(REPO_ROOT, "src", "scripts") == []

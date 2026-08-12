"""Render smoke tests for the APPLY_FILTER block of mapping.py.j2.

The block renders `lib.filter(...)` from `step.params["lib_filter_cfg"]`,
using the `pyrepr` filter (plain repr) and a hand-rolled dict loop for
substitutions. A rendering mistake produces a Python SyntaxError in EVERY
generated mapping that hits the branch — e.g. the substitutions line
previously rendered `substitutions='$$v_x': v_x,` (missing dict braces).
Render the real block from the template with a synthetic step and
ast.parse the output.
"""

import ast
import textwrap
from pathlib import Path
from types import SimpleNamespace

from jinja2 import Environment

from informatica_sparker.ir import IRStepType

TEMPLATE = (
    Path(__file__).resolve().parents[1]
    / "informatica_sparker" / "templates" / "mapping.py.j2"
)

_BLOCK_START = "{% elif step.step_type == IRStepType.APPLY_FILTER %}"
_BLOCK_END = "{% elif step.step_type == IRStepType.APPLY_EXPRESSION %}"


def _apply_filter_block():
    """Extract the APPLY_FILTER branch from the real template."""
    source = TEMPLATE.read_text()
    start = source.index(_BLOCK_START)
    end = source.index(_BLOCK_END)
    # Wrap in `{% if False %}` so the standalone `{% elif %}` header parses
    # AND the elif branch is evaluated (a True opener would take its empty
    # first branch and never reach the elif); the wrapper tags emit nothing,
    # so the rendered output is exactly the branch body.
    return "{% if False %}\n" + source[start:end] + "\n{% endif %}"


def _render_block(step):
    env = Environment()
    env.filters["pyrepr"] = repr  # matches codegen.py registration
    return env.from_string(_apply_filter_block()).render(
        step=step, IRStepType=IRStepType
    )


def test_apply_filter_block_renders_parseable_lib_filter_call():
    """All cfg branches (rename / condition / substitutions / sequence
    attach) must render valid Python with the dict braces."""
    step = SimpleNamespace(
        step_type=IRStepType.APPLY_FILTER,
        step_name="apply_SMOKE",
        df_input="df_in",
        df_output="df_out",
        params={
            "lib_filter_cfg": {
                "rename_columns": [("OLD", "NEW")],
                "condition": "NEW != 0 AND $$v_rpt_mth >= 0",
                # Runtime identifiers (clean var names), per the handler:
                # the generated mapping defines v_rpt_mth from UTL_JOB_PARAM.
                "substitutions": {"$$v_rpt_mth": "v_rpt_mth"},
                "sequence_attach": [{"col": "NEXTVAL", "start": 100}],
            }
        },
    )
    out = _render_block(step)

    assert "lib.filter(" in out
    assert "rename_columns=[('OLD', 'NEW')]," in out
    assert "condition='NEW != 0 AND $$v_rpt_mth >= 0'," in out
    assert "substitutions={'$$v_rpt_mth': v_rpt_mth}," in out
    assert "sequence_attach=[{'col': 'NEXTVAL', 'start': 100}]," in out
    # No unreplaced Jinja tags / stray braces leaked into the output
    assert "{%" not in out and "{{" not in out
    ast.parse(textwrap.dedent(out))


def test_apply_filter_block_without_cfg_renders_passthrough():
    """A step with no lib_filter_cfg must still render valid Python (the
    default condition form), not an unterminated argument list."""
    step = SimpleNamespace(
        step_type=IRStepType.APPLY_FILTER,
        step_name="apply_SMOKE",
        df_input="df_in",
        df_output="df_out",
        params={},
    )
    out = _render_block(step)
    assert "lib.filter(" in out
    assert "condition='TRUE'," in out
    ast.parse(textwrap.dedent(out))

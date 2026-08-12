"""Render smoke tests for the APPLY_EXPRESSION block of mapping.py.j2.

The block renders `lib.expression(...)` from `step.params["expression_cfg"]`,
using the `pyrepr` filter (plain repr) and hand-rolled dict loops. A
rendering mistake produces a Python SyntaxError in EVERY generated mapping
that hits the branch — e.g. the substitutions line previously rendered
`substitutions='$$v_x': v_x,` (missing dict braces). Render the real block
from the template with a synthetic step and ast.parse the output.
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

_BLOCK_START = "{% elif step.step_type == IRStepType.APPLY_EXPRESSION %}"
_BLOCK_END = "{% elif step.step_type == IRStepType.APPLY_LOOKUP %}"


def _apply_expression_block():
    """Extract the APPLY_EXPRESSION branch from the real template."""
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
    return env.from_string(_apply_expression_block()).render(
        step=step, IRStepType=IRStepType
    )


def test_apply_expression_block_renders_parseable_lib_expression_call():
    """All cfg branches (rename / computed / pass-through / substitutions /
    inline lookups / SP calls) must render valid Python with the braces."""
    step = SimpleNamespace(
        step_type=IRStepType.APPLY_EXPRESSION,
        step_name="apply_SMOKE",
        df_input="df_in",
        df_output="df_out",
        params={
            "expression_cfg": {
                "rename_columns": [("OLD", "NEW")],
                "computed_columns": [{"name": "C1", "expr": "lit(1)"}],
                "pass_through_cols": ["P1"],
                "substitutions": {"$$v_rpt_mth": "v_rpt_mth"},
                "inline_lookup_joins": [
                    {
                        "lookup_df": "df_lkp_chain_1",
                        "return_port": "RET",
                        "join_predicates": [{"source_col": "K", "lookup_col": "K"}],
                    }
                ],
                "sp_calls": [
                    {
                        "col": "SPC",
                        "sp_call": "PKG.FN",
                        "sp_schema": "S",
                        "args": ["1"],
                    }
                ],
            }
        },
    )
    out = _render_block(step)

    assert "lib.expression(" in out
    assert "name='" not in out
    assert "rename_columns=[('OLD', 'NEW')]," in out
    assert "substitutions={'$$v_rpt_mth': v_rpt_mth}," in out
    assert "sp_conn=conn_oracle," in out
    # No unreplaced Jinja tags / stray braces leaked into the output
    assert "{%" not in out and "{{" not in out
    ast.parse(textwrap.dedent(out))


def test_apply_expression_block_without_cfg_renders_passthrough():
    """A step with no expression_cfg must still render valid Python (the
    pass-through form), not an unterminated argument list."""
    step = SimpleNamespace(
        step_type=IRStepType.APPLY_EXPRESSION,
        step_name="apply_SMOKE",
        df_input="df_in",
        df_output="df_out",
        params={},
    )
    out = _render_block(step)
    assert "lib.expression(" in out
    ast.parse(textwrap.dedent(out))

"""Regression tests for sq_output_cfg wiring.

The generator renders APPLY_SOURCE_QUALIFIER steps via `lib.sq_output(...)`,
which reads ONLY `step.params["sq_output_cfg"]`. Any step missing its cfg
silently drops the SQ port handling (rename, port select, type casts,
filter/distinct) in the generated code.

The cfg form is the runtime contract (Task 1):
  - `port_cols`: SQ output port names (always present, non-empty);
  - `column_types`: {port: datatype} — PUSHDOWN only (the non-pushdown path
    applies no casts; the old inline type-cast block lived in the pushdown
    tail);
  - `filter_condition` / `substitutions` / `distinct`: NON-PUSHDOWN only.

Source of truth is real PowerCenter XML only: WF_EMS_DDS_APLY_MTH (143 Source
Qualifiers; 100 SQL-pushdown). It has exactly ONE SQ with a Source Filter
(Housed_Under_Offer_Refusal) and it is pushdown — its filter folds into the
SQL query text and the handler stores filter_inner only for non-pushdown SQs
— so no non-pushdown `filter_condition` exists in this workflow's data;
that path is covered by the render smoke test and tests/test_lib_sq_output.py.
No SQ here uses Select Distinct either — `distinct` is exercised by the
render smoke test as well.
"""

import ast
import re
import textwrap
from pathlib import Path
from types import SimpleNamespace

from jinja2 import Environment

from informatica_sparker.handlers import TransformHandlers
from informatica_sparker.ir import ApplySourceQualifierStep, IRStepType
from informatica_sparker.models import UserConfig
from informatica_sparker.parser import InfaXMLParser

XML_DIR = Path(__file__).resolve().parents[2] / "PowerCenter_workflows" / "dds"
WORKFLOW_XML = XML_DIR / "WF_EMS_DDS_APLY_MTH.XML"

TEMPLATE = (
    Path(__file__).resolve().parents[1]
    / "informatica_sparker" / "templates" / "mapping.py.j2"
)


def _load_steps():
    parser = InfaXMLParser(WORKFLOW_XML.read_bytes())
    assert parser.parse(), f"parse failed: {WORKFLOW_XML.name}"
    mappings = parser.get_mappings()
    assert mappings, f"no mappings parsed from {WORKFLOW_XML.name}"
    for mapping in mappings:
        plan = TransformHandlers(mapping, UserConfig()).build_ir_plan()
        assert plan is not None
        for step in plan.steps:
            if isinstance(step, ApplySourceQualifierStep):
                yield mapping, step


def test_every_sq_step_carries_sq_output_cfg():
    """Every ApplySourceQualifierStep must carry sq_output_cfg with non-empty
    port_cols; pushdown steps carry column_types, non-pushdown steps never do."""
    seen = 0
    n_pushdown_with_types = 0
    for mapping, step in _load_steps():
        seen += 1
        assert "sq_output_cfg" in step.params, (
            f"{mapping.name}: {step.step_name} carries no sq_output_cfg - "
            "lib.sq_output rendering would drop its port handling"
        )
        cfg = step.params["sq_output_cfg"]
        assert cfg.get("port_cols"), (
            f"{mapping.name}: {step.step_name} sq_output_cfg port_cols empty"
        )
        if step.params.get("use_sql_override"):
            # Pushdown SQs carry column_types (drives the runtime type casts);
            # the old inline cast block read output_column_types, which the
            # handler now stores for pushdown steps.
            assert "column_types" in cfg, (
                f"{mapping.name}: {step.step_name} pushdown sq_output_cfg "
                "missing column_types"
            )
            n_pushdown_with_types += 1
        else:
            assert "column_types" not in cfg, (
                f"{mapping.name}: {step.step_name} non-pushdown sq_output_cfg "
                "must not carry column_types (pushdown-only per contract)"
            )
    assert seen >= 40, f"expected >= 40 Source Qualifier steps, saw {seen}"
    assert n_pushdown_with_types >= 1, (
        "no pushdown SQ carries column_types - type casts would never render"
    )


def test_sq_filter_condition_state_in_this_workflow():
    """WF_EMS_DDS_APLY_MTH has no non-pushdown SQ with a Source Filter: the
    only filtered SQ (Housed_Under_Offer_Refusal) is SQL-pushdown, so its
    filter folds into the SQL query text and no DataFrame-side
    filter_condition is produced. Pushdown SQs must never carry one either.
    The filter_condition / substitutions path is covered by the render smoke
    test below and tests/test_lib_sq_output.py."""
    n_filter = 0
    for mapping, step in _load_steps():
        cfg = step.params["sq_output_cfg"]
        if cfg.get("filter_condition"):
            n_filter += 1
            assert not step.params.get("use_sql_override"), (
                f"{mapping.name}: {step.step_name} pushdown SQ carries "
                "filter_condition - the Source Filter should have folded "
                "into the SQL query text"
            )
    assert n_filter == 0, (
        f"expected no non-pushdown SQ filter_condition in this workflow, "
        f"saw {n_filter}; if one exists now, the sq_output_cfg wiring must "
        "be re-verified end-to-end (filter_condition is only stored for "
        "non-pushdown SQs with a Source Filter)"
    )


def test_apply_source_qualifier_block_renders_parseable_lib_sq_output_call():
    """The APPLY_SOURCE_QUALIFIER block must render a parseable
    lib.sq_output(...) call for BOTH paths: pushdown (after lib.read_sql,
    input_df is the read result) and non-pushdown (after the df_input
    assignment), with port_cols/column_types/filter_condition/substitutions/
    distinct rendered only when present, and ctx.register_df after."""
    env = Environment()
    env.filters["pyrepr"] = repr  # matches codegen.py registration
    env.filters["ireplace"] = (
        lambda s, old, new: re.sub(  # matches codegen.py registration
            re.escape(old), new, s, flags=re.IGNORECASE)
    )
    source = TEMPLATE.read_text()
    start = source.index(
        "{% elif step.step_type == IRStepType.APPLY_SOURCE_QUALIFIER %}"
    )
    end = source.index("{% elif step.step_type == IRStepType.APPLY_FILTER %}")
    # Wrap in `{% if False %}` so the standalone `{% elif %}` header parses
    # AND the branch is evaluated (a True opener would take its empty first
    # branch); the wrapper tags emit nothing.
    block = "{% if False %}\n" + source[start:end] + "\n{% endif %}"

    # Full cfg: column_types + filter_condition + substitutions + distinct
    step = SimpleNamespace(
        step_type=IRStepType.APPLY_SOURCE_QUALIFIER,
        step_name="apply_SMOKE",
        df_input="df_src",
        df_output="df_sq_smoke",
        params={
            "use_sql_override": False,
            "sql_query": "",
            "sq_output_cfg": {
                "port_cols": ["C1", "C2"],
                "column_types": {"C1": "decimal", "C2": "string"},
                "filter_condition": "C1 > $$v_min",
                "substitutions": {"$$v_min": "v_min"},
                "distinct": True,
            },
        },
    )
    out = env.from_string(block).render(
        step=step, IRStepType=IRStepType, mapping_variables={},
    )

    assert "lib.sq_output(" in out
    assert "input_df=df_sq_smoke," in out
    assert "port_cols=['C1', 'C2']," in out
    # column_types / filter_condition pyrepr'd as strings
    assert "column_types={'C1': 'decimal', 'C2': 'string'}," in out
    assert 'filter_condition=\'C1 > $$v_min\',' in out
    # substitutions value renders UNQUOTED (runtime variable identifier)
    assert "{'$$v_min': v_min}," in out
    assert "distinct=True," in out
    # Opt 1: sq_output renders NEITHER spark=spark NOR config=config
    assert "spark=" not in out
    assert "config=" not in out
    assert 'ctx.register_df("df_sq_smoke", df_sq_smoke)' in out
    # No unreplaced Jinja tags / stray braces leaked into the output
    assert "{%" not in out and "{{" not in out
    ast.parse(textwrap.dedent(out))

    # Pushdown path: the read block must be preserved and feed the call
    step2 = SimpleNamespace(
        step_type=IRStepType.APPLY_SOURCE_QUALIFIER,
        step_name="apply_SMOKE2",
        df_input="df_src",
        df_output="df_sq_push",
        params={
            "use_sql_override": True,
            "sql_query": "SELECT C1, C2 FROM SRC_TBL",
            "source_schema": "psor",
            "sq_output_cfg": {
                "port_cols": ["C1", "C2"],
                "column_types": {"C1": "decimal", "C2": "string"},
            },
        },
    )
    out2 = env.from_string(block).render(
        step=step2, IRStepType=IRStepType,
        mapping_variables={}, source_conn_name="src_db",
    )
    # read block stays: get_db_config + read_sql
    assert 'lib.get_db_config(config, "src_db")' in out2
    assert "lib.read_sql(spark, _conn, query=query)" in out2
    # schema parameterization loop from the read block is intact
    assert "_schema = _conn.get(\"schema\", \"\") or \"psor\"" in out2
    # single lib.sq_output call feeding from the read result
    assert out2.count("lib.sq_output(") == 1
    assert "input_df=df_sq_push," in out2
    assert "column_types={'C1': 'decimal', 'C2': 'string'}," in out2
    # non-pushdown-only keys must NOT render for pushdown
    assert "filter_condition=" not in out2
    assert "substitutions=" not in out2
    assert "distinct=" not in out2
    # Opt 1: spark/config omitted on the pushdown path too
    assert "spark=" not in out2
    assert "config=" not in out2
    assert 'ctx.register_df("df_sq_push", df_sq_push)' in out2
    assert "{%" not in out2 and "{{" not in out2
    ast.parse(textwrap.dedent(out2))

    # Minimal cfg (port_cols only): no optional kwarg leaks
    step3 = SimpleNamespace(
        step_type=IRStepType.APPLY_SOURCE_QUALIFIER,
        step_name="apply_SMOKE3",
        df_input="df_src",
        df_output="df_sq_min",
        params={
            "use_sql_override": False,
            "sql_query": "",
            "sq_output_cfg": {"port_cols": ["C1"]},
        },
    )
    out3 = env.from_string(block).render(
        step=step3, IRStepType=IRStepType, mapping_variables={},
    )
    assert "lib.sq_output(" in out3
    assert "port_cols=['C1']," in out3
    assert "filter_condition=" not in out3
    assert "column_types=" not in out3
    assert "substitutions=" not in out3
    assert "distinct=" not in out3
    # Opt 1: spark/config omitted in the minimal-cfg variant too
    assert "spark=" not in out3
    assert "config=" not in out3
    assert 'ctx.register_df("df_sq_min", df_sq_min)' in out3
    assert "{%" not in out3 and "{{" not in out3
    ast.parse(textwrap.dedent(out3))

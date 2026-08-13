"""Regression tests for union_cfg / sorter_cfg / sequence_cfg wiring.

The generator renders APPLY_UNION / APPLY_SORTER / APPLY_SEQUENCE steps via
`lib.union(...)` / `lib.sorter(...)` / `lib.sequence(...)`, which read ONLY
`step.params["union_cfg"]` / `["sorter_cfg"]` / `["sequence_cfg"]`. Any step
missing its cfg silently drops the transformation in the generated code.

The cfg form is the runtime contract (Tasks 2-4):
  - `union_selects[].df_input` is a DataFrame NAME (the template renders it
    UNQUOTED); `selects` are FROM-only string lists in output-column order
    (selects[j] aliases positionally to output_columns[j]), rendered via
    pyrepr one entry per line;
  - `rename_columns` are (from, to) TUPLES (the runtime applies them via
    withColumnRenamed);
  - `sort_columns` keep their {column, direction} dicts.

Source of truth is real PowerCenter XML only: WF_EMS_DDS_APLY_MTH (multiple
Union and Sorter transformations). No ApplySequenceStep appears in this
workflow (connected sequence generators attach via lib.filter's
sequence_attach) - asserted explicitly so the count is never silently
assumed.
"""

import ast
import textwrap
from pathlib import Path
from types import SimpleNamespace

from jinja2 import Environment

from informatica_sparker.handlers import TransformHandlers
from informatica_sparker.ir import (
    ApplySorterStep, ApplySequenceStep, ApplyUnionStep, IRStepType,
)
from informatica_sparker.models import UserConfig
from informatica_sparker.parser import InfaXMLParser

XML_DIR = Path(__file__).resolve().parents[2] / "PowerCenter_workflows" / "dds"
WORKFLOW_XML = XML_DIR / "WF_EMS_DDS_APLY_MTH.XML"

TEMPLATE = (
    Path(__file__).resolve().parents[1]
    / "informatica_sparker" / "templates" / "mapping.py.j2"
)


def _load_steps(step_cls):
    parser = InfaXMLParser(WORKFLOW_XML.read_bytes())
    assert parser.parse(), f"parse failed: {WORKFLOW_XML.name}"
    mappings = parser.get_mappings()
    assert mappings, f"no mappings parsed from {WORKFLOW_XML.name}"
    for mapping in mappings:
        plan = TransformHandlers(mapping, UserConfig()).build_ir_plan()
        assert plan is not None
        for step in plan.steps:
            if isinstance(step, step_cls):
                yield mapping, step


def test_every_union_step_carries_union_cfg():
    """Every ApplyUnionStep must carry union_cfg with the runtime contract:
    inputs (df names), flag_column/output_columns, and union_selects whose
    df_input is a df NAME string (the template renders it unquoted)."""
    seen = 0
    for mapping, step in _load_steps(ApplyUnionStep):
        seen += 1
        assert "union_cfg" in step.params, (
            f"{mapping.name}: {step.step_name} carries no union_cfg - "
            "lib.union rendering would drop its inputs"
        )
        cfg = step.params["union_cfg"]
        # inputs wired from the constructor's df_inputs
        assert cfg.get("inputs") == step.params.get("df_inputs"), (
            f"{mapping.name}: {step.step_name} union_cfg inputs mismatch"
        )
        assert "flag_column" in cfg and "output_columns" in cfg, (
            f"{mapping.name}: {step.step_name} union_cfg missing "
            "flag_column/output_columns"
        )
        for _us in cfg.get("union_selects", []):
            # df_input must be a NAME (unquoted render); selects are FROM-only
            # strings in output-column order, at most one per output column
            assert isinstance(_us["df_input"], str), (
                f"{mapping.name}: {step.step_name} union_select df_input "
                f"not a df name: {_us['df_input']!r}"
            )
            assert isinstance(_us["selects"], list) and all(
                isinstance(_s, str) for _s in _us["selects"]
            ), (
                f"{mapping.name}: {step.step_name} union_selects entries "
                "must be from-only string lists"
            )
            assert len(_us["selects"]) <= len(cfg["output_columns"]), (
                f"{mapping.name}: {step.step_name} union_selects longer than "
                "output_columns - positional aliasing would overflow"
            )
    assert seen >= 1, "no ApplyUnionStep found in the workflow"


def test_every_sorter_step_carries_sorter_cfg():
    """Every ApplySorterStep must carry sorter_cfg: rename_columns as
    (from, to) TUPLES and sort_columns as {column, direction} dicts."""
    seen = 0
    for mapping, step in _load_steps(ApplySorterStep):
        seen += 1
        assert "sorter_cfg" in step.params, (
            f"{mapping.name}: {step.step_name} carries no sorter_cfg - "
            "lib.sorter rendering would drop its renames/orderBy"
        )
        cfg = step.params["sorter_cfg"]
        for _r in cfg.get("rename_columns", []):
            assert isinstance(_r, tuple) and len(_r) == 2, (
                f"{mapping.name}: {step.step_name} rename not a tuple: {_r!r}"
            )
        for _sc in cfg.get("sort_columns", []):
            assert isinstance(_sc, dict) and _sc.get("column") \
                and _sc.get("direction"), (
                f"{mapping.name}: {step.step_name} bad sort_column: {_sc!r}"
            )
    assert seen >= 1, "no ApplySorterStep found in the workflow"


def test_sequence_steps_absent_or_carry_sequence_cfg():
    """WF_EMS_DDS_APLY_MTH has no standalone ApplySequenceStep (connected
    sequence generators attach via lib.filter's sequence_attach). If one ever
    appears, it must carry sequence_cfg with output_col/start."""
    seen = 0
    for mapping, step in _load_steps(ApplySequenceStep):
        seen += 1
        assert "sequence_cfg" in step.params, (
            f"{mapping.name}: {step.step_name} carries no sequence_cfg"
        )
        cfg = step.params["sequence_cfg"]
        assert isinstance(cfg.get("output_col"), str) \
            and isinstance(cfg.get("start"), int), (
            f"{mapping.name}: {step.step_name} bad sequence_cfg: {cfg!r}"
        )
    assert seen == 0, (
        "expected no standalone ApplySequenceStep in this workflow "
        f"(sequence generators attach via filter); found {seen}"
    )


def test_apply_union_block_renders_parseable_lib_union_call():
    """The APPLY_UNION block must render a parseable lib.union(...) call:
    union_selects with df_input UNQUOTED (df name) + pyrepr'd from-only
    selects (one per line), the inputs fallback with unquoted df names, and
    ctx.register_df."""
    env = Environment()
    env.filters["pyrepr"] = repr  # matches codegen.py registration
    source = TEMPLATE.read_text()
    start = source.index(
        "{% elif step.step_type == IRStepType.APPLY_UNION %}"
    )
    end = source.index(
        "{% elif step.step_type == IRStepType.APPLY_ROUTER %}"
    )
    # Wrap in `{% if False %}` so the standalone `{% elif %}` header parses
    # AND the branch is evaluated (a True opener would take its empty first
    # branch); the wrapper tags emit nothing.
    block = "{% if False %}\n" + source[start:end] + "\n{% endif %}"

    step = SimpleNamespace(
        step_type=IRStepType.APPLY_UNION,
        step_name="apply_SMOKE",
        df_input="df_in_a",
        df_output="df_un",
        params={
            "union_cfg": {
                "inputs": ["df_in_a", "df_in_b"],
                "flag_column": "",
                "output_columns": ["C1", "C2"],
                "union_selects": [
                    {"df_input": "df_in_a",
                     "selects": ["SRC1", "SRC2"]},
                    {"df_input": "df_in_b",
                     "selects": ["SRC1"]},
                ],
            }
        },
    )
    out = env.from_string(block).render(step=step, IRStepType=IRStepType)

    assert "lib.union(" in out
    # df_input renders UNQUOTED (df name); selects render as pyrepr'd
    # from-only string lists, one entry per line (collapsed for the check)
    _collapsed = "".join(out.split())
    assert "{'df_input':df_in_a,'selects':['SRC1','SRC2']}," in _collapsed
    assert "{'df_input':df_in_b,'selects':['SRC1']}," in _collapsed
    # flag_column dead branch NOT rendered when empty
    assert "flag_column=" not in out
    assert "output_columns=['C1', 'C2']," in out
    assert 'ctx.register_df("df_un", df_un)' in out
    # Opt 1: union renders NEITHER spark=spark NOR config=config
    assert "spark=" not in out
    assert "config=" not in out
    # No unreplaced Jinja tags / stray braces leaked into the output
    assert "{%" not in out and "{{" not in out
    ast.parse(textwrap.dedent(out))

    # inputs fallback path: no union_selects -> unquoted df names in inputs
    step2 = SimpleNamespace(
        step_type=IRStepType.APPLY_UNION,
        step_name="apply_SMOKE2",
        df_input="df_in_a",
        df_output="df_un2",
        params={
            "union_cfg": {
                "inputs": ["df_in_a", "df_in_b"],
                "flag_column": "",
                "output_columns": [],
            }
        },
    )
    out2 = env.from_string(block).render(step=step2, IRStepType=IRStepType)
    assert "inputs=[df_in_a, df_in_b]," in out2
    # Opt 1: spark/config omitted in the inputs-fallback variant too
    assert "spark=" not in out2
    assert "config=" not in out2
    ast.parse(textwrap.dedent(out2))


def test_apply_sorter_block_renders_parseable_lib_sorter_call():
    """The APPLY_SORTER block must render a parseable lib.sorter(...) call:
    rename_columns as pyrepr'd tuples and sort_columns as pyrepr'd dicts."""
    env = Environment()
    env.filters["pyrepr"] = repr  # matches codegen.py registration
    source = TEMPLATE.read_text()
    start = source.index(
        "{% elif step.step_type == IRStepType.APPLY_SORTER %}"
    )
    end = source.index(
        "{% elif step.step_type == IRStepType.APPLY_UNION %}"
    )
    block = "{% if False %}\n" + source[start:end] + "\n{% endif %}"

    step = SimpleNamespace(
        step_type=IRStepType.APPLY_SORTER,
        step_name="apply_SMOKE",
        df_input="df_in",
        df_output="df_srt",
        params={
            "sorter_cfg": {
                "rename_columns": [("SRC_COL", "OUT_COL")],
                "sort_columns": [
                    {"column": "A", "direction": "ASC"},
                    {"column": "B", "direction": "DESC"},
                ],
            }
        },
    )
    out = env.from_string(block).render(step=step, IRStepType=IRStepType)

    assert "lib.sorter(" in out
    assert "input_df=df_in," in out
    # renames render as pyrepr'd (from, to) tuples
    assert "('SRC_COL', 'OUT_COL')" in out
    assert "{'column': 'A', 'direction': 'ASC'}" in out
    assert "{'column': 'B', 'direction': 'DESC'}" in out
    assert 'ctx.register_df("df_srt", df_srt)' in out
    # Opt 1: sorter renders NEITHER spark=spark NOR config=config
    assert "spark=" not in out
    assert "config=" not in out
    # No unreplaced Jinja tags / stray braces leaked into the output
    assert "{%" not in out and "{{" not in out
    ast.parse(textwrap.dedent(out))

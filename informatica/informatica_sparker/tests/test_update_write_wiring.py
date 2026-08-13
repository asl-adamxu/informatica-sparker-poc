"""Regression tests for update_strategy_cfg / write_target_cfg wiring.

The generator renders APPLY_UPDATE_STRATEGY and WRITE_TARGET steps via
`lib.update_strategy(...)` / `lib.write_target(...)`, which read ONLY
`step.params["update_strategy_cfg"]` / `step.params["write_target_cfg"]`.
A step missing its cfg silently degrades in the generated code (the write
falls back to a plain append, the strategy pass-through drops the I/U/D
split).

The cfg form is the runtime contract (Tasks 2-3):
  - update_strategy_cfg: `{'strategy_field': <column>}` ONLY when the
    strategy expression is a dynamic field reference; static DD_* strategies
    carry an empty cfg (the write step applies them directly);
  - write_target_cfg: all write keys; `table` is ALWAYS non-empty (the
    template renders `table={{ wtcfg['table'] | pyrepr }}` directly — an
    empty table would emit `table=''`).

Source of truth is real PowerCenter XML only: WF_EMS_DDS_APLY_MTH (49
mappings with many Update Strategy + Target instances).
"""

import ast
import textwrap
from pathlib import Path
from types import SimpleNamespace

from jinja2 import Environment

from informatica_sparker.handlers import TransformHandlers
from informatica_sparker.ir import (
    ApplyUpdateStrategyStep, IRStepType, WriteTargetStep,
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


def test_every_write_target_step_carries_write_target_cfg():
    """Every WriteTargetStep must carry write_target_cfg with a non-empty
    table — the template renders `table={{ wtcfg['table'] | pyrepr }}`
    unconditionally."""
    seen = 0
    for mapping, step in _load_steps(WriteTargetStep):
        seen += 1
        assert "write_target_cfg" in step.params, (
            f"{mapping.name}: {step.step_name} carries no write_target_cfg - "
            "lib.write_target rendering would crash on wtcfg['table']"
        )
        cfg = step.params["write_target_cfg"]
        assert cfg.get("table"), (
            f"{mapping.name}: {step.step_name} write_target_cfg table is "
            f"empty: {cfg.get('table')!r}"
        )
        # Keys the runtime reads must be present (defaults tolerated but the
        # shape must be right).
        assert isinstance(cfg.get("mode"), str) and cfg["mode"], (
            f"{mapping.name}: {step.step_name} write_target_cfg mode missing"
        )
        assert "sink_type" in cfg, (
            f"{mapping.name}: {step.step_name} write_target_cfg sink_type missing"
        )
        assert isinstance(cfg.get("target_columns"), list), (
            f"{mapping.name}: {step.step_name} write_target_cfg "
            "target_columns not a list"
        )
        assert isinstance(cfg.get("unmapped_columns"), list), (
            f"{mapping.name}: {step.step_name} write_target_cfg "
            "unmapped_columns not a list"
        )
        assert isinstance(cfg.get("is_delete"), bool) \
            and isinstance(cfg.get("cast_nulltype"), bool) \
            and isinstance(cfg.get("has_update_flag"), bool), (
            f"{mapping.name}: {step.step_name} write_target_cfg bool flags "
            "not bools"
        )
    assert seen >= 40, (
        f"expected >= 40 WriteTargetStep in this workflow, found {seen}"
    )


def test_every_update_strategy_step_carries_update_strategy_cfg():
    """Every ApplyUpdateStrategyStep must carry update_strategy_cfg. The
    template reads it via `.get(..., {})` so a missing cfg silently renders
    a passthrough lib.update_strategy call with no strategy_field."""
    seen = 0
    for mapping, step in _load_steps(ApplyUpdateStrategyStep):
        seen += 1
        assert "update_strategy_cfg" in step.params, (
            f"{mapping.name}: {step.step_name} carries no update_strategy_cfg"
        )
        cfg = step.params["update_strategy_cfg"]
        # cfg may be empty (static DD_*) or carry strategy_field (dynamic)
        if cfg.get("strategy_field"):
            assert isinstance(cfg["strategy_field"], str), (
                f"{mapping.name}: {step.step_name} strategy_field not a str: "
                f"{cfg['strategy_field']!r}"
            )
        assert isinstance(cfg, dict), (
            f"{mapping.name}: {step.step_name} update_strategy_cfg not a dict"
        )
    assert seen >= 1, "no ApplyUpdateStrategyStep found in the workflow"


def test_write_target_cfg_has_static_dd_and_update_flag_cases():
    """Non-vacuity: at least one write step carries a static_dd (DD_UPDATE /
    DD_DELETE) and at least one carries has_update_flag, so the I/U/D branch
    logic is actually exercised in this workflow."""
    static_dd_seen = 0
    update_flag_seen = 0
    for _mapping, step in _load_steps(WriteTargetStep):
        cfg = step.params["write_target_cfg"]
        if cfg.get("static_dd"):
            static_dd_seen += 1
        if cfg.get("has_update_flag"):
            update_flag_seen += 1
    assert static_dd_seen >= 1, (
        "no static_dd found in any write_target_cfg - DD_UPDATE/DD_DELETE "
        "write path untested by real XML"
    )
    assert update_flag_seen >= 1, (
        "no has_update_flag found in any write_target_cfg - I/U/D split "
        "write path untested by real XML"
    )


def test_dynamic_and_static_update_strategy_cases_present():
    """Non-vacuity for the update-strategy side: at least one step uses a
    dynamic strategy_field and at least one uses a static DD_* (empty cfg)."""
    dynamic_seen = 0
    static_seen = 0
    for _mapping, step in _load_steps(ApplyUpdateStrategyStep):
        cfg = step.params["update_strategy_cfg"]
        if cfg.get("strategy_field"):
            dynamic_seen += 1
        else:
            static_seen += 1
    assert dynamic_seen >= 1, (
        "no dynamic strategy_field found in any update_strategy_cfg"
    )
    assert static_seen >= 1, (
        "no static (empty-cfg) update strategy found - passthrough path "
        "untested by real XML"
    )


def test_apply_update_strategy_block_renders_parseable_lib_call():
    """The APPLY_UPDATE_STRATEGY block must render a parseable
    lib.update_strategy(...) call. strategy_field renders pyrepr'd only when
    the cfg carries it (dynamic); static strategies render the bare call."""
    env = Environment()
    env.filters["pyrepr"] = repr  # matches codegen.py registration
    source = TEMPLATE.read_text()
    start = source.index(
        "{% elif step.step_type == IRStepType.APPLY_UPDATE_STRATEGY %}"
    )
    end = source.index(
        "{% elif step.step_type == IRStepType.EXECUTE_SQL %}"
    )
    # Wrap in `{% if False %}` so the standalone `{% elif %}` header parses
    # AND the branch is evaluated (a True opener would take its empty first
    # branch); the wrapper tags emit nothing.
    block = "{% if False %}\n" + source[start:end] + "\n{% endif %}"

    # Dynamic field strategy — full cfg
    step = SimpleNamespace(
        step_type=IRStepType.APPLY_UPDATE_STRATEGY,
        step_name="apply_SMOKE",
        df_input="df_in",
        df_output="df_upd",
        params={
            "strategy_expression": "UPDATE_FLAG",
            "update_strategy_cfg": {"strategy_field": "UPDATE_FLAG"},
        },
    )
    out = env.from_string(block).render(step=step, IRStepType=IRStepType)
    assert "lib.update_strategy(" in out
    assert "input_df=df_in," in out
    assert "strategy_field='UPDATE_FLAG'," in out
    assert 'ctx.register_df("df_upd", df_upd)' in out
    # Opt 1: update_strategy renders NEITHER spark=spark NOR config=config
    assert "spark=" not in out
    assert "config=" not in out
    # No unreplaced Jinja tags / stray braces leaked into the output
    assert "{%" not in out and "{{" not in out
    ast.parse(textwrap.dedent(out))

    # Static DD_* strategy — empty cfg, no strategy_field kwarg
    step2 = SimpleNamespace(
        step_type=IRStepType.APPLY_UPDATE_STRATEGY,
        step_name="apply_SMOKE2",
        df_input="df_in",
        df_output="df_upd2",
        params={
            "strategy_expression": "DD_UPDATE",
            "update_strategy_cfg": {},
        },
    )
    out2 = env.from_string(block).render(step=step2, IRStepType=IRStepType)
    assert "lib.update_strategy(" in out2
    assert "strategy_field=" not in out2
    # Opt 1: spark/config omitted in the static (empty-cfg) variant too
    assert "spark=" not in out2
    assert "config=" not in out2
    assert "{%" not in out2 and "{{" not in out2
    ast.parse(textwrap.dedent(out2))


def test_write_target_block_renders_parseable_lib_call():
    """The WRITE_TARGET block must render a parseable lib.write_target(...)
    call as a STATEMENT (no assignment) with all cfg keys, and keep the
    'write completed' log line. The generated call returns None, so the
    rendered line must not be an assignment to df_write."""
    env = Environment()
    env.filters["pyrepr"] = repr  # matches codegen.py registration
    source = TEMPLATE.read_text()
    start = source.index(
        "{% elif step.step_type == IRStepType.WRITE_TARGET %}"
    )
    end = source.index(
        "{% elif step.step_type == IRStepType.WORKFLOW_DAG %}"
    )
    block = "{% if False %}\n" + source[start:end] + "\n{% endif %}"

    step = SimpleNamespace(
        step_type=IRStepType.WRITE_TARGET,
        step_name="write_SMOKE",
        df_input="df_in",
        df_output="",
        params={
            "write_target_cfg": {
                "table": "EMS.FACT",
                "mode": "append",
                "sink_type": "delta",
                "target_columns": ["A", "B"],
                "unmapped_columns": ["C"],
                "is_delete": True,
                "delete_keys": ["A"],
                "cast_nulltype": True,
                "has_update_flag": True,
                "static_dd": "DD_UPDATE",
                "field_map": {"B": "SRC_B"},
            }
        },
    )
    out = env.from_string(block).render(step=step, IRStepType=IRStepType)

    assert "lib.write_target(" in out
    assert "df=df_in," in out
    assert "conn=conn_target," in out
    assert "table='EMS.FACT'," in out
    assert "mode='append'," in out
    assert "target_columns=['A', 'B']," in out
    assert "unmapped_columns=['C']," in out
    assert "field_map={'B': 'SRC_B'}," in out
    assert "is_delete=True," in out
    assert "delete_keys=['A']," in out
    assert "cast_nulltype=True," in out
    assert "has_update_flag=True," in out
    assert "static_dd='DD_UPDATE'," in out
    # Opt 1: write_target keeps BOTH spark=spark and config=config
    assert "spark=spark," in out
    assert "config=config," in out
    # A statement, not an assignment — lib.write_target returns None
    assert "df_write = lib.write_target" not in out
    assert "df_write = df_in" not in out
    # The log line survives below the block
    assert 'logger.info("write_SMOKE write completed")' in out
    # No unreplaced Jinja tags / stray braces leaked into the output
    assert "{%" not in out and "{{" not in out
    ast.parse(textwrap.dedent(out))


def test_write_target_block_omits_dead_kwargs():
    """Dead-kwarg branch coverage: default sink_type (delta) and absent
    optional keys render NO kwargs — lib.write_target defaults apply."""
    env = Environment()
    env.filters["pyrepr"] = repr  # matches codegen.py registration
    source = TEMPLATE.read_text()
    start = source.index(
        "{% elif step.step_type == IRStepType.WRITE_TARGET %}"
    )
    end = source.index(
        "{% elif step.step_type == IRStepType.WORKFLOW_DAG %}"
    )
    block = "{% if False %}\n" + source[start:end] + "\n{% endif %}"

    step = SimpleNamespace(
        step_type=IRStepType.WRITE_TARGET,
        step_name="write_SMOKE2",
        df_input="df_in",
        df_output="",
        params={
            "write_target_cfg": {
                "table": "EMS.SIMPLE",
                "mode": "append",
                "sink_type": "delta",
                "target_columns": [],
                "unmapped_columns": [],
                "is_delete": False,
                "delete_keys": [],
                "cast_nulltype": False,
                "has_update_flag": False,
                "static_dd": None,
            }
        },
    )
    out = env.from_string(block).render(step=step, IRStepType=IRStepType)

    assert "lib.write_target(" in out
    assert "table='EMS.SIMPLE'," in out
    assert "mode='append'," in out
    # All optional/dead kwargs omitted
    assert "sink_type=" not in out
    assert "target_columns=" not in out
    assert "unmapped_columns=" not in out
    assert "field_map=" not in out
    assert "is_delete=" not in out
    assert "delete_keys=" not in out
    assert "cast_nulltype=" not in out
    assert "has_update_flag=" not in out
    assert "static_dd=" not in out
    # Opt 1: write_target keeps BOTH spark=spark and config=config
    assert "spark=spark," in out
    assert "config=config," in out
    assert 'logger.info("write_SMOKE2 write completed")' in out
    assert "{%" not in out and "{{" not in out
    ast.parse(textwrap.dedent(out))

"""Regression tests for router_cfg wiring on every ApplyRouterStep.

The generator renders APPLY_ROUTER steps via `lib.router(...)`, which reads
ONLY `step.params["router_cfg"]` (groups / multi_feed / feeds /
substitutions). Any ApplyRouterStep that carries groups in its params but
lacks router_cfg silently drops every output group in the generated code
(NameError on the group DataFrames at runtime).

The cfg form is the runtime contract (Task 1):
  - `condition` is RAW text (the runtime wraps it in expr() itself) — a
    leftover `expr("...")` wrapper would double-wrap and emit invalid SQL;
  - `default_negated` is a list of group NAMES (the runtime chains
    filter(~_conds[name]));
  - `renames` are (from, to) tuples.

Source of truth is real PowerCenter XML only: WF_EMS_DDS_APLY_MTH (6 Router
transformations, incl. DEFAULT negation chains and connector renames).
"""

import ast
import textwrap
from pathlib import Path
from types import SimpleNamespace

from jinja2 import Environment

from informatica_sparker.handlers import TransformHandlers
from informatica_sparker.ir import ApplyRouterStep, IRStepType
from informatica_sparker.models import UserConfig
from informatica_sparker.parser import InfaXMLParser

XML_DIR = Path(__file__).resolve().parents[2] / "PowerCenter_workflows" / "dds"
ROUTER_XML = XML_DIR / "WF_EMS_DDS_APLY_MTH.XML"

TEMPLATE = (
    Path(__file__).resolve().parents[1]
    / "informatica_sparker" / "templates" / "mapping.py.j2"
)


def _router_steps(handlers):
    plan = handlers.build_ir_plan()
    assert plan is not None
    return [s for s in plan.steps if isinstance(s, ApplyRouterStep)]


def _load_router_steps():
    parser = InfaXMLParser(ROUTER_XML.read_bytes())
    assert parser.parse(), f"parse failed: {ROUTER_XML.name}"
    mappings = parser.get_mappings()
    assert mappings, f"no mappings parsed from {ROUTER_XML.name}"
    for mapping in mappings:
        for step in _router_steps(TransformHandlers(mapping, UserConfig())):
            yield mapping, step


def test_all_router_steps_carry_router_cfg():
    """Every ApplyRouterStep must carry router_cfg with non-empty groups,
    and every group must satisfy the runtime contract (df_output, raw
    condition, name-based default_negated, tuple renames, exactly one of
    condition / default_negated / pass-through)."""
    seen = 0
    for mapping, step in _load_router_steps():
        seen += 1
        assert "router_cfg" in step.params, (
            f"{mapping.name}: {step.step_name} carries groups but no "
            "router_cfg — lib.router rendering would drop them"
        )
        cfg = step.params["router_cfg"]
        groups = cfg.get("groups") or []
        assert groups, f"{mapping.name}: {step.step_name} empty groups"
        for g in groups:
            assert g.get("df_output"), (
                f"{mapping.name}: {step.step_name} group {g.get('name')!r} "
                "missing df_output"
            )
            cond = g.get("condition") or ""
            dneg = g.get("default_negated") or []
            # Exactly one of condition / default_negated / pass-through
            assert not (bool(cond) and bool(dneg)), (
                f"{mapping.name}: {step.step_name} group {g.get('name')!r} "
                "has both condition and default_negated"
            )
            # Conditions must be RAW text — lib.router wraps in expr() itself
            assert not cond.startswith("expr(") and not cond.startswith("~"), (
                f"{mapping.name}: {step.step_name} group {g.get('name')!r} "
                f"condition still wrapped/unrenderable: {cond!r}"
            )
            # default_negated entries are group NAMES (strings)
            assert all(isinstance(n, str) for n in dneg), (
                f"{mapping.name}: {step.step_name} group {g.get('name')!r} "
                f"default_negated not name-based: {dneg!r}"
            )
            # renames are (from, to) tuples
            for r in g.get("renames", []):
                assert isinstance(r, tuple) and len(r) == 2, (
                    f"{mapping.name}: {step.step_name} group {g.get('name')!r} "
                    f"rename not a tuple: {r!r}"
                )
    assert seen >= 1, "no ApplyRouterStep found in the workflow"


def test_default_groups_use_name_based_negation():
    """Single-feed routers compose the DEFAULT group as a ~(expr(...)) &
    chain; the cfg must express it via default_negated group NAMES (runtime
    chains filter(~_conds[name])) with the composed condition dropped — the
    old composed text would double-wrap in the runtime's expr()."""
    default_groups = 0
    negated_defaults = 0
    for mapping, step in _load_router_steps():
        cfg = step.params["router_cfg"]
        for g in cfg.get("groups", []):
            if g["name"].upper() != "DEFAULT":
                continue
            default_groups += 1
            if g.get("default_negated"):
                negated_defaults += 1
                for n in g["default_negated"]:
                    names = {x["name"] for x in cfg["groups"]}
                    assert n in names, (
                        f"{mapping.name}: {step.step_name} DEFAULT negates "
                        f"unknown group {n!r}"
                    )
            cond = g.get("condition") or ""
            assert "~" not in cond and not cond.startswith("expr("), (
                f"{mapping.name}: {step.step_name} DEFAULT keeps unrenderable "
                f"composed condition {cond!r}"
            )
    assert default_groups >= 1, (
        "expected at least one DEFAULT group across the router steps"
    )
    assert negated_defaults >= 1, (
        "expected at least one DEFAULT group expressed via default_negated — "
        "the negation-chain conversion is not exercised"
    )


def test_apply_router_block_renders_parseable_lib_router_call():
    """The APPLY_ROUTER block must render a parseable lib.router(...) call
    with per-group dict unpacking, multi_feed feeds (df names UNQUOTED),
    substitutions dict braces, and ctx.register_df per group."""
    env = Environment()
    env.filters["pyrepr"] = repr  # matches codegen.py registration
    source = TEMPLATE.read_text()
    start = source.index(
        "{% elif step.step_type == IRStepType.APPLY_ROUTER %}"
    )
    end = source.index(
        "{% elif step.step_type == IRStepType.APPLY_SEQUENCE %}"
    )
    # Wrap in `{% if False %}` so the standalone `{% elif %}` header parses
    # AND the branch is evaluated (a True opener would take its empty first
    # branch); the wrapper tags emit nothing.
    block = "{% if False %}\n" + source[start:end] + "\n{% endif %}"

    step = SimpleNamespace(
        step_type=IRStepType.APPLY_ROUTER,
        step_name="apply_SMOKE",
        df_input="df_in",
        params={
            "router_cfg": {
                "multi_feed": True,
                "feeds": [("df_feed_a", {"SRC": "PORT1"}), ("df_feed_b", {})],
                "groups": [
                    {"name": "G1", "df_output": "df_rtr_G1",
                     "condition": "GRP = 'A'",
                     "filter_inner": "V > $$v_min",
                     "renames": [("GRP", "GRP1")]},
                    {"name": "DEFAULT", "df_output": "df_rtr_DEFAULT",
                     "default_negated": ["G1"]},
                ],
                "substitutions": {"$$v_min": "v_min"},
            }
        },
    )
    out = env.from_string(block).render(step=step, IRStepType=IRStepType)

    assert "lib.router(" in out
    # multi_feed: input_df is NOT rendered — the handler sets df_input to the
    # literal "df_rtr_input", a variable that no longer exists now that the
    # template's union-building block is gone (lib.router builds the union
    # from feeds and ignores input_df in multi_feed mode)
    assert "input_df=" not in out
    # top-level feed df names render UNQUOTED; aliases pyrepr-quoted
    assert "(df_feed_a, {'SRC': 'PORT1'})," in out
    assert "(df_feed_b, {})," in out
    # whole-group dicts one per line (pyrepr), conditions raw (repr picks
    # double quotes because the raw condition contains single quotes)
    assert "'condition': \"GRP = 'A'\"" in out
    assert "'default_negated': ['G1']" in out
    # per-group dict unpacking from the returned dict + registration
    assert "df_rtr_G1 = _rtr['df_rtr_G1']" in out
    assert "df_rtr_DEFAULT = _rtr['df_rtr_DEFAULT']" in out
    assert 'ctx.register_df("df_rtr_G1", df_rtr_G1)' in out
    # substitutions dict braces (the {{ '{' }} escape) render correctly
    assert "substitutions={'$$v_min': v_min}," in out
    # No unreplaced Jinja tags / stray braces leaked into the output
    assert "{%" not in out and "{{" not in out
    ast.parse(textwrap.dedent(out))

    # Single-feed variant: input_df IS rendered (the fix must not drop it)
    single = SimpleNamespace(
        step_type=IRStepType.APPLY_ROUTER,
        step_name="apply_SMOKE",
        df_input="df_in",
        params={
            "router_cfg": {
                "groups": [
                    {"name": "G1", "df_output": "df_rtr_G1",
                     "condition": "GRP = 'A'"},
                    {"name": "DEFAULT", "df_output": "df_rtr_DEFAULT",
                     "default_negated": ["G1"]},
                ],
            }
        },
    )
    out_single = env.from_string(block).render(
        step=single, IRStepType=IRStepType
    )
    assert "input_df=df_in," in out_single
    assert "multi_feed=True" not in out_single
    ast.parse(textwrap.dedent(out_single))

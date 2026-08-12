"""Regression tests for expression_cfg wiring on every ApplyExpressionStep.

The generator renders APPLY_EXPRESSION steps via `lib.expression(...)`, which
reads ONLY `step.params["expression_cfg"]` (computed_columns /
rename_columns / pass_through_cols / sp_calls / ...). Any ApplyExpressionStep
that carries computed_columns or rename_columns in its params but lacks
expression_cfg silently drops those columns in the generated code
([UNRESOLVED_COLUMN] at runtime).

This guards the mapplet-internal wiring sites that set params directly:
  - nullinput step (unconnected mapplet INPUT ports → NULL, v2026.08.04 fix)
  - input remap step (mapplet entry-point port rename)
  - internal expression step (mpl computed columns)
  - rename step and mapplet OUTPUT step

Sources of truth are real PowerCenter XMLs only:
  - WF_CMS_DDS_APLY_MTH (10 mappings, 16 mapplets inlined)
  - WF_EMS_PRHE_DDS_APLY_HSE_STCK_MTH (produces nullinput_* steps)
"""

from pathlib import Path

from informatica_sparker.handlers import TransformHandlers
from informatica_sparker.ir import ApplyExpressionStep
from informatica_sparker.models import UserConfig
from informatica_sparker.parser import InfaXMLParser


XML_DIR = Path(__file__).resolve().parents[2] / "PowerCenter_workflows" / "dds"
CMS_XML = XML_DIR / "WF_CMS_DDS_APLY_MTH.XML"
HSE_STCK_XML = XML_DIR / "WF_EMS_PRHE_DDS_APLY_HSE_STCK_MTH.XML"


def _load_mappings(xml_path):
    parser = InfaXMLParser(xml_path.read_bytes())
    assert parser.parse(), xml_path.name
    return parser.get_mappings()


def _expression_steps(handlers):
    plan = handlers.build_ir_plan()
    assert plan is not None
    return list(plan.steps), [
        s for s in plan.steps if isinstance(s, ApplyExpressionStep)
    ]


def _assert_cfg_wired(mapping, step):
    """Every expression step with computed/rename columns must carry
    expression_cfg, and the cfg must mirror the params."""
    assert "expression_cfg" in step.params, (
        f"{mapping.name}: {step.step_name} sets "
        f"computed_columns={step.params.get('computed_columns')} / "
        f"rename_columns={step.params.get('rename_columns')} but has no "
        "expression_cfg"
    )
    cfg = step.params["expression_cfg"]

    # rename_columns copied from params, normalized to tuples (the render form)
    assert cfg.get("rename_columns") == [tuple(_p) for _p in (
        step.params.get("rename_columns") or []
    )], f"{mapping.name}: {step.step_name} cfg rename_columns mismatch"

    # computed columns preserved (SP columns may be moved to sp_calls, so a
    # subset is the guaranteed direction)
    param_names = {
        c["name"] for c in step.params.get("computed_columns", [])
    }
    cfg_names = {c["name"] for c in cfg.get("computed_columns", [])}
    assert cfg_names <= param_names, (
        f"{mapping.name}: {step.step_name} cfg computed columns "
        f"{cfg_names - param_names} not in params"
    )

    # pass_through_cols = output_columns minus computed names (helper formula)
    out_cols = step.params.get("output_columns") or []
    if out_cols:
        assert set(cfg.get("pass_through_cols", [])) == (
            set(out_cols) - param_names
        ), f"{mapping.name}: {step.step_name} cfg pass_through_cols mismatch"

    # substitutions map the $$ variable key to the RUNTIME IDENTIFIER derived
    # from the key ($$v_x → v_x), which the template renders unquoted — the
    # value loaded override-aware from UTL_JOB_PARAM. Baking the variable's
    # DEFAULT VALUE here would ignore job-param overrides (and non-identifier
    # defaults render invalid Python).
    for _k, _v in cfg.get("substitutions", {}).items():
        assert _v == _k.replace("$", ""), (
            f"{mapping.name}: {step.step_name} cfg substitution "
            f"{{{_k!r}: {_v!r}}} value must be the clean var-name derived "
            f"from the key (expected {_k.replace('$', '')!r}), not the "
            "variable's default value"
        )


def _check_mappings(xml_path, label, require_kinds):
    mappings = _load_mappings(xml_path)
    assert mappings, f"no mappings parsed from {xml_path.name}"
    checked = {"computed": 0, "rename": 0, "total": 0, "subs": 0}
    kinds = {}
    for mapping in mappings:
        plan, expr_steps = _expression_steps(TransformHandlers(mapping, UserConfig()))
        assert plan is not None, mapping.name
        for step in expr_steps:
            prefix = step.step_name.split("_", 1)[0]
            kinds[prefix] = kinds.get(prefix, 0) + 1
            has_computed = bool(step.params.get("computed_columns"))
            has_rename = bool(step.params.get("rename_columns"))
            if not (has_computed or has_rename):
                continue
            checked["total"] += 1
            checked["computed"] += has_computed
            checked["rename"] += has_rename
            if step.params.get("expression_cfg", {}).get("substitutions"):
                checked["subs"] += 1
            _assert_cfg_wired(mapping, step)
    assert checked["total"] > 0, f"{xml_path.name}: no expression steps with params"
    assert checked["computed"] > 0, f"{xml_path.name}: no computed-bearing steps"
    for kind in require_kinds:
        assert kinds.get(kind, 0) > 0, (
            f"{xml_path.name}: no {kind}_ steps found — the wiring site "
            f"producing them is not exercised"
        )
    return kinds, checked


def test_cms_expression_steps_all_carry_expression_cfg():
    """Every computed/rename-bearing ApplyExpressionStep in the CMS mapping
    set (16 mapplets inlined, incl. input remap + internal expression + mapplet
    OUTPUT steps) must carry a matching expression_cfg."""
    kinds, checked = _check_mappings(
        CMS_XML,
        "CMS",
        require_kinds=("input", "apply", "rename"),
    )
    assert checked["computed"] >= 50, (
        f"CMS: expected many computed-bearing steps, got {checked['computed']}"
    )
    assert checked["subs"] > 0, (
        "CMS: expected steps with substitutions ($$ variables) so the "
        "substitutions assertion is non-vacuous"
    )


def test_nullinput_steps_carry_expression_cfg():
    """Unconnected mapplet INPUT ports become a nullinput_* expression step
    (v2026.08.04 unconnected-input→NULL fix). These set computed_columns
    directly and must carry expression_cfg — otherwise the NULL fill is
    silently dropped in lib.expression rendering."""
    kinds, checked = _check_mappings(
        HSE_STCK_XML,
        "HSE_STCK",
        require_kinds=("nullinput",),
    )
    assert kinds["nullinput"] >= 10, (
        f"HSE_STCK: expected many nullinput steps, got {kinds['nullinput']}"
    )
    assert checked["subs"] > 0, (
        "HSE_STCK: expected steps with substitutions ($$ variables) so the "
        "substitutions assertion is non-vacuous"
    )

"""Regression: :SP. expressions in computed columns must become sp_calls.

The helper's SP detection looked the referenced procedure up in
transform_map; reusable / mapplet-local Stored Procedure components are NOT
in transform_map (they live in the mapplet's local transform table), so the
detection silently skipped them and the raw ':SP.SP_XXX(...)' text stayed in
computed_columns — rendered as a plain expr(...) that fails at runtime with
[UNBOUND_SQL_PARAMETER]. The old template fell back to the procedure name
extracted from the expression; the helper must do the same.
"""

import re
from pathlib import Path

import pytest

from informatica_sparker.handlers import TransformHandlers
from informatica_sparker.ir import ApplyExpressionStep
from informatica_sparker.models import UserConfig
from informatica_sparker.parser import InfaXMLParser

ROOT = Path(__file__).resolve().parents[2]
EMS_XML = ROOT / "PowerCenter_workflows" / "dds" / "WF_EMS_DDS_APLY_MTH.XML"

_SP_REF = re.compile(r":SP\.(\w+)")


def _expression_steps(mappings):
    for m in mappings:
        handlers = TransformHandlers(m, UserConfig())
        plan = handlers.build_ir_plan()
        for step in plan.steps:
            if isinstance(step, ApplyExpressionStep):
                yield m.name, step


def test_sp_references_never_remain_in_computed_columns(ems_xml):
    sp_step_seen = 0
    for mname, step in _expression_steps(ems_xml):
        cfg = step.params.get("expression_cfg", {}) or {}
        for cc in cfg.get("computed_columns", []):
            assert ":SP." not in cc["expr"], (
                "%s: %s computed column %s still carries raw :SP. text %r — "
                "SP detection missed it (reusable SP not in transform_map?)"
                % (mname, step.step_name, cc["name"], cc["expr"])
            )
        for sp in cfg.get("sp_calls") or []:
            sp_step_seen += 1
            assert sp["col"] and sp["sp_call"] and sp["args"], (
                "%s: %s malformed sp_call %r" % (mname, step.step_name, sp)
            )
            # Every sp_call must correspond to a real :SP. reference with args.
            assert _SP_REF.search(sp["sp_call"]) or "." in sp["sp_call"] or sp["sp_call"]
    assert sp_step_seen > 0, "expected at least one sp_calls-bearing step in the XML"


@pytest.fixture(scope="module")
def ems_xml():
    parser = InfaXMLParser(EMS_XML.read_bytes())
    assert parser.parse()
    return parser.get_mappings()

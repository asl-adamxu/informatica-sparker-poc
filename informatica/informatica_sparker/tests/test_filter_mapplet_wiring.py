"""Regression: mapplet-internal filter steps must carry lib_filter_cfg.

The template reads ONLY lib_filter_cfg (falling back to condition='TRUE'),
so any ApplyFilterStep created without it silently passes all rows. The
mapplet-internal filter path (handlers.py, _handle_mapplet) was found to
miss the cfg — this test nets every ApplyFilterStep from real XML.
"""

from pathlib import Path

import pytest

from informatica_sparker.handlers import TransformHandlers
from informatica_sparker.ir import ApplyFilterStep
from informatica_sparker.models import UserConfig
from informatica_sparker.parser import InfaXMLParser

ROOT = Path(__file__).resolve().parents[2]
EMS_TL_XML = ROOT / "PowerCenter_workflows" / "dds" / "WF_EMS_DDS_APLY_MTH.XML"


def _parse(xml_path):
    parser = InfaXMLParser(xml_path.read_bytes())
    assert parser.parse()
    return parser.get_mappings()


def _filter_steps(mappings):
    """Yield (mapping_name, step) for every ApplyFilterStep across mappings."""
    for m in mappings:
        handlers = TransformHandlers(m, UserConfig())
        plan = handlers.build_ir_plan()
        for step in plan.steps:
            if isinstance(step, ApplyFilterStep):
                yield m.name, step


def _inner_text(expr_wrapped):
    """Strip expr("...") wrapper — same extraction the handler uses."""
    import re
    _m = re.match(r'expr\("(.*)"\)$', expr_wrapped)
    return _m.group(1) if _m else expr_wrapped


def test_mapplet_filter_steps_have_cfg(ems_xml):
    steps = list(_filter_steps(ems_xml))
    # Non-vacuity: the buggy mapping's mapplet filter must be present.
    assert any("MPLT_DDS_APPLY_DELETE" in s.step_name for _, s in steps), \
        "expected at least one mapplet-internal filter step in the parsed mapping"
    for mname, step in steps:
        cfg = step.params.get("lib_filter_cfg")
        assert cfg is not None, \
            "%s: ApplyFilterStep %s has no lib_filter_cfg — template renders condition='TRUE' and the filter passes ALL rows" % (
                mname, step.step_name)
        cond = step.params.get("condition", "True")
        if str(cond) != "True":
            assert cfg.get("condition") == _inner_text(str(cond)), \
                "%s: %s cfg condition %r != params condition %r" % (
                    mname, step.step_name, cfg.get("condition"), cond)


@pytest.fixture(scope="module")
def ems_xml():
    return _parse(EMS_TL_XML)

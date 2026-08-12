"""Regression tests for bare numeric Filter conditions (FILTER_NOT_BOOLEAN).

Informatica allows a bare numeric port as a Filter condition (non-zero =
TRUE, zero = FALSE). Spark 3.5 rejects non-boolean filter expressions with
[DATATYPE_MISMATCH.FILTER_NOT_BOOLEAN], so the generator must rewrite a bare
numeric port condition into an explicit `!= 0` comparison.

Reported via M_S5_SSAL2_TRANSFORM_EMS_TAM_FLAT_RCVR FILTRANS3
(WF_EMS_TL.XML): Filter Condition VALUE="OUT_DLPK_SOR_CACHE".
"""

from informatica_sparker.handlers import TransformHandlers
from informatica_sparker.ir import ApplyFilterStep
from informatica_sparker.models import UserConfig
from informatica_sparker.parser import InfaXMLParser

MINIMAL_XML = b"""\
<POWERMART VERSION="1.0">
<REPOSITORY NAME="TEST_REPO" VERSION="1.0" DATABASETYPE="Oracle"/>
<FOLDER NAME="TEST_FOLDER">
  <MAPPING DESCRIPTION="" ISVALID="YES" NAME="M_TEST_BARE_FILTER">
    <TRANSFORMATION DESCRIPTION="" NAME="FILTRANS" TYPE="Filter" VERSIONNUMBER="1">
      <TRANSFORMFIELD DATATYPE="integer" DEFAULTVALUE="" DESCRIPTION="" NAME="OUT_DLPK_SOR_CACHE" PORTTYPE="INPUT/OUTPUT" PRECISION="10" SCALE="0"/>
      <TABLEATTRIBUTE NAME="Filter Condition" VALUE="OUT_DLPK_SOR_CACHE"/>
    </TRANSFORMATION>
    <TRANSFORMATION DESCRIPTION="" NAME="FILTRANS_EQ" TYPE="Filter" VERSIONNUMBER="1">
      <TRANSFORMFIELD DATATYPE="integer" DEFAULTVALUE="" DESCRIPTION="" NAME="OUT_DLPK_SOR_CACHE" PORTTYPE="INPUT/OUTPUT" PRECISION="10" SCALE="0"/>
      <TABLEATTRIBUTE NAME="Filter Condition" VALUE="OUT_DLPK_SOR_CACHE = 1"/>
    </TRANSFORMATION>
    <TRANSFORMATION DESCRIPTION="" NAME="FILTRANS_STR" TYPE="Filter" VERSIONNUMBER="1">
      <TRANSFORMFIELD DATATYPE="string" DEFAULTVALUE="" DESCRIPTION="" NAME="CODE" PORTTYPE="INPUT/OUTPUT" PRECISION="10" SCALE="0"/>
      <TABLEATTRIBUTE NAME="Filter Condition" VALUE="CODE"/>
    </TRANSFORMATION>
    <INSTANCE DESCRIPTION="" NAME="FILTRANS" REUSABLE="NO" TRANSFORMATION_NAME="FILTRANS" TRANSFORMATION_TYPE="Filter" TYPE="TRANSFORMATION"/>
    <INSTANCE DESCRIPTION="" NAME="FILTRANS_EQ" REUSABLE="NO" TRANSFORMATION_NAME="FILTRANS_EQ" TRANSFORMATION_TYPE="Filter" TYPE="TRANSFORMATION"/>
    <INSTANCE DESCRIPTION="" NAME="FILTRANS_STR" REUSABLE="NO" TRANSFORMATION_NAME="FILTRANS_STR" TRANSFORMATION_TYPE="Filter" TYPE="TRANSFORMATION"/>
  </MAPPING>
</FOLDER>
</POWERMART>
"""


def _filter_steps():
    parser = InfaXMLParser(MINIMAL_XML)
    assert parser.parse()
    mapping = parser.get_mappings()[0]
    handlers = TransformHandlers(mapping, UserConfig())
    plan = handlers.build_ir_plan()
    steps = {
        s.step_name[6:]: s
        for s in plan.steps
        if isinstance(s, ApplyFilterStep)
    }
    assert steps, "expected ApplyFilterStep steps"
    return steps


def test_bare_numeric_filter_condition_is_rewritten_to_nonzero():
    """A bare numeric port (Informatica truthy) becomes an explicit != 0."""
    steps = _filter_steps()
    assert steps["FILTRANS"].params["condition"] == 'expr("OUT_DLPK_SOR_CACHE != 0")'


def test_explicit_comparison_filter_condition_is_unchanged():
    """An explicit comparison must not be touched."""
    steps = _filter_steps()
    assert steps["FILTRANS_EQ"].params["condition"] == 'expr("OUT_DLPK_SOR_CACHE = 1")'


def test_bare_string_filter_condition_is_unchanged():
    """A bare string port is not numeric: leave as-is (old behavior)."""
    steps = _filter_steps()
    assert steps["FILTRANS_STR"].params["condition"] == 'expr("CODE")'

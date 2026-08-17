"""Regression: mapplet-internal expressions replace unconnected INPUT ports
with NULL (Informatica semantics).

EXP_NULL_BKEY in MPLT_AGMT_EMS_RVC_COST_CTR declares INPUT port IN_BKEY with
NO connector (internal or external) and references it in OUT_BKEY's decode
expression. The generated code used to keep the reference, crashing with
UNRESOLVED_COLUMN in M_EMS_SSAL2_TRANS_SEC_ORG_UNIT_COST_CTR (WF_EMS_TL).
"""

import textwrap

from informatica_sparker.handlers import TransformHandlers
from informatica_sparker.ir import ApplyExpressionStep
from informatica_sparker.models import UserConfig
from informatica_sparker.parser import InfaXMLParser

XML = textwrap.dedent("""\
<?xml version="1.0" encoding="UTF-8"?>
<POWERMART CREATION_DATE="2026-08-17" REPOSITORY_VERSION="1.0">
  <REPOSITORY NAME="R" VERSION="1.0">
    <FOLDER NAME="TEST" GROUP="0" OWNER="t" SHARED="NOTSHARED">
      <MAPPING DESCRIPTION="" ISVALID="YES" NAME="M_TEST">
        <TRANSFORMATION DESCRIPTION="" NAME="SQ1" OBJECTVERSION="1" REUSABLE="NO" TYPE="Source Qualifier" VERSIONNUMBER="1">
          <TRANSFORMFIELD DATATYPE="string" DESCRIPTION="" GROUP="OUTPUT" NAME="KEY1" PORTTYPE="OUTPUT" PRECISION="10" SCALE="0"/>
        </TRANSFORMATION>
        <INSTANCE DESCRIPTION="" NAME="SQ1" TRANSFORMATION_NAME="SQ1" TRANSFORMATION_TYPE="Source Qualifier" TYPE="TRANSFORMATION"/>
        <INSTANCE DESCRIPTION="" NAME="MPLT" TRANSFORMATION_NAME="MPLT_TEST" TRANSFORMATION_TYPE="Mapplet" TYPE="Mapplet"/>
        <CONNECTOR FROMFIELD="KEY1" FROMINSTANCE="SQ1" TOFIELD="IN_COST_CTR_BK" TOINSTANCE="MPLT"/>
        <MAPPLET DESCRIPTION="" ISVALID="YES" NAME="MPLT_TEST">
          <TRANSFORMATION DESCRIPTION="" NAME="INPUT" OBJECTVERSION="1" REUSABLE="NO" TYPE="Input Transformation" VERSIONNUMBER="1">
            <TRANSFORMFIELD DATATYPE="string" DESCRIPTION="" GROUP="OUTPUT" NAME="IN_COST_CTR_BK" PORTTYPE="OUTPUT" PRECISION="10" SCALE="0"/>
          </TRANSFORMATION>
          <TRANSFORMATION DESCRIPTION="" NAME="EXP_NULL_BKEY" OBJECTVERSION="1" REUSABLE="NO" TYPE="Expression" VERSIONNUMBER="1">
            <TRANSFORMFIELD DATATYPE="string" DESCRIPTION="" GROUP="INPUT" NAME="IN_BKEY" PORTTYPE="INPUT" PRECISION="10" SCALE="0"/>
            <TRANSFORMFIELD DATATYPE="string" DESCRIPTION="" GROUP="INPUT" NAME="IN_SHORT_BKEY" PORTTYPE="INPUT" PRECISION="10" SCALE="0"/>
            <TRANSFORMFIELD DATATYPE="string" DESCRIPTION="" GROUP="OUTPUT" NAME="OUT_BKEY" PORTTYPE="OUTPUT" PRECISION="10" SCALE="0" EXPRESSION="decode(LTRIM(RTRIM(IN_BKEY)),NULL,'UNKNOWN','','UNKNOWN',IN_BKEY)"/>
            <TRANSFORMFIELD DATATYPE="string" DESCRIPTION="" GROUP="OUTPUT" NAME="OUT_SHORT_BKEY" PORTTYPE="OUTPUT" PRECISION="10" SCALE="0" EXPRESSION="decode(LTRIM(RTRIM(IN_SHORT_BKEY)),NULL,'?','','?',IN_SHORT_BKEY)"/>
          </TRANSFORMATION>
          <TRANSFORMATION DESCRIPTION="" NAME="OUTPUT" OBJECTVERSION="1" REUSABLE="NO" TYPE="Output Transformation" VERSIONNUMBER="1">
            <TRANSFORMFIELD DATATYPE="string" DESCRIPTION="" GROUP="OUTPUT" NAME="OUT_BKEY" PORTTYPE="OUTPUT" PRECISION="10" SCALE="0"/>
          </TRANSFORMATION>
          <INSTANCE DESCRIPTION="" NAME="INPUT" TRANSFORMATION_NAME="INPUT" TRANSFORMATION_TYPE="Input Transformation" TYPE="TRANSFORMATION"/>
          <INSTANCE DESCRIPTION="" NAME="EXP_NULL_BKEY" TRANSFORMATION_NAME="EXP_NULL_BKEY" TRANSFORMATION_TYPE="Expression" TYPE="TRANSFORMATION"/>
          <INSTANCE DESCRIPTION="" NAME="OUTPUT" TRANSFORMATION_NAME="OUTPUT" TRANSFORMATION_TYPE="Output Transformation" TYPE="TRANSFORMATION"/>
          <CONNECTOR FROMFIELD="IN_COST_CTR_BK" FROMINSTANCE="INPUT" TOFIELD="IN_SHORT_BKEY" TOINSTANCE="EXP_NULL_BKEY"/>
          <CONNECTOR FROMFIELD="OUT_BKEY" FROMINSTANCE="EXP_NULL_BKEY" TOFIELD="OUT_BKEY" TOINSTANCE="OUTPUT"/>
        </MAPPLET>
      </MAPPING>
    </FOLDER>
  </REPOSITORY>
</POWERMART>
""")


def _mapping():
    parser = InfaXMLParser(XML.encode("utf-8"))
    assert parser.parse()
    mappings = parser.get_mappings()
    assert len(mappings) == 1
    return mappings[0]


def test_unconnected_mapplet_input_replaced_with_null():
    handlers = TransformHandlers(_mapping(), UserConfig())
    plan = handlers.build_ir_plan()
    assert plan is not None

    exprs = {}
    for step in plan.steps:
        if not isinstance(step, ApplyExpressionStep):
            continue
        for cc in step.params.get("computed_columns") or []:
            exprs[cc["name"]] = cc["expression"]

    # The unconnected IN_BKEY reference becomes NULL...
    assert "OUT_BKEY" in exprs
    assert "IN_BKEY" not in exprs["OUT_BKEY"]
    assert "ltrim(rtrim(NULL))" in exprs["OUT_BKEY"]
    # ...while the connected IN_SHORT_BKEY reference is untouched.
    assert "IN_SHORT_BKEY" in exprs["OUT_SHORT_BKEY"]

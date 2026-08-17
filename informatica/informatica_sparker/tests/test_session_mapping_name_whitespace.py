"""Regression tests for trailing-whitespace mapping/session names.

Legacy XML may carry trailing whitespace in `<MAPPING NAME="... ">` and the
session's `MAPPINGNAME="... "` (12 such sessions in WF_EMS_TL, e.g.
`M_S5_SOR_LOAD_EMS_TEM_TNT_WARN_FOR_UPDATE `). The generated mapping FILE
name sanitizes the space to `_` (`_make_safe_name`), but an unstripped
`mapping_name` in the execution plan never matches the module key at workflow
runtime → `ValueError: mapping 'M_..._FOR_UPDATE ' not found` when running
`python wf_ems_tl.py`.
"""

from informatica_sparker.models import UserConfig
from informatica_sparker.parser import InfaXMLParser

MINIMAL_XML = b"""\
<POWERMART VERSION="1.0">
<REPOSITORY NAME="TEST_REPO" VERSION="1.0" DATABASETYPE="Oracle"/>
<FOLDER NAME="TEST_FOLDER">
  <MAPPING DESCRIPTION="" ISVALID="YES" NAME="M_DEMO_FOR_UPDATE " OBJECTVERSION="1" VERSIONNUMBER="1">
    <TRANSFORMATION DESCRIPTION="" NAME="SQ_1" OBJECTVERSION="1" REUSABLE="NO" TYPE="Source Qualifier" VERSIONNUMBER="1">
      <TRANSFORMFIELD DATATYPE="integer" DEFAULTVALUE="" DESCRIPTION="" NAME="KEY" PICTURETEXT="" PORTTYPE="INPUT/OUTPUT" PRECISION="10" SCALE="0"/>
    </TRANSFORMATION>
    <INSTANCE DESCRIPTION="" NAME="SQ_1" REUSABLE="NO" TRANSFORMATION_NAME="SQ_1" TRANSFORMATION_TYPE="Source Qualifier" TYPE="TRANSFORMATION"/>
  </MAPPING>
  <WORKFLOW DESCRIPTION="" ISVALID="YES" NAME="WF_DEMO" OBJECTVERSION="1" VERSIONNUMBER="1">
    <TASK DESCRIPTION="" ISVALID="YES" NAME="Start" TASKTYPE="Start" TYPE="TASK"/>
    <TASK DESCRIPTION="" ISVALID="YES" NAME="S_DEMO" TASKTYPE="Session" TYPE="TASK">
      <SESSION DESCRIPTION="" ISVALID="YES" MAPPINGNAME="M_DEMO_FOR_UPDATE " NAME="S_DEMO" REUSABLE="NO" SORTORDER="Binary" VERSIONNUMBER="1"/>
    </TASK>
    <WORKFLOWLINK CONDITION="" FROMTASK="Start" TOTASK="S_DEMO"/>
  </WORKFLOW>
</FOLDER>
</POWERMART>
"""


def test_mapping_name_is_stripped():
    """The MAPPING NAME drives the generated file name — a trailing space
    must not leak into it (it would sanitize to a trailing '_')."""
    parser = InfaXMLParser(MINIMAL_XML)
    assert parser.parse()
    mapping = parser.get_mappings()[0]
    assert mapping.name == "M_DEMO_FOR_UPDATE", mapping.name


def test_session_mapping_name_is_stripped():
    """The session MAPPINGNAME must equal the mapping's (stripped) name —
    the workflow runtime looks up the module key by mapping_name.lower()."""
    parser = InfaXMLParser(MINIMAL_XML)
    assert parser.parse()
    analysis = parser.get_workflow_analysis()
    sessions = [s for s in analysis["sessions"] if s["name"] == "S_DEMO"]
    assert sessions, "session S_DEMO not parsed"
    assert sessions[0]["mapping_name"] == "M_DEMO_FOR_UPDATE", sessions[0]


def test_plan_name_matches_module_key_invariant():
    """End-to-end invariant that broke: the plan's mapping_name must
    lowercase-equal the generated module key (safe-name of the mapping
    name). Both parser fixes together guarantee it."""
    parser = InfaXMLParser(MINIMAL_XML)
    assert parser.parse()
    mapping = parser.get_mappings()[0]
    analysis = parser.get_workflow_analysis()
    session = next(s for s in analysis["sessions"] if s["name"] == "S_DEMO")
    # Module key = safe file name of the mapping name (codegen _make_safe_name)
    import re as _re
    safe = _re.sub(r'[^a-zA-Z0-9_]', '_', mapping.name).lower()
    assert session["mapping_name"].lower() == safe, (
        f"plan mapping_name {session['mapping_name']!r} != module key {safe!r}"
    )

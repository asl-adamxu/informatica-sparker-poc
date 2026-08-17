"""Regression tests for numeric Update Strategy Expressions.

Informatica accepts the numeric constants in the Update Strategy Expression
(0=DD_INSERT, 1=DD_UPDATE, 2=DD_DELETE, 3=DD_REJECT) in addition to the DD_*
constants and identifier field references. The generator must normalize
numeric literals to their DD_* constant so the Update Strategy step and the
Write Target step classify identically — otherwise a bare "0" is misread as a
dynamic field strategy, the write step splits on `_update_flag`, and the
column was never created (UNRESOLVED_COLUMN).

Reported via M_S5_SSAL2_TRANSFORM_EMS_CSA_DRP_SWD_PYMT UPDTRANS
(WF_EMS_TL.XML): Update Strategy Expression VALUE="0", crashed at
write_SSA_EMS_CSA_DRP_SWD_PYMT1 with `Filter ('_update_flag = I)`.
"""

from informatica_sparker.handlers import TransformHandlers
from informatica_sparker.ir import ApplyUpdateStrategyStep, WriteTargetStep
from informatica_sparker.models import UserConfig
from informatica_sparker.parser import InfaXMLParser

# One Update Strategy + Target pair per strategy expression form: numeric
# 0/1/2, and an identifier field reference (existing behavior guard).
MINIMAL_XML = b"""\
<POWERMART VERSION="1.0">
<REPOSITORY NAME="TEST_REPO" VERSION="1.0" DATABASETYPE="Oracle"/>
<FOLDER NAME="TEST_FOLDER">
  <MAPPING DESCRIPTION="" ISVALID="YES" NAME="M_TEST_NUM_UPD_STRAT">
    <TRANSFORMATION DESCRIPTION="" NAME="UPDTRANS_0" OBJECTVERSION="1" REUSABLE="NO" TYPE="Update Strategy" VERSIONNUMBER="1">
      <TRANSFORMFIELD DATATYPE="decimal" DEFAULTVALUE="" DESCRIPTION="" NAME="KEY" PICTURETEXT="" PORTTYPE="INPUT/OUTPUT" PRECISION="10" SCALE="0"/>
      <TRANSFORMFIELD DATATYPE="string" DEFAULTVALUE="" DESCRIPTION="" NAME="VAL" PICTURETEXT="" PORTTYPE="INPUT/OUTPUT" PRECISION="20" SCALE="0"/>
      <TABLEATTRIBUTE NAME="Update Strategy Expression" VALUE="0"/>
      <TABLEATTRIBUTE NAME="Tracing Level" VALUE="Normal"/>
    </TRANSFORMATION>
    <TRANSFORMATION DESCRIPTION="" NAME="UPDTRANS_1" OBJECTVERSION="1" REUSABLE="NO" TYPE="Update Strategy" VERSIONNUMBER="1">
      <TRANSFORMFIELD DATATYPE="decimal" DEFAULTVALUE="" DESCRIPTION="" NAME="KEY" PICTURETEXT="" PORTTYPE="INPUT/OUTPUT" PRECISION="10" SCALE="0"/>
      <TRANSFORMFIELD DATATYPE="string" DEFAULTVALUE="" DESCRIPTION="" NAME="VAL" PICTURETEXT="" PORTTYPE="INPUT/OUTPUT" PRECISION="20" SCALE="0"/>
      <TABLEATTRIBUTE NAME="Update Strategy Expression" VALUE="1"/>
      <TABLEATTRIBUTE NAME="Tracing Level" VALUE="Normal"/>
    </TRANSFORMATION>
    <TRANSFORMATION DESCRIPTION="" NAME="UPDTRANS_2" OBJECTVERSION="1" REUSABLE="NO" TYPE="Update Strategy" VERSIONNUMBER="1">
      <TRANSFORMFIELD DATATYPE="decimal" DEFAULTVALUE="" DESCRIPTION="" NAME="KEY" PICTURETEXT="" PORTTYPE="INPUT/OUTPUT" PRECISION="10" SCALE="0"/>
      <TRANSFORMFIELD DATATYPE="string" DEFAULTVALUE="" DESCRIPTION="" NAME="VAL" PICTURETEXT="" PORTTYPE="INPUT/OUTPUT" PRECISION="20" SCALE="0"/>
      <TABLEATTRIBUTE NAME="Update Strategy Expression" VALUE="2"/>
      <TABLEATTRIBUTE NAME="Tracing Level" VALUE="Normal"/>
    </TRANSFORMATION>
    <TRANSFORMATION DESCRIPTION="" NAME="UPDTRANS_F" OBJECTVERSION="1" REUSABLE="NO" TYPE="Update Strategy" VERSIONNUMBER="1">
      <TRANSFORMFIELD DATATYPE="decimal" DEFAULTVALUE="" DESCRIPTION="" NAME="KEY" PICTURETEXT="" PORTTYPE="INPUT/OUTPUT" PRECISION="10" SCALE="0"/>
      <TRANSFORMFIELD DATATYPE="string" DEFAULTVALUE="" DESCRIPTION="" NAME="VAL" PICTURETEXT="" PORTTYPE="INPUT/OUTPUT" PRECISION="20" SCALE="0"/>
      <TRANSFORMFIELD DATATYPE="integer" DEFAULTVALUE="" DESCRIPTION="" NAME="UPD_FLAG" PICTURETEXT="" PORTTYPE="INPUT/OUTPUT" PRECISION="10" SCALE="0"/>
      <TABLEATTRIBUTE NAME="Update Strategy Expression" VALUE="UPD_FLAG"/>
      <TABLEATTRIBUTE NAME="Tracing Level" VALUE="Normal"/>
    </TRANSFORMATION>
    <TARGET BUSINESSNAME="" CONSTRAINT="" DATABASETYPE="Oracle" DESCRIPTION="" NAME="TGT_0_DEF" OBJECTVERSION="1" TABLEOPTIONS="" VERSIONNUMBER="1">
      <TARGETFIELD BUSINESSNAME="" DATATYPE="number(p,s)" DESCRIPTION="" FIELDNUMBER="1" KEYTYPE="PRIMARY KEY" NAME="KEY" NULLABLE="NULL" PICTURETEXT="" PRECISION="10" SCALE="0"/>
      <TARGETFIELD BUSINESSNAME="" DATATYPE="varchar2" DESCRIPTION="" FIELDNUMBER="2" KEYTYPE="NOT A KEY" NAME="VAL" NULLABLE="NULL" PICTURETEXT="" PRECISION="20" SCALE="0"/>
    </TARGET>
    <TARGET BUSINESSNAME="" CONSTRAINT="" DATABASETYPE="Oracle" DESCRIPTION="" NAME="TGT_1_DEF" OBJECTVERSION="1" TABLEOPTIONS="" VERSIONNUMBER="1">
      <TARGETFIELD BUSINESSNAME="" DATATYPE="number(p,s)" DESCRIPTION="" FIELDNUMBER="1" KEYTYPE="PRIMARY KEY" NAME="KEY" NULLABLE="NULL" PICTURETEXT="" PRECISION="10" SCALE="0"/>
      <TARGETFIELD BUSINESSNAME="" DATATYPE="varchar2" DESCRIPTION="" FIELDNUMBER="2" KEYTYPE="NOT A KEY" NAME="VAL" NULLABLE="NULL" PICTURETEXT="" PRECISION="20" SCALE="0"/>
    </TARGET>
    <TARGET BUSINESSNAME="" CONSTRAINT="" DATABASETYPE="Oracle" DESCRIPTION="" NAME="TGT_2_DEF" OBJECTVERSION="1" TABLEOPTIONS="" VERSIONNUMBER="1">
      <TARGETFIELD BUSINESSNAME="" DATATYPE="number(p,s)" DESCRIPTION="" FIELDNUMBER="1" KEYTYPE="PRIMARY KEY" NAME="KEY" NULLABLE="NULL" PICTURETEXT="" PRECISION="10" SCALE="0"/>
      <TARGETFIELD BUSINESSNAME="" DATATYPE="varchar2" DESCRIPTION="" FIELDNUMBER="2" KEYTYPE="NOT A KEY" NAME="VAL" NULLABLE="NULL" PICTURETEXT="" PRECISION="20" SCALE="0"/>
    </TARGET>
    <TARGET BUSINESSNAME="" CONSTRAINT="" DATABASETYPE="Oracle" DESCRIPTION="" NAME="TGT_F_DEF" OBJECTVERSION="1" TABLEOPTIONS="" VERSIONNUMBER="1">
      <TARGETFIELD BUSINESSNAME="" DATATYPE="number(p,s)" DESCRIPTION="" FIELDNUMBER="1" KEYTYPE="PRIMARY KEY" NAME="KEY" NULLABLE="NULL" PICTURETEXT="" PRECISION="10" SCALE="0"/>
      <TARGETFIELD BUSINESSNAME="" DATATYPE="varchar2" DESCRIPTION="" FIELDNUMBER="2" KEYTYPE="NOT A KEY" NAME="VAL" NULLABLE="NULL" PICTURETEXT="" PRECISION="20" SCALE="0"/>
    </TARGET>
    <INSTANCE DESCRIPTION="" NAME="UPDTRANS_0" REUSABLE="NO" TRANSFORMATION_NAME="UPDTRANS_0" TRANSFORMATION_TYPE="Update Strategy" TYPE="TRANSFORMATION"/>
    <INSTANCE DESCRIPTION="" NAME="UPDTRANS_1" REUSABLE="NO" TRANSFORMATION_NAME="UPDTRANS_1" TRANSFORMATION_TYPE="Update Strategy" TYPE="TRANSFORMATION"/>
    <INSTANCE DESCRIPTION="" NAME="UPDTRANS_2" REUSABLE="NO" TRANSFORMATION_NAME="UPDTRANS_2" TRANSFORMATION_TYPE="Update Strategy" TYPE="TRANSFORMATION"/>
    <INSTANCE DESCRIPTION="" NAME="UPDTRANS_F" REUSABLE="NO" TRANSFORMATION_NAME="UPDTRANS_F" TRANSFORMATION_TYPE="Update Strategy" TYPE="TRANSFORMATION"/>
    <INSTANCE DESCRIPTION="" NAME="TGT_0" REUSABLE="NO" TRANSFORMATION_NAME="TGT_0_DEF" TRANSFORMATION_TYPE="Target Definition" TYPE="TARGET"/>
    <INSTANCE DESCRIPTION="" NAME="TGT_1" REUSABLE="NO" TRANSFORMATION_NAME="TGT_1_DEF" TRANSFORMATION_TYPE="Target Definition" TYPE="TARGET"/>
    <INSTANCE DESCRIPTION="" NAME="TGT_2" REUSABLE="NO" TRANSFORMATION_NAME="TGT_2_DEF" TRANSFORMATION_TYPE="Target Definition" TYPE="TARGET"/>
    <INSTANCE DESCRIPTION="" NAME="TGT_F" REUSABLE="NO" TRANSFORMATION_NAME="TGT_F_DEF" TRANSFORMATION_TYPE="Target Definition" TYPE="TARGET"/>
    <CONNECTOR FROMFIELD="KEY" FROMINSTANCE="UPDTRANS_0" FROMINSTANCETYPE="Update Strategy" TOFIELD="KEY" TOINSTANCE="TGT_0" TOINSTANCETYPE="Target Definition"/>
    <CONNECTOR FROMFIELD="VAL" FROMINSTANCE="UPDTRANS_0" FROMINSTANCETYPE="Update Strategy" TOFIELD="VAL" TOINSTANCE="TGT_0" TOINSTANCETYPE="Target Definition"/>
    <CONNECTOR FROMFIELD="KEY" FROMINSTANCE="UPDTRANS_1" FROMINSTANCETYPE="Update Strategy" TOFIELD="KEY" TOINSTANCE="TGT_1" TOINSTANCETYPE="Target Definition"/>
    <CONNECTOR FROMFIELD="VAL" FROMINSTANCE="UPDTRANS_1" FROMINSTANCETYPE="Update Strategy" TOFIELD="VAL" TOINSTANCE="TGT_1" TOINSTANCETYPE="Target Definition"/>
    <CONNECTOR FROMFIELD="KEY" FROMINSTANCE="UPDTRANS_2" FROMINSTANCETYPE="Update Strategy" TOFIELD="KEY" TOINSTANCE="TGT_2" TOINSTANCETYPE="Target Definition"/>
    <CONNECTOR FROMFIELD="VAL" FROMINSTANCE="UPDTRANS_2" FROMINSTANCETYPE="Update Strategy" TOFIELD="VAL" TOINSTANCE="TGT_2" TOINSTANCETYPE="Target Definition"/>
    <CONNECTOR FROMFIELD="KEY" FROMINSTANCE="UPDTRANS_F" FROMINSTANCETYPE="Update Strategy" TOFIELD="KEY" TOINSTANCE="TGT_F" TOINSTANCETYPE="Target Definition"/>
    <CONNECTOR FROMFIELD="VAL" FROMINSTANCE="UPDTRANS_F" FROMINSTANCETYPE="Update Strategy" TOFIELD="VAL" TOINSTANCE="TGT_F" TOINSTANCETYPE="Target Definition"/>
  </MAPPING>
</FOLDER>
</POWERMART>
"""


def _steps():
    parser = InfaXMLParser(MINIMAL_XML)
    assert parser.parse()
    mapping = parser.get_mappings()[0]
    handlers = TransformHandlers(mapping, UserConfig())
    plan = handlers.build_ir_plan()
    upd = {
        s.step_name[len("apply_"):]: s
        for s in plan.steps
        if isinstance(s, ApplyUpdateStrategyStep)
    }
    wrt = {
        s.step_name[len("write_"):]: s
        for s in plan.steps
        if isinstance(s, WriteTargetStep)
    }
    assert upd, "expected ApplyUpdateStrategyStep steps"
    assert wrt, "expected WriteTargetStep steps"
    return upd, wrt


def test_numeric_zero_strategy_is_static_insert():
    """VALUE="0" (Informatica constant for DD_INSERT) must be a static
    DD_INSERT on BOTH sides: the strategy step passes through (no
    strategy_field, no _update_flag) and the write step appends plainly
    (no has_update_flag) — the _update_flag split never renders."""
    upd, wrt = _steps()
    # Strategy step: static DD_INSERT, normalized expression, empty cfg
    us = upd["UPDTRANS_0"]
    assert us.params.get("static_dd") == "DD_INSERT", us.params
    assert us.params.get("strategy_expression") == "DD_INSERT", us.params
    assert "strategy_field" not in us.params, us.params
    assert not us.params.get("update_strategy_cfg", {}).get("strategy_field")
    # Write step: plain append — no I/U/D split, no static DD_*
    wt = wrt["TGT_0"].params["write_target_cfg"]
    assert wt["has_update_flag"] is False, wt
    assert wt["static_dd"] is None, wt
    assert wt["is_delete"] is False, wt


def test_numeric_one_strategy_is_static_update():
    """VALUE="1" (DD_UPDATE) must batch-update all rows by target primary
    key — static DD_UPDATE on both sides, NOT a dynamic field split."""
    upd, wrt = _steps()
    us = upd["UPDTRANS_1"]
    assert us.params.get("static_dd") == "DD_UPDATE", us.params
    assert "strategy_field" not in us.params, us.params
    wt = wrt["TGT_1"].params["write_target_cfg"]
    assert wt["has_update_flag"] is True, wt
    assert wt["static_dd"] == "DD_UPDATE", wt
    assert "KEY" in wt["delete_keys"], wt


def test_numeric_two_strategy_is_static_delete():
    """VALUE="2" (DD_DELETE) must delete all rows by target primary key."""
    upd, wrt = _steps()
    us = upd["UPDTRANS_2"]
    assert us.params.get("static_dd") == "DD_DELETE", us.params
    assert "strategy_field" not in us.params, us.params
    wt = wrt["TGT_2"].params["write_target_cfg"]
    assert wt["is_delete"] is True, wt
    assert wt["static_dd"] == "DD_DELETE", wt
    assert "KEY" in wt["delete_keys"], wt


def test_identifier_field_strategy_still_dynamic():
    """A real identifier field strategy (UPD_FLAG) keeps the dynamic
    _update_flag split on both sides — the numeric normalization must not
    affect the existing dynamic path."""
    upd, wrt = _steps()
    us = upd["UPDTRANS_F"]
    assert us.params.get("strategy_field") == "UPD_FLAG", us.params
    assert us.params.get("update_strategy_cfg", {}).get("strategy_field") == "UPD_FLAG"
    assert "static_dd" not in us.params, us.params
    wt = wrt["TGT_F"].params["write_target_cfg"]
    assert wt["has_update_flag"] is True, wt
    assert wt["static_dd"] is None, wt

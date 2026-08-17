"""Tests for lib.update_strategy. Fixtures (runtime_lib, spark) come from conftest.py."""


def test_dynamic_strategy_flag(runtime_lib, spark):
    df = spark.createDataFrame([("DD_INSERT",), ("DD_UPDATE",), ("DD_DELETE",), ("X",)],
                               ["FLAG"])
    out = runtime_lib.update_strategy(
        spark=spark, input_df=df, name="UPD", strategy_field="FLAG")
    assert sorted(r["_update_flag"] for r in out.collect()) == ["D", "I", "I", "U"]


def test_static_pass_through(runtime_lib, spark):
    df = spark.createDataFrame([(1,)], ["V"])
    out = runtime_lib.update_strategy(spark=spark, input_df=df, name="UPD")
    assert out.columns == ["V"] and out.count() == 1


def test_connector_renames_applied_before_flag(runtime_lib, spark):
    """Connector renames (upstream column → Update Strategy port name, e.g.
    OUT_V_LAST_REC_TXN_DATE → OUT_LAST_REC_TXN_DATE) must land on the OUTPUT
    frame — downstream mapplet input remaps reference the renamed names
    (M_EMS_SSAL2_TRANS_RVN_CLCT_TRML regression)."""
    df = spark.createDataFrame(
        [("OUT_V_LAST_REC_TXN_DATE_val", "DD_INSERT")],
        ["OUT_V_LAST_REC_TXN_DATE", "OUT_V_UPD_STRATEGY_STATUS"],
    )
    out = runtime_lib.update_strategy(
        input_df=df,
        strategy_field="OUT_V_UPD_STRATEGY_STATUS",
        rename_columns=[
            ("OUT_V_LAST_REC_TXN_DATE", "OUT_LAST_REC_TXN_DATE"),
            ("OUT_V_UPD_STRATEGY_STATUS", "OUT_V_UPD_STRATEGY_STATUS"),
        ],
    )
    assert "OUT_LAST_REC_TXN_DATE" in out.columns
    assert "OUT_V_LAST_REC_TXN_DATE" not in out.columns
    assert "_update_flag" in out.columns
    row = out.collect()[0]
    assert row["OUT_LAST_REC_TXN_DATE"] == "OUT_V_LAST_REC_TXN_DATE_val"
    assert row["_update_flag"] == "I"

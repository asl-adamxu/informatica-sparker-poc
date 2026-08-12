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

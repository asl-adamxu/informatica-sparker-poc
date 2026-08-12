"""Tests for lib.union. Fixtures (runtime_lib, spark) come from conftest.py."""


def test_union_selects(runtime_lib, spark):
    a = spark.createDataFrame([(1, "x")], ["K", "A"])
    b = spark.createDataFrame([(2, "y")], ["K", "B"])
    out = runtime_lib.union(
        spark=spark, input_df=a, name="UN",
        union_selects=[
            {"df_input": a, "selects": [{"from": "K", "to": "K"}, {"from": "A", "to": "V"}]},
            {"df_input": b, "selects": [{"from": "K", "to": "K"}, {"from": "B", "to": "V"}]},
        ],
    )
    assert sorted((r["K"], r["V"]) for r in out.collect()) == [(1, "x"), (2, "y")]


def test_union_simple_inputs(runtime_lib, spark):
    a = spark.createDataFrame([(1,)], ["K"])
    b = spark.createDataFrame([(2,)], ["K"])
    out = runtime_lib.union(
        spark=spark, input_df=a, name="UN",
        inputs=[a, b],
    )
    assert sorted(r["K"] for r in out.collect()) == [1, 2]


def test_union_output_columns_fill(runtime_lib, spark):
    a = spark.createDataFrame([(1,)], ["K"])
    b = spark.createDataFrame([(1,)], ["K"])
    out = runtime_lib.union(
        spark=spark, input_df=a, name="UN",
        inputs=[a, b],
        output_columns=["K", "MISSING"],
    )
    assert out.columns == ["K", "MISSING"]
    assert out.collect()[0]["MISSING"] is None


def test_union_flag_column_raises(runtime_lib, spark):
    a = spark.createDataFrame([(1,)], ["K"])
    try:
        runtime_lib.union(
            spark=spark, input_df=a, name="UN",
            inputs=[a], flag_column="FLAG",
        )
        assert False, "expected ValueError"
    except ValueError:
        pass

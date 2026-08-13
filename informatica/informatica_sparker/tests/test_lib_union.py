"""Tests for lib.union. Fixtures (runtime_lib, spark) come from conftest.py."""


def test_union_selects(runtime_lib, spark):
    a = spark.createDataFrame([(1, "x")], ["K", "A"])
    b = spark.createDataFrame([(2, "y")], ["K", "B"])
    out = runtime_lib.union(
        spark=spark, input_df=a, name="UN",
        union_selects=[
            # selects are FROM names in output-column order; selects[j]
            # aliases positionally to output_columns[j] (A -> V, B -> V)
            {"df_input": a, "selects": ["K", "A"]},
            {"df_input": b, "selects": ["K", "B"]},
        ],
        output_columns=["K", "V"],
    )
    assert sorted((r["K"], r["V"]) for r in out.collect()) == [(1, "x"), (2, "y")]


def test_union_selects_missing_output_columns_defensive(runtime_lib, spark):
    """Without output_columns, the from names are kept as-is (defensive)."""
    a = spark.createDataFrame([(1, "x")], ["K", "A"])
    out = runtime_lib.union(
        spark=spark, input_df=a, name="UN",
        union_selects=[{"df_input": a, "selects": ["K", "A"]}],
    )
    assert sorted(r["K"] for r in out.collect()) == [1]
    assert out.columns == ["K", "A"]


def test_union_selects_shorter_than_output_columns_filled(runtime_lib, spark):
    """selects shorter than output_columns: absent ports are lit(None)-filled
    after the union (the final select drives the fill)."""
    a = spark.createDataFrame([(1, "x")], ["K", "A"])
    b = spark.createDataFrame([(2, "y")], ["K", "B"])
    out = runtime_lib.union(
        spark=spark, input_df=a, name="UN",
        union_selects=[
            {"df_input": a, "selects": ["K", "A"]},
            {"df_input": b, "selects": ["K"]},
        ],
        output_columns=["K", "V", "W"],
    )
    assert out.columns == ["K", "V", "W"]
    assert out.collect()[0]["W"] is None
    assert sorted(r["K"] for r in out.collect()) == [1, 2]


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

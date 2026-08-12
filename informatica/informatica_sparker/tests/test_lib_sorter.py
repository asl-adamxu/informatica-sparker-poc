"""Tests for lib.sorter. Fixtures (runtime_lib, spark) come from conftest.py."""


def test_sorter_renames_and_order(runtime_lib, spark):
    df = spark.createDataFrame([(2,), (1,)], ["A"])
    out = runtime_lib.sorter(
        spark=spark, input_df=df, name="SRT",
        rename_columns=[("A", "B")],
        sort_columns=[{"column": "B", "direction": "ASC"}],
    )
    assert out.columns == ["B"]
    assert [r["B"] for r in out.collect()] == [1, 2]


def test_sorter_desc(runtime_lib, spark):
    df = spark.createDataFrame([(1,), (2,)], ["A"])
    out = runtime_lib.sorter(
        spark=spark, input_df=df, name="SRT",
        sort_columns=[{"column": "A", "direction": "DESC"}],
    )
    assert [r["A"] for r in out.collect()] == [2, 1]


def test_sorter_no_sort_passthrough(runtime_lib, spark):
    df = spark.createDataFrame([(1,)], ["A"])
    out = runtime_lib.sorter(spark=spark, input_df=df, name="SRT")
    assert out.count() == 1

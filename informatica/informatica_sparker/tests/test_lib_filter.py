"""Tests for lib.filter. Fixtures (runtime_lib, spark) come from conftest.py."""


def test_filter_basic(runtime_lib, spark):
    df = spark.createDataFrame([(1,), (2,), (3,)], ["A"])
    out = runtime_lib.filter(spark=spark, input_df=df, name="FILTRANS",
                             condition="A > 1")
    assert sorted(r["A"] for r in out.collect()) == [2, 3]


def test_filter_rename_before_condition(runtime_lib, spark):
    df = spark.createDataFrame([(1,), (2,)], ["A"])
    out = runtime_lib.filter(spark=spark, input_df=df, name="FILTRANS",
                             rename_columns=[("A", "B")], condition="B > 1")
    assert out.columns == ["B"] and sorted(r["B"] for r in out.collect()) == [2]


def test_filter_substitution_or_zero(runtime_lib, spark):
    df = spark.createDataFrame([(1,), (2,)], ["A"])
    out = runtime_lib.filter(spark=spark, input_df=df, name="FILTRANS",
                             condition="A > $$v_min", substitutions={"$$v_min": ""})
    assert sorted(r["A"] for r in out.collect()) == [1, 2]  # "A > 0"


def test_filter_empty_condition_true(runtime_lib, spark):
    df = spark.createDataFrame([(1,), (2,)], ["A"])
    out = runtime_lib.filter(spark=spark, input_df=df, name="FILTRANS",
                             condition="TRUE")
    assert out.count() == 2


def test_filter_sequence_attach_after(runtime_lib, spark):
    # repartition(1): monotonically_increasing_id is per-partition, so the
    # conftest local[2] session would split the 2 rows across partitions and
    # NEXTVAL would not be contiguous ([100, 2**33 + 100] instead of [100, 101]).
    df = spark.createDataFrame([(1,), (2,)], ["A"]).repartition(1)
    out = runtime_lib.filter(spark=spark, input_df=df, name="FILTRANS",
                             condition="A >= 1",
                             sequence_attach=[{"col": "NEXTVAL", "start": 100}])
    assert out.columns == ["A", "NEXTVAL"]
    assert sorted(r["NEXTVAL"] for r in out.collect()) == [100, 101]

"""Tests for lib.sq_output. Fixtures (runtime_lib, spark) come from conftest.py."""


def test_rename_name_match_first(runtime_lib, spark):
    # SQL returns an unaliased expression column plus a matching one
    df = spark.createDataFrame([(1, "A|B")], ["KEY", "DEL_STS.A||DEL_STS.B"])
    out = runtime_lib.sq_output(
        spark=spark, input_df=df, name="SQ",
        port_cols=["KEY", "COMBINED"],
    )
    # name-match consumes KEY; positional fallback takes the expression column
    assert out.columns == ["KEY", "COMBINED"]
    assert out.collect()[0]["COMBINED"] == "A|B"


def test_missing_port_becomes_null(runtime_lib, spark):
    df = spark.createDataFrame([(1,)], ["A"])
    out = runtime_lib.sq_output(
        spark=spark, input_df=df, name="SQ",
        port_cols=["A", "MISSING"],
    )
    assert out.columns == ["A", "MISSING"]
    assert out.collect()[0]["MISSING"] is None


def test_type_casts(runtime_lib, spark):
    from pyspark.sql.types import LongType

    df = spark.createDataFrame([("1",)], ["NUM"])
    out = runtime_lib.sq_output(
        spark=spark, input_df=df, name="SQ",
        port_cols=["NUM"],
        column_types={"NUM": "INTEGER"},
    )
    assert str(out.schema["NUM"].dataType) == str(LongType())


def test_filter_condition_and_distinct(runtime_lib, spark):
    df = spark.createDataFrame([(1,), (1,), (2,)], ["V"])
    out = runtime_lib.sq_output(
        spark=spark, input_df=df, name="SQ",
        port_cols=["V"],
        filter_condition="V > $$v_min",
        substitutions={"$$v_min": "1"},
        distinct=True,
    )
    assert sorted(r["V"] for r in out.collect()) == [2]

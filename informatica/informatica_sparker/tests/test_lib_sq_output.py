"""Tests for lib.sq_output. Fixtures (runtime_lib, spark) come from conftest.py.

port_cols is an ordered dict {port_name: cast_type} — dict insertion order is
the port order (drives the two-pass rename AND the final select); the value is
the cast type ('' or None → no cast).
"""


def test_rename_name_match_first(runtime_lib, spark):
    # SQL returns an unaliased expression column plus a matching one
    df = spark.createDataFrame([(1, "A|B")], ["KEY", "DEL_STS.A||DEL_STS.B"])
    out = runtime_lib.sq_output(
        spark=spark, input_df=df, name="SQ",
        port_cols={"KEY": "", "COMBINED": ""},
    )
    # name-match consumes KEY; positional fallback takes the expression column
    assert out.columns == ["KEY", "COMBINED"]
    assert out.collect()[0]["COMBINED"] == "A|B"


def test_missing_port_becomes_null(runtime_lib, spark):
    df = spark.createDataFrame([(1,)], ["A"])
    out = runtime_lib.sq_output(
        spark=spark, input_df=df, name="SQ",
        port_cols={"A": "", "MISSING": ""},
    )
    assert out.columns == ["A", "MISSING"]
    assert out.collect()[0]["MISSING"] is None


def test_type_casts(runtime_lib, spark):
    from pyspark.sql.types import LongType, StringType

    df = spark.createDataFrame([("1", "2")], ["NUM", "PLAIN"])
    out = runtime_lib.sq_output(
        spark=spark, input_df=df, name="SQ",
        port_cols={"NUM": "INTEGER", "PLAIN": "string"},
    )
    # port_cols value drives the cast; the select order follows dict order
    assert out.columns == ["NUM", "PLAIN"]
    assert str(out.schema["NUM"].dataType) == str(LongType())
    assert str(out.schema["PLAIN"].dataType) == str(StringType())


def test_none_or_empty_type_means_no_cast(runtime_lib, spark):
    from pyspark.sql.types import StringType

    df = spark.createDataFrame([("1",)], ["NUM"])
    out = runtime_lib.sq_output(
        spark=spark, input_df=df, name="SQ",
        port_cols={"NUM": None},
    )
    assert str(out.schema["NUM"].dataType) == str(StringType())
    out2 = runtime_lib.sq_output(
        spark=spark, input_df=df, name="SQ",
        port_cols={"NUM": ""},
    )
    assert str(out2.schema["NUM"].dataType) == str(StringType())


def test_filter_condition_and_distinct(runtime_lib, spark):
    df = spark.createDataFrame([(1,), (1,), (2,)], ["V"])
    out = runtime_lib.sq_output(
        spark=spark, input_df=df, name="SQ",
        port_cols={"V": ""},
        filter_condition="V > $$v_min",
        substitutions={"$$v_min": "1"},
        distinct=True,
    )
    assert sorted(r["V"] for r in out.collect()) == [2]

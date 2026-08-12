"""Tests for the component-method shared helpers in runtime_lib."""


def test_with_column_expr_path(runtime_lib, spark):
    df = spark.createDataFrame([(1,), (2,)], ["A"])
    out = runtime_lib._with_column(df, "B", "A + 1")
    assert out.columns == ["A", "B"]
    assert sorted(r["B"] for r in out.collect()) == [2, 3]


def test_with_column_api_path(runtime_lib, spark):
    df = spark.createDataFrame([(1,), (2,)], ["A"])
    out = runtime_lib._with_column(df, "B", "monotonically_increasing_id() + 1")
    vals = sorted(r["B"] for r in out.collect())
    assert vals[0] >= 1 and len(vals) == 2


def test_with_column_api_last_when(runtime_lib, spark):
    df = spark.createDataFrame([("x", 1), ("y", 2)], ["K", "V"])
    expr_str = 'last(when(col("K") == "y", col("V")), True).over(Window.orderBy(lit(1)))'
    out = runtime_lib._with_column(df, "C", expr_str)
    assert out.collect()[1]["C"] == 2


def test_rename_columns_basic(runtime_lib, spark):
    df = spark.createDataFrame([(1, 2)], ["A", "B"])
    out = runtime_lib._rename_columns(df, [("A", "C")])
    assert out.columns == ["C", "B"]


def test_rename_columns_drop_target_protection(runtime_lib, spark):
    df = spark.createDataFrame([(1, 2)], ["A", "B"])
    out = runtime_lib._rename_columns(df, [("A", "B")])
    assert out.columns == ["B"]  # no duplicate B after drop-first


def test_rename_columns_skips_same_case(runtime_lib, spark):
    df = spark.createDataFrame([(1, 2)], ["A", "B"])
    out = runtime_lib._rename_columns(df, [("a", "A")])
    assert out.columns == ["A", "B"]


def test_fill_missing(runtime_lib, spark):
    df = spark.createDataFrame([(1,)], ["A"])
    out = runtime_lib._fill_missing(df, ["A", "B"])
    assert out.columns == ["A", "B"]
    assert out.collect()[0]["B"] is None


def test_fill_missing_case_insensitive_no_dup(runtime_lib, spark):
    df = spark.createDataFrame([(1,)], ["A"])
    out = runtime_lib._fill_missing(df, ["a"])
    assert out.columns == ["A"]


def test_substitute_or_zero(runtime_lib):
    text = "COL > $$v_rpt_mth"
    out = runtime_lib._substitute(text, {"$$v_rpt_mth": ""}, or_zero=True)
    assert out == "COL > 0"


def test_substitute_plain(runtime_lib):
    text = "rpad($$v_x, 10, ' ')"
    out = runtime_lib._substitute(text, {"$$v_x": "AB"}, or_zero=False)
    assert out == "rpad(AB, 10, ' ')"

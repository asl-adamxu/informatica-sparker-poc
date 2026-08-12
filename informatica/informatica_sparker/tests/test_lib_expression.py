"""Tests for lib.expression — the flagship component method.

Fixtures (runtime_lib, spark) come from tests/conftest.py.
"""


def test_expression_basic(runtime_lib, spark):
    df = spark.createDataFrame([(1, 10), (2, 20)], ["K", "V"])
    out = runtime_lib.expression(
        spark=spark, input_df=df, name="EXPTRANS",
        computed_columns=[{"name": "V2", "expr": "V * 2"}],
        pass_through_cols=["K", "V", "MISSING"],
    )
    assert out.columns == ["K", "V", "V2", "MISSING"]
    rows = {r["K"]: r for r in out.collect()}
    assert rows[1]["V2"] == 20 and rows[1]["MISSING"] is None


def test_expression_renames_before_computed(runtime_lib, spark):
    df = spark.createDataFrame([(1,)], ["A"])
    out = runtime_lib.expression(
        spark=spark, input_df=df, name="EXPR",
        rename_columns=[("A", "B")],
        computed_columns=[{"name": "C", "expr": "B + 1"}],
    )
    assert out.columns == ["B", "C"]
    assert out.collect()[0]["C"] == 2


def test_expression_api_computed(runtime_lib, spark):
    df = spark.createDataFrame([(1,), (2,)], ["A"])
    out = runtime_lib.expression(
        spark=spark, input_df=df, name="EXPR",
        computed_columns=[{"name": "SEQ", "expr": "monotonically_increasing_id() + 1"}],
    )
    assert sorted(r["SEQ"] for r in out.collect()) == [1, 2]


def test_expression_substitution(runtime_lib, spark):
    df = spark.createDataFrame([(1,)], ["A"])
    out = runtime_lib.expression(
        spark=spark, input_df=df, name="EXPR",
        computed_columns=[{"name": "B", "expr": "A + $$v_off"}],
        substitutions={"$$v_off": "5"},
    )
    assert out.collect()[0]["B"] == 6


def test_expression_inline_lookup_join(runtime_lib, spark):
    main = spark.createDataFrame([(1,), (2,)], ["IN_KEY"])
    lkp = spark.createDataFrame([(1, "X"), (2, "Y")], ["KEY", "VAL"])
    out = runtime_lib.expression(
        spark=spark, input_df=main, name="EXPR",
        inline_lookup_joins=[{
            "lookup_df": lkp,
            "join_predicates": [{"source_col": "IN_KEY", "lookup_col": "KEY"}],
            "return_port": "VAL",
        }],
    )
    assert out.columns == ["IN_KEY", "VAL"]
    assert {r["IN_KEY"]: r["VAL"] for r in out.collect()} == {1: "X", 2: "Y"}


def test_expression_sp_calls(runtime_lib, spark, monkeypatch):
    df = spark.createDataFrame([(1,), (2,)], ["COL"])
    calls = []
    monkeypatch.setattr(runtime_lib, "call_stored_procedure",
                        lambda s, c, sp, args: calls.append((sp, list(args))))
    out = runtime_lib.expression(
        spark=spark, input_df=df, name="EXPR",
        sp_calls=[{"col": "OUT", "sp_call": "PKG.SP_X", "sp_schema": "PDPA", "args": ["COL"]}],
        sp_conn={"schema": "PSOR"},
    )
    assert calls == [("PSOR.PKG.SP_X", [1]), ("PSOR.PKG.SP_X", [2])]
    assert out.collect()[0]["OUT"] == "SUCCESS"

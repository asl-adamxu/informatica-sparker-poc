"""Tests for lib.router. Fixtures (runtime_lib, spark) come from conftest.py."""


def test_group_split_and_renames(runtime_lib, spark):
    df = spark.createDataFrame([("A", 1), ("B", 2), ("A", 3)], ["GRP", "V"])
    out = runtime_lib.router(
        spark=spark, input_df=df, name="RTR",
        groups=[
            {"name": "G1", "df_output": "df_rtr_G1",
             "condition": "GRP = 'A'", "renames": [("GRP", "GRP1")]},
            {"name": "G2", "df_output": "df_rtr_G2",
             "condition": "GRP = 'B'"},
        ],
    )
    g1 = sorted(r["V"] for r in out["df_rtr_G1"].collect())
    g2 = sorted(r["V"] for r in out["df_rtr_G2"].collect())
    assert g1 == [1, 3] and g2 == [2]
    assert out["df_rtr_G1"].columns == ["GRP1", "V"]


def test_default_negated_group(runtime_lib, spark):
    df = spark.createDataFrame([("A", 1), ("B", 2)], ["GRP", "V"])
    out = runtime_lib.router(
        spark=spark, input_df=df, name="RTR",
        groups=[
            {"name": "G1", "df_output": "df_rtr_G1", "condition": "GRP = 'A'"},
            {"name": "G2", "df_output": "df_rtr_G2",
             "default_negated": ["G1"]},
        ],
    )
    assert [r["GRP"] for r in out["df_rtr_G2"].collect()] == ["B"]


def test_pass_through_group(runtime_lib, spark):
    df = spark.createDataFrame([(1,)], ["V"])
    out = runtime_lib.router(
        spark=spark, input_df=df, name="RTR",
        groups=[{"name": "G1", "df_output": "df_rtr_G1"}],
    )
    assert out["df_rtr_G1"].count() == 1


def test_substitution(runtime_lib, spark):
    df = spark.createDataFrame([(1,), (5,)], ["V"])
    out = runtime_lib.router(
        spark=spark, input_df=df, name="RTR",
        groups=[{"name": "G1", "df_output": "df_rtr_G1",
                 "condition": "V > $$v_min"}],
        substitutions={"$$v_min": "2"},
    )
    assert sorted(r["V"] for r in out["df_rtr_G1"].collect()) == [5]


def test_substitution_in_default_negated_chain(runtime_lib, spark):
    """A DEFAULT group negating $$-carrying conditions substitutes each named
    group's condition before the ~expr() chain filter."""
    df = spark.createDataFrame([("A", 1), ("B", 2)], ["GRP", "V"])
    out = runtime_lib.router(
        spark=spark, input_df=df, name="RTR",
        groups=[
            {"name": "G1", "df_output": "df_rtr_G1",
             "condition": "GRP = '$$v_keep'"},
            {"name": "G2", "df_output": "df_rtr_G2",
             "default_negated": ["G1"]},
        ],
        substitutions={"$$v_keep": "A"},
    )
    assert sorted(r["V"] for r in out["df_rtr_G2"].collect()) == [2]


def test_multi_feed_union_input(runtime_lib, spark):
    df1 = spark.createDataFrame([("A", 1)], ["GRP", "V"])
    df2 = spark.createDataFrame([("B", 2)], ["GRP", "V"])
    out = runtime_lib.router(
        spark=spark, input_df=df1, name="RTR",
        multi_feed=True,
        feeds=[(df1, {}), (df2, {})],
        groups=[
            {"name": "G1", "df_output": "df_rtr_G1", "condition": "GRP = 'A'"},
            {"name": "G2", "df_output": "df_rtr_G2", "condition": "GRP = 'B'"},
        ],
    )
    assert out["df_rtr_G1"].count() == 1 and out["df_rtr_G2"].count() == 1


def test_multi_feed_aliases_fill(runtime_lib, spark):
    df1 = spark.createDataFrame([("A", 1)], ["SRC", "V"])
    df2 = spark.createDataFrame([("B", 2)], ["SRC", "V"])
    out = runtime_lib.router(
        spark=spark, input_df=df1, name="RTR",
        multi_feed=True,
        feeds=[(df1, {"SRC": "PORT1"}), (df2, {"SRC": "PORT1"})],
        groups=[{"name": "G1", "df_output": "df_rtr_G1",
                 "condition": "PORT1 = 'A'"}],
    )
    assert out["df_rtr_G1"].collect()[0]["PORT1"] == "A"

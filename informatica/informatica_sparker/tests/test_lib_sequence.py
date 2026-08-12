"""Tests for lib.sequence. Fixtures (runtime_lib, spark) come from conftest.py."""


def test_sequence_attaches_nexval(runtime_lib, spark):
    df = spark.createDataFrame([(1,), (2,)], ["A"]).repartition(1)
    out = runtime_lib.sequence(
        spark=spark, input_df=df, name="SEQ",
        output_col="NEXTVAL", start=100,
    )
    assert sorted(r["NEXTVAL"] for r in out.collect()) == [100, 101]

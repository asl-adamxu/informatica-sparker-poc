import os
import sys
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder.master("local[2]").appName("pytest-m_utl_dpa_truncate").getOrCreate()
    yield spark
    spark.stop()


def test_run_mapping_truncate(spark, monkeypatch):
    mod_dir = os.path.dirname(os.path.dirname(__file__))
    if mod_dir not in sys.path:
        sys.path.insert(0, mod_dir)

    import m_utl_dpa_truncate as mod

    # fake read_file to load local CSV
    def fake_read_file(spark_arg, path, format="csv", options=None):
        data_path = os.path.join(os.path.dirname(__file__), "data", "utl_ssa_tbl_list.csv")
        return spark.read.option("header", "true").csv(data_path)

    monkeypatch.setattr(mod, "read_file", fake_read_file, raising=False)
    monkeypatch.setattr(mod, "normalize_column_names", lambda df: df, raising=False)

    # Make expr that contains SP_TRUNCATE return a literal so expression won't fail
    orig_expr = getattr(mod, "expr")

    from pyspark.sql.functions import lit

    def fake_expr(s):
        if "SP_TRUNCATE" in s:
            return lit("SP_CALLED")
        return orig_expr(s)

    monkeypatch.setattr(mod, "expr", fake_expr, raising=False)

    # Allow DataFrame.select() with zero args to be a no-op (some generated code calls select([]))
    from pyspark.sql.dataframe import DataFrame
    orig_select = DataFrame.select

    def fake_select(self, *cols):
        if len(cols) == 0:
            return self
        return orig_select(self, *cols)

    monkeypatch.setattr(DataFrame, "select", fake_select, raising=False)

    # Capture saved table rows
    captured = []
    from pyspark.sql.dataframe import DataFrameWriter

    def fake_saveAsTable(self, name):
        for r in self._df.collect():
            captured.append(r.asDict())

    monkeypatch.setattr(DataFrameWriter, "saveAsTable", fake_saveAsTable)

    class Ctx:
        def __init__(self, spark):
            self.spark = spark

        def register_df(self, name, df):
            return None

    ctx = Ctx(spark)

    res = mod.run_mapping(ctx, metrics=None)
    assert res is True

    # Validate captured contains our table rows and the fake OUTPUT value
    assert len(captured) >= 1
    assert all("OUTPUT" in r for r in captured)
    assert set(r["OUTPUT"] for r in captured) == {"SP_CALLED"}

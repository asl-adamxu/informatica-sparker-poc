import os
import sys
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder.master("local[2]").appName("pytest-m_dpa_sum_fact_gms_dly_dog_rgstr").getOrCreate()
    yield spark
    spark.stop()


def test_run_mapping_basic(spark, monkeypatch):
    mod_dir = os.path.dirname(os.path.dirname(__file__))
    if mod_dir not in sys.path:
        sys.path.insert(0, mod_dir)

    import m_dpa_sum_fact_gms_dly_dog_rgstr as mod

    def fake_read_sql(spark_arg, conn, table=None, query=None):
        if query:
            return spark.createDataFrame([{"TIME_DMNS_KEY": 1}])
        if table:
            return spark.createDataFrame([{"TIME_DMNS_KEY": 1, "EST_SCD_KEY": 0}])
        return spark.createDataFrame([{}])

    monkeypatch.setattr(mod, "read_sql", fake_read_sql, raising=False)
    monkeypatch.setattr(mod, "normalize_column_names", lambda df: df, raising=False)

    captured = []
    from pyspark.sql.dataframe import DataFrameWriter

    def fake_saveAsTable(self, name):
        captured.append(name)

    monkeypatch.setattr(DataFrameWriter, "saveAsTable", fake_saveAsTable)

    class Ctx:
        def __init__(self, spark):
            self.spark = spark

        def register_df(self, name, df):
            return None

    ctx = Ctx(spark)

    res = mod.run_mapping(ctx, metrics=None)
    assert res is True
    assert "DPA_FACT_GMS_DLY_DOG_RGSTR" in captured

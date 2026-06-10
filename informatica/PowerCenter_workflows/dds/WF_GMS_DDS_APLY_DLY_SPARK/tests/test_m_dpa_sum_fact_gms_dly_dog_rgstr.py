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
        # return full set of columns expected by the mapping
        if query:
            row = {"TIME_DMNS_KEY": 1, "EST_SCD_KEY": 0, "DOG_RGSTR_APRV_CNT": 0, "DOG_RGSTR_APRV_CNCL_CNT": 0, "AUTH_DOG_PNT_ALLT_CASE_CNT": 0, "UNAUTH_DOG_PNT_ALLT_CASE_CNT": 0, "REC_RLS_IND": "Y", "LAST_REC_TXN_DATE": "2020-01-01", "LAST_REC_TXN_TYPE_CODE": "X"}
            return spark.createDataFrame([row])
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

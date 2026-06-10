import os
import sys
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder.master("local[2]").appName("pytest-m_dds_apl_fact_gms_dly_msd_smry").getOrCreate()
    yield spark
    spark.stop()


def test_run_mapping_basic(spark, monkeypatch):
    mod_dir = os.path.dirname(os.path.dirname(__file__))
    if mod_dir not in sys.path:
        sys.path.insert(0, mod_dir)

    import m_dds_apl_fact_gms_dly_msd_smry as mod

    # For this mapping we need a pushed-down query and a DPA table
    def fake_read_sql(spark_arg, conn, table=None, query=None):
        if table and table.upper().startswith("DDS"):
            return spark.createDataFrame([{"TIME_DMNS_KEY": 1}])
        if table and table.upper().startswith("DPA"):
            row = {"TIME_DMNS_KEY": 1, "EST_SCD_KEY": 0, "OFCR_TYPE_DMNS_KEY": 0, "HSHLD_SIZE_DMNS_KEY": 0, "MSD_CODE_SCD_KEY": 0, "OFNDR_GNDR_DMNS_KEY": 0, "OFNDR_AGE_GRP_DMNS_KEY": 0, "OFNC_SCORE_GRP_DMNS_KEY": 0, "ACTV_OFNC_TNCY_CNT": 0, "CMLT_OFNC_TNCY_CNT": 0, "AFT_CMLT_WRT_WARN_TNCY_CNT": 0, "CMLT_WRT_WARN_CASE_CNT": 0, "AFT_CMLT_WRT_WARN_CASE_CNT": 0, "ACTV_PNT_ALLT_CASE_CNT": 0, "CMLT_PNT_ALLT_CASE_CNT": 0, "CMLT_MSD_TOT_CASE_CNT": 0, "REC_RLS_IND": "Y", "LAST_REC_TXN_DATE": "2020-01-01", "LAST_REC_TXN_TYPE_CODE": "X"}
            return spark.createDataFrame([row])
        if query:
            return spark.createDataFrame([{"TIME_DMNS_KEY": 1}])
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
    assert "DDS_FACT_GMS_DLY_MSD_SMRY" in captured

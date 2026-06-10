import os
import sys
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder.master("local[2]").appName("pytest-m_dds_apl_fact_gms_dly_msd_incdt").getOrCreate()
    yield spark
    spark.stop()


def test_run_mapping_basic(spark, monkeypatch):
    mod_dir = os.path.dirname(os.path.dirname(__file__))
    if mod_dir not in sys.path:
        sys.path.insert(0, mod_dir)

    import m_dds_apl_fact_gms_dly_msd_incdt as mod

    cols = [
        "MSD_INCDT_DATE_DMNS_KEY", "MSD_CRE_DATE_DMNS_KEY", "EST_SCD_KEY", "OFCR_TYPE_DMNS_KEY",
        "HSHLD_SIZE_DMNS_KEY", "MSD_CODE_SCD_KEY", "OFNDR_GNDR_DMNS_KEY", "OFNDR_AGE_GRP_DMNS_KEY",
        "OFNC_SCORE_GRP_DMNS_KEY", "AFT_CMLT_WRT_WARN_CASE_CNT", "CMLT_PNT_ALLT_CASE_CNT", "REC_RLS_IND",
        "LAST_REC_TXN_DATE", "LAST_REC_TXN_TYPE_CODE"
    ]

    def fake_read_sql(spark_arg, conn, table=None, query=None):
        row = {c: 1 for c in cols}
        row["LAST_REC_TXN_DATE"] = "2020-01-01"
        return spark.createDataFrame([row])

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
    assert "DDS_FACT_GMS_DLY_MSD_INCDT" in captured

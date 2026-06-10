import os
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder.master("local[2]").appName("pytest-m_utl_param_setup").getOrCreate()
    yield spark
    spark.stop()


def test_run_mapping_basic(spark, monkeypatch):
    # import the module from the same directory
    import sys
    mod_dir = os.path.dirname(os.path.dirname(__file__))
    if mod_dir not in sys.path:
        sys.path.insert(0, mod_dir)

    import m_utl_param_setup as mod

    # Provide input file from tests/data for easier inspection
    def fake_read_file(spark_arg, path, format="csv", options=None):
        data_path = os.path.join(os.path.dirname(__file__), "data", "utl_session_list.csv")
        return spark.read.option("header", "true").csv(f"file://{data_path}")

    # Provide fake lookup table rows: match PRPTY = 'V1' -> VAL='VAL1'
    def fake_read_sql(spark_arg, conn, query=None):
        """
        If the environment variable `USE_TEST_DB` is set to '1', attempt to read
        the lookup from the Oracle test database using cx_Oracle and return a
        Spark DataFrame. Otherwise fall back to the CSV file under tests/data.
        """
        use_db = os.environ.get("USE_TEST_DB", "0") == "1"
        if use_db:
            try:
                import cx_Oracle
            except Exception:
                use_db = False

        if use_db:
            # Read DB connection info from environment variables (set these when running tests)
            user = os.environ.get("ORACLE_USER")
            password = os.environ.get("ORACLE_PASSWORD")
            host = os.environ.get("ORACLE_HOST")
            port = os.environ.get("ORACLE_PORT", "1521")
            service = os.environ.get("ORACLE_SERVICE")
            if not all([user, password, host, service]):
                raise RuntimeError("ORACLE_USER/ORACLE_PASSWORD/ORACLE_HOST/ORACLE_SERVICE must be set to use DB-backed tests")
            dsn = cx_Oracle.makedsn(host, port, service_name=service)
            conn = cx_Oracle.connect(user=user, password=password, dsn=dsn)
            cur = conn.cursor()
            cur.execute("SELECT VAL, PRPTY_DESP, PRPTY FROM SOR_SYS_PRPTY")
            rows = cur.fetchall()
            cur.close()
            conn.close()
            # Convert to Spark DataFrame; ensure column names that mapping expects
            return spark.createDataFrame(rows, ["VAL", "PRPTY_DESP", "PROPERTY"])
        else:
            data_path = os.path.join(os.path.dirname(__file__), "data", "sor_sys_prpty.csv")
            return spark.read.option("header", "true").csv(f"file://{data_path}")

    monkeypatch.setattr(mod, "read_file", fake_read_file, raising=False)
    monkeypatch.setattr(mod, "read_sql", fake_read_sql, raising=False)
    # keep normalize_column_names identity for test
    monkeypatch.setattr(mod, "normalize_column_names", lambda df: df, raising=False)

    # Capture saved table rows
    captured = []
    from pyspark.sql.dataframe import DataFrameWriter

    orig_save = DataFrameWriter.saveAsTable

    def fake_saveAsTable(self, name):
        # collect the LINE column values
        vals = [r.LINE for r in self._df.select("LINE").collect()]
        captured.extend(vals)

    monkeypatch.setattr(DataFrameWriter, "saveAsTable", fake_saveAsTable)

    class Ctx:
        def __init__(self, spark):
            self.spark = spark

        def register_df(self, name, df):
            # no-op for tests
            return None

    ctx = Ctx(spark)

    # Run mapping
    res = mod.run_mapping(ctx, metrics=None)
    assert res is True

    # Validate results: expected lines are 'P1=VAL1' and 'ONLYP'
    assert set(captured) == {"P1=VAL1", "ONLYP"}

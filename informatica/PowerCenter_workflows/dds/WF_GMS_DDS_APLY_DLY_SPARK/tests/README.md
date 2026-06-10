Schema DDL for integration tests

Files in this directory:

- `psor_sor_sys_prpty.sql` — creates `PSOR.SOR_SYS_PRPTY` and inserts sample rows.
- `utl_session_list.sql` — creates `UTL_SESSION_LIST` and inserts sample rows.

How to apply

1. Using `sqlplus` (example):

   ```bash
   sqlplus airflow@//192.168.55.107:1521/CDCUATPDB
   -- enter password when prompted
   @psor_sor_sys_prpty.sql
   @utl_session_list.sql
   ```

2. From Python (cx_Oracle or SQLAlchemy) or Airflow's `OracleHook` — execute the SQL files in order.

Notes
- The scripts expect the `PSOR` schema to exist and the connecting user to have privileges to create tables in that schema. Adjust schema names or run as a DBA if needed.
- Do NOT commit production credentials here; use secure mechanisms (Airflow connections, environment variables) when running tests.

Running tests against the real Oracle test DB

- To run the pytest that reads the DB-backed `PSOR.SOR_SYS_PRPTY`, set the environment variable `USE_TEST_DB=1` and provide DB connection environment variables before running pytest. Example:

```bash
export USE_TEST_DB=1
export ORACLE_USER=airflow
export ORACLE_PASSWORD='airflow'
export ORACLE_HOST=192.168.55.107
export ORACLE_PORT=1521
export ORACLE_SERVICE=CDCUATPDB
export SPARK_HOME=/opt/cloudera/parcels/SPARK3/lib/spark3
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip
export PYSPARK_PYTHON=/usr/bin/python3.6
export PYSPARK_DRIVER_PYTHON=/usr/bin/python3.6
/usr/bin/python3.6 -m pytest -q /var/lib/airflow/dags/adam/informatica/PowerCenter_workflows/dds/WF_GMS_DDS_APLY_DLY_SPARK/tests/test_m_utl_param_setup.py
```

- If the env var `USE_TEST_DB` is not set (or set to `0`), the test will use the CSV file at `tests/data/sor_sys_prpty.csv` as a fallback. The `UTL_SESSION_LIST` is always read from `tests/data/utl_session_list.csv` (since in the original workflow it is a flat file source).


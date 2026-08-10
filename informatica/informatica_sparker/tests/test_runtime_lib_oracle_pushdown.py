"""Regression guard for ORA-00920 from Spark JDBC predicate pushdown.

Spark pushes filters containing non-table columns (e.g. NewLookupRow /
_update_flag) to Oracle as TRUE/FALSE boolean literals, which Oracle rejects
with "ORA-00920: invalid relational operator". The runtime library disables
predicate pushdown for Oracle JDBC reads so those filters stay in Spark.
"""

from pathlib import Path


TEMPLATE = (
    Path(__file__).resolve().parents[2]
    / "informatica_sparker"
    / "informatica_sparker"
    / "templates"
    / "runtime_lib.py.j2"
)


def test_oracle_read_disables_jdbc_predicate_pushdown():
    text = TEMPLATE.read_text()
    assert 'option("pushDownPredicate", "false")' in text
    assert 'conn_config.get("type", "oracle")' in text
    # The option must be applied inside read_sql (before query/dbtable load).
    read_sql_idx = text.index("def read_sql(")
    option_idx = text.index('option("pushDownPredicate", "false")')
    load_idx = text.index("return reader.load()", read_sql_idx)
    assert read_sql_idx < option_idx < load_idx

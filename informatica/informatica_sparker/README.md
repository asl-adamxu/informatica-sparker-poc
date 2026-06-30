# informatica-sparker

**Version 2026.06.30** — A Python framework that converts Informatica PowerCenter workflow/mapping XML exports into PySpark code deployable to Databricks or YARN Spark clusters. **Tested against Informatica output — data results match.**

Conversion pipeline: **XML → Models → IR Plan → Jinja2 Templates → Generated Python Files**

## Features

- **Multi-Mapping & Multi-Workflow Support**: Handles any number of mappings and workflows per XML file, generating separate `m_<name>.py` files for each mapping and `wf_<name>.py` for each workflow
- **Auto Source Detection**: Automatically identifies source types and connection details:
  - SQL databases (SQL Server, Oracle, MySQL, PostgreSQL, DB2, Teradata, Netezza, Sybase, Informix)
  - File formats: CSV, Parquet, DAT, XML, JSON, Text, Fixed-Width, Avro, ORC, Excel
  - Files without extensions
  - JDBC/ODBC connections with driver JAR detection
  - File location detection: Local, S3, ADLS, GCS, DBFS
- **Expression Translation**: Comprehensive Informatica-to-PySpark function mapping (60+ functions):
  - String: `SUBSTR`, `INSTR`, `CONCAT`, `REPLACE`, `REG_REPLACE`, `LPAD`, `RPAD`, `TRIM`, `UPPER`, `LOWER`, `INITCAP`, `REVERSE`, `SOUNDEX`, `REPLACESTR`, `REPLACECHR`
  - Numeric: `ROUND`, `TRUNC`, `ABS`, `MOD`, `POWER`, `SQRT`, `FLOOR`, `CEIL`, `LOG`, `LN`, `EXP`, `SIGN`, `GREATEST`, `LEAST`
  - Date: `SYSDATE`, `TO_DATE`, `ADD_TO_DATE`, `DATE_DIFF`, `GET_DATE_PART`, `SET_DATE_PART`, `LAST_DAY`, `NEXT_DAY`, `MONTHS_BETWEEN`, `ADD_MONTHS`, `MAKE_DATE_TIME`
  - Conversion: `TO_CHAR`, `TO_DECIMAL`, `TO_INTEGER`, `TO_BIGINT`, `TO_FLOAT`, `IS_NUMBER`, `IS_DATE`
  - Conditional: `IIF`, `DECODE`, `NVL`, `NVL2`, `COALESCE`, `NULLIF`, `ISNULL`
  - Aggregate: `SUM`, `COUNT`, `AVG`, `MIN`, `MAX`, `FIRST`, `LAST`, `MEDIAN`, `STDDEV`, `VARIANCE`
  - Window: `CUME`, `MOVINGSUM`, `MOVINGAVG`
- **SQL Dialect Translation**: Module `utils/sql_dialect.py` provides Oracle-to-Spark SQL translation:
  - `DECODE()` → `CASE WHEN` conversion
  - Dialect detection (`auto`, `oracle`, `sqlserver`, `spark`)
  - Cross-dialect SQL translation
- **Complete Output Package**: Generates a full deployment-ready package:
  - `m_<mapping_name>.py` — PySpark script for each mapping
  - `wf_<workflow_name>.py` — Workflow orchestration (thin wrapper using runtime_lib)
  - `wf_<workflow_name>.md` — Markdown with Mermaid flowchart visualization
  - `env/config.yml` — Unified YAML configuration with Hadoop CredentialProvider password support
  - `env/runtime_lib.py` — Shared runtime library (Spark session, JDBC helpers, metrics, workflow runner)
  - `env/all_sql_queries.sql` — All extracted SQL queries organized by mapping
  - `env/conversion_log.txt` — Detailed conversion log with warnings, errors, and source detection results
- **Transformation Coverage**: Source Qualifier, Expression, Filter, Lookup, Joiner, Aggregator, Sorter, Union, Router, Sequence Generator, Update Strategy (DD_INSERT/UPDATE/DELETE), Stored Procedure (inline via `:SP.` pattern in Expression), Mapplet (with mini-DAG inlining)
- **Workflow DAG Orchestration**: Supports nested execution plans with:
  - `session` — single mapping execution
  - `parallel_group` — concurrent execution using `ThreadPoolExecutor`
  - `worklet` — nested sub-plan matching Informatica worklet topology
  - `task` — email notifications with Informatica-style placeholder substitution
  - Sequential execution is achieved via topological DAG levels — sessions at the same level run in parallel, different levels run sequentially
- **Schema Parameterization**: Hardcoded schema prefixes in SQL queries (e.g. `PSOR.`) are replaced with `{_schema}` resolved from the connection's `schema` field in config.yml at runtime
- **Dynamic Connection Resolution**: READ_SQL and lookup steps dynamically resolve connections by alias (`lib.get_db_config(config, "SOR")`) instead of hardcoding `conn_source`/`conn_target`
- **Lookup Connection Inference**: Lookup schema prefix is matched to a source definition's `owner_name` to determine the correct database connection for `$Source` lookups in flat-file mappings
- **Flat File Column Mapping**: CSV columns renamed by position to match Informatica source definition field names
- **Empty File Handling**: Empty source files produce a warning instead of crashing; empty DataFrame created with expected schema
- **Command Task Support**: Workflow Command tasks (shell commands) extracted from XML and logged during execution
- **Task Completion Tracking**: Tasks counted in workflow completion stats with start/completed logging
- **Password Credential Deferred Save**: Passwords only persisted to CredentialProvider after successful connection validation
- **Workflow Run Markers**: START/END separators in logs for multi-run visibility
- **Logging Isolation**: Mapping DEBUG logs stay in mapping log; workflow log only shows INFO start/completed/error
- **Type Casting**: Automatic column type casting to match target schema (Decimal→String with scientific notation prevention)
- **Spark Connection Profiles**: Supports `spark_local`, `spark3_client` (YARN client), and `spark3_on_yarn` (YARN cluster) modes
- **Python 3.10+ Compatible**

## Installation

```bash
pip install informatica-sparker
```

## Quick Start

### Command Line

```bash
# Convert XML to PySpark
informatica-sparker convert mapping_export.xml -o output_dir

# Analyze XML without converting
informatica-sparker analyze mapping_export.xml

# Analyze with JSON output
informatica-sparker analyze mapping_export.xml --json

# Use custom config
informatica-sparker convert mapping_export.xml -o output_dir -c my_config.yml

# Override source/target database types
informatica-sparker convert mapping_export.xml -o output_dir --source-db oracle --target-db spark
```

### Python API

```python
from informatica_sparker import ConversionService, UserConfig

# Basic conversion
service = ConversionService()
result = service.convert_file("mapping_export.xml", output_dir="output")

print(f"Mappings converted: {result.mappings_processed}/{result.mapping_count}")
print(f"Files generated: {len(result.files)}")
print(f"SQL queries found: {len(result.sql_queries)}")

# Check source detections
for detection in result.source_detections:
    print(f"  {detection.source_name}: {detection.detected_type.value}")
    if detection.file_format:
        print(f"    Format: {detection.file_format.value}")
    for note in detection.detection_notes:
        print(f"    {note}")

# Inspect extracted SQL queries
for query in result.sql_queries:
    print(f"  [{query.query_type}] {query.step_name}: {query.query[:80]}...")
```

### With Custom Configuration

```python
from informatica_sparker import ConversionService, UserConfig

config = UserConfig(
    db_connections={
        "source_db": {
            "host": "myserver.database.windows.net",
            "database": "mydb",
            "user": "admin",
            "password": "secret",
        }
    }
)

service = ConversionService(user_config=config)
result = service.convert_file("export.xml", output_dir="spark_output")
```

## Output Structure

```
output/
  m_mapping_1.py                # PySpark code for each mapping
  m_mapping_2.py                # (prefixed with m_ for auto-discovery)
  wf_workflow_name.py           # Workflow orchestration (thin wrapper)
  wf_workflow_name.md           # Mermaid flowchart documentation
  env/
    runtime_lib.py              # Shared runtime library (SparkContext, JDBC helpers,
                                #   MappingMetrics, run_workflow runner)
    config.yml                  # Unified YAML config (connections, spark profiles,
                                #   credential provider, email, logging)
    all_sql_queries.sql         # All SQL queries extracted from all mappings
    conversion_log.txt          # Conversion log with warnings, errors, detections
```

## Source Type Detection

The framework automatically identifies what each source in the XML is:

| Source Type | Detection Method |
|------------|-----------------|
| SQL Server | `DATABASETYPE` attribute, connection properties |
| Oracle | `DATABASETYPE` attribute, JDBC driver class |
| Flat File (CSV) | File extension, `DATABASETYPE=Flat File`, delimiter attributes |
| Parquet | `.parquet` file extension in source attributes |
| DAT | `.dat` file extension |
| XML | `.xml` extension or `DATABASETYPE=XML` |
| JSON | `.json` extension or `DATABASETYPE=JSON` |
| Text | `.txt`/`.text`/`.log` extension |
| No Extension | File source with no recognizable extension |
| Fixed Width | `DATABASETYPE=Fixed-Width` or file type attribute |
| TSV | `.tsv` extension |
| Avro | `.avro` extension |
| ORC | `.orc` extension |
| Excel | `.xls`/`.xlsx` extension |

Connection details (JDBC URLs, driver JARs, host/port) are automatically extracted and included in the generated `config.yml`.

## Supported Transformations

| Informatica Transform | PySpark Equivalent |
|----------------------|-------------------|
| Source Qualifier | `spark.read.format("jdbc")` / `spark.read.csv()` etc. (with SQL override support) |
| Expression | `.withColumn()` / `.select()` with `expr()` for translated expressions |
| Filter | `.filter()` / `.where()` |
| Lookup | `.join()` with `broadcast()` hint (equi-join or complex expression) |
| Joiner | `.join()` (inner, left, right, full) with MASTER/DETAIL port detection |
| Aggregator | `.groupBy().agg()` |
| Sorter | `.orderBy()` (asc/desc per column) |
| Union | `.unionByName()` with optional flag column normalization |
| Router | Multiple `.filter()` branches per output group |
| Sequence Generator | `monotonically_increasing_id()` |
| Update Strategy | Insert/Update/Delete flags with `_update_flag` column |
| Stored Procedure | Inline via Expression `:SP.` pattern — qualified procedure name resolved from `Stored Procedure Name` attribute (e.g. `PKG_CDI_UTIL.SP_TRUNCATE`) |
| Mapplet | Inlined mini-DAG with topological sort (supports Lookup Procedure, Expression, Input/Output port mapping) |
| Target | `.write.format("jdbc")` / `.write.format("delta")` with type casting and column mapping |

## Workflow Execution Plan

Workflow orchestration uses a nested DAG structure converted from Informatica workflow links:

```
EXECUTION_PLAN = [
  {"type": "parallel_group", "steps": [...]},     # Concurrent sessions
  {"type": "worklet", "plan": [...]},              # Nested sub-plan
  {"type": "session", "mapping_name": "..."},       # Single mapping
  {"type": "task", "name": "T_MAIL_SUCCESS"},       # Email notification
]
```

Features:
- **Topological ordering** with cycle detection (back-edge `visiting` set)
- **fail_fast** mode — stops at first failure, sends T_MAIL_FAIL before propagating
- **Parallel execution** via `ThreadPoolExecutor` with thread-safe failure tracking
- **Email notifications** with Informatica-style `%s`, `%n`, `%e` placeholder substitution; `[Workflow Name]` placeholder resolved at codegen time
- **T_MAIL_FAIL on any failure** — sent from both normal completion path and `RuntimeError` exception handler
- **Job parameters** loaded once and passed to all mappings
- **Connection validation** before any mapping runs; passwords saved to credential provider only after verification
- **Mermaid flowchart** generated alongside the workflow file
- **Workflow run markers** — `===== ... START / END =====` separators in logs for multi-run visibility

## Runtime Library (`runtime_lib.py`)

The generated `runtime_lib.py` provides:

| Function / Class | Purpose |
|-----------------|---------|
| `SparkSession` management | YARN/local profile resolution |
| `SparkContext` | DataFrame registry with case-insensitive column access |
| `MappingMetrics` / `NullMetrics` | Execution tracking and reporting |
| `read_sql()` | JDBC read with query or table name |
| `read_file()` | File read with format options |
| `write_table()` / `write_file()` | JDBC/file write with configurable mode |
| `execute_sql()` | Execute arbitrary SQL on a connection |
| `batch_delete()` | JDBC-prepared-statement batch delete for DD_DELETE strategy |
| `get_db_config()` | Prefix-matched connection resolution from config |
| `test_connection()` | Validates all DB connections before execution |
| `run_workflow()` | Reusable workflow engine (accepts `EXECUTION_PLAN` + `MAPPING_FUNCTIONS`) |
| `discover_mappings()` | Auto-discovers `m_*.py` modules in output directory |
| `_resolve_password()` | Hadoop CredentialProvider with interactive fallback; password deferred until connection verified |
| `_flush_pending_passwords()` | Persists interactively-entered passwords only after successful connection validation |
| `smart_repartition()` | Adaptive repartitioning based on row count |
| `send_email()` | SMTP email with Informatica template formatting |

## SQL Dialect Translation

The `utils/sql_dialect.py` module provides SQL translation between dialects:

```python
from informatica_sparker.utils.sql_dialect import translate_sql, detect_sql_dialect

# Auto-detect and translate
spark_sql = translate_sql("SELECT DECODE(status, 1, 'Active', 2, 'Inactive') FROM tab")
# Result: SELECT CASE status WHEN 1 THEN 'Active' WHEN 2 THEN 'Inactive' END FROM tab

# Explicit dialect
spark_sql = translate_sql("SELECT SYSDATE FROM DUAL", source_dialect="oracle", target_dialect="spark")
```

Functions:

| Function | Description |
|----------|-------------|
| `detect_sql_dialect(sql)` | Detects dialect (oracle / sqlserver / spark) from SQL text |
| `translate_sql(sql, source, target)` | Translates between dialects |
| `_convert_decode(sql)` | Converts Oracle `DECODE()` to `CASE WHEN` |
| `_split_args(s)` | Splits function arguments respecting nested parentheses |

## Configuration File (`config.yml`)

The generated `config.yml` supports:

```yaml
# Spark connection profiles (local, YARN client, YARN cluster)
spark_connections:
  spark3_client:
    master: "yarn"
    deploy-mode: "client"
    config:
      spark.kerberos.keytab: "/appl/hadoop/cdp/keytabs/etl_user.keytab"

# Database connections with credential provider
connections:
  oracle-defaults:
    type: "oracle"
    host: "${ORACLE_HOST}"
    port: 1521
    password: "credential:connections.oracle.password"  # Hadoop CredentialProvider

# Email notifications
email:
  mail_to: "${MAIL_TO:asl@example.com}"
  smtp_host: "${SMTP_HOST:localhost}"
  smtp_port: "${SMTP_PORT:25}"

# Logging configuration (mapping detail logs use DEBUG level;
# mapping start/completed/errors use INFO and appear in workflow log)
logging:
  dir: "logs"
  level: INFO
  backup_count: 7
```

## Requirements

- Python >= 3.10
- lxml >= 4.9.0
- pydantic >= 2.0.0
- jinja2 >= 3.1.0
- networkx >= 3.0
- pyyaml >= 6.0

## Templates

Code generation uses Jinja2 templates in `informatica_sparker/templates/`:

| Template | Purpose |
|----------|---------|
| `mapping.py.j2` | PySpark mapping script with full transformation support |
| `workflow_orchestration.py.j2` | Thin workflow wrapper (execution plan + task info) |
| `runtime_lib.py.j2` | Shared runtime library with Spark helpers |
| `config.yml.j2` | Unified YAML configuration |
| `objects.yml.j2` | File object definitions for source/target paths |
| `job.py.j2` | Job entry point for standalone execution |

## Classifiers

- `Development Status :: 5 - Production/Stable`
- `Topic :: Software Development :: Code Generators`
- `Topic :: Database`

## License

MIT

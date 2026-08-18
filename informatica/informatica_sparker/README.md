# informatica-sparker

**Version v2026.08.18** — A Python framework that converts Informatica PowerCenter workflow/mapping XML exports into PySpark code deployable to Databricks or YARN Spark clusters. **Tested against Informatica output — data results match.**

**v2026.08.18 highlights (transform & load workflow WF_EMS_TL, the largest)**: all **581 mappings runtime-verified** (585 workflow steps, 0 failures) — every workflow (dds / extract / transform-and-load) is now validated end-to-end. This round added: **numeric Update Strategy expressions** (0/1/2/3 = `DD_INSERT`/`DD_UPDATE`/`DD_DELETE`/`DD_REJECT`) with a shared classifier used by both the strategy and write-target handlers, and numeric strategy *fields* dual-compared at runtime; **trailing-whitespace mapping/session names** stripped at parse time (plan names now always resolve to generated modules); mapplet-internal unconnected INPUT → NULL; update_strategy connector renames applied before `_update_flag` derivation; `sq_output` "date/time" port → TimestampType cast; dynamic-lookup base-hit compare-and-update with `Output Old Value On Update=YES`. Earlier core: exact Dynamic Lookup cache conversion via `applyInPandas` (equivalent RDD fallback) with `NewLookupRow` 0/1/2 semantics, base duplicate-key Report Error / CMN_1650, and global Sequence-Id pre-allocation (all 174 WF_NHS_TL mappings runtime-verified). Generated code is validated end-to-end through the E2E pytest suites in `tests/`, which are generated and maintained outside this repository.

Conversion pipeline: **XML → Models → IR Plan → Jinja2 Templates → Generated Python Files**

## Features

- **Multi-Mapping & Multi-Workflow Support**: Handles any number of mappings and workflows per XML file, generating separate `m_<name>.py` files for each mapping and `wf_<name>.py` for each workflow
- **Auto Source Detection**: Automatically identifies source types and connection details:
  - SQL databases (SQL Server, Oracle, MySQL, PostgreSQL, DB2, Teradata, Netezza, Sybase, Informix)
  - File formats: CSV, Parquet, DAT, XML, JSON, Text, Fixed-Width, Avro, ORC, Excel
  - Files without extensions
  - JDBC/ODBC connections with driver JAR detection (ODBC → JDBC auto-conversion)
  - File location detection: Local, S3, ADLS, GCS, DBFS
- **Expression Translation**: Comprehensive Informatica-to-PySpark function mapping (60+ functions):
  - String: `SUBSTR`, `INSTR`, `CONCAT`, `REPLACE`, `REG_REPLACE`, `LPAD`, `RPAD`, `TRIM`, `UPPER`, `LOWER`, `INITCAP`, `REVERSE`, `SOUNDEX`, `REPLACESTR`, `REPLACECHR` (4-arg start_pos → 3-arg translate, NULL → '')
  - Numeric: `ROUND`, `TRUNC`, `ABS`, `MOD`, `POWER`, `SQRT`, `FLOOR`, `CEIL`, `LOG`, `LN`, `EXP`, `SIGN`, `GREATEST`, `LEAST`
  - Date: `SYSDATE`, `TO_DATE`, `ADD_TO_DATE`, `DATE_DIFF`, `GET_DATE_PART`, `SET_DATE_PART`, `LAST_DAY`, `NEXT_DAY`, `MONTHS_BETWEEN`, `ADD_MONTHS`, `MAKE_DATE_TIME`; date format patterns translated (`YYYY`→`yyyy`, `DD`→`dd`, `MI`→`mm`); nested-paren `to_date()` supported
  - Conversion: `TO_CHAR`, `TO_DECIMAL`, `TO_INTEGER`, `TO_BIGINT`, `TO_FLOAT`, `TO_NUMBER`→`cast(... as decimal)`, `IS_NUMBER`, `IS_DATE`
  - Conditional: `IIF`, `DECODE`, `NVL`, `NVL2`, `COALESCE`, `NULLIF`, `ISNULL`
  - Aggregate: `SUM`, `COUNT`, `AVG`, `MIN`, `MAX` (conditional `FUNC(col, cond)`→`when`), `FIRST`, `LAST`, `MEDIAN`, `STDDEV`, `VARIANCE`
  - Window: `CUME`, `MOVINGSUM`, `MOVINGAVG`
  - Local Variable: Counter pattern `X+1`→`monotonically_increasing_id()+1`; retain pattern `IIF(cond,val,X)`→`last(when(...),True).over(Window.orderBy(...))`
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
- **E2E Validation via Test Suites**: generated code is validated against a real Oracle database through the E2E pytest suites in `tests/` (schema DDL, reference-data seed SQL, fixtures) — generated and maintained outside this repository
- **Component-Method Encapsulation**: every transformation renders as a single kwargs call to a shared `lib.<component>(...)` method in the runtime library — the Informatica component shape (input df, ports, attributes) is preserved in generated code. Three-layer separation: `handlers.py` builds step params from the XML → `mapping.py.j2` renders kwargs calls → `runtime_lib.py.j2` owns the runtime semantics. Methods: `lib.expression` (renames → inline `:LKP.xxx()` joins → computed columns → `:SP.xxx()` calls → pass-through fills), `lib.filter` (renames before condition, `$$` substitution, sequence attach), `lib.router` (ordered group split, DEFAULT negation, multi-feed union), `lib.union` (position-aligned per-input selects + `unionByName`), `lib.sorter`, `lib.sequence`, `lib.sq_output` (port casts incl. `date/time` → TimestampType), `lib.update_strategy` (connector renames before `_update_flag`, numeric 0/1/2/3 strategy expressions), `lib.write_target` (positional source/target rename, static DD_* or dynamic I/U/D split with batch update/delete by key), `lib.dynamic_lookup` (exact cache state machine, applyInPandas with RDD fallback, NewLookupRow 0/1/2, Sequence-Id pre-allocation), `lib.load_mapping_variables` (shared UTL_JOB_PARAM reader). Static lookups, joiners, aggregators and stored procedures keep their inline forms (`_main`/`_lkp` broadcast joins, chain accumulation `df_lkp_merge_*`).
- **Transformation Coverage**: Source Qualifier, Application Source Qualifier, Expression, Filter, Lookup Procedure (including exact Dynamic Lookup cache semantics), Joiner (inner/left/right/full with MASTER/DETAIL detection), Aggregator (with literal GROUPBY fields and conditional MIN/MAX/SUM/COUNT), Sorter (ISSORTKEY filtered, connector field rename), Union (per-input-group select+alias), Router (filter conditions, per-group output renaming), Sequence Generator, Update Strategy (DD_INSERT/UPDATE/DELETE constants, numeric 0/1/2/3 equivalents and numeric strategy fields; DD_REJECT rows dropped), Normalizer (posexplode with GENERATED_KEY), Rank (Window row_number/dense_rank), Stored Procedure (inline via `:SP.` pattern), Transaction Control (no-op), Mapplet (mini-DAG inlining), ODBC→JDBC auto-conversion
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
| ODBC | `DATABASETYPE=ODBC`, auto-resolved to underlying DB (Oracle/SQL Server/MySQL/DB2/PostgreSQL) via connection string or sub-type attribute → JDBC |

Connection details (JDBC URLs, driver JARs, host/port) are automatically extracted and included in the generated `config.yml`.

## Supported Transformations

| Informatica Transform | PySpark Equivalent |
|----------------------|-------------------|
| Source Qualifier / Application Source Qualifier | `spark.read.format("jdbc")` / `spark.read.csv()` etc. (with SQL override support) |
| Expression | `.withColumn()` / `.select()` with `expr()` for translated expressions |
| Filter | `.filter()` / `.where()` with connector-based field rename and `$$` variable runtime replacement |
| Lookup | Static: `.join()` with `broadcast()` hint; chain-join onto accumulating DF; dedup / Report Error by join keys. Dynamic: `lib.dynamic_lookup()` — base duplicate-key check (Report Error / CMN_1650), left-join base cache, group-by-key `applyInPandas` state machine (`NewLookupRow` 1=insert / 2=update / 0=no change), global Sequence-Id pre-allocation, automatic RDD fallback when pyarrow is unavailable |
| Joiner | `.join()` (inner, left/right/full outer) with MASTER/DETAIL port detection; select+alias for upstream columns; connector field rename |
| Aggregator | `.groupBy().agg()` with select+alias for upstream columns; literal GROUPBY fields (`0 as DUMMY`) via `expr()`; conditional MIN/MAX/SUM/COUNT with `when()` |
| Sorter | `.orderBy()` filtered by `ISSORTKEY="YES"`; connector field rename with `drop+rename`; ASC/DESC per sort key |
| Union | `.unionByName()` with per-input-group intermediate DataFrames and select+alias; Router-aware upstream resolution |
| Router | Multiple `.filter()` branches per output group; REF_FIELD suffix rename; negated DEFAULT group |
| Sequence Generator | `monotonically_increasing_id()` |
| Normalizer | `posexplode()` over the repeating group's array; 1-based `GENERATED_KEY`; NULL-row drop |
| Rank | Window `row_number()` / `dense_rank()` over the sort ports |
| Update Strategy | Insert/Update/Delete flags with `_update_flag` column (static `DD_*` constants or dynamic field strategies; numeric 0/1/2/3 equivalents; `DD_REJECT` rows dropped) |
| Stored Procedure | Inline via Expression `:SP.` pattern — qualified procedure name (e.g. `PKG_CDI_UTIL.SP_TRUNCATE`), runtime `USER_ARGUMENTS` signature probing with OUT-param binding (`lib.call_stored_procedure`) |
| Mapplet | Inlined mini-DAG with topological sort (includes Lookup Procedure, Expression, Input/Output port mapping) |
| Target | `.write.format("jdbc")` / `.write.format("delta")` with type casting, column mapping, and DD_DELETE support |

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
- **Timer tasks** — `START_RELATIVE_TO_PREVIOUSTASK` + `RECURRING` interval converted to an actual wait (`_apply_task_timer`) so downstream waves start after the configured delay, matching Informatica
- **Decision tasks** — kept as real plan steps (batch barriers between parallel session waves); tasks without a special handler log a non-alarming pass-through (no "will skip")

## Runtime Library (`runtime_lib.py`)

The generated `runtime_lib.py` provides:

| Function / Class | Purpose |
|-----------------|---------|
| `get_spark_session()` | YARN/local profile resolution, auto-kinit, executor interpreter lockstep, `env/` zip shipping to executors (`addPyFile`) |
| `SparkContext` | DataFrame registry with case-insensitive column access |
| `MappingMetrics` / `NullMetrics` | Execution tracking and reporting |
| `read_sql()` | JDBC read with query or table name |
| `read_file()` | File read with format options; YARN-safe local file handling (driver-side CSV read / HDFS staging) |
| `write_table()` / `write_file()` / `write_sql()` | JDBC/file write with configurable mode; YARN-safe local CSV write on the driver |
| `execute_sql()` | Execute arbitrary SQL on a connection |
| `dynamic_lookup()` | Exact dynamic cache state machine — `applyInPandas` (or equivalent RDD fallback), base duplicate-key Report Error, per-key compare-and-update, `NewLookupRow` 0/1/2, Sequence-Id pre-allocation, `Output Old Value On Update` |
| `load_mapping_variables()` | Shared `UTL_JOB_PARAM` reader (`{clean_name: value}`; callers `.get()` declared defaults) |
| `call_stored_procedure()` / `execute_stored_procedure()` | `USER_ARGUMENTS` signature probing with `DECLARE`'d VARCHAR2 OUT/IN OUT binding, cached per call name; legacy all-literals fallback |
| `batch_delete()` / `batch_update()` / `batch_delete_composite()` | JDBC-prepared-statement batch DML for DD_DELETE / DD_UPDATE strategies |
| `get_db_config()` | Prefix-matched connection resolution from config |
| `test_connection()` | Validates all DB connections before execution |
| `run_workflow()` | Reusable workflow engine (accepts `EXECUTION_PLAN` + `MAPPING_FUNCTIONS` + optional `SESSION_SQLS`) |
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

# Dynamic lookup execution engine: apply_in_pandas (default) or rdd
dynamic_lookup:
  executor: apply_in_pandas

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

- Python >= 3.10 (classifiers cover 3.10 – 3.13)
- lxml >= 4.9.0
- pydantic >= 2.0.0
- jinja2 >= 3.1.0
- networkx >= 3.0
- pyyaml >= 6.0
- Runtime (driver + executors): pandas and pyarrow >= the Spark minimum for `applyInPandas` dynamic lookup; an RDD fallback is used automatically when pyarrow is missing/too old (v2026.08.11: the RDD fallback is YARN-verified — `get_spark_session` ships `env/` to executors via `addPyFile`, since worker closures are cloudpickled by reference as `env.runtime_lib.<function>` and YARN executor nodes don't have the workflow directory on disk; a `spark.executorEnv.PYTHONPATH` entry alone is not enough)

## Templates

Code generation uses Jinja2 templates in `informatica_sparker/templates/`:

| Template | Purpose |
|----------|---------|
| `mapping.py.j2` | PySpark mapping script with full transformation support |
| `workflow_orchestration.py.j2` | Thin workflow wrapper (execution plan + task info) |
| `runtime_lib.py.j2` | Shared runtime library with Spark helpers (component methods, workflow engine, dynamic lookup) |
| `config.yml.j2` | Unified YAML configuration |
| `objects.yml.j2` | File object definitions for source/target paths |
| `job.py.j2` | Job entry point for standalone execution |

## Classifiers

- `Development Status :: 5 - Production/Stable`
- `Intended Audience :: Developers`
- `License :: OSI Approved :: MIT License`
- `Programming Language :: Python :: 3` (3.10, 3.11, 3.12, 3.13)
- `Topic :: Software Development :: Code Generators`
- `Topic :: Database`

## Known Issues
- **Remaining limits**: `Synchronize Dynamic Cache=YES` and cross-session persistent cache reuse are not emulated (the cache is seeded from the lookup source on every run); `Update Else Insert=YES` / non-insert row types are not supported; the dynamic-cache `Use First Value` multiple-match policy is converted with `Report Error` semantics (9 such dynamic lookups in WF_EMS_TL — validated against current base data, but duplicate condition keys in the base would fail rather than take the first value).
- **Runtime dependency**: `applyInPandas` requires pandas + pyarrow (Spark minimum version) on driver and executors. If pyarrow is absent or below the Spark minimum, `runtime_lib.dynamic_lookup` automatically uses an equivalent RDD implementation (`config.yml` `dynamic_lookup.executor: apply_in_pandas` / `rdd`).

## License

MIT

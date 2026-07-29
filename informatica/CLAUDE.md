# Informatica-Sparker Code Generation Rules

This file captures conventions, patterns, and rules established during development of the `informatica-sparker` PySpark code generator. Follow these when modifying templates, handlers, or generated code.

## Project Overview

`informatica-sparker` converts Informatica PowerCenter XML exports to PySpark code via a pipeline: XML → models → IR plan → Jinja2 templates → generated Python files. Key source files:

- `templates/` — Jinja2 templates for generated code
- `service.py` — Main orchestrator
- `handlers.py` — XML parsing, IR step building & mapplet inlining
- `workflow_builder.py` — Workflow/woklet DAG builder
- `expr_translator.py` — Expression conversion (Informatica → PySpark SQL)
- `models.py` — Data models
- `ir.py` — Intermediate representation steps

## Recent Architecture Changes

### Workflow Orchestration (moved to runtime_lib)
- All shared workflow logic (`run_workflow`, `execute_plan_step`, `run_sessions_parallel`, `run_sessions_sequential`, `validate_execution_plan`) lives in **runtime_lib.py.j2**.
- Generated workflow files (`wf_*.py`) are **thin wrappers** — only define `EXECUTION_PLAN`, `TASK_INFO`, `MAPPING_FUNCTIONS = lib.discover_mappings()`, and call `lib.run_workflow(...)`.
- Shared helpers also in runtime_lib: `discover_mappings()`, `send_email()`, `format_infa_template()`.

### DAG Execution Plan (new structure)
- Execution plan uses a **nested DAG** structure with four step types:
  - `session` — single mapping execution (has `name`, `mapping_name`)
  - `parallel_group` — concurrent execution (has `steps: [...]`)
  - `worklet` — nested sub-plan matching Informatica worklet topology (has `plan: [...]`)
  - `task` — email/command tasks
- Worklets get their own mini execution plan built from worklet-internal `WORKFLOWLINK` dependencies.
- `_build_dag_plan()` generates correct topological levels with cycle detection.
- **No cycle falls into infinite recursion** — back-edge detection (`visiting` set) in `workflow_builder.py`.

### Mapplet Handler (complete rewrite)
- **`_handle_mapplet()`** now builds a mini-DAG from mapplet instances + connectors, dispatches each internal instance (Lookup Procedure → SQL read + join, Expression → computed columns, Input/Output → port mapping).
- Mapplet-local transformations + mapping-level `transform_map` are combined for reusable lookup resolution.
- Parallel group subgraphs in **mermaid DAG** show `[Parallel]` label.

### Expression Translation
- `REPLACE` maps to `replace` (literal), not `regexp_replace`.
- `SYSDATE` maps to `current_timestamp()`, not `current_date()`.
- To specify day-of-week, `GET_DATE_PART('D')` maps to `dayofweek`.
- Unresolved `$$` mapping variables kept as-is in expressions; template-level `.replace()` handles substitution.

### SQL Schema Parameterization
- Hardcoded schema prefixes in SQL queries (e.g. `PSOR.`) are replaced with a dynamic `{_schema}` variable at codegen time.
- The schema comes from `_conn.get("schema", "")` using the dynamically resolved connection from config.yml.
- When no matching connection is found, the original schema is kept as the fallback value.
- Lookup SQL queries extract the schema prefix via regex on `FROM <schema>.` and pass it as `source_schema` in step params.
- Lookup connection resolution (`_find_lookup_connection`) matches the SQL's schema prefix to a source definition's `owner_name` to use the correct db_name.

### Stored Procedure Handling
- Stored procedures referenced via `:SP.xxx()` in expression transforms are handled by detecting the reference and reading the `Stored Procedure Name` attribute (e.g. `PDPA.PKG_CDI_UTIL.SP_TRUNCATE`).
- The schema prefix is stripped, so the generated call text uses `PKG_CDI_UTIL.SP_TRUNCATE`.
- The `Stored Procedure` instance type is recognized in the dispatch chain (logged at INFO, not WARNING).

### Email Enhancements
- T_MAIL_FAIL is now sent on ANY mapping failure, including when `fail_fast` raises `RuntimeError`. A local `_send_fail()` helper in `run_workflow` is called from both the normal completion path and the exception handler.
- The exception handler extracts failed session names from the RuntimeError message.
- `[Workflow Name]` placeholder in email subjects is replaced with the actual workflow name at code generation time in `service.py`.

### Password Credential Handling
- Interactively entered passwords are cached in memory (`_PASSWORD_PENDING`) but NOT saved to Hadoop CredentialProvider until connection validation passes.
- `_flush_pending_passwords()` is called after all connections are verified successfully.
- On connection failure, no password is persisted to the credential provider.

### Debug/DETAIL Logging Isolation
- Mapping step-level logs (`"Step: ..."`, `"... write completed"`, `"Loaded ... from job_params"`) use `logger.debug()` instead of `logger.info()`.
- Mapping loggers set `propagate = False` so DETAIL messages only go to the mapping's own log file, not the workflow log.
- Workflow-level start/completed/error messages are logged by `run_sessions_sequential`/`run_sessions_parallel` in `runtime_lib.py`, using the module-level `logger` which propagates to root (workflow log).
- Mapping `main()` function calls `lib._flush_pending_passwords()` after successful execution to persist passwords to CredentialProvider.

### Workflow Run Markers
- Each `run_workflow()` call prints `===== ... START` / `===== ... END` separators so multiple runs are visually distinguishable in the log.

### Task Command Handling
- Command tasks (e.g. `T_RM_CMS_CACHE_FACT`) are extracted from XML `<VALUEPAIR>` elements and stored in `TASK_INFO` with `"type": "command"` and the command list.
- `execute_plan_step` handles both the standalone `"task"` type step AND tasks embedded inside `parallel_group` steps.
- Tasks now log `"Task X completed: SUCCESS"` and add to the workflow's `completed` set, so they appear in final workflow counts.

### `_get_input_df` Downstream Preference (v2026.07.20)
- When multiple upstream instances connect to a component, `_get_input_df` prefers:
  1. Special results (`df_lkp_result`, `df_jnr_result`)
  2. Non-chain DataFrames (not `df_lkp_merge*`, `df_merge*`, `df_sq_*`)
  3. Among non-chain matches, the instance that is downstream of another upstream (detected via connector edges)
- This prevents selecting a raw chain/merge DF over the full transformation DF.

### Mapping Logging from Workflow Runtime
- `run_sessions_sequential` and `run_sessions_parallel` log `"Start to run mapping X"` and `"Mapping X completed: SUCCESS"` for every mapping execution.
- In `run_sessions_parallel`, `future_map` stores `(session_name, mapping_name)` tuples so each completion correctly reports the right mapping name regardless of completion order.

### Inline Lookup (`:LKP.xxx()`) in Expressions
- When an Expression transform uses `:LKP.lookup_name(args...)` in a LOCAL VARIABLE port, it is an **inline lookup** — the lookup has NO connectors and is called directly from the expression.
- **Fix in `_handle_expression`** (handlers.py):
  - Pre-scans all fields for `:LKP.xxx()` patterns.
  - For each referenced lookup, extracts INPUT/LOOKUP/RETURN ports and the Lookup Condition from the lookup transformation definition.
  - Builds join predicates by mapping: expression arguments → INPUT ports → Lookup Condition → LOOKUP columns.
  - Stores the join info in `step.params["inline_lookup_joins"]`.
  - Replaces `:LKP.xxx(args)` with the RETURN port name in the expression text before calling `translate()`.
  - Promotes LOCAL VARIABLE ports with expressions to computed columns (previously they were silently dropped).
- **Template** (mapping.py.j2): Before processing `computed_columns`, generates a left join with the lookup DataFrame for each inline lookup in `inline_lookup_joins`.
- **`_translate_lkp_references`** (expr_translator.py) is now a fallback — inline lookups are resolved before translate() is called.

### Oracle JDBC Log Suppression
- Oracle JDBC driver trace messages (e.g. "Closing down clientserver connection") are suppressed by `_SuppressOracleTraceFilter` on the root logger in `init_logger`.
- `test_connection` also sets `java.util.logging.Logger.getLogger("oracle.jdbc").setLevel(Level.SEVERE)` as a second line of defense.
- JDBC reads must NOT set `isolationLevel` — Oracle JDBC driver rejects explicit isolation levels. Rely on Oracle's default `READ_COMMITTED`.

### Multi-Input Expression & Mapplet Handling
- **Problem**: Expressions/mapplets with multiple upstream DataFrames (e.g. EXPTRANS1 with 12 upstream mapplets) only got one input DF; column references like `IN_HSHLD_SIZE` failed.
- **Fix**: `_handle_expression` and `_handle_mapplet` detect multiple upstreams via `_get_all_input_dfs()` and generate `join_` pre-steps that left-join extra DFs on common columns (`__common_cols__`).
- **Output merge**: Mapplet OUTPUT with multiple internal feeders generates `join_output_*` merge steps.

### Connector Field Remapping (mapplet internal + main mapping)
- **Problem**: Mapplet INPUT ports (`IN_CMS_HSE_UNIT_KEY`) differ from upstream column names (`ACTL_CMS_HSE_UNIT_KEY`). Expressions/lookups inside mapplets also reference internal connector port names that differ from the actual upstream column.
- **Fix for mapplets**: Build `inst_field_remap` from both external connectors (`input_field_map`) and internal mapplet connectors. Apply to join predicates (equality + complex `expr()`) and expression texts.
- **Fix for main mapping**: Build `_agg_field_remap` / `_expr_field_remap` from main mapping connectors. Apply to aggregator and expression transformations.
- **Entry-point remapping**: When a mapplet's external input columns differ from its INPUT port names, generate an `input_*` remap step that adds `withColumn("IN_PORT", expr("ACTUAL_COL"))`.

### Router Transformation Support (v2026.07.20)
- **`_handle_router`** (handlers.py): Parses GROUP elements for filter conditions per output group; builds rename maps from REF_FIELD attributes (e.g. `PRPL_CSSA_APLY_ID_TYPE_CODE` → `_CODE1`, `_CODE2` per group).
- **Router output registration**: Each output group gets its own DataFrame (`df_rtr_<group>`) registered under multiple key patterns (`instance_group`, `trans_group`, etc.) for downstream connector resolution.
- **DEFAULT group**: When conditions exist, the DEFAULT/else group gets the negated conjunction `~(cond1) & ~(cond2) & ...`.
- **Template** (mapping.py.j2): Per-group `.filter(expr(...))` with `.withColumnRenamed()` for REF_FIELD suffix columns.
- **BFS walk skip**: The lookup chain BFS walk skips Router instances (they have multiple outputs registered under suffixed keys).
- **`expr("TRUE")`**: Bare `True`/`False` in filter conditions are wrapped as `expr("TRUE")`/`expr("FALSE")` to avoid `NameError` (Router templates).

### Union Per-Group Select+Alias (v2026.07.20)
- **Per-input-group intermediate DFs**: Each Union input group gets an intermediate DataFrame (`df_<union>_<group>`) that `select`s only the needed upstream columns with proper aliases.
- **Router-aware upstream resolution**: When an upstream is a Router, resolves to the correct Router output group DataFrame using `from_field` and `group_name`.
- **Best upstream DF selection**: Prefers `EXPTRANS`/`EXP_` (downstream transformations that inherit all columns) over raw chain/merge DFs.

### Joiner Enhancements (v2026.07.20)
- **Master/detail port detection**: Uses `PORTTYPE` markers (`INPUT/OUTPUT/MASTER`) on TRANSFORMFIELD elements to identify master vs detail, with fallback to exclusion when only MASTER is marked.
- **Select+alias intermediate DFs**: For both master and detail sides, generates `select(col("upstream").alias("joiner_port"))` to avoid column conflicts.
- **Join type mapping**: `Detail Outer` → `left`, `Master Outer` → `right`, `Full Outer` → `full`.
- **Deferred processing**: Joiners whose inputs aren't ready are deferred and re-processed after all upstream transforms complete.

### Aggregator Enhancements
- **GROUPBY detection**: Parser now detects `EXPRESSIONTYPE="GROUPBY"` (not just `GROUPBY="YES"`).
- **Complex aggregation**: `_translate_aggregation_expr` now supports complex inner expressions (e.g. `SUM(DECODE(...))`) by translating the inner expression and wrapping with `expr()`. Simple column references use the short form `sum("col")`.
- **Runtime $$ substitution**: Mapping variables (`$$v_rpt_mth`) in aggregate expressions use Python f-strings (`expr(f"""...{v_rpt_mth}...""")`) for runtime resolution from job_params.
- **Literal GROUPBY fields**: Fields with `EXPRESSIONTYPE=GROUPBY` but constant expressions (e.g. `0 as DUMMY`, `'U' as HSHLD_AEM_IND`) are treated as literal columns — added to both `_agg_input.select()` via `expr("0").alias("DUMMY")` AND `groupBy()` to survive `agg()`.
- **Conditional MIN/MAX**: `MIN(col, cond)` and `MAX(col, cond)` now use `when()` (previously only COUNT/SUM were supported).
- **Complex agg column**: Non-simple column expressions in `FUNC(col_expr, cond)` use `expr("col_expr")` instead of `col("col_expr")` (e.g. `MTH_RENT_AMT_IN - VOID_RENT_AMT_IN`).

### $$ Mapping Variable Handling (v2026.07.20)
- **`translate_for_filter`** (expr_translator.py): Temporarily removes `$$` mapping variables from `pm_variables` during translation so functions are converted but `$$` variables are preserved (prevents double-quoting when variables appear inside SQL string literals).
- **Filter template** (mapping.py.j2): Filter expressions with `$$` variables use `_filter_text` + `.replace()` runtime substitution pattern (same as Source Qualifier).
- **Source Qualifier filter_inner**: Extracted from the translated result (not raw expression) to include function translation while preserving `$$` for runtime `.replace()`.

### Filter & Sorter Connector Field Rename (v2026.07.20)
- **Filter renames**: Connector mappings where `from_field ≠ to_field` (e.g. `CUST_TNT_CODE_OUT` → `CUST_TNT_CODE`) are applied via `drop("to").withColumnRenamed("from", "to")` before the filter condition.
- **Sorter renames**: Same pattern for Sorter component; `ISSORTKEY="YES"` fields only in `orderBy()`.
- **Duplicate protection**: All `withColumnRenamed` calls are preceded by `.drop(target)` to prevent duplicate columns after rename.

### LOCAL VARIABLE Support (v2026.07.20)
- **Counter pattern** (`IIF(ISNULL(X),1,X+1)`): Detected as self-referencing increment → converted to `monotonically_increasing_id() + 1` (partition-safe, no shuffle).
- **Retain pattern** (`IIF(cond, val, X)`): Detected as self-referencing non-increment → converted to `last(when(cond, val), True).over(Window.orderBy(lit(1)))` (carries forward last non-null value).
- Both patterns are promoted from LOCAL VARIABLE to computed columns; downstream OUTPUT ports referencing them work via `expr("var_name")`.

### Topological Sort Fix (v2026.07.20)
- **Word boundary dependency detection**: Changed from `_dep_name in expression` (substring match) to `re.search(r'\b' + re.escape(_dep_name) + r'\b', expression)` — prevents false dependencies when one column name is a substring of another (e.g. `FILE_MTH` falsely detected inside `FILE_MTH_V`).

### `get_input_df` Downstream Preference (v2026.07.20)
- When multiple upstream instances feed a component and all are non-chain DataFrames, prefers the instance that is downstream of another upstream instance (detected via connector graph). E.g., if `JNRTRANS → MPLT_LKP → AGG_SUM`, then `MPLT_LKP_RENT_ENQ_RMDR_SMRY` is preferred over `df_JNRTRANS`.

### Expression Translation Fixes (v2026.07.20)
- **REPLACECHR**: Custom handler `_translate_replacechr` converts 4-arg Informatica `REPLACECHR(start, str, from, to)` to 3-arg PySpark `translate(str, from, to)` (start_pos dropped, NULL→'').
- **TO_NUMBER**: Added to FUNCTION_MAP as `cast({} as decimal)`.
- **Date format patterns**: Rewritten `_translate_date_format_patterns` uses `_extract_function_args` (handles nested parentheses) instead of regex `[^)]*?`. Correctly converts `YYYYMMDD` to `yyyyMMdd` in complex `to_date(cast(...)||'01','YYYYMMDD')` expressions.
- **`expr("TRUE")`**: `wrap_with_expr` wraps bare True/False as `expr("TRUE")`/`expr("FALSE")` instead of returning bare `True`/`False` (which caused `NameError` in Router templates).

### Flat File config.yml Fix (v2026.07.20)
- **`is_flat_file` flag**: Source table entries in service.py now include `"is_flat_file": is_flat` so flat file objects (e.g. `FLAT_DRP_MP`) are correctly emitted in config.yml's `objects:` section.

### Template Jinja2 Patterns (v2026.07.20)
- **`last(when(...))` direct API**: New check `'last(when(' in expr_str` to bypass `expr()` wrapping for PySpark API expressions.
- **`monotonically_increasing_id()` direct API**: Template checks for `'monotonically_increasing_id' in expr_str` to avoid `expr()` wrapping.
- **`drop + withColumnRenamed`**: All `withColumnRenamed` calls for Filter and Router component renames are preceded by `drop(target_name)` to prevent duplicate columns.

### Expression Translation: Bare function handling
- `_translate_functions` now handles bare keywords without parentheses (e.g. bare `SYSDATE` → `current_timestamp()`), using a negative lookahead `(?!\s*\()` to avoid double-matching functions with parentheses.

### Fallback Connections in config.yml
- Added `source_db`, `target_db`, `target`, `source`, `lookup_conn` as fallback connections in config.yml template, pointing to `oracle-defaults` with `DPA` database. Override as needed.

## Code Generation Rules

use python3.11 to recompile or build informatica-sparker: `cd /var/lib/airflow/dags/adam/informatica/informatica_sparker && python3.11 -m build 2>&1 | tail -5`
then use informatica-sparker command to convert like 
`OUT_ROOT=/var/lib/airflow/dags/adam/informatica/PySpark_workflows; informatica-sparker convert WF_GMS_DDS_APLY_DLY.XML -o $OUT_ROOT/WF_GMS_DDS_APLY_DLY`
use default python to run pyspark workflow or mapping 

### Output Quality

- **No Chinese comments in generated code.** All comments and docstrings must be in English.
- **Generated filenames** use `_make_safe_name()` (lowercase with underscores), e.g. `WF_GMS_DDS_APLY_DLY` → `wf_gms_dds_aply_dly.py`.
- **Workflow file** goes in the **root output directory** (not in `env/`), alongside `m_*.py` mapping files.
- **Log file names** are lowercase (e.g., `wf_gms_dds_aply_dly.log`), matching the workflow filename.
- **Use `| topython` filter** instead of `| tojson` for embedding Python data structures in templates, because `tojson` produces JSON `true`/`false`/`null` but Python needs `True`/`False`/`None`.
- **.md files** now written to output directory alongside workflow file (not `Path.cwd()`).

### Logging

- **Root logger** gets console handler (once) + **file handler** (once, on first `init_logger` call). This means messages from `logging.getLogger(__name__)` in runtime_lib (e.g. `env.runtime_lib` logger) also appear in the workflow log file.
- **Named loggers** get separate file handlers for per-module log files.
- **Always use `lib.init_logger()` then `lib.get_logger(name)`** — pass the explicit name to `get_logger()` so it matches the named logger created by `init_logger()`.
- Workflow template: `lib.init_logger("{{ workflow_name | lower }}")` and `logger = lib.get_logger("{{ workflow_name | lower }}")`.

### Lazy Initialization

- **Mapping modules must have zero side effects at import time.** All initialization (logging, config loading, Spark session, DB connections, variable loading) must be inside `run_mapping()`.
- The `MAPPING_FUNCTIONS` dict in the workflow discovers modules via `importlib.import_module()`, which executes top-level code. Any top-level code in mapping modules will run eagerly for ALL mappings before any mapping executes.
- `main()` in mapping files must also load config independently: `config = lib.load_config('env/config.yml')` then `spark = lib.get_spark_session(name, config)`.

### Workflow Execution

- **Strict EXECUTION_PLAN ordering.** Workflow steps execute in the order defined by `EXECUTION_PLAN`. Parallel groups execute concurrently; worklet sub-plans execute sequentially step-by-step.
- **fail_fast** — When `fail_fast=True` (default), stop the workflow at the first failure. Subsequent steps must not execute.
- **Check return values, not just exceptions.** `run_mapping()` returns `False` on failure (not an exception). The workflow must check `if not _ok: raise RuntimeError(...)`.
- **DAG topology preserved** — `execute_plan_step()` recursively handles `parallel_group`, `worklet` (with nested plan), `session`, and `task` types.
- **run_workflow is reusable** — accepts any `execution_plan` + `mapping_functions` + `workflow_name`; every workflow just passes its own EXECUTION_PLAN.

### Connections & Passwords

- **Test all database connections before running mappings.** Use `lib.test_connection(spark, conn_config)` which does a `SELECT 1 FROM DUAL` via JDBC.
- **Cache resolved passwords** in `_PASSWORD_CACHE` dict (module-level). The first caller enters the password via `getpass` and saves it to Hadoop CredentialProvider; subsequent callers reuse the cached value without prompting.
- **Connection name resolution** uses prefix matching in `get_db_config()`: if `"DPA_FACT_..."` is the requested name, try `"DPA"` first by checking if any connection key is a prefix of the requested name.
- **Hardcoded default connection names** (`"source_db"`, `"target_db"`, `"default_conn"`, `"lookup_conn"`) must be filtered out in `codegen.py` so they don't overwrite properly resolved connection aliases.

### Mapping Variable Loading (Per-Mapping from Param File)
- Each mapping reads `$$` variables directly from the `UTL_JOB_PARAM` file on disk (not from a workflow-passed `job_params` dict), so upstream mappings' updates to the file are always visible.
- The `job_params` dict is still passed by the workflow for compatibility but is **ignored** by mapping templates in favor of the file.
- Only mappings that declare `$$` mapping variables in their Informatica definition generate the file-reading code.
- The param file is the single source of truth for variable values across sequential mappings.

### Job Parameters

- **Parameter file path** comes from config: `config["objects"]["UTL_JOB_PARAM"]["path"]`.
- Each mapping reads the file independently at runtime via `try: open(_param_path) ... except: pass`.

### Email Notifications

- **Email config** is in `config.yml` under `email:` section, not hardcoded in generated Python.
  ```yaml
  email:
    mail_to: "${MAIL_TO:asl}"
    mail_from: "noreply@example.com"
    smtp_host: "${SMTP_HOST:localhost}"
    smtp_port: "${SMTP_PORT:25}"
  ```
- `run_workflow` reads `email.mail_to` from config at runtime and injects it into all email tasks in `TASK_INFO`.
- **Informatica-style placeholders** `%s`, `%n`, `%e`, `%b`, `%c`, `%i`, `%g` in email templates are replaced by `format_infa_template()` before sending.

### Type Handling

- **Decimal → String casting** — When casting a Decimal column to StringType, first cast through `DecimalType(38,0)` to avoid scientific notation:
  ```python
  when(col(c).cast(DecimalType(38,0)).isNotNull(),
       col(c).cast(DecimalType(38,0)).cast(StringType()))
  .otherwise(col(c).cast(StringType()))
  ```
- **`MIMEText`** must use `_charset="utf-8"` for proper encoding.

### Thread Safety

- `run_sessions_parallel()` uses `threading.Lock` to protect shared variables (`completed`, `failed`, `results` sets/dicts).
- When `fail_fast` triggers during parallel execution, cancel remaining futures via `future.cancel()` and log the cancellation.

### Jinja2 Template Patterns

- Use `| tojson` for simple dict/list embedding in Python files only when no booleans are present. Use `| topython` (custom filter) when booleans may appear.
- The `topython` filter is registered in `codegen.py` and converts `true`/`false`/`null` to `True`/`False`/`None`.
- All templates are in `templates/` directory: `mapping.py.j2`, `workflow_orchestration.py.j2`, `runtime_lib.py.j2`, `config.yml.j2`, `objects.yml.j2`, `job.py.j2`.
- **`row_number()` / `last(when())` / `monotonically_increasing_id()`**: PySpark API expressions must NOT be wrapped in `expr()`. Template checks for `'row_number()' in expr_str`, `'monotonically_increasing_id' in expr_str`, and `'last(when(' in expr_str` to use direct `withColumn("col", expression)` instead of `withColumn("col", expr("expression"))`.
- **`withColumnRenamed` with drop**: Always use `df.drop("target").withColumnRenamed("src", "target")` when renaming columns via connectors to prevent duplicates.
- **`_filter_text` pattern**: Filter/Source Qualifier expressions with `$$` mapping variables use `_filter_text = """..."""; _filter_text = _filter_text.replace("$$var", var)` for runtime substitution.

### PySpark `max`/`min` Shadowing

- `from pyspark.sql.functions import *` imports `max(col)` / `min(col)`, shadowing Python builtins.
- Save builtins before the import:
  ```python
  _builtin_max = max
  _builtin_min = min
  from pyspark.sql.functions import *
  ```
- Use `_builtin_max([a, b])` and `_builtin_min([a, b])` where the Python builtin is intended.
- `_builtin_min`/`_builtin_max` MUST be assigned **before** `from pyspark.sql.functions import *` at module level (not inside a function after the import), or they will capture the PySpark column functions instead of Python builtins.

### Flat File Lookup Support (v2026.07.29)
- **Flat file lookups** — Lookup Procedure with `Source Type = Flat File` and no SQL/table name now reads from a file instead of skipping.
- **Parser** (`parser.py`): Added `Flat File Lookup` session extension extraction capturing `Lookup source filename` and `Lookup source file directory`.
- **Handler** (`handlers.py`): `_handle_lookup` now checks `session_file_sources` for flat file lookup info, creates a `ReadFileStep` with source field names for column-by-position renaming.
- **Config gen** (`service.py`): Flat file lookups are registered in config.yml `objects:` section with `__SOURCE_FILE_DIR__` path markers.
- **Replaces the previous behavior of silently skipping the lookup** — now a proper read step is generated.

### Same-Name Source + Source Qualifier Fix (v2026.07.29)
- **Problem**: When Source Definition and Source Qualifier share the same instance name (e.g. `HA_PRH_RNTL_UNIT`), the Source was silently overwritten in `instance_map` and never dispatched, causing `NameError: name 'df_source' is not defined`.
- **Fix** (`handlers.py:675-700`): In `_handle_source_qualifier`, when `_get_input_df()` returns None and a same-named Source Definition exists in `mapping.sources`, generate the source read step inline by calling `_handle_source` on a minimal Source instance.

### Aggregator Multi-Upstream Merge (v2026.07.29)
- **`_handle_aggregator`** now detects multiple upstream DataFrames via `_get_all_input_dfs()` and generates common-column left-join pre-steps before the aggregation, matching the Expression handler's multi-input pattern.
- **DISTINCT path** (template): GROUP BY with no aggregations now uses `agg_selects` (connector field mappings) with proper `col("from").alias("to")` patterns instead of raw `group_by` names — ensures correct field name remapping.
- **DUMMY port fallback**: Both the DISTINCT path and regular `_agg_input.select()` path treat `from == 'DUMMY'` as unconnected (uses `lit(None)` instead of `col("DUMMY")`).

### Mapplet Multiple OUTPUT Instances (v2026.07.29)
- **Problem**: Mapplets with multiple Output Transformation instances (e.g. `OUTPUT_RLS_CNTL` + `OUTPUT_DUMMY`) only used the last one found, which could pick a dummy/spill output over the main data path.
- **Fix** (`handlers.py:2674-2702`): Collects all output instances and selects the one with the most upstream connectors (the "main" data path).

### $$ Mapping Variable in Aggregator (v2026.07.29)
- **Set-literal fix**: In `_translate_aggregation_expr`, bare mapping variable placeholders like `{v_REC_RLS_IND}` are now wrapped with `lit(v_REC_RLS_IND)` instead of being rendered as `{v_REC_RLS_IND}.alias("REC_RLS_IND")` — which Python interprets as a set literal.

### TRUNC Numeric Detection (v2026.07.29)
- **`_translate_date_trunc`**: Single-argument `TRUNC(expr)` now skips `date_trunc('day', ...)` conversion when the argument contains `datediff` or arithmetic operators — numeric TRUNC falls through to the generic `TRUNC → floor` mapping instead.

### TO_DATE Numeric Cast (v2026.07.29)
- **`_translate_to_date_to_string`**: `to_date(numeric_expr, format)` wraps the first argument with `cast(... as string)` because PySpark's `to_date` expects a string, not a number. Skips string literals and simple column references.

### TO_CHAR Date Format (v2026.07.29)
- **`_translate_to_char`**: Two-argument `TO_CHAR(date, format)` now produces `date_format(date, format)` instead of `cast(date as string)` (which ignored the format). Single-argument `TO_CHAR(value)` still uses `cast(value as string)`.

### SQL Duplicate Column Alias (v2026.07.29)
- **`_alias_sql_columns`**: When a SQL pushdown query's SELECT has duplicate bare column names (e.g. two `APLY_KEY` columns from different tables), inline `AS` aliases are added to the duplicates only, using the corresponding `output_columns` port names — prevents `ORA-00918: column ambiguously defined`.

### Filter Variable Substitution (v2026.07.29)
- **Conditional replace**: The `_filter_text.replace("$$var", ...)` calls are only generated for mapping variables that actually appear in the filter expression — unused variables don't generate unnecessary replace code.
- **Empty variable fallback**: Uses `str(v_var or "0")` to prevent invalid SQL when a mapping variable's value is empty (e.g. `COL > ` → `COL > 0`).

### Target Column Case-Insensitive Drop (v2026.07.29)
- **Template** (mapping.py.j2): The target field_map rename loop now drops any column that case-insensitively matches the target name before `withColumnRenamed`, preventing Spark ambiguity errors when a column and its computed replacement share the same name (e.g. `vcnt_ind` vs `VCNT_IND`).

### Union Missing Column Fallback (v2026.07.29)
- **Template** (mapping.py.j2): Union output column select now adds `lit(None)` for any column not present in the DataFrame before the select, preventing `cannot resolve column` errors.

### Config.yml Path Defaults (v2026.07.29)
- Added `paths:` section to config.yml with `source_file_dir` and `target_file_dir` documented at the top.
- Object paths use `__SOURCE_FILE_DIR__` marker replaced at render time; defaults changed from `/tmp` to `/var/lib/airflow/dags`.
- Path variables use `${VAR:default}` syntax resolvable via `os.environ` at runtime.

### Code Quality (v2026.07.29)
- **StructField list comprehension**: Empty DataFrame creation uses `[StructField(f, StringType(), True) for f in _src_cols]` instead of listing 40+ individual StructField lines.
- **`_src_cols` / `_csv_cols` scope**: `_src_cols` is defined before `if _csv_cols:` so both branches can access it.

## Conversion Progress

As of **2026-07-29** (version **v2026.07.29**), 10 workflows (~1,130 mappings) have been converted from Informatica PowerCenter XML to PySpark.

### ✅ Runtime Verified
| Workflow | Mappings | XML Size | Status |
|----------|----------|----------|--------|
| WF_GMS_DDS_APLY_DLY | 8 | 340K | **Data-validated**, zero warnings |
| WF_CMS_DDS_APLY_MTH | 10 | 1.2M | **3/3 mappings SUCCESS**, 16 mapplets inlined |
| WF_HOMES_DDS_APLY_DMNS | 67 | 2.4M | **Zero warnings**, Sequence Generator tested |
| WF_EMS_PRHE_DDS_APLY_RVN_MTH | 25 | 3.1M | **Zero warnings** (fixed 8) |
| WF_EMS_PRHE_DDS_APLY_HSE_STCK_MTH | 28 | 3.9M | **Data-validated** (fixed 15+) |

### ⚠️ Converted (Not Yet Runtime Tested)
| Workflow | Mappings | XML Size | Notes |
|----------|----------|----------|-------|
| WF_EMS_DDS_APLY_MTH | 49 | 4.0M | dds/ layer |
| WF_EMS_EX | 142 | 9.6M | Source system extract (SSAL1) |
| WF_NHS_EX | 46 | 2.2M | Source system extract (SSAL1) |
| WF_EMS_TL | 581 | 36M | Transform & load (largest) |
| WF_NHS_TL | 174 | 11M | Transform & load |

### Layer Architecture
- **`dds/`** — Data Delivery Service (subject-area marts). Current testing focus.
- **`extract/`** — Source system extraction (SSAL1). Simple Source Qualifier → Target.
- **`transform and load/`** — Complex transformation and dimensional load.

### Feature Coverage
| Feature | Tested In | Status |
|---------|-----------|--------|
| Source Qualifier (DB) | All | ✅ |
| Source Qualifier (File / Flat File) | WF_EMS_TL, WF_EMS_PRHE_DDS_APLY_RVN_MTH | ✅ |
| Expression / Filter (with $$ vars) | All | ✅ |
| Aggregator (complex expr, literal GROUPBY, conditional MIN/MAX) | WF_CMS_DDS_APLY_MTH, WF_EMS_PRHE_DDS_APLY_RVN_MTH | ✅ |
| Lookup Procedure (chain-join, dedup, merge dedup) | WF_CMS_DDS_APLY_MTH, WF_EMS_PRHE_DDS_APLY_RVN_MTH | ✅ |
| Joiner (master/detail, join types, select+alias) | WF_CMS_DDS_APLY_MTH, WF_EMS_PRHE_DDS_APLY_RVN_MTH | ✅ |
| Sequence Generator | WF_HOMES_DDS_APLY_DMNS | ✅ |
| Mapplet (inline mini-DAG) | WF_CMS_DDS_APLY_MTH | ✅ |
| Stored Procedure | WF_CMS_DDS_APLY_MTH | ✅ |
| Update Strategy / Sorter (ISSORTKEY, rename) | WF_CMS_DDS_APLY_MTH, WF_EMS_PRHE_DDS_APLY_RVN_MTH | ✅ |
| Inline Lookup (`:LKP.xxx()`) | WF_CMS_DDS_APLY_MTH | ✅ |
| Multi-input merge | WF_CMS_DDS_APLY_MTH | ✅ |
| Mapping Variables ($$) runtime replace | WF_CMS_DDS_APLY_MTH, WF_EMS_PRHE_DDS_APLY_RVN_MTH | ✅ |
| Folder-level shared transforms | WF_CMS_DDS_APLY_MTH | ✅ |
| Worklet / Parallel Sessions | WF_GMS_DDS_APLY_DLY | ✅ |
| Email / Command Tasks | WF_GMS_DDS_APLY_DLY | ✅ |
| Router (filter groups, REF_FIELD rename) | WF_EMS_PRHE_DDS_APLY_RVN_MTH | ✅ |
| Union (per-group select+alias) | WF_EMS_PRHE_DDS_APLY_RVN_MTH | ✅ |
| LOCAL VARIABLE (counter + retain) | WF_EMS_PRHE_DDS_APLY_RVN_MTH | ✅ |
| Filter / Sorter connector field rename | WF_EMS_PRHE_DDS_APLY_RVN_MTH | ✅ |
| Flat File config.yml object registration | WF_EMS_PRHE_DDS_APLY_RVN_MTH | ✅ |
| REPLACECHR, TO_NUMBER, date format | WF_EMS_PRHE_DDS_APLY_RVN_MTH | ✅ |
| Source Filter table prefix stripping | WF_EMS_PRHE_DDS_APLY_RVN_MTH | ✅ |
| Flat File Lookup | WF_EMS_PRHE_DDS_APLY_HSE_STCK_MTH | ✅ |
| Same-name Source+SQ (flat file) | WF_EMS_PRHE_DDS_APLY_HSE_STCK_MTH | ✅ |
| Aggregator multi-upstream merge | WF_EMS_PRHE_DDS_APLY_HSE_STCK_MTH | ✅ |
| Mapplet multiple OUTPUT instances | WF_EMS_PRHE_DDS_APLY_HSE_STCK_MTH | ✅ |
| SQL duplicate column alias (ORA-00918) | WF_EMS_PRHE_DDS_APLY_HSE_STCK_MTH | ✅ |
| TO_CHAR → date_format | WF_EMS_PRHE_DDS_APLY_HSE_STCK_MTH | ✅ |
| TO_DATE numeric cast | WF_EMS_PRHE_DDS_APLY_HSE_STCK_MTH | ✅ |
| Normalizer | — | ⏳ Pending |

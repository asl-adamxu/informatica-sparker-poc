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

### Aggregator Enhancements
- **GROUPBY detection**: Parser now detects `EXPRESSIONTYPE="GROUPBY"` (not just `GROUPBY="YES"`).
- **Complex aggregation**: `_translate_aggregation_expr` now supports complex inner expressions (e.g. `SUM(DECODE(...))`) by translating the inner expression and wrapping with `expr()`. Simple column references use the short form `sum("col")`.
- **Runtime $$ substitution**: Mapping variables (`$$v_rpt_mth`) in aggregate expressions use Python f-strings (`expr(f"""...{v_rpt_mth}...""")`) for runtime resolution from job_params.

### Expression Translation: Bare function handling
- `_translate_functions` now handles bare keywords without parentheses (e.g. bare `SYSDATE` → `current_timestamp()`), using a negative lookahead `(?!\s*\()` to avoid double-matching functions with parentheses.

### Fallback Connections in config.yml
- Added `source_db`, `target_db`, `target`, `source`, `lookup_conn` as fallback connections in config.yml template, pointing to `oracle-defaults` with `DPA` database. Override as needed.

## Code Generation Rules

use python3.11 to recompile or build informatica-sparker: `cd /var/lib/airflow/dags/adam/informatica/informatica_sparker && python3.11 -m build 2>&1 | tail -5`
then use informatica-sparker command to convert like `informatica-sparker convert WF_GMS_DDS_APLY_DLY.XML -o WF_GMS_DDS_APLY_DLY_SPARK`
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

### PySpark `max`/`min` Shadowing

- `from pyspark.sql.functions import *` imports `max(col)` / `min(col)`, shadowing Python builtins.
- Save builtins before the import:
  ```python
  _builtin_max = max
  _builtin_min = min
  from pyspark.sql.functions import *
  ```
- Use `_builtin_max([a, b])` and `_builtin_min([a, b])` where the Python builtin is intended.

## Conversion Progress

As of **2026-07-02** (version **v2026.07.02**), 10 workflows (~1,130 mappings) have been converted from Informatica PowerCenter XML to PySpark.

### ✅ Runtime Verified
| Workflow | Mappings | XML Size | Status |
|----------|----------|----------|--------|
| WF_GMS_DDS_APLY_DLY | 8 | 340K | **Data-validated**, zero warnings |
| WF_CMS_DDS_APLY_MTH | 10 | 1.2M | **3/3 mappings SUCCESS**, 16 mapplets inlined |
| WF_HOMES_DDS_APLY_DMNS | 67 | 2.4M | **Zero warnings**, Sequence Generator tested |
| WF_EMS_PRHE_DDS_APLY_RVN_MTH | 25 | 3.1M | **Zero warnings** (fixed 8) |
| WF_EMS_PRHE_DDS_APLY_HSE_STCK_MTH | 28 | 3.9M | **Zero warnings** (fixed 2) |

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
| Source Qualifier (File) | WF_EMS_TL | ✅ (basic) |
| Expression / Filter | All | ✅ |
| Aggregator (complex expr) | WF_CMS_DDS_APLY_MTH | ✅ |
| Lookup Procedure | WF_CMS_DDS_APLY_MTH, WF_GMS_DDS_APLY_DLY | ✅ |
| Joiner | WF_CMS_DDS_APLY_MTH | ✅ |
| Sequence Generator | WF_HOMES_DDS_APLY_DMNS | ✅ |
| Mapplet (inline mini-DAG) | WF_CMS_DDS_APLY_MTH | ✅ |
| Stored Procedure | WF_CMS_DDS_APLY_MTH | ✅ |
| Update Strategy / Sorter | WF_CMS_DDS_APLY_MTH | ✅ |
| Inline Lookup (`:LKP.xxx()`) | WF_CMS_DDS_APLY_MTH | ✅ |
| Multi-input merge | WF_CMS_DDS_APLY_MTH | ✅ |
| Mapping Variables ($$) | WF_CMS_DDS_APLY_MTH | ✅ |
| Folder-level shared transforms | WF_CMS_DDS_APLY_MTH | ✅ |
| Worklet / Parallel Sessions | WF_GMS_DDS_APLY_DLY | ✅ |
| Email / Command Tasks | WF_GMS_DDS_APLY_DLY | ✅ |
| Router / Normalizer | — | ⏳ Pending |

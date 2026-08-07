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

### WF_NHS_TL Round (v2026.08.07)
- **Lookup / FILTRANS upstream wiring**: multi-upstream `_get_input_df` prefers the lookup chain result; post-process no longer rewrites the input of lookup-fed filters (`FILTRANS_STS` / `FILTRANS_MSTR` now receive the correct merge DataFrame).
- **Mapplet rename & source flow**: base flow resolves through the outermost mapplet input, mapplet outputs merge on the right side so source values are not overwritten with NULL; empty mapplet rename steps are omitted.
- **Dynamic Lookup / DECODE NULL**: unique per-instance `NewLookupRow_<instance>` 0/1 columns; `DECODE(x, NULL, ...)` becomes `WHEN x IS NULL`; mapplet Lookup multiple-match policy (Report Error / dedup) is converted.
- **Stable naming**: semantic DataFrame names via `_df_name()`; redundant pass-through steps are removed.

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
- **Case-insensitive replace (v2026.08.03)**: the replacement uses the `ireplace` Jinja filter (registered in codegen.py, `re.sub(..., re.IGNORECASE)`), NOT plain `replace` — the XML owner may be lowercase (`psor`) while the SQL text is uppercase (`FROM PSOR.TABLE`), and case-sensitive `replace` silently misses the prefix, leaving the hardcoded schema in the query.
- Lookup SQL queries extract the schema prefix via regex on `FROM <schema>.` and pass it as `source_schema` in step params.
- Lookup connection resolution (`_find_lookup_connection`) matches the SQL's schema prefix to a source definition's `owner_name` to use the correct db_name.

### Stored Procedure Handling
- Stored procedures referenced via `:SP.xxx()` in expression transforms are handled by detecting the reference and reading the `Stored Procedure Name` attribute (e.g. `PDPA.PKG_CDI_UTIL.SP_TRUNCATE`).
- **Schema parameterization (v2026.08.03)**: the owner schema (`PDPA`) is NOT stripped — it is kept in `step.params["sp_schema"]` and rendered as `{_schema}` at runtime: `_schema = _sp_conn.get("schema", "") or "PDPA"` then `_sp_call = _schema + "." + "PKG_CDI_UTIL.SP_TRUNCATE"`. Matches the SQL `{_schema}` pattern — the connection's `schema` field wins, the XML owner is the fallback.
- **Runtime signature resolution (v2026.08.03)**: the Informatica XML only lists the transformation's input ports; the real Oracle procedure may differ (e.g. the UAT wrapper `PYSPARK.SP_DELETE_DDS_FACT` has 4 params — 3 IN + `RET_MSG OUT` — while the metadata lists 3). Calling with metadata-only args → `ORA-06550/PLS-00306 wrong number or types of arguments`. Generated code now calls `lib.call_stored_procedure(spark, conn, sp_call, arg_values)` (runtime_lib), which probes `USER_ARGUMENTS` (fallback `ALL_ARGUMENTS`, cached per call name via `_SP_SIG_CACHE`) and binds OUT/IN OUT params to `DECLARE`'d VARCHAR2 variables (literals are rejected for them by PL/SQL); IN params get quoted literals (None → NULL). Falls back to the legacy all-literals call when the signature is unresolvable.
- The `Stored Procedure` instance type is recognized in the dispatch chain (logged at INFO, not WARNING).

### Session Target Pre/Post SQL (v2026.08.03)
- **Problem**: carrier mappings (e.g. `DUAL → DUAL`) hold their REAL logic in the session's Target **Pre-SQL / Post-SQL** attributes, not in the mapping graph. The converter only converted the mapping graph → 5 ETL mappings (BKP_DELETE/BKP_INSERT/DMNS_DELETE×2 sessions/FACT_DELETE/RECOVERY in WF_HOMES) became empty shells, and the DUAL write failed with `ORA-01031: insufficient privileges`.
- **Parser** (`parser.py`): the Pre/Post SQL nest under the target's `SESSTRANSFORMATIONINST → ATTRIBUTE` — iterate `session_elem.iter("ATTRIBUTE")` (NOT `findall`, which only sees direct children), concatenate multiple targets' SQL.
- **Workflow-layer SESSION_SQLS (refined)**: matching Informatica's layer architecture, `SESSION_SQLS` lives in the **generated wf_*.py** (flat, keyed by session name), NOT in the mappings. Only **non-empty fields** are emitted (a `"post_sql": ""` entry is omitted). `service.py` builds it from `workflow_analysis["sessions"]` (non-empty pre/post SQL only).
- **Runtime**: `lib.run_workflow(..., session_sqls=SESSION_SQLS)` → `execute_plan_step` → `run_sessions_sequential`/`run_sessions_parallel` pass `session_sqls=(session_sqls or {}).get(session_name)` to the mapping's `run_mapping(..., session_sqls=None)`. The two `S_HOM_ETL_DDS_DMNS_DELETE` sessions (different SQL per invocation) select their own SQL this way; standalone `main()` runs without session SQL. Pre-SQL executes after connection setup, Post-SQL after the pipeline succeeds — both via `lib.execute_sql(spark, conn_target, stmt)` (statements split on `;` at runtime).
- **DUAL write skip**: the WRITE_TARGET template treats `DUAL` like `/dev/null` (no-op target) — writing to DUAL would fail with ORA-01031.

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

### Decision Task (v2026.08.04)
- Informatica **Decision** tasks in WF_EMS_EX have empty `Decision Name` attributes and conditionless links — they are **batch barriers** between waves of parallel extract sessions (wave N sessions → Decision → wave N+1). **They ARE converted as real plan steps** (user preference: keep the structure visible); their DAG levels keep the session waves ordered, and the sequential `parallel_group` execution enforces the barrier.
- `TASK_INFO` entry: `{"type": "decision"}` — the `condition` field (Decision Name attr) is emitted **only when non-empty** (non-default config rule).
- **Misleading log removed** (`runtime_lib.py.j2`): the validation warning `"Task 'X' has no TASK_INFO entry, will skip"` is now `logger.debug("Task 'X' has no special handler, pass-through")` — tasks without a special handler (conditionless Decision, etc.) are pass-throughs, not skips, and debug is invisible at INFO level.
- **Never delete Decision nodes from the DAG** before leveling — that merges adjacent waves into one giant parallel group (all sessions parallel), losing the barrier semantics.

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

### py4j Log Suppression (v2026.08.04)
- **"Closing down clientserver connection"** (seen in every workflow log after parallel mapping groups) is **py4j's own Python-side logger** (`py4j/clientserver.py` `ClientServerConnection.close()` logs at INFO), NOT the Oracle JDBC driver — the message propagates from the `py4j` logger to root and lands in workflow log files with the Python formatter.
- **Fix** (`init_logger` in runtime_lib.py.j2): `logging.getLogger("py4j").setLevel(logging.WARNING)` — quiets `py4j`, `py4j.clientserver`, `py4j.java_gateway` (children inherit), keeps WARNING+ visible.
- `test_connection` sets `java.util.logging.Logger.getLogger("oracle.jdbc").setLevel(Level.SEVERE)` as defense against JVM-side oracle trace.
- JDBC reads must NOT set `isolationLevel` — Oracle JDBC driver rejects explicit isolation levels. Rely on Oracle's default `READ_COMMITTED`.

### Timer Task Support (v2026.08.04)
- **Problem**: Informatica Timer tasks (`TYPE="Timer"`, e.g. `START_RELATIVE_TO_PREVIOUSTASK` + `RECURRING 25 MINUTES`) generated as an empty `task` step — runtime logged "no commands defined" and completed instantly, so downstream worklets started immediately instead of after the 25-minute wait.
- **Semantics**: `START_RELATIVE_TO_PREVIOUSTASK` = start counting when the previous task completes, fire downstream after the RECURRING interval.
- **Parser** (`parser.py`): extracts `<TIMER TIMERTYPE=...><RECURRING DAYS/HOURS/MINUTES/></TIMER>` into `task["timer"]` (`timertype` + `days`/`hours`/`minutes`, ints with TypeError/ValueError guards).
- **service.py**: `task_info[name] = {"type": "timer", "timer": {...}}`.
- **Runtime** (`runtime_lib.py.j2`): `_apply_task_timer(tcfg, task_name)` sleeps `days*86400 + hours*3600 + minutes*60` seconds; called in BOTH task branches (`parallel_group` loop + standalone `task` step) after `tcfg` is fetched. Timer steps stay in their parallel group — since the group starts right after the timer's previous task completes, the sleep is equivalent to Informatica's relative timing.

### Unconnected INPUT Port → NULL (v2026.08.04)
- **Problem**: Expression transforms can declare INPUT ports that NO connector feeds (e.g. `ELDR_MBR_ID_TYPE_CODE/NUM/UNIT_KEY` in `M_S5_SSAL1_EXTRACT_TRF_REF_CASE` — added after the upstream SQ was refreshed). Generated code referenced them directly → `[UNRESOLVED_COLUMN]` crash.
- **Informatica semantics**: unconnected input ports hold **NULL** at runtime (or the port's DEFAULTVALUE if set) — legal, no error.
- **Fix** (`handlers.py` `_handle_expression`): compute `_connected_inputs` (all `conn.to_field` feeding this instance, lowercased); any INPUT-typed field not in the set is replaced in expression texts (word-boundary, case-insensitive) with `'defaultvalue'` (single-quoted, quotes doubled) or `NULL`. Runs before `:LKP.` replacement/remap so downstream translation sees valid SQL — `expr("ltrim(rtrim(NULL))")` returns NULL in Spark.

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

### $$ Variable Case-Insensitive Replacement (v2026.07.30)
- **Problem**: SQL queries in XML may reference `$$` mapping variables in any case (e.g. `$$V_SNSH_DATE` vs `$$v_snsh_date`), but the code generator uses the declared variable name's case for `.replace()` in the generated Python. Case-sensitive `.replace()` misses the mismatch, leaving raw `$$V_SNSH_DATE` in SQL → `ORA-00911: invalid character`.
- **Fix** (`handlers.py`): Added `_normalize_var_case(text, plan)` static method that case-insensitively normalizes all `$$` variable references in SQL/filter/expression texts to match the declared variable name's case before storing them in step params. Applied at every `$$`-containing text storage point: `sql_query`, `filter_inner` (3 places), `lookup_sql` (2 places), `expr_text`, and aggregator expressions.

### TO_DATE `$$` Variable Quote Wrapping (v2026.07.30, refined 2026.07.31)
- **Problem**: Some XML SQL texts have `TO_DATE($$v_rpt_mth, 'YYYYMM')` without quotes around the `$$` variable. After `.replace("$$v_rpt_mth", v_rpt_mth)`, the bare numeric value `TO_DATE(202606, 'YYYYMM')` causes `ORA-00936` because Oracle's `TO_DATE` expects a string first argument.
- **Fix** (`handlers.py`): Two separate normalization methods to avoid double-quoting:
  - **`_normalize_var_case(text, plan)`** — case normalization ONLY. Use for **expression texts** (computed columns, aggregators): `expr_translator._replace_pm_variables` already wraps `$$` variables in quotes (empty-value else branch adds `'$$var'`), so adding TO_DATE quotes here would produce `''20260615''` → `[PARSE_SYNTAX_ERROR]`.
  - **`_normalize_sql_text(text, plan)`** — case normalization + TO_DATE quote wrapping. Use for texts that keep `$$` markers until template `.replace()`: SQL pushdown `sql_query`, lookup SQL, and filter conditions (where `translate_for_filter` strips `$$` before translation, so `_replace_pm_variables` never sees them).
- **Call sites**: `sql_query`, `filter_inner` (SQ + Apply Filter + Router), `lookup_sql` (2 places) → `_normalize_sql_text`; expression `expr_text` → `_normalize_var_case`.
- **Failure mode if wrong**: expression texts with TO_DATE quote wrapping produce `to_date(''20260615'','yyyymmdd')` → Spark `[PARSE_SYNTAX_ERROR] Syntax error at or near '20260615'`.

### Dynamic Lookup NewLookupRow (v2026.07.31)
- **Problem**: Dynamic Lookup transforms expose a `NewLookupRow` output port (1 = no match, 0 = match), referenced by downstream filters (e.g. `NewLookupRow > 0` keeps only unmatched rows). The generated merge DataFrame had no such column → `[UNRESOLVED_COLUMN] A column ... with name \`NewLookupRow\` cannot be resolved`.
- **Fix** (`handlers.py` + `mapping.py.j2`): Handler detects `NewLookupRow` with `DYNLOOKUP` port type on the lookup transform and sets `new_lookup_row_key` param. Template generates the column after the merge select: `withColumn("NewLookupRow", expr("CASE WHEN \`<probe>\` IS NULL THEN 0 ELSE 1 END"))`.
- **Semantics (confirmed with user)**: `NewLookupRow` = **0 when no match, >0 when a match was found** — `NewLookupRow > 0` keeps MATCHED rows (UPSERT update path). (Informatica docs sometimes state the opposite; this project follows the user's verified behavior.)
- **Probe column**: chosen at runtime as the first lookup column that **survived the merge select** (`c not in _lkp_input.columns`) — same-named columns are shadowed by main-table values (never NULL), so join keys like `CODE_ADDR` cannot be used. Falls back to `lit(0)` if no lookup column survives.

### SQL Column Name-Match-First Rename (v2026.07.31)
- **Problem**: Pure positional SQL→port renaming misaligns when the SQL returns fewer columns than `_port_cols` and the gap is NOT at the end. E.g. SQL returns 11 cols but 13 ports with `TNCY_AGRMT_TRMT_DATE`/`REC_RLS_IND` missing in the middle — `EST_DMNS_KEY` position receives `LAST_REC_TXN_DATE`'s TIMESTAMP value → `[DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES] "TIMESTAMP" and "DECIMAL(10,0)"` in join conditions.
- **Fix** (template `mapping.py.j2`): Two-pass rename map — ① case-insensitive **name match first** (SQL column → same-named port), ② **positional fallback** only for remaining SQL columns (unaliased expressions like `DEL_STS.A||DEL_STS.B`). Missing ports stay absent and get `lit(None)` in the final select.

### Exit Code Propagation (v2026.07.31)
- **Problem**: `main()` returned 1 on failure but `if __name__ == "__main__": main()` discarded it — process exited 0, YARN reported `SUCCEEDED` even when the mapping failed.
- **Fix 1**: Both mapping and workflow templates use `import sys as _sys; _sys.exit(main())`.
- **Fix 2 (client mode)**: A normal `spark.stop()` + python exit code still reports `SUCCEEDED` — the AM lives in the driver JVM and exits cleanly regardless of the python exit code. On failure, `main()` now calls `spark.sparkContext._jvm.System.exit(1)` (before `spark.stop()` in `finally`), which exits the JVM non-zero so YARN marks the application `FAILED`.

### SQ Port Select lit(None) Fallback (v2026.07.31)
- **Problem**: When an SQL pushdown query returns fewer columns than the Source Qualifier's output ports (`_port_cols`), the final `.select(*_port_cols)` references columns that don't exist → `cannot resolve 'LAST_REC_TXN_DATE' given input columns: [...]`.
- **Fix** (template `mapping.py.j2`): Both SQL pushdown and non-pushdown Source Qualifier selects now use `df.select([col(c) if c in df.columns else lit(None).alias(c) for c in _port_cols])` — ports the SQL didn't return become `lit(None)` so downstream references never fail.
- **Renaming order**: Positional rename loop first aligns SQL columns to port names, then the select fills in missing ports with `lit(None)`.

### Unaliased SQL Expression Column Backtick (v2026.07.31)
- **Problem**: SQL pushdown queries with unaliased expressions (e.g. `SELECT est.est_key, del_sts.ADTN_DEL_RSN_CATG_CODE || del_sts.ADTN_DEL_RSN_CODE, ...`) make Oracle return the expression text as the column name (`DEL_STS.ADTN_DEL_RSN_CATG_CODE||DEL_STS.ADTN_DEL_RSN_CODE`). `col("DEL_STS.ADTN_DEL_RSN_CATG_CODE||...")` fails with `cannot resolve 'DEL_STS.\`ADTN_DEL_RSN_CATG_CODE||DEL_STS\`.ADTN_DEL_RSN_CODE'` because Spark splits dotted names into qualifiers.
- **Fix** (template `mapping.py.j2`): The position-based rename select uses `col(f"`{old}`").alias(new)` — backtick-quoting the actual column name so Spark treats dots/pipes as part of a single column name.

### Aggregator Fallback expr() Wrapping (v2026.07.31)
- **Problem**: Complex aggregation expressions without an aggregate function wrapper (e.g. raw `CASE WHEN UNIT_ALCT_STS_CODE = '1' THEN 'NPA' ELSE ... END`) fell through to `_translate_aggregation_expr`'s fallback path, which returned the raw translated text. The template rendered `{{ agg_expr }}.alias(...)` → `CASE WHEN ... END END.alias("VCNT_FLAT_TYPE_CODE")` → `SyntaxError: invalid syntax`.
- **Fix** (`handlers.py`, `_translate_aggregation_expr` fallback): The fallback now always wraps translated text with `expr("""...""")` — `expr("""CASE WHEN ... END""").alias("VCNT_FLAT_TYPE_CODE")` renders valid Python. The f-string path (`expr(f"""...""")`) for `$$` mapping variables is unchanged.

### Lookup Merge Case-Insensitive Exclusion (v2026.08.03)
- **Problem**: the lookup merge select excluded same-named lookup columns with a case-sensitive check (`c not in _lkp_input.columns`). SQ ports may be lowercase (`dstr_grp_disp_seq_num`) while Oracle lookup columns are uppercase (`DSTR_GRP_DISP_SEQ_NUM`) → the exclusion missed, both columns survived the merge, and Spark's case-insensitive resolution raised `Reference 'dstr_grp_disp_seq_num' is ambiguous`.
- **Fix** (template `mapping.py.j2`, all four spots): case-insensitive membership tests —
  - `_cc` common-column detection: `c.lower() in [x.lower() for x in lookup.columns]`
  - `__lkp_dup` duplicate detection: lookup col matched case-insensitively against input columns and `_cc`
  - merge select exclusion: `c.lower() not in [x.lower() for x in _lkp_input.columns]`
  - NewLookupRow probe columns (`_nlr_lkp_cols`): same case-insensitive exclusion
- **Full class sweep (same session)**: every remaining case-sensitive column-membership check in `mapping.py.j2` is now case-insensitive — SQ port selects (`col(c) if c.lower() in [x.lower() for x in df.columns] else lit(None)`), expression/joiner/union passthrough fills (`if _col.lower() not in [...]`), target `_field_map` rename guard, Update Strategy `_upd_set_cols` key exclusions, and the final `_target_cols` select.

### Union NullType → JDBC void (v2026.08.03)
- **Problem**: `unionByName(allowMissingColumns=True)` can leave missing-side columns as `NullType` (and upstream `lit(None)` fills stay NullType through the union); JDBC writes then fail with `Can't get JDBC type for void`.
- **Fix** (template `mapping.py.j2` WRITE_TARGET block): cast any NullType column to `StringType` at write time — but ONLY when the target is fed **directly by a union output** (`_handle_union` records its `df_output` in `self._union_output_dfs`; `_handle_target` sets `cast_nulltype` when `input_df` is in that set). The runtime check is a no-op when the union produced no NullType columns. Mappings without a union in the write path generate no cast code (66 → 30 files in WF_HOMES).

### Connected Sequence Generator (v2026.08.03)
- **Problem**: Connected Sequence Generators (no upstream connectors) only provide the `NEXTVAL` port to downstream transformations (all 30 in WF_HOMES feed a `FIL_NEW` Filter). `_handle_sequence` fell back to `input_df = "df_input"` when no input resolved → generated `df_SEQ_XXX = df_input.withColumn(...)` → `NameError: name 'df_input' is not defined`. It also registered `df_SEQ_*` in `current_df_map`, so the downstream filter sometimes resolved ITS input from the sequence df instead of the real data path (`__fil_input = df_SEQ_*`).
- **Fix** (`handlers.py`): when a sequence has no upstream, do NOT emit a standalone step or register a df — record the attachment in `self._sequence_attachments` (`{consumer_instance: [{"col": to_field, "start": start_value}]}` via connector `from_instance` lookup) and return None. `_handle_filter` copies it to `step.params["sequence_attach"]`, and the filter template renders `df_FIL_NEW = df_FIL_NEW.withColumn("NEXTVAL", monotonically_increasing_id() + <start>)` after the filter (post-filter placement, matching validated manual fixes). The filter's input now correctly resolves to the data path (`df_EXPTRANS`/`df_EXPTRANS1`).
- Sequences WITH an upstream still emit the standalone `ApplySequenceStep` (df_input path unchanged).

### Sequential Lookup Merge Chain (v2026.07.30)
- **Problem**: When multiple lookups feed from different intermediate instances, the code generator created parallel merge branches from the same base DataFrame. Downstream components needing columns from BOTH branches (e.g. `CUR_RENT` + `PREV_RENT`) got `cannot resolve 'CUR_RENT'`.
- **Fix** (`handlers.py`): Added `_last_chain_output` field tracking the most recent chain merge. When a lookup starts a new chain, it chains onto `_last_chain_output` instead of the raw `input_df`, making lookups sequential: `merge_1 → merge_2 → merge_3`.
- **Refinement (v2026.07.31)**: Only use `_last_chain_output` when the resolved `input_df` is a raw chain/merge/SQ result (`df_lkp_merge*` / `df_merge*` / `df_sq_*`). If the lookup is fed by a downstream transformation (e.g. `EXPTRANS` after an Aggregator), that DataFrame already carries all computed columns — use it directly instead of a stale merge (e.g. `LKP_DDS_DMNS_EMS_RGN` fed by `df_EXPTRANS`, not `df_lkp_merge_1`).
- **Example**: `LKP_CUR_RENT1 → df_lkp_merge_2 (has CUR_RENT)`, then `LKP_PREV_RENT1 → df_lkp_merge_3` uses `df_lkp_merge_2` as input (has `CUR_RENT`) instead of `df_lkp_merge_1`.

## Runtime Environment (v2026.07.31)

### YARN Mode Configuration
- **Profile switching**: `env/config.yml` `spark.connection` selects the runtime environment — `spark_local` (local[*]), `spark3_client` (YARN client), `spark3_on_yarn` (YARN cluster). Default is `spark3_client`. Override per-run via `SPARK_CONNECTION` env var.
- **`spark.home`** (config.yml): CDP parcel path (`/opt/cloudera/parcels/SPARK3-3.5.4.../lib/spark3`). The **`runtime_lib.py` module-level bootstrap block** (before any pyspark import) reads it and injects `${home}/python` into `sys.path` — lets `python m_xxx.py` run directly without `PYTHONPATH`/`SPARK_HOME` env vars. Generated `m_*.py`/`wf_*.py` stay clean (`import env.runtime_lib as lib` only).
- **Worker/driver interpreter lockstep (v2026.08.03)**: `get_spark_session` sets `spark.pyspark.python = sys.executable` before `getOrCreate()` — executors always use the same interpreter as the driver, eliminating `[PYTHON_VERSION_MISMATCH]`. Run with plain `python` (3.6) or `python3.11` — both work as long as that interpreter exists on the executor nodes. **Do NOT hardcode `spark.pyspark.python` in config.yml** (was previously `/usr/bin/python3.11`, which mismatched a 3.6 driver).
- **Auto-kinit**: `get_spark_session` runs `klist -s` when a kerberos profile is selected; if no valid ticket, automatically `kinit -kt <keytab> <principal>` from the profile's `spark.kerberos.keytab` / `spark.kerberos.principal` — no manual kinit needed.
- **Executor memory**: cluster max container is 8192 MB → executor memory must be ≤ ~7g (8g + 10% overhead = 9011 MB exceeds `yarn.scheduler.maximum-allocation-mb` → `IllegalArgumentException`). Currently 4g.
- **OJBC driver loading**: `addJar()` in `get_spark_session` loads jars from `spark.driver.extraClassPath`/`spark.jars` at runtime — `spark.driver.extraClassPath` only takes effect at JVM startup via spark-submit; direct `python` runs need `addJar()`.

### Update Strategy Semantics (v2026.07.31)
- **Problem**: Static `DD_UPDATE` strategies were not detected in `_handle_write_target` (only `DD_DELETE` and dynamic field strategies were) → the write fell through to plain `append`, silently discarding `_update_flag` — no key-based update/delete happened.
- **Fix** (`handlers.py`): Detect static `DD_UPDATE` → `has_update_flag=True`; UPDATE/DELETE key columns come from the **target definition's KEYTYPE** (fields containing "PRIMARY"), falling back to the connector's `from_field`.
- **Semantics (static DD_*, no `_update_flag` split)**:
  - `DD_INSERT` → Update Strategy step passes through; target write appends directly
  - `DD_UPDATE` → Update Strategy step passes through; target write `lib.batch_update()`s ALL rows by primary key, then writes an empty INSERT df (`filter(lit(False))`)
  - `DD_DELETE` → Update Strategy step passes through; target write `lib.batch_delete_composite()`s ALL rows by primary key
  - Only **dynamic field strategies** (strategy expr references a field, e.g. `UPDATE_FLAG`) generate the `_update_flag` when() split + INSERT/UPDATE/DELETE branch code
- **Distinction**: `static_dd` param ("DD_INSERT"/"DD_UPDATE"/"DD_DELETE") is set on both the Update Strategy step and the write step; the write template branches on `static_dd == 'DD_UPDATE'` vs dynamic-field split.
- **Template order fix** (`mapping.py.j2`): the connector `_field_map` rename now runs **before** the `_update_flag` split, so `batch_update`/`batch_delete` use target column names (e.g. `TNCY_AGRMT_CMNC_DATE1` → `TNCY_AGRMT_CMNC_DATE`), not source names.

### Target Type Cast Removed (v2026.07.31)
- The "Cast columns to match target schema data types" block was **removed** from the target write template (per user request, simplify generated code). JDBC writes rely on Spark's native type coercion instead.
- Kept: connector `_field_map` rename loop, unmapped-column `lit(None)` fill, and the final `_target_cols` select.

### Spark Log Level (v2026.07.31)
- `spark.log_level` in config.yml (`${SPARK_LOG_LEVEL:ERROR}`) — applied via `spark.sparkContext.setLogLevel()` in `get_spark_session` right after `getOrCreate()`, suppressing the default WARN noise and Java log4j chatter.
- Valid values: ALL, DEBUG, INFO, WARN, ERROR, FATAL, OFF. Override per-run via `SPARK_LOG_LEVEL` env var.

### Credential Provider (passwords.jceks)
- **`_resolve_password` force-set**: Before `getPassword(alias)`, the code force-sets `hadoop.security.credential.provider.path` to the `_resolve_path()`-resolved value. The raw config value contains `$(pwd)` literals; if Spark auto-injected it at SparkContext init without resolution (or injection was skipped), `getPassword` looks up a literal path and fails → interactive password prompt. Force-setting guarantees resolution.
- **Injection isolation**: `addJar()` loading and `spark.hadoop.*` injection are in **separate try blocks** in `get_spark_session` — a jar-loading failure cannot skip the provider-path injection.
- **Save flow**: interactive passwords cached in `_PASSWORD_PENDING`, flushed to jceks via `hadoop credential create/update` only after successful connection validation (`_flush_pending_passwords`).

### Local File Read/Write (YARN-safe, v2026.08.03)
- **Write side** (`write_file`): local targets (bare absolute paths without a URI scheme) are written on the DRIVER, never by Spark executors. Two failure modes were hit in YARN mode: ① Spark resolves bare paths against the default filesystem (HDFS) → data lands on HDFS, driver-side rename finds nothing locally → target file silently never appears (mapping still reports SUCCESS; `....__tmp__/part-*.csv` found on HDFS); ② forcing `file://` → every executor tries to `Mkdirs` the target dir on its OWN node → `java.io.IOException: Mkdirs failed` (dir doesn't exist / not writable on worker nodes). Fix: CSV local targets are collected on the driver and written with Python's `csv` module (`_write_local_csv`, single file, header like Spark); non-CSV local targets are staged on HDFS (`/tmp/pcis01_output/<basename>`) then copied back with `copyToLocalFile`; explicit URIs (`hdfs://`, `s3://`, ...) are written by Spark as-is.
- **Problem**: `addFile()` + `"file://" + SparkFiles.get(...)` broke in YARN client mode — `SparkFiles.get()` on the DRIVER returns the driver's own temp path (`/tmp/spark-<app>/userFiles-<driverUUID>/...`), which does not exist on executors (separate JVMs, separate roots) → `SparkFileNotFoundException` on the executor. It only worked in local mode because driver and workers share the JVM.
- **Fix** (`runtime_lib.py` `read_file`): three tiers for local files —
  1. **Small CSV control files (≤64 MB)** are read on the DRIVER (`_read_local_csv` → `spark.createDataFrame`) — no executors involved, works in YARN and local mode regardless of HDFS. Header=true → first row becomes column names; no header → `_c0, _c1, ...` (matches Spark CSV defaults); empty file → empty schema (mapping falls into the "empty or has no columns" warning branch).
  2. **Larger local files** are staged to HDFS (`/tmp/pcis01_input/<basename>` via `copyFromLocalFile`) and read from the default filesystem, so every executor sees the same path.
  3. **Fallback** (`addFile` + driver-side `SparkFiles` path) only if staging fails — usable when driver and executors share the filesystem (local mode).
- The `hdfs://nameservice1/...` re-resolution problem (bare paths being resolved against HDFS) is bypassed by tiers 1–2.
- **Missing local file (v2026.08.03)**: a local-style path (bare absolute / `file://`) that does NOT exist on the driver is NOT passed to Spark (would resolve to HDFS → confusing `[PATH_NOT_FOUND] hdfs://...`). `read_file` warns and returns an empty DataFrame instead, so mappings over absent control files (table lists, session lists) run empty through the callers' "empty or has no columns" path.

### Direct Run (no env vars, no spark-submit)
```bash
cd .../WF_EMS_DDS_APLY_MTH
python m_<mapping>.py        # or wf_<workflow>.py   (python3.11 also works)
```
Requires: cwd = workflow dir (import env.runtime_lib), `spark.home` configured, keytab path valid. Default `python` (3.6 on RHEL8) works — the CDP parcel's pyspark 3.5.4 runs under the system python3; `python3.11` also works.

## Conversion Progress

As of **2026-08-07** (version **v2026.08.07**), **9 of 10 workflows (549 mappings)** have converted output present in the current workspace (`PySpark_workflows/`); every completed conversion run reports **0 warnings / 0 errors**. WF_NHS_TL (174 mappings) has been runtime-verified. WF_EMS_TL (581 mappings) is the only workflow without converted output in the current workspace. Feature Coverage rows that reference `WF_EMS_TL` reflect earlier-round testing (v2026.07.20-era output) and must be re-validated once WF_EMS_TL is reconverted.

### ✅ Runtime Verified
| Workflow | Mappings | XML Size | Status |
|----------|----------|----------|--------|
| WF_GMS_DDS_APLY_DLY | 8 | 340K | **Data-validated**, zero warnings |
| WF_CMS_DDS_APLY_MTH | 10 | 1.2M | **Data-validated**, 16 mapplets inlined |
| WF_HOMES_DDS_APLY_DMNS | 67 | 2.4M | **Round complete (v2026.08.04)** — case-insensitive columns, Sequence Generator, NullType cast, session Pre/Post SQL (SESSION_SQLS in workflow layer), DUAL no-op, conditional connections, local file read |
| WF_EMS_PRHE_DDS_APLY_RVN_MTH | 25 | 3.1M | **Round complete** (fixed 8) |
| WF_EMS_PRHE_DDS_APLY_HSE_STCK_MTH | 28 | 3.9M | **Round complete** (fixed 15+) |
| WF_EMS_DDS_APLY_MTH | 49 | 4.0M | **Round complete (v2026.08.03)** — schema `{_schema}` ireplace, SP signature probing (`call_stored_procedure` + OUT binding), YARN-safe local file reads, interpreter lockstep, Update Strategy |
| WF_EMS_EX | 142 | 9.6M | **Round complete (v2026.08.04)** — Oracle date format (`hh24/mi` → Spark patterns), py4j log suppression, Timer task wait, unconnected input port → NULL, Decision task barrier |
| WF_NHS_EX | 46 | 2.2M | **Round complete (v2026.08.04)** — Oracle date format (`hh24/mi` → Spark patterns), py4j log suppression |
| WF_NHS_TL | 174 | 11M | **Round complete (v2026.08.07)** — all mappings runtime verified; lookup/FILTRANS wiring, mapplet rename/dynamic lookup, DECODE NULL semantics, stable df naming |

### ❌ Not Converted (current workspace)
| Workflow | Mappings | XML Size | Notes |
|----------|----------|----------|-------|
| WF_EMS_TL | 581 | 36M | Transform & load (largest). **No output directory in the current workspace.** Conversion attempt logged 2026-08-04 18:18 (`convert_infa-pyspark.log`) did not complete; the earlier v2026.07.20-era output was removed (git commit 239eb18). TODO: rerun conversion, then runtime-validate |

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
| Timer task (START_RELATIVE_TO_PREVIOUSTASK + RECURRING) | WF_EMS_EX | ✅ |
| Decision task (conditionless batch barrier) | WF_EMS_EX | ✅ |
| Unconnected Expression input port → NULL | WF_EMS_EX | ✅ |
| Oracle date-format literal → Spark pattern (`hh24`, `mi`) | WF_NHS_EX, WF_EMS_EX | ✅ |
| py4j log suppression ("Closing down clientserver connection") | All workflows | ✅ |
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
| Dynamic Lookup / dynamic components (full semantics) | — | ⏳ Pending — 动态组件的转换待修复；当前仅生成 `NewLookupRow` 0/1 |

## Known Manual-Fix Bugs (Deferred)

The following bugs are **not fixable in the current round** — they live in already-generated mapping code or need fixes the converter cannot yet automate. They are recorded here so they are not lost, and each item must be revisited (ideally fixed at the generator level) in a future round.

Source of record: `convert_informatica_pyspark.md` (# 需手动fix的Bug). The `Workspace check` column records whether the pattern still appears in the current generated output as of 2026-08-07.

| # | Affected generated file(s) | Problem | Required manual fix | Workspace check (2026-08-07) |
|---|---------------------------|---------|---------------------|------------------------------|
| 1 | `m_dpa_summarize_fact_cms_case_smry.py`, `m_dpa_summarize_fact_cms_case_ostd_smry.py` | Multiple lookups expose same-named fields (e.g. `CASE_CATG_KEY`) | Modify SQL to distinguish the field names (e.g. alias `CASE_CATG_KEY` per lookup) | Pattern still present in current output — **open** |
| 2 | `m_s5_dds_aply_fact_ems_sms_aply_type_txn.py` | `RLS_CNTL_DMNS_TYPE_CODE` collides with other columns | Rename to `DDS_RLS_CNTL_DMNS_TYPE_CODE` | Rename already present in current output — verify at runtime |
| 3 | Numeric → string columns (e.g. `rec_rls_ind`) | Scientific notation in output | Explicit decimal type before string cast | No `rec_rls_ind` match in current output; framework "Decimal → String casting" rule applies — verify at runtime |

## Known Pending Items (待修复)

- **动态组件的转换待修复**: full semantics of Informatica dynamic components (e.g. Dynamic Lookup cache/update behavior, dynamic update-strategy flows) are not yet converted. Current generated code covers only partial aspects (e.g. `NewLookupRow` 0/1 indicators). This must be revisited at the converter level in a future round.
- **WF_NHS_TL 临时注释**: the generated `WF_NHS_TL/wf_nhs_tl.py` currently has the `WL_NHS_SOR_LOAD_FOR_UPD_END` worklet block commented out (temporary disable) while dynamic component conversion is pending; it should be restored once the converter handles the full dynamic semantics.

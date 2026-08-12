# Component Methods Refactor — lib.xxx Wrappers for All Transformation Types (2026-08-12)

## Goal

Generalize the `lib.dynamic_lookup(...)` pattern to every Informatica
transformation type: generated mapping files become a declarative data flow of
`lib.<component>(...)` calls, uniform across components. The flagship target is
the Expression component — `lib.expression(input_df, input/output/variable
fields, expressions)`.

Motivation (user-confirmed): uniform, concise generated code; each component
is a reusable method that defines a dataset/step of the data flow; the
generated file reads like the Informatica mapping it came from.

## Architecture

Three-layer separation (the dynamic_lookup precedent, generalized):

| Layer | Owns | Example |
|---|---|---|
| Generator (`handlers.py`, `expr_translator.py`) | **Syntax conversion**: XML → step config (translated expressions, column-name pairs, join predicates) | Lookup condition → `join_predicates`; Informatica expr → Spark SQL string |
| `runtime_lib.py.j2` methods | **Runtime semantics**: state machines, ordering, null handling, dedup policies, probe-column selection, fallbacks | dynamic_lookup cache state machine; `_rename_columns` drop-first; NewLookupRow probe |
| Generated mapping files | **Declarative data flow** | `df = lib.expression(spark=spark, input_df=..., name='...', ...)` |

**Confirmed decisions**
- Translation stays at codegen time. `lib.*` methods receive translated
  results only (same convention as `dynamic_lookup` kwargs).
- All 12 component types are wrapped (Approach A, user-selected).

## Conventions (every lib method)

1. **kwargs call form**: `lib.xxx(spark=spark, input_df=..., name='...', <config
   keys>, config=config)` — matches `dynamic_lookup`. `spark` is passed for
   signature uniformity (only used by methods that collect/JDBC, e.g. expression
   with `:SP.` calls).
2. **Rendering**: scalar values via `pyrepr` filter; complex lists (e.g.
   `computed_columns`) one dict per line — same pattern as
   `lookup_output_fields` (whole-list repr breaks continuation indentation).
3. **Shared helpers in runtime_lib** (private, unit-tested):
   - `_with_column(df, name, expr_str)` — centralized expr-vs-API detection
     (`'row_number()' in s`, `'monotonically_increasing_id' in s`,
     `'last(when(' in s` → direct API, else `expr(...)`). The template's string
     heuristics move verbatim.
   - `_rename_columns(df, renames)` — drop-target + withColumnRenamed,
     case-insensitive duplicate protection, skip `src == tgt`.
   - `_fill_missing(df, cols)` — case-insensitive membership check, `lit(None)`
     for absent columns.
4. **`$$` mapping-variable substitution rules are per-context, moved into the
   methods**: Filter/SQ condition → `str(v or "0")`; Expression computed
   columns → `str(v)`; SQL query → raw value. Generated code only reads values
   from UTL_JOB_PARAM and passes `substitutions={'$$v_x': v_x}`; the method
   applies its documented rule. Semantics identical to today's templates.
5. **Error handling**: `ValueError`/`RuntimeError` messages carry the component
   name (`Filter FILTRANS3: ...`). Report Error policies keep `raise`.
6. **Logging**: generated `logger.info("Step: ...")` stays in the generated
   file (unchanged); lib methods log at debug only.
7. **Compatibility**: new method names; existing generated files are never
   rewritten and keep working (inline form). No cfg-dict legacy layer needed
   (old files don't call the new methods). Already-verified workflows
   (WF_NHS_TL etc.) keep their inline form until migrated opportunistically.
8. **Multi-output components** (Router): method returns `dict[group -> df]`;
   generated code unpacks `df_rtr_G1 = _rtr['G1']`.

## Component classification & feasibility

| Component | Moves into lib (runtime semantics) | Stays in generator (conversion) | Risk |
|---|---|---|---|
| **Expression** (flagship) | renames (drop+dup-protect), computed columns (expr/API detect + $$), passthrough fills, inline-lookup joins, `:SP.` per-row calls | expr_translator, unconnected-port→NULL, multi-input join pre-steps | 🟡 medium |
| **Filter** | renames, condition ($$ `or "0"`), sequence_attach (NEXTVAL after filter) | condition translation, bare-numeric→`!= 0` | 🟢 low |
| **Static Lookup** | alias join + broadcast, merge select (case-insensitive exclusion), **NewLookupRow probe-column runtime selection** (+`lit(0)` fallback), dedup (Report Error / Last / First), common-cols synthetic-key fallback, lkp port pre-rename | join predicate columns, lookup SQL | 🟠 medium-high (most branches: 4 join_spec kinds) |
| **SQ (port handling)** | **two-pass rename (name-first + positional + backtick) — depends on actual query result columns, cannot be precomputed at codegen**; port select + lit(None); type casts | SQL build, schema parameterization, filter translation | 🟡 medium |
| **Joiner** | master/detail select+alias intermediates, join execution, missing-col fills | port detection (master/detail), join condition build | 🟡 medium |
| **Aggregator** | groupBy+agg, literal GROUPBY columns, DISTINCT path, missing-col fills | agg expression translation, groupby detection, multi-input pre-steps | 🟠 medium-high |
| **Router** | per-group filter + REF_FIELD renames, DEFAULT group | condition translation, group build | 🟢 low |
| **Union** | per-group select + lit(None) fills, unionByName | column list build | 🟢 low |
| **Sorter** | renames, orderBy (ISSORTKEY/direction) | sort-key detection | 🟢 low |
| **Sequence** | NEXTVAL = monotonically_increasing_id + start; connected case stays as filter `sequence_attach` | attachment-point detection | 🟢 low |
| **Update Strategy** | dynamic when() branch → `_update_flag`, static_dd pass-through | strategy expr translation, KEYTYPE primary-key detection | 🟡 medium |
| **Write Target** | field_map rename (case-insensitive drop), unmapped lit(None) fill, target select, NullType→String, DD_UPDATE/DELETE branches (batch_update/delete_composite), DUAL no-op | key/update-column detection | 🟡 medium |
| Read / SQL / SP | already lib (`read_sql`/`read_file`/`call_stored_procedure`) | — | — |

Feasibility: all 12 types are feasible under the codegen-translation
convention. Benefit scales with the amount of runtime semantics in the
template block; thin components (sorter, sequence) are wrapped purely for
form uniformity.

**Correctness discipline**: every template block is moved **verbatim** —
a unit test locks the behavior first, then the template code is deleted. No
"while we're here" optimizations.

## Method signatures (draft)

```python
# Flagship
df = lib.expression(
    spark=spark, input_df=_in, name='EXPTRANS',
    rename_columns=[('OUT_BKEY', 'IN_TNCY_CNCL_BK')],
    computed_columns=[{'name': 'DUMMY_DATE', 'expr': 'NULL'},
                      {'name': 'FLAT_RCVR_BK', 'expr': "rpad(...) || ..."}],
    pass_through_cols=['CUST_KEY', ...],
    substitutions={'$$v_rpt_mth': v_rpt_mth},            # only when $$ present; rule: str(v)
    inline_lookup_joins=[{'lookup_df': ..., 'join_predicates': [...], 'return_port': ...}],  # optional
    sp_calls=[{'sp_call': ..., 'sp_conn': ..., 'args': [...]}],                              # optional
    config=config,
)

df = lib.filter(
    spark=spark, input_df=_in, name='FILTRANS3',
    rename_columns=[...], condition='OUT_DLPK_SOR_CACHE != 0',
    substitutions={'$$v_rpt_mth': v_rpt_mth},            # rule: str(v or "0")
    sequence_attach=[{'col': 'NEXTVAL', 'start': 1}],    # optional; attached after filter
    config=config,
)

df = lib.static_lookup(
    spark=spark, input_df=_lkp_input, lookup_df=_lkp, name='LKP_CUR_RENT1',
    join_spec={'kind': 'predicates'|'common_cols'|'expr'|'cross',
               'predicates': [{'source_col': ..., 'lookup_col': ...}]},
    lookup_type='left', broadcast=True,
    lkp_field_remap={'IN_PORT': 'ACTUAL_COL'},           # optional
    dedup={'policy': 'report_error'|'last'|'first', 'keys': [...]},  # optional
    new_lookup_row_key='NewLookupRow',                   # optional; probe chosen at runtime
    config=config,
)
```

Remaining skeletons:

- `lib.sq_output(input_df, port_cols, column_types, filter_condition,
  substitutions, distinct)`
- `lib.joiner(master_df, detail_df, join_type, master_selects,
  detail_selects, on, missing_cols)`
- `lib.aggregator(input_df, group_by, literal_groupby, aggs, distinct,
  agg_selects, missing_cols)`
- `lib.router(input_df, groups=[{name, condition, renames}])` → dict
- `lib.union(name, inputs=[{df, selects}], output_cols)`
- `lib.sorter(input_df, rename_columns, sort_by=[{col, asc}])`
- `lib.sequence(input_df, output_col, start)`
- `lib.update_strategy(input_df, strategy, strategy_expr, static_dd)`
- `lib.write_target(df, conn, table, mode, field_map, target_cols,
  update_flag_col, key_cols, static_dd, cast_nulltype, is_dual)`

## What stays in generated code (unchanged)

- Step logs (`logger.info("Step: ...")`), `ctx.register_df(...)` registration
- UTL_JOB_PARAM variable loading (per-mapping, values passed into
  `substitutions`)
- Read steps (`lib.read_sql` / `lib.read_file` calls), multi-input join
  pre-steps (`join_*` — pure data-flow joins, not component semantics)
- `lib.dynamic_lookup` calls (already in the target form)

## Verification strategy

1. **Unit tests lock behavior (primary gate)** — new tests in
   `informatica_sparker/tests/` following `test_dynamic_lookup.py` (local
   SparkSession):
   - helpers: `_rename_columns`, `_with_column` (expr/API detection),
     `_fill_missing`, $$ substitution rules
   - methods: expression full ordering (rename→inline lookup→computed→
     passthrough), filter (rename-before-condition, sequence_attach after),
     static_lookup (4 join_spec kinds, merge case-insensitive exclusion,
     probe runtime selection + lit(0) fallback, 3 dedup policies),
     sq_output (two-pass rename, backtick, lit(None)), router multi-output,
     union missing cols, write_target DD_* branches + DUAL no-op
2. **Reconversion gate**: rebuild wheel → reconvert test workflow → **0
   warnings / 0 errors**.
3. **Sample runtime verification**: run 3-5 representative mappings on the
   cluster (SUCCESS + data sanity) — pick mappings that mix the most covered
   semantics (expression chains + dynamic lookup + update strategy + union)
   so each phase's methods are exercised. Standard per-round practice.
4. **Optional old/new equivalence** (Gate 4): back up the test workflow's
   current generated output to the sibling directory
   `PySpark_workflows/dds/_pre_refactor_WF_EMS_DDS_APLY_MTH/` (before the
   first reconversion), then for 2-3 of the most complex mappings run both
   old and new generated files against the same source data and compare
   target-table row deltas + key column values. Note: writes hit the real
   target tables (append/upsert semantics, deltas comparable); Gate 4 can be
   skipped if touching real targets is unacceptable — Gates 1-3 suffice.

## Test workflow

**WF_EMS_DDS_APLY_MTH** (49 mappings, runtime-verified round v2026.08.03 —
known-good behavior baseline). Component coverage: Expression 49/49, Lookup
48, Aggregator 38, Filter 25, Update Strategy 22, Union 19, Router 6, Joiner
2, Sorter 1, SQ 49/49, Write Target 49/49. **Sequence: 0** (only WF_HOMES has
it) — covered by unit tests as primary gate; runtime verification deferred to
a later workflow migration.

WF_EMS_TL (581 mappings) stays a later full-conversion round (it is already
queued for the dynamic-lookup variants).

## Phased plan

Each phase: change template + handlers → add unit tests → rebuild wheel →
reconvert WF_EMS_DDS_APLY_MTH → sample runtime verification → phase sign-off.

| Phase | Scope | Risk |
|---|---|---|
| **0** | Infrastructure: shared helpers in `runtime_lib.py.j2` + helper unit tests; templates untouched | 🟢 |
| **1** | `lib.expression` + `lib.filter` (flagship + most common) | 🟡 |
| **2** | `lib.static_lookup` + `lib.joiner` + `lib.aggregator` (heaviest semantics) | 🟠 |
| **3** | `lib.router` + `lib.union` + `lib.sorter` + `lib.sequence` | 🟢 |
| **4** | `lib.sq_output` + `lib.update_strategy` + `lib.write_target` (full coverage finish) | 🟡 |

After all phases: update CLAUDE.md with the component-method conventions and
per-method responsibilities.

## Out of scope

- No expression-translation changes (`expr_translator` untouched).
- No behavior changes — every template block moves verbatim.
- `lib.dynamic_lookup` unchanged.
- No regeneration of already-verified workflows (WF_NHS_TL etc.).
- Read/File/Stored-Procedure steps already in lib form — unchanged.

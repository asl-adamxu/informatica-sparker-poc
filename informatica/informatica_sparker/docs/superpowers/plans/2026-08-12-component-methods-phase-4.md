# Component Methods Phase 4 Implementation Plan — sq_output + update_strategy + write_target

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Complete the component-method coverage of the data flow with the source-side and sink-side components: `lib.sq_output` (SQL-pushdown two-pass rename + port select + type casts + non-pushdown filter/distinct), `lib.update_strategy` (dynamic `_update_flag` when() split / static pass-through), and `lib.write_target` (field-map rename, `_update_flag` I/U/D branches, static DD_* batch update/delete, unmapped fills, target select, DUAL no-op, csv/DB write).

**Architecture:** Same three-layer separation (design doc `docs/specs/2026-08-12-component-methods-design.md`). Template blocks move verbatim — test locks behavior first, then the template code is deleted.

**Tech Stack:** Jinja2 templates, Python 3.11 generator build, PySpark 3.5.4, pytest with conftest fixtures, informatica-sparker CLI.

**Execution order note:** Phase 3 and Phase 4 run BEFORE Phase 2 (user decision). This plan's gate task (Task 6) runs a SINGLE reconversion covering both Phase 3 and Phase 4 changes (both modify mapping.py.j2).

## Global Constraints

- Translation stays at codegen time: `lib.*` methods receive **translated** results only.
- Verbatim move: no behavior changes; a test locks behavior before the template block is deleted.
- kwargs call form: `lib.xxx(spark=spark, input_df=..., <keys>, config=config)`; `name` NOT rendered.
- Complex lists one dict per line (`map('pyrepr') | join(',\n<indent>')`); scalars via `pyrepr`; `rename_columns` one tuple per line.
- `substitutions` values render as identifiers with `{{ '{' }}...{{ '}' }}` braces.
- No Chinese comments in generated code. Zero import side effects.
- **Phase 1 lessons (binding)**: (1) mapplet-internal construction paths wired too; (2) wiring regression tests parse real XML; (3) gate compares per-step semantics vs the backup; (4) render smoke tests ast.parse the real blocks.
- **Update Strategy / write semantics (from CLAUDE.md, binding)**: static `DD_INSERT` → append; `DD_UPDATE` → `lib.batch_update` ALL rows by primary key then write empty INSERT df (`filter(lit(False))`); `DD_DELETE` → `lib.batch_delete_composite` ALL rows; dynamic field strategies → `_update_flag` when() split + I/U/D branch code. UPDATE/DELETE keys come from `delete_keys` (KEYTYPE "PRIMARY" fields, fallback connector from_field). The connector `_field_map` rename runs BEFORE the `_update_flag` split.
- Test workflow: WF_EMS_DDS_APLY_MTH (Update Strategy 22 mappings, 49 targets, SQ 49).
- Build/convert cycle: `python3.11 -m build` → `python3.11 -m pip install --user --force-reinstall dist/informatica_sparker-*.whl` → `informatica-sparker convert <XML> -o <OUT_ROOT>`; gate 0 warnings / 0 errors; `python3.11 -m py_compile m_*.py`.
- Commit per task in the git repo (branch `enhance`); pytest from the repo root.
- Runtime-lib tests render `runtime_lib.py.j2` via Jinja2 (conftest); SparkSession `local[2]`.

## MOVE PROTOCOL

1. Source of truth = the template block (line ranges per task).
2. `{{ step.df_output }}` → `df`; `{{ step.df_input }}` → `input_df`; Jinja control flow → Python control flow over config lists.
3. Translated-string decisions move into lib verbatim (`_with_column`, `_substitute`, `_fill_missing` from Phase 1).
4. XML-metadata decisions stay in the generator as config keys.
5. `logger.info("Step: ...")` / `logger.info("... write completed")` stay in the generated file; methods log at debug only.

---

### Task 1: `lib.sq_output`

**Files:**
- Modify: `informatica_sparker/informatica_sparker/templates/runtime_lib.py.j2`
- Create: `informatica_sparker/tests/test_lib_sq_output.py`

**Interfaces:**
- Consumes: `_substitute` (Phase 1).
- Produces: `sq_output(spark, input_df, name=None, port_cols=None, column_types=None, filter_condition=None, substitutions=None, distinct=False, config=None, **kwargs)` → DataFrame.
  - SQL-pushdown semantics (the two-pass rename is RUNTIME — it depends on the actual query-result columns): name-match-first (case-insensitive) then positional fallback for remaining SQL columns, with backtick-quoted `col(f"`{old}`").alias(new)`; then port select with `lit(None)` for missing ports; then type casts (LongType/DoubleType/Decimal(38,10)/DateType/TimestampType per `column_types`).
  - Non-pushdown semantics: optional `filter_condition` (raw text; `$$` substituted with the `or "0"` rule — matches the SQ filter path), optional `distinct`, then the port select with `lit(None)` fills.
  - The method does NOT know whether the input is pushdown or not — the two-pass rename runs on whatever columns `input_df` has; it is a no-op when the input columns already match the ports by name (name-match-first consumes them all). So ONE method serves both paths: the handler always emits `port_cols` (+`column_types` when pushdown, +`filter_condition`/`distinct`/`substitutions` when non-pushdown).

- [ ] **Step 1: Write the failing test** — `tests/test_lib_sq_output.py`

```python
"""Tests for lib.sq_output. Fixtures (runtime_lib, spark) come from conftest.py."""


def test_rename_name_match_first(runtime_lib, spark):
    # SQL returns an unaliased expression column plus a matching one
    df = spark.createDataFrame([(1, "A|B")], ["KEY", "DEL_STS.A||DEL_STS.B"])
    out = runtime_lib.sq_output(
        spark=spark, input_df=df, name="SQ",
        port_cols=["KEY", "COMBINED"],
    )
    # name-match consumes KEY; positional fallback takes the expression column
    assert out.columns == ["KEY", "COMBINED"]
    assert out.collect()[0]["COMBINED"] == "A|B"


def test_missing_port_becomes_null(runtime_lib, spark):
    df = spark.createDataFrame([(1,)], ["A"])
    out = runtime_lib.sq_output(
        spark=spark, input_df=df, name="SQ",
        port_cols=["A", "MISSING"],
    )
    assert out.columns == ["A", "MISSING"]
    assert out.collect()[0]["MISSING"] is None


def test_type_casts(runtime_lib, spark):
    df = spark.createDataFrame([("1",)], ["NUM"])
    out = runtime_lib.sq_output(
        spark=spark, input_df=df, name="SQ",
        port_cols=["NUM"],
        column_types={"NUM": "INTEGER"},
    )
    assert str(out.schema["NUM"].dataType) == "LongType"


def test_filter_condition_and_distinct(runtime_lib, spark):
    df = spark.createDataFrame([(1,), (1,), (2,)], ["V"])
    out = runtime_lib.sq_output(
        spark=spark, input_df=df, name="SQ",
        port_cols=["V"],
        filter_condition="V > $$v_min",
        substitutions={"$$v_min": "1"},
        distinct=True,
    )
    assert sorted(r["V"] for r in out.collect()) == [2]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /var/lib/airflow/dags/adam/informatica/informatica_sparker && python3.11 -m pytest tests/test_lib_sq_output.py -v 2>&1 | tail -4`
Expected: FAIL — `AttributeError: ... no attribute 'sq_output'`.

- [ ] **Step 3: Implement `sq_output`** — append to `templates/runtime_lib.py.j2` after `sequence` (or anywhere in the component-methods section):

```python
def sq_output(spark, input_df, name=None, port_cols=None, column_types=None,
              filter_condition=None, substitutions=None, distinct=False,
              config=None, **kwargs):
    """Convert one Source Qualifier's output-port handling.

    SQL-pushdown results are renamed to the SQ output ports: name-match first
    (case-insensitive), then positional fallback for unaliased expression
    columns (backtick-quoted so dots/pipes stay part of the column name), then
    the port select with lit(None) for missing ports, then type casts.
    Non-pushdown SQs apply the optional filter condition ($$ with the
    str(v or "0") rule) and DISTINCT before the same port select.
    """
    df = input_df
    if filter_condition:
        df = df.filter(expr(_substitute(filter_condition, substitutions, or_zero=True)))
    if distinct:
        df = df.distinct()
    _port_cols = list(port_cols or [])
    if _port_cols:
        _sql_cols = list(df.columns)
        _rename_map = {}
        _used = set()
        for _sc in _sql_cols:
            for _pi, _port in enumerate(_port_cols):
                if _pi not in _used and _sc.lower() == _port.lower():
                    _rename_map[_sc] = _port
                    _used.add(_pi)
                    break
        _pi = 0
        for _sc in _sql_cols:
            if _sc in _rename_map:
                continue
            while _pi in _used:
                _pi += 1
            if _pi < len(_port_cols):
                _rename_map[_sc] = _port_cols[_pi]
                _used.add(_pi)
                _pi += 1
        if _rename_map:
            df = df.select(*[col("`" + _old + "`").alias(_new)
                             for _old, _new in _rename_map.items()])
        df = df.select([
            col(_c) if _c.lower() in [x.lower() for x in df.columns]
            else lit(None).alias(_c)
            for _c in _port_cols
        ])
    for _cname, _ctype in (column_types or {}).items():
        _lower = _cname.lower()
        _cast = None
        _t = str(_ctype or "").upper()
        if _t in ("INTEGER", "INT", "BIGINT", "SMALLINT", "TINYINT"):
            _cast = LongType()
        elif _t in ("FLOAT", "DOUBLE", "REAL"):
            _cast = DoubleType()
        elif "DECIMAL" in _t or "NUMERIC" in _t or _t == "NUMBER":
            _cast = DecimalType(38, 10)
        elif _t in ("DATE",):
            _cast = DateType()
        elif _t in ("DATETIME", "TIMESTAMP"):
            _cast = TimestampType()
        if _cast is not None:
            for _c in df.columns:
                if _c.lower() == _lower:
                    df = df.withColumn(_c, col(_c).cast(_cast))
    return df
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python3.11 -m pytest tests/test_lib_sq_output.py -v 2>&1 | tail -4`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add informatica_sparker/templates/runtime_lib.py.j2 tests/test_lib_sq_output.py
git commit -m "feat: lib.sq_output component method (two-pass rename, port select, type casts, filter/distinct)"
```

---

### Task 2: `lib.update_strategy`

**Files:**
- Modify: `informatica_sparker/informatica_sparker/templates/runtime_lib.py.j2`
- Create: `informatica_sparker/tests/test_lib_update_strategy.py`

**Interfaces:**
- Produces: `update_strategy(spark, input_df, name=None, strategy_field=None, config=None, **kwargs)` → DataFrame.
  - `strategy_field` set (dynamic field strategy): `withColumn("_update_flag", when(col(sf) == "DD_INSERT", lit("I")).when(col(sf) == "DD_UPDATE", lit("U")).when(col(sf) == "DD_DELETE", lit("D")).otherwise(lit("I")))`.
  - No `strategy_field` (static DD_* or none): pass-through (the write step applies the strategy directly).

- [ ] **Step 1: Write the failing test** — `tests/test_lib_update_strategy.py`

```python
"""Tests for lib.update_strategy. Fixtures (runtime_lib, spark) come from conftest.py."""


def test_dynamic_strategy_flag(runtime_lib, spark):
    df = spark.createDataFrame([("DD_INSERT",), ("DD_UPDATE",), ("DD_DELETE",), ("X",)],
                               ["FLAG"])
    out = runtime_lib.update_strategy(
        spark=spark, input_df=df, name="UPD", strategy_field="FLAG")
    assert sorted(r["_update_flag"] for r in out.collect()) == ["D", "I", "I", "U"]


def test_static_pass_through(runtime_lib, spark):
    df = spark.createDataFrame([(1,)], ["V"])
    out = runtime_lib.update_strategy(spark=spark, input_df=df, name="UPD")
    assert out.columns == ["V"] and out.count() == 1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3.11 -m pytest tests/test_lib_update_strategy.py -v 2>&1 | tail -4`
Expected: FAIL — `AttributeError: ... no attribute 'update_strategy'`.

- [ ] **Step 3: Implement `update_strategy`** — append to `templates/runtime_lib.py.j2` after `sq_output`:

```python
def update_strategy(spark, input_df, name=None, strategy_field=None,
                    config=None, **kwargs):
    """Convert one Informatica Update Strategy.

    Dynamic field strategies derive the _update_flag column (I/U/D) from the
    strategy field. Static strategies (DD_INSERT/DD_UPDATE/DD_DELETE) pass
    through — the target write applies them directly.
    """
    if strategy_field:
        return input_df.withColumn(
            "_update_flag",
            when(col(strategy_field) == "DD_INSERT", lit("I"))
            .when(col(strategy_field) == "DD_UPDATE", lit("U"))
            .when(col(strategy_field) == "DD_DELETE", lit("D"))
            .otherwise(lit("I")),
        )
    return input_df
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python3.11 -m pytest tests/test_lib_update_strategy.py -v 2>&1 | tail -4`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add informatica_sparker/templates/runtime_lib.py.j2 tests/test_lib_update_strategy.py
git commit -m "feat: lib.update_strategy component method (dynamic _update_flag, static pass-through)"
```

---

### Task 3: `lib.write_target`

**Files:**
- Modify: `informatica_sparker/informatica_sparker/templates/runtime_lib.py.j2`
- Create: `informatica_sparker/tests/test_lib_write_target.py`

**Interfaces:**
- Consumes: `_fill_missing` (Phase 1); `batch_update` / `batch_delete_composite` / `write_table` / `write_file` (already in runtime_lib).
- Produces: `write_target(spark, df, conn, table, mode='append', sink_type='delta', target_columns=None, field_map=None, unmapped_columns=None, is_delete=False, delete_keys=None, cast_nulltype=False, has_update_flag=False, static_dd=None, config=None, name=None, **kwargs)` → None.
  - Semantics (verbatim from the template): DUAL/DEV_NULL/`/dev/null` no-op → log + return; `cast_nulltype` → NullType columns to StringType; `field_map` rename (case-insensitive guard, drop conflicting target names) BEFORE the `_update_flag` split; `has_update_flag` → static `DD_UPDATE` (batch_update ALL rows by `delete_keys`, then `df.filter(lit(False))`) or dynamic I/U/D split (DELETE batch_delete_composite, UPDATE batch_update, INSERT flows to the normal write); `is_delete` → `batch_delete_composite` ALL rows; `unmapped_columns` → `lit(None).cast(StringType())` fills (excluding `src_rowid`); `target_columns` → select (case-insensitive); write: `sink_type == 'csv'` → `write_file` with config.yml `objects` path/format (skip when path resolves to /dev/null) else `write_table(df, conn, table, mode=mode)`.
  - `config` is REQUIRED for the csv path (`config["objects"][table]["path"/"format"]`); the generated call always passes `config=config`.

- [ ] **Step 1: Write the failing test** — `tests/test_lib_write_target.py`

```python
"""Tests for lib.write_target. Fixtures (runtime_lib, spark) come from conftest.py."""

import pytest


def test_dual_noop(runtime_lib, spark, monkeypatch):
    monkeypatch.setattr(runtime_lib, "write_table",
                        lambda *a, **k: pytest.fail("write_table called for DUAL"))
    runtime_lib.write_target(
        spark=spark, df=spark.createDataFrame([(1,)], ["V"]),
        conn={}, table="DUAL", mode="append", config={}, name="WT")


def test_field_map_and_target_select(runtime_lib, spark, monkeypatch):
    written = {}
    monkeypatch.setattr(runtime_lib, "write_table",
                        lambda df, conn, table, mode="append": written.update(
                            df=df, table=table, mode=mode))
    df = spark.createDataFrame([(1, "A")], ["SRC", "V"])
    runtime_lib.write_target(
        spark=spark, df=df, conn={}, table="T", mode="append", config={},
        field_map={"TGT": "SRC"}, target_columns=["TGT", "V"],
    )
    assert written["table"] == "T" and written["mode"] == "append"
    assert written["df"].columns == ["TGT", "V"]
    assert written["df"].collect()[0]["TGT"] == 1


def test_static_dd_update_batches_all_rows(runtime_lib, spark, monkeypatch):
    updates, written = [], {}
    monkeypatch.setattr(runtime_lib, "batch_update",
                        lambda s, c, t, set_c, key_c, rows, b=1000: updates.append(
                            (t, set_c, key_c, rows)))
    monkeypatch.setattr(runtime_lib, "write_table",
                        lambda df, conn, table, mode="append": written.update(
                            df=df, table=table))
    df = spark.createDataFrame([(1, "A"), (2, "B")], ["K", "V"])
    runtime_lib.write_target(
        spark=spark, df=df, conn={}, table="T", mode="append", config={},
        has_update_flag=True, static_dd="DD_UPDATE", delete_keys=["K"],
    )
    assert len(updates) == 1
    assert updates[0][1] == ["V"] and updates[0][2] == ["K"]
    assert sorted(r[0] for r in updates[0][3]) == [1, 2]
    assert written["df"].count() == 0  # filter(lit(False))


def test_dynamic_split_insert_only_write(runtime_lib, spark, monkeypatch):
    written = {}
    monkeypatch.setattr(runtime_lib, "write_table",
                        lambda df, conn, table, mode="append": written.update(
                            df=df, table=table))
    df = spark.createDataFrame([(1, "I"), (2, "U"), (3, "D")], ["K", "_update_flag"])
    runtime_lib.write_target(
        spark=spark, df=df, conn={}, table="T", mode="append", config={},
        has_update_flag=True, delete_keys=["K"],
    )
    # I rows flow to the normal write; U/D rows handled via batch (no rows here)
    assert written["df"].count() == 1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3.11 -m pytest tests/test_lib_write_target.py -v 2>&1 | tail -4`
Expected: FAIL — `AttributeError: ... no attribute 'write_target'`.

- [ ] **Step 3: Implement `write_target`** — append to `templates/runtime_lib.py.j2` after `update_strategy`:

```python
def write_target(spark, df, conn, table, mode="append", sink_type="delta",
                 target_columns=None, field_map=None, unmapped_columns=None,
                 is_delete=False, delete_keys=None, cast_nulltype=False,
                 has_update_flag=False, static_dd=None, config=None,
                 name=None, **kwargs):
    """Convert one Informatica Target write.

    Order matches the mapping: DUAL/DEV_NULL no-op → NullType cast → connector
    field-map rename (BEFORE the _update_flag split, so UPDATE/DELETE use
    target column names) → static DD_UPDATE (batch_update ALL rows, then an
    empty INSERT frame) or dynamic I/U/D split (batch delete/update, INSERT
    flows to the normal write) → static DD_DELETE (batch_delete_composite ALL
    rows) → unmapped lit(None) fills → target-column select → csv/DB write.
    """
    _table = table or ""
    if _table.endswith("DEV_NULL") or "/dev/null" in _table.lower() or _table.upper() == "DUAL":
        logging.info("Target %s is a no-op target (/dev/null or DUAL), skipping write", name or "?")
        return
    _df = df
    if cast_nulltype:
        for _c in _df.columns:
            if isinstance(_df.schema[_c].dataType, NullType):
                _df = _df.withColumn(_c, col(_c).cast(StringType()))
    _field_map = field_map or {}
    for _tgt_col, _src_col in _field_map.items():
        if (_tgt_col.lower() not in [x.lower() for x in _df.columns]
                and _src_col.lower() in [x.lower() for x in _df.columns]):
            for _c in list(_df.columns):
                if _c.lower() == _tgt_col.lower() and _c != _src_col:
                    _df = _df.drop(_c)
            _df = _df.withColumnRenamed(_src_col, _tgt_col)
    _keys = delete_keys or []
    if has_update_flag:
        if static_dd == "DD_UPDATE":
            if _keys and not _df.rdd.isEmpty():
                _set_cols = [c for c in _df.columns
                             if c.lower() not in [k.lower() for k in _keys]
                             and c != "_update_flag"]
                if _set_cols:
                    _rows = [tuple(r[c] for c in _set_cols + _keys)
                             for r in _df.collect()]
                    batch_update(spark, conn, _table, _set_cols, _keys, _rows, 1000)
            _df = _df.filter(lit(False))
        else:
            _df_ins = _df.filter(col("_update_flag") == "I").drop("_update_flag")
            _df_upd = _df.filter(col("_update_flag") == "U").drop("_update_flag")
            _df_del = _df.filter(col("_update_flag") == "D").drop("_update_flag")
            _df = _df.drop("_update_flag")
            if _keys and not _df_del.rdd.isEmpty():
                _del_rows = [tuple(r[c] for c in _keys)
                             for r in _df_del.select(*_keys).distinct().collect()]
                if _del_rows:
                    batch_delete_composite(spark, conn, _table, _keys, _del_rows, 1000)
            if _keys and not _df_upd.rdd.isEmpty():
                _set_cols = [c for c in _df_upd.columns
                             if c.lower() not in [k.lower() for k in _keys]]
                if _set_cols:
                    _rows = [tuple(r[c] for c in _set_cols + _keys)
                             for r in _df_upd.collect()]
                    batch_update(spark, conn, _table, _set_cols, _keys, _rows, 1000)
            _df = _df_ins
    if is_delete and _keys:
        if not _df.rdd.isEmpty():
            _del_rows = [tuple(r[c] for c in _keys)
                         for r in _df.select(*_keys).distinct().collect()]
            if _del_rows:
                batch_delete_composite(spark, conn, _table, _keys, _del_rows, 1000)
    else:
        for _col in (unmapped_columns or []):
            if _col.lower() not in ["src_rowid"]:
                _df = _df.withColumn(_col, lit(None).cast(StringType()))
        if target_columns:
            _df = _df.select(*[_c for _c in target_columns
                               if _c.lower() in [x.lower() for x in _df.columns]])
        if sink_type == "csv":
            _obj = ((config or {}).get("objects") or {}).get(_table) or {}
            _path = _obj.get("path", "/tmp/" + _table)
            _fmt = _obj.get("format", sink_type)
            if _path and _path.strip() in ("/dev/null", "NUL"):
                logging.info("Target %s resolved to /dev/null, skipping write", name or "?")
            else:
                write_file(_df, _path, format=_fmt, mode="overwrite")
        else:
            write_table(_df, conn, _table, mode=mode)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python3.11 -m pytest tests/test_lib_write_target.py -v 2>&1 | tail -4`
Expected: all PASS. (Note: `batch_update`/`batch_delete_composite`/`write_table`/`write_file` are module-level names — monkeypatch targets `runtime_lib.batch_update` etc., which the function resolves at call time.)

- [ ] **Step 5: Commit**

```bash
git add informatica_sparker/templates/runtime_lib.py.j2 tests/test_lib_write_target.py
git commit -m "feat: lib.write_target component method (field-map, I/U/D split, static DD_*, DUAL no-op, csv/DB write)"
```

---

### Task 4: Generator wiring — Source Qualifier (handler cfg + template)

**Files:**
- Modify: `informatica_sparker/informatica_sparker/handlers.py` (`_handle_source_qualifier`, ~line 894)
- Modify: `informatica_sparker/informatica_sparker/templates/mapping.py.j2` (APPLY_SOURCE_QUALIFIER block, lines 163-272)
- Create: `informatica_sparker/tests/test_sq_wiring.py`

**Interfaces:**
- Consumes: `lib.sq_output` signature (Task 1).
- Produces: `step.params["sq_output_cfg"]` with keys: `port_cols`, `column_types` (pushdown only), `filter_condition`/`substitutions`/`distinct` (non-pushdown only).

- [ ] **Step 1: Build the cfg**

In `_handle_source_qualifier`, after the existing param stores, add:

```python
        _sq_cfg: Dict[str, Any] = {"port_cols": step.params.get("output_columns", [])}
        if step.params.get("output_column_types"):
            _sq_cfg["column_types"] = step.params["output_column_types"]
        if step.params.get("filter_inner") and '$$' not in step.params.get("filter_inner", ""):
            _sq_cfg["filter_condition"] = step.params["filter_inner"]
        elif step.params.get("filter_inner"):
            _sq_cfg["filter_condition"] = step.params["filter_inner"]
            _sq_cfg["substitutions"] = {
                _var: _val.replace('$', '')
                for _var, _val in (plan.mapping_variables or {}).items()
                if _var in step.params["filter_inner"]
            }
        if step.params.get("distinct"):
            _sq_cfg["distinct"] = True
        step.params["sq_output_cfg"] = _sq_cfg
```

Note: the handler's SQ `filter_inner` is the TRANSLATED text (extracted from the translated `expr(...)` in `_handle_source_qualifier` — verify the local/param name; the template reads `filter_inner`/`filter_condition`/`distinct`/`output_columns`/`output_column_types`/`use_sql_override`/`sql_query`). The SQL-pushdown `sql_query`/read step stays UNCHANGED in the template (it is a read, not a port transform); only the port-handling tail (two-pass rename + type casts + port select) moves into the `lib.sq_output` call.

- [ ] **Step 2: Replace the port-handling tails of the APPLY_SOURCE_QUALIFIER block**

The block has TWO tails (pushdown: lines 184-246; non-pushdown: lines 248-270). Replace BOTH with a single tail after the read step:

```jinja2
        {% if step.params.get('sq_output_cfg') %}
        {% set sqcfg = step.params['sq_output_cfg'] %}
        {{ step.df_output }} = lib.sq_output(
            spark=spark,
            input_df={{ step.df_output }},
            port_cols={{ sqcfg['port_cols'] | pyrepr }},
            {% if sqcfg.get('column_types') %}
            column_types={{ sqcfg['column_types'] | pyrepr }},
            {% endif %}
            {% if sqcfg.get('filter_condition') %}
            filter_condition={{ sqcfg['filter_condition'] | pyrepr }},
            {% endif %}
            {% if sqcfg.get('substitutions') %}
            substitutions={{ '{' }}{% for _k, _v in sqcfg['substitutions'].items() %}'{{ _k }}': {{ _v }}{% if not loop.last %}, {% endif %}{% endfor %}{{ '}' }},
            {% endif %}
            {% if sqcfg.get('distinct') %}
            distinct=True,
            {% endif %}
            config=config,
        )
        {% endif %}
        ctx.register_df("{{ step.df_output }}", {{ step.df_output }})
```

The pushdown read block (query build, `read_sql`) stays; the non-pushdown filter (`{{ step.params.get('filter_condition') }}` / `_filter_text`) is replaced by the cfg-based `filter_condition` (the `$$` `_filter_text` replace loop is dropped — `lib.sq_output` substitutes at runtime).

- [ ] **Step 3: Add the wiring regression test** — `tests/test_sq_wiring.py`

Parse `WF_EMS_DDS_APLY_MTH.XML`: every `ApplySourceQualifierStep` carries `sq_output_cfg` with non-empty `port_cols` (`seen >= 40`); spot-check at least one mapping has `column_types` and at least one has a non-pushdown `filter_condition` (grep the XML's SQ for "Source Filter" — if none have one, keep the filter_condition path covered by the unit tests only). Plus a render smoke test with ast.parse (synthetic step with column_types + filter_condition + substitutions).

- [ ] **Step 4: Run tests**

Run: `python3.11 -m pytest tests/test_sq_wiring.py tests/ -q 2>&1 | tail -3`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add informatica_sparker/handlers.py informatica_sparker/templates/mapping.py.j2 tests/test_sq_wiring.py
git commit -m "feat: generator emits lib.sq_output calls for SQ port handling + wiring regression test"
```

---

### Task 5: Generator wiring — update strategy + write target (handler cfgs + template)

**Files:**
- Modify: `informatica_sparker/informatica_sparker/handlers.py` (`_handle_update_strategy` ~3085, `_handle_target` — the WRITE_TARGET param builder)
- Modify: `informatica_sparker/informatica_sparker/templates/mapping.py.j2` (APPLY_UPDATE_STRATEGY lines 946-965, WRITE_TARGET lines 908-1021)
- Create: `informatica_sparker/tests/test_update_write_wiring.py`

**Interfaces:**
- Consumes: `lib.update_strategy`/`lib.write_target` signatures (Tasks 2-3).
- Produces: `step.params["update_strategy_cfg"]` (`{'strategy_field'}` optional), `step.params["write_target_cfg"]` (all write keys; `conn`/`table`/`mode` render as the template's `conn_target`/`{{ table_name }}`/`{{ mode }}`).

- [ ] **Step 1: Build the cfgs**

Update strategy (`_handle_update_strategy` — the handler already computes `strategy_field`/`static_dd` params):

```python
        _us_cfg: Dict[str, Any] = {}
        if step.params.get("strategy_field"):
            _us_cfg["strategy_field"] = step.params["strategy_field"]
        step.params["update_strategy_cfg"] = _us_cfg
```

Write target (`_handle_target` — the handler already stores `table_name`, `mode`, `sink_type`, `target_columns`, `target_column_types`, `unmapped_columns`, `is_delete`, `delete_keys`, `field_map`, `cast_nulltype`, `has_update_flag`, `static_dd` params):

```python
        _wt_cfg: Dict[str, Any] = {
            "table": step.params.get("table_name", ""),
            "mode": step.params.get("mode", "append"),
            "sink_type": step.params.get("sink_type", "delta"),
            "target_columns": step.params.get("target_columns", []),
            "unmapped_columns": step.params.get("unmapped_columns", []),
            "is_delete": bool(step.params.get("is_delete")),
            "delete_keys": step.params.get("delete_keys", []),
            "cast_nulltype": bool(step.params.get("cast_nulltype")),
            "has_update_flag": bool(step.params.get("has_update_flag")),
            "static_dd": step.params.get("static_dd"),
        }
        if step.params.get("field_map"):
            _wt_cfg["field_map"] = step.params["field_map"]
        step.params["write_target_cfg"] = _wt_cfg
```

- [ ] **Step 2: Replace the template blocks**

APPLY_UPDATE_STRATEGY (lines 946-965):

```jinja2
        {% elif step.step_type == IRStepType.APPLY_UPDATE_STRATEGY %}
        # Update Strategy: {{ step.step_name }}
        # Strategy: {{ step.params.get('strategy_expression', 'DD_INSERT') }}
        {% set uscfg = step.params.get('update_strategy_cfg', {}) %}
        {{ step.df_output }} = lib.update_strategy(
            spark=spark,
            input_df={{ step.df_input }},
            {% if uscfg.get('strategy_field') %}
            strategy_field={{ uscfg['strategy_field'] | pyrepr }},
            {% endif %}
            config=config,
        )
        ctx.register_df("{{ step.df_output }}", {{ step.df_output }})
```

WRITE_TARGET (lines 908-1021) — replace the whole block:

```jinja2
        {% elif step.step_type == IRStepType.WRITE_TARGET %}
        # Write to Target: {{ step.step_name }}
        {% set wtcfg = step.params['write_target_cfg'] %}
        lib.write_target(
            spark=spark,
            df={{ step.df_input }},
            conn=conn_target,
            table={{ wtcfg['table'] | pyrepr }},
            mode={{ wtcfg['mode'] | pyrepr }},
            {% if wtcfg.get('sink_type') != 'delta' %}
            sink_type={{ wtcfg['sink_type'] | pyrepr }},
            {% endif %}
            {% if wtcfg.get('target_columns') %}
            target_columns={{ wtcfg['target_columns'] | pyrepr }},
            {% endif %}
            {% if wtcfg.get('field_map') %}
            field_map={{ wtcfg['field_map'] | pyrepr }},
            {% endif %}
            {% if wtcfg.get('unmapped_columns') %}
            unmapped_columns={{ wtcfg['unmapped_columns'] | pyrepr }},
            {% endif %}
            {% if wtcfg.get('is_delete') %}
            is_delete=True,
            {% endif %}
            {% if wtcfg.get('delete_keys') %}
            delete_keys={{ wtcfg['delete_keys'] | pyrepr }},
            {% endif %}
            {% if wtcfg.get('cast_nulltype') %}
            cast_nulltype=True,
            {% endif %}
            {% if wtcfg.get('has_update_flag') %}
            has_update_flag=True,
            {% endif %}
            {% if wtcfg.get('static_dd') %}
            static_dd={{ wtcfg['static_dd'] | pyrepr }},
            {% endif %}
            config=config,
        )
```

(The template's `logger.info("... write completed")` line and the `session_sqls` Post-SQL block stay untouched below the block.)

- [ ] **Step 3: Add the wiring regression test** — `tests/test_update_write_wiring.py`

Parse `WF_EMS_DDS_APLY_MTH.XML`: every `WriteTargetStep` carries `write_target_cfg` with a non-empty `table` (`seen >= 40`); every `ApplyUpdateStrategyStep` carries `update_strategy_cfg`; at least one `static_dd` value and at least one dynamic `strategy_field` among the steps (non-vacuity). Plus render smoke tests for both blocks with ast.parse (synthetic steps with all cfg keys).

- [ ] **Step 4: Run tests**

Run: `python3.11 -m pytest tests/test_update_write_wiring.py tests/ -q 2>&1 | tail -3`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add informatica_sparker/handlers.py informatica_sparker/templates/mapping.py.j2 tests/test_update_write_wiring.py
git commit -m "feat: generator emits lib.update_strategy/lib.write_target calls + wiring regression test"
```

---

### Task 6: Reconvert WF_EMS_DDS_APLY_MTH and verify (Phase 3 + Phase 4 gate)

**Files:**
- Run: build + CLI convert (backup exists from Phase 1); the reconversion covers BOTH Phase 3 and Phase 4 template changes.

- [ ] **Step 1: Rebuild and reinstall the wheel**

```bash
cd /var/lib/airflow/dags/adam/informatica/informatica_sparker
python3.11 -m build 2>&1 | tail -2
python3.11 -m pip install --user --force-reinstall dist/informatica_sparker-*.whl 2>&1 | tail -1
```

- [ ] **Step 2: Reconvert the test workflow**

```bash
OUT_ROOT=/var/lib/airflow/dags/adam/informatica/PySpark_workflows
informatica-sparker convert /var/lib/airflow/dags/adam/informatica/PowerCenter_workflows/dds/WF_EMS_DDS_APLY_MTH.XML -o $OUT_ROOT/dds/WF_EMS_DDS_APLY_MTH 2>&1 | tail -3
```
Expected: **0 warnings / 0 errors**.

- [ ] **Step 3: Static gates**

```bash
cd $OUT_ROOT/dds/WF_EMS_DDS_APLY_MTH
python3.11 -m py_compile m_*.py && echo "PY_COMPILE OK"
grep -c "lib.sq_output(" m_*.py | grep -v ":0" | wc -l      # 49 expected
grep -c "lib.update_strategy(" m_*.py | grep -v ":0" | wc -l  # 22 expected
grep -c "lib.write_target(" m_*.py | grep -v ":0" | wc -l    # 49 expected
grep -rn "withColumn(" m_*.py | wc -l                        # only pre-step/expression-lib remnants
```

- [ ] **Step 4: Per-step semantic comparison vs the backup (Phase 1 lesson)**

For every `lib.sq_output` call, compare `port_cols` to the backup's `_port_cols` list of the same-named SQ step; compare `column_types` presence. For every `lib.write_target` call, compare `target_columns`/`field_map`/`delete_keys`/`static_dd` to the backup's `_target_cols`/`_field_map`/`_upd_key_cols`/strategy comment; for every `lib.update_strategy` call, compare the `_update_flag` when() chain columns to the backup's. Write `/tmp/compare_phase4_steps.py` following the line-based approach of `/tmp/compare_filter_conditions.py`. Report counts: `compared`, `mismatches` (must be 0).

- [ ] **Step 5: Full test suite**

```bash
cd /var/lib/airflow/dags/adam/informatica/informatica_sparker
python3.11 -m pytest tests/ -q 2>&1 | tail -2
```
Expected: all PASS.

- [ ] **Step 6: Runtime verification (user checkpoint)**

User runs the full workflow on the cluster (`cd PySpark_workflows/dds/WF_EMS_DDS_APLY_MTH && python wf_ems_dds_aply_mth.py`) — expect all mappings SUCCESS (this round exercises SQ ports, update strategies, and every target write in lib form). Report coverage: 49 SQs, 22 update strategies, 49 targets, plus Phase 3's routers/unions/sorter.

- [ ] **Step 7: Update CLAUDE.md**

Append the Phase 3+4 methods to the "Component Methods" section in `/var/lib/airflow/dags/adam/informatica/CLAUDE.md`: `lib.router` (multi-feed union input, ordered group split, DEFAULT negation, dict return), `lib.union` (per-group selects; flag_column unsupported), `lib.sorter`, `lib.sequence` (standalone), `lib.sq_output` (two-pass runtime rename, port select, type casts), `lib.update_strategy` (dynamic flag, static pass-through), `lib.write_target` (field-map, I/U/D split, static DD_*, DUAL no-op, csv/DB). Edit in place, no commit.

---

## Out of scope (this plan)

- static_lookup, joiner, aggregator, stored_procedure — Phase 2, executed LAST (user decision), plan at `docs/superpowers/plans/2026-08-12-component-methods-phase-2.md`.
- Dynamic lookup — already in lib form, untouched.
- The SQL-pushdown read itself (`lib.read_sql` query build) — stays in the generated code; only the port-handling tail moves.
- No expression-translation changes (`expr_translator` untouched).
- No regeneration of other workflows.

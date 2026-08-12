# Component Methods Phase 1 Implementation Plan — Shared Helpers + lib.expression + lib.filter

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the runtime foundation (4 shared helpers) and the first two component methods (`lib.expression`, `lib.filter`) in `runtime_lib.py.j2`, rewire the generator (handlers.py + mapping.py.j2) to emit `lib.expression(...)` / `lib.filter(...)` calls, and verify via reconversion of WF_EMS_DDS_APLY_MTH.

**Architecture:** Three-layer separation (design doc `docs/specs/2026-08-12-component-methods-design.md`): generator = syntax conversion (translation stays at codegen time), runtime_lib methods = runtime semantics, generated files = declarative data flow. Every template block moves **verbatim** — a unit test locks the behavior first, then the template code is deleted.

**Tech Stack:** Jinja2 templates (mapping.py.j2, runtime_lib.py.j2), Python 3.11 generator build, PySpark 3.5.4, pytest with local SparkSession, informatica-sparker CLI.

## Global Constraints

- Translation stays at codegen time: `lib.*` methods receive **translated** results only (same convention as `dynamic_lookup` kwargs).
- Verbatim move: no behavior changes, no "while we're here" optimizations. A test locks behavior before the template block is deleted.
- kwargs call form: `lib.xxx(spark=spark, input_df=..., name='...', <keys>, config=config)`.
- Complex lists render one dict per line via `map('pyrepr') | join(',\n<indent>')` (line-width rule, same as `lookup_output_fields`); scalar values via `pyrepr`.
- `substitutions` values render as **identifiers** (`'$$v_x': v_x`), never quoted strings — the template renders them specially (see Task 4/5 template code).
- No Chinese comments in generated code. Generated files unchanged in their non-component parts.
- Zero import side effects for mapping modules; lib methods must not require per-mapping state.
- Test workflow: WF_EMS_DDS_APLY_MTH (`PowerCenter_workflows/dds/WF_EMS_DDS_APLY_MTH.XML` → `PySpark_workflows/dds/WF_EMS_DDS_APLY_MTH`).
- Build/convert cycle (CLI reads site-packages wheel, not repo source): `python3.11 -m build` → `python3.11 -m pip install --user --force-reinstall dist/informatica_sparker-*.whl` → `informatica-sparker convert <XML> -o <OUT_ROOT>`.
- Reconversion gate: 0 warnings / 0 errors.
- Commit per task in the `informatica_sparker` git repo (branch `enhance`).
- Runtime-lib tests follow `tests/test_dynamic_lookup.py`: render `runtime_lib.py.j2` via Jinja2 and import as a module; SparkSession fixture `local[2]`, `spark.ui.enabled=false`.

## MOVE PROTOCOL (apply to every method task)

The source of truth is the template block in `mapping.py.j2`. Moving it into a lib method follows these mechanical rules:

1. `{{ step.df_output }}` → `df`, `{{ step.df_input }}` → `input_df` (the method's `df`).
2. Jinja control flow (`{% for %}`, `{% if %}`) → Python control flow over config lists.
3. Template-time decisions that depend on the **translated string** (e.g. expr-vs-API marker detection) move into the lib method verbatim (shared helper `_with_column`).
4. Template-time decisions that depend on **XML metadata** (port classification, SP detection, $$ variable presence) stay in the generator and are stored as config keys.
5. The generated `logger.info("Step: ...")` stays in the generated file; methods log at debug only.

---

### Task 1: Shared helpers — `_with_column`, `_rename_columns`, `_fill_missing`, `_substitute`

**Files:**
- Modify: `informatica_sparker/informatica_sparker/templates/runtime_lib.py.j2` (add helpers after the `dynamic_lookup` function, before `_read_local_csv`)
- Create: `informatica_sparker/tests/test_lib_helpers.py`

**Interfaces:**
- Produces (later tasks depend on these exact signatures):
  - `_with_column(df, name, expr_str)` → DataFrame — adds column `name`; wraps in `expr()` unless `expr_str` contains an API marker (`'row_number()'`, `'monotonically_increasing_id'`, `'last(when('`), in which case it is evaluated as Python source against the pyspark functions + Window namespace.
  - `_rename_columns(df, renames)` → DataFrame — `drop(new).withColumnRenamed(old, new)` per pair, skipping `old.lower() == new.lower()`.
  - `_fill_missing(df, cols)` → DataFrame — `withColumn(c, lit(None))` for any `c` absent case-insensitively.
  - `_substitute(text, substitutions, or_zero=False)` → str — replaces `$$var` occurrences; `or_zero=True` uses `str(v or "0")`, else `str(v)`.

- [ ] **Step 1: Write the failing test** — create `tests/conftest.py` (shared fixtures) and `tests/test_lib_helpers.py`

`tests/conftest.py` (pytest auto-discovers it; the existing test files define their own local fixtures which shadow these, so nothing breaks):

```python
"""Shared fixtures for runtime-lib component-method tests.

Renders runtime_lib.py.j2 via Jinja2 and imports it as a module, so the
tests cover the exact code deployed into every workflow env.
"""

import os
import sys
import types
from pathlib import Path

import pytest
from jinja2 import Environment, FileSystemLoader

ROOT = Path(__file__).resolve().parents[1]
TEMPLATES_DIR = ROOT / "informatica_sparker" / "informatica_sparker" / "templates"


def _render_runtime_lib():
    try:
        import pyspark  # noqa: F401
    except ImportError:
        spark_home = (
            "/opt/cloudera/parcels/"
            "SPARK3-3.5.4.3.5.7191000.0-30-1.p0.68499982/lib/spark3"
        )
        sys.path.insert(0, str(Path(spark_home) / "python"))
        sys.path.insert(
            0,
            str(Path(spark_home) / "python" / "lib" / "py4j-0.10.9.7-src.zip"),
        )
    env = Environment(loader=FileSystemLoader(str(TEMPLATES_DIR)))
    source = env.get_template("runtime_lib.py.j2").render()
    module = types.ModuleType("runtime_lib_render")
    exec(compile(source, "runtime_lib.py.j2", "exec"), module.__dict__)
    return module


@pytest.fixture(scope="module")
def runtime_lib():
    return _render_runtime_lib()


@pytest.fixture(scope="module")
def spark():
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    from pyspark.sql import SparkSession

    session = SparkSession.builder.master("local[2]").appName(
        "component_methods_test"
    ).config("spark.ui.enabled", "false").getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
```

`tests/test_lib_helpers.py` (uses the conftest fixtures):

```python
"""Tests for the component-method shared helpers in runtime_lib."""


def test_with_column_expr_path(runtime_lib, spark):
    df = spark.createDataFrame([(1,), (2,)], ["A"])
    out = runtime_lib._with_column(df, "B", "A + 1")
    assert out.columns == ["A", "B"]
    assert sorted(r["B"] for r in out.collect()) == [2, 3]


def test_with_column_api_path(runtime_lib, spark):
    df = spark.createDataFrame([(1,), (2,)], ["A"])
    out = runtime_lib._with_column(df, "B", "monotonically_increasing_id() + 1")
    vals = sorted(r["B"] for r in out.collect())
    assert vals[0] >= 1 and len(vals) == 2


def test_with_column_api_last_when(runtime_lib, spark):
    df = spark.createDataFrame([("x", 1), ("y", 2)], ["K", "V"])
    expr_str = 'last(when(col("K") == "y", col("V")), True).over(Window.orderBy(lit(1)))'
    out = runtime_lib._with_column(df, "C", expr_str)
    assert out.collect()[1]["C"] == 2


def test_rename_columns_basic(runtime_lib, spark):
    df = spark.createDataFrame([(1, 2)], ["A", "B"])
    out = runtime_lib._rename_columns(df, [("A", "C")])
    assert out.columns == ["C", "B"]


def test_rename_columns_drop_target_protection(runtime_lib, spark):
    df = spark.createDataFrame([(1, 2)], ["A", "B"])
    out = runtime_lib._rename_columns(df, [("A", "B")])
    assert out.columns == ["B"]  # no duplicate B after drop-first


def test_rename_columns_skips_same_case(runtime_lib, spark):
    df = spark.createDataFrame([(1, 2)], ["A", "B"])
    out = runtime_lib._rename_columns(df, [("a", "A")])
    assert out.columns == ["A", "B"]


def test_fill_missing(runtime_lib, spark):
    df = spark.createDataFrame([(1,)], ["A"])
    out = runtime_lib._fill_missing(df, ["A", "B"])
    assert out.columns == ["A", "B"]
    assert out.collect()[0]["B"] is None


def test_fill_missing_case_insensitive_no_dup(runtime_lib, spark):
    df = spark.createDataFrame([(1,)], ["A"])
    out = runtime_lib._fill_missing(df, ["a"])
    assert out.columns == ["A"]


def test_substitute_or_zero(runtime_lib):
    text = "COL > $$v_rpt_mth"
    out = runtime_lib._substitute(text, {"$$v_rpt_mth": ""}, or_zero=True)
    assert out == "COL > 0"


def test_substitute_plain(runtime_lib):
    text = "rpad($$v_x, 10, ' ')"
    out = runtime_lib._substitute(text, {"$$v_x": "AB"}, or_zero=False)
    assert out == "rpad(AB, 10, ' ')"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /var/lib/airflow/dags/adam/informatica/informatica_sparker && python3.11 -m pytest tests/test_lib_helpers.py -v 2>&1 | tail -15`
Expected: FAIL — `AttributeError: 'module' object has no attribute '_with_column'` (helpers do not exist yet).

- [ ] **Step 3: Implement the helpers** — append to `templates/runtime_lib.py.j2` after the `dynamic_lookup` function:

```python
# ---------------------------------------------------------------------------
# Shared component-method helpers (used by lib.<component> wrappers)
# ---------------------------------------------------------------------------

_API_EXPR_MARKERS = ('row_number()', 'monotonically_increasing_id', 'last(when(')
_PYSPARK_API_NS = None


def _api_namespace():
    """Lazily build the namespace used to evaluate direct PySpark API
    expressions (the generator renders these as Python source, not SQL)."""
    global _PYSPARK_API_NS
    if _PYSPARK_API_NS is None:
        from pyspark.sql.window import Window as _Window
        _ns = dict(globals())
        _ns['Window'] = _Window
        _PYSPARK_API_NS = _ns
    return _PYSPARK_API_NS


def _with_column(df, name, expr_str):
    """Add column `name` with a translated expression.

    Spark SQL text is wrapped in expr(). Text containing a direct PySpark API
    marker (row_number() / monotonically_increasing_id / last(when() — the
    generator emits these as Python source) is evaluated against the pyspark
    namespace instead.
    """
    if any(_m in expr_str for _m in _API_EXPR_MARKERS):
        return df.withColumn(name, eval(expr_str, {"__builtins__": {}}, _api_namespace()))
    return df.withColumn(name, expr(expr_str))


def _rename_columns(df, renames):
    """Apply connector renames (drop target first, skip src == tgt)."""
    for _old, _new in (renames or []):
        if _old.lower() == _new.lower():
            continue
        df = df.drop(_new).withColumnRenamed(_old, _new)
    return df


def _fill_missing(df, cols):
    """Add lit(None) for any listed column absent from df (case-insensitive)."""
    for _c in (cols or []):
        if _c.lower() not in [x.lower() for x in df.columns]:
            df = df.withColumn(_c, lit(None))
    return df


def _substitute(text, substitutions, or_zero=False):
    """Replace $$ mapping variables in translated text with runtime values.

    or_zero=True (Filter/SQ conditions): str(v or "0") so an empty value
    cannot produce invalid SQL. or_zero=False (Expression columns): str(v).
    """
    for _var, _val in (substitutions or {}).items():
        if _var in text:
            text = text.replace(_var, str(_val or "0") if or_zero else str(_val))
    return text
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python3.11 -m pytest tests/test_lib_helpers.py -v 2>&1 | tail -8`
Expected: all 9 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add informatica_sparker/templates/runtime_lib.py.j2 tests/conftest.py tests/test_lib_helpers.py
git commit -m "feat: shared component-method helpers (_with_column/_rename_columns/_fill_missing/_substitute) in runtime_lib"
```

---

### Task 2: `lib.expression` (flagship)

**Files:**
- Modify: `informatica_sparker/informatica_sparker/templates/runtime_lib.py.j2`
- Create: `informatica_sparker/tests/test_lib_expression.py`

**Interfaces:**
- Consumes: `_rename_columns`, `_with_column`, `_fill_missing`, `_substitute` (Task 1).
- Produces:
  - `expression(spark, input_df, name, rename_columns=None, computed_columns=None, pass_through_cols=None, substitutions=None, inline_lookup_joins=None, sp_calls=None, sp_conn=None, config=None, **kwargs)` → DataFrame.
  - `computed_columns`: `[{'name': str, 'expr': str}]` — translated expressions, `$$` placeholders allowed, API-marker detection inside the method.
  - `inline_lookup_joins`: `[{'lookup_df': DataFrame, 'join_predicates': [{'source_col', 'lookup_col'}], 'return_port': str}]`.
  - `sp_calls`: `[{'col': str, 'sp_call': str, 'sp_schema': str, 'args': [str]}]`.
  - Execution order (Informatica semantics): renames → inline lookup joins → computed columns → SP calls → pass-through fills. All upstream columns preserved.

- [ ] **Step 1: Write the failing test** — `tests/test_lib_expression.py` (uses the conftest fixtures from Task 1)

```python
"""Tests for lib.expression — the flagship component method.

Fixtures (runtime_lib, spark) come from tests/conftest.py.
"""


def test_expression_basic(runtime_lib, spark):
    df = spark.createDataFrame([(1, 10), (2, 20)], ["K", "V"])
    out = runtime_lib.expression(
        spark=spark, input_df=df, name="EXPTRANS",
        computed_columns=[{"name": "V2", "expr": "V * 2"}],
        pass_through_cols=["K", "V", "MISSING"],
    )
    assert out.columns == ["K", "V", "V2", "MISSING"]
    rows = {r["K"]: r for r in out.collect()}
    assert rows[1]["V2"] == 20 and rows[1]["MISSING"] is None


def test_expression_renames_before_computed(runtime_lib, spark):
    df = spark.createDataFrame([(1,)], ["A"])
    out = runtime_lib.expression(
        spark=spark, input_df=df, name="EXPR",
        rename_columns=[("A", "B")],
        computed_columns=[{"name": "C", "expr": "B + 1"}],
    )
    assert out.columns == ["B", "C"]
    assert out.collect()[0]["C"] == 2


def test_expression_api_computed(runtime_lib, spark):
    df = spark.createDataFrame([(1,), (2,)], ["A"])
    out = runtime_lib.expression(
        spark=spark, input_df=df, name="EXPR",
        computed_columns=[{"name": "SEQ", "expr": "monotonically_increasing_id() + 1"}],
    )
    assert sorted(r["SEQ"] for r in out.collect()) == [1, 2]


def test_expression_substitution(runtime_lib, spark):
    df = spark.createDataFrame([(1,)], ["A"])
    out = runtime_lib.expression(
        spark=spark, input_df=df, name="EXPR",
        computed_columns=[{"name": "B", "expr": "A + $$v_off"}],
        substitutions={"$$v_off": "5"},
    )
    assert out.collect()[0]["B"] == 6


def test_expression_inline_lookup_join(runtime_lib, spark):
    main = spark.createDataFrame([(1,), (2,)], ["IN_KEY"])
    lkp = spark.createDataFrame([(1, "X"), (2, "Y")], ["KEY", "VAL"])
    out = runtime_lib.expression(
        spark=spark, input_df=main, name="EXPR",
        inline_lookup_joins=[{
            "lookup_df": lkp,
            "join_predicates": [{"source_col": "IN_KEY", "lookup_col": "KEY"}],
            "return_port": "VAL",
        }],
    )
    assert out.columns == ["IN_KEY", "VAL"]
    assert {r["IN_KEY"]: r["VAL"] for r in out.collect()} == {1: "X", 2: "Y"}


def test_expression_sp_calls(runtime_lib, spark, monkeypatch):
    df = spark.createDataFrame([(1,), (2,)], ["COL"])
    calls = []
    monkeypatch.setattr(runtime_lib, "call_stored_procedure",
                        lambda s, c, sp, args: calls.append((sp, list(args))))
    out = runtime_lib.expression(
        spark=spark, input_df=df, name="EXPR",
        sp_calls=[{"col": "OUT", "sp_call": "PKG.SP_X", "sp_schema": "PDPA", "args": ["COL"]}],
        sp_conn={"schema": "PSOR"},
    )
    assert calls == [("PSOR.PKG.SP_X", [1]), ("PSOR.PKG.SP_X", [2])]
    assert out.collect()[0]["OUT"] == "SUCCESS"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3.11 -m pytest tests/test_lib_expression.py -v 2>&1 | tail -10`
Expected: FAIL — `AttributeError: 'module' object has no attribute 'expression'`.

- [ ] **Step 3: Implement `expression`** — append to `templates/runtime_lib.py.j2` after the shared helpers:

```python
def expression(spark, input_df, name, rename_columns=None,
               computed_columns=None, pass_through_cols=None,
               substitutions=None, inline_lookup_joins=None,
               sp_calls=None, sp_conn=None, config=None, **kwargs):
    """Convert one Informatica Expression into PySpark column operations.

    Order matches the Informatica mapping:
      1. Connector renames (drop target first, case-insensitive dup guard).
      2. Inline lookup joins (:LKP.xxx() ports): broadcast left join with
         prefixed join keys, dropped after the join.
      3. Computed columns in port order ($$ mapping variables substituted;
         direct-API expressions detected inside _with_column).
      4. Stored-procedure calls (:SP.xxx()): collect argument columns on the
         driver, call call_stored_procedure per row, set the column to
         'SUCCESS'.
      5. Pass-through fills: output ports absent from the frame become
         lit(None).
    All upstream columns are preserved.
    """
    df = _rename_columns(input_df, rename_columns or [])
    for _lkp in (inline_lookup_joins or []):
        _lkp_df = _lkp["lookup_df"]
        _sub = _lkp_df.select(
            *[col(_jp["lookup_col"]).alias("_lkp_jk_" + _jp["lookup_col"])
              for _jp in _lkp["join_predicates"]],
            col(_lkp["return_port"]),
        )
        _cond = None
        for _jp in _lkp["join_predicates"]:
            _part = df[_jp["source_col"]] == _sub["_lkp_jk_" + _jp["lookup_col"]]
            _cond = _part if _cond is None else (_cond & _part)
        df = df.join(broadcast(_sub), on=_cond, how="left")
        for _jp in _lkp["join_predicates"]:
            df = df.drop("_lkp_jk_" + _jp["lookup_col"])
    for _col_def in (computed_columns or []):
        df = _with_column(df, _col_def["name"],
                          _substitute(_col_def["expr"], substitutions))
    for _sp in (sp_calls or []):
        _sp_call = _sp["sp_call"]
        if _sp.get("sp_schema"):
            _schema = (sp_conn or {}).get("schema", "") or _sp["sp_schema"]
            _sp_call = _schema + "." + _sp_call
        _args = _sp["args"]
        if len(_args) == 1:
            _vals = [r[_args[0]] for r in df.select(_args[0]).collect()]
            for _v in _vals:
                call_stored_procedure(spark, sp_conn, _sp_call, [_v])
        else:
            _rows = [r for r in df.select(*_args).collect()]
            for _r in _rows:
                call_stored_procedure(spark, sp_conn, _sp_call, [_r[c] for c in _args])
        df = df.withColumn(_sp["col"], lit("SUCCESS"))
    df = _fill_missing(df, pass_through_cols or [])
    return df
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python3.11 -m pytest tests/test_lib_expression.py tests/test_lib_helpers.py -v 2>&1 | tail -8`
Expected: all tests PASS (helpers + expression).

- [ ] **Step 5: Commit**

```bash
git add informatica_sparker/templates/runtime_lib.py.j2 tests/test_lib_expression.py
git commit -m "feat: lib.expression component method (renames, inline lookups, computed, SP calls, passthrough)"
```

---

### Task 3: `lib.filter`

**Files:**
- Modify: `informatica_sparker/informatica_sparker/templates/runtime_lib.py.j2`
- Create: `informatica_sparker/tests/test_lib_filter.py`

**Interfaces:**
- Consumes: `_rename_columns`, `_substitute` (Task 1).
- Produces: `filter(spark, input_df, name, rename_columns=None, condition=None, substitutions=None, sequence_attach=None, config=None, **kwargs)` → DataFrame.
  - `condition`: raw translated text (no `expr()` wrapper); `$$` placeholders allowed; empty → treated as `"TRUE"`.
  - `sequence_attach`: `[{'col': str, 'start': int}]` — NEXTVAL attached AFTER the filter.
  - Execution order: renames → filter → sequence attach.

- [ ] **Step 1: Write the failing test** — `tests/test_lib_filter.py` (uses the conftest fixtures from Task 1)

```python
"""Tests for lib.filter. Fixtures (runtime_lib, spark) come from conftest.py."""


def test_filter_basic(runtime_lib, spark):
    df = spark.createDataFrame([(1,), (2,), (3,)], ["A"])
    out = runtime_lib.filter(spark=spark, input_df=df, name="FILTRANS",
                             condition="A > 1")
    assert sorted(r["A"] for r in out.collect()) == [2, 3]


def test_filter_rename_before_condition(runtime_lib, spark):
    df = spark.createDataFrame([(1,), (2,)], ["A"])
    out = runtime_lib.filter(spark=spark, input_df=df, name="FILTRANS",
                             rename_columns=[("A", "B")], condition="B > 1")
    assert out.columns == ["B"] and sorted(r["B"] for r in out.collect()) == [2]


def test_filter_substitution_or_zero(runtime_lib, spark):
    df = spark.createDataFrame([(1,), (2,)], ["A"])
    out = runtime_lib.filter(spark=spark, input_df=df, name="FILTRANS",
                             condition="A > $$v_min", substitutions={"$$v_min": ""})
    assert sorted(r["A"] for r in out.collect()) == [1, 2]  # "A > 0"


def test_filter_empty_condition_true(runtime_lib, spark):
    df = spark.createDataFrame([(1,), (2,)], ["A"])
    out = runtime_lib.filter(spark=spark, input_df=df, name="FILTRANS",
                             condition="TRUE")
    assert out.count() == 2


def test_filter_sequence_attach_after(runtime_lib, spark):
    df = spark.createDataFrame([(1,), (2,)], ["A"])
    out = runtime_lib.filter(spark=spark, input_df=df, name="FILTRANS",
                             condition="A >= 1",
                             sequence_attach=[{"col": "NEXTVAL", "start": 100}])
    assert out.columns == ["A", "NEXTVAL"]
    assert sorted(r["NEXTVAL"] for r in out.collect()) == [100, 101]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3.11 -m pytest tests/test_lib_filter.py -v 2>&1 | tail -6`
Expected: FAIL — `AttributeError: 'module' object has no attribute 'filter'`.

- [ ] **Step 3: Implement `filter`** — append to `templates/runtime_lib.py.j2` after `expression`:

```python
def filter(spark, input_df, name, rename_columns=None, condition=None,
           substitutions=None, sequence_attach=None, config=None, **kwargs):
    """Convert one Informatica Filter.

    Connector renames run BEFORE the condition (the condition references the
    filter's input port names). Connected Sequence Generators attach NEXTVAL
    AFTER the filter (post-filter placement). $$ mapping variables in the
    condition use the str(v or "0") rule so an empty value cannot produce
    invalid SQL.
    """
    df = _rename_columns(input_df, rename_columns or [])
    if not condition:
        raise ValueError("Filter %s: empty condition" % name)
    df = df.filter(expr(_substitute(condition, substitutions, or_zero=True)))
    for _att in (sequence_attach or []):
        df = df.withColumn(_att["col"],
                           monotonically_increasing_id() + int(_att["start"]))
    return df
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python3.11 -m pytest tests/test_lib_filter.py tests/test_lib_helpers.py -v 2>&1 | tail -6`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add informatica_sparker/templates/runtime_lib.py.j2 tests/test_lib_filter.py
git commit -m "feat: lib.filter component method (renames, $$ or-zero substitution, sequence attach)"
```

---

### Task 4: Generator wiring — Expression (handler config + template block)

**Files:**
- Modify: `informatica_sparker/informatica_sparker/handlers.py` (`_handle_expression`, around line 1267-1560)
- Modify: `informatica_sparker/informatica_sparker/templates/mapping.py.j2` (APPLY_EXPRESSION block, lines 313-413)

**Interfaces:**
- Consumes: `lib.expression` signature (Task 2); existing handler locals: `computed_columns` (list of `ComputedColumn` with `.name`/`.expression`), `output_columns`, `inline_lkp_info` (values are dicts `{'lookup_name', 'lookup_df', 'return_port', 'join_predicates'}` where `lookup_df` is a DataFrame **name string**), `transform`, `_re`, `plan.mapping_variables`.
- Produces: `step.params["expression_cfg"]` dict with keys: `rename_columns` (list of `(old, new)` tuples, optional), `computed_columns` (`[{'name', 'expr'}]`, optional), `pass_through_cols` (list, optional), `substitutions` (`{'$$v_x': 'v_x'}`, optional — values are **identifier names**, not values), `inline_lookup_joins` (optional), `sp_calls` (`[{'col', 'sp_call', 'sp_schema', 'args'}]`, optional).

- [ ] **Step 1: Add the `_build_expression_cfg` helper and wire the three call sites**

1a. Add a module-level helper to `handlers.py` (near the other module-level helpers, above the `TransformHandlers` class):

```python
def _build_expression_cfg(step, computed_columns, output_columns, transform,
                          plan, inline_lkp_info, re_module, transform_map):
    """Build the lib.expression config.

    Translation (expression texts) already happened in the handler; this
    classifies the translated columns into the config keys lib.expression
    consumes. SP columns (:SP.xxx()) are excluded from computed_columns and
    become sp_calls entries.
    """
    _sp_cols = set()
    _sp_calls = []
    if transform:
        for _f in transform.fields:
            if not (_f.expression and ':SP.' in _f.expression):
                continue
            _sp_match = re_module.search(r':SP\.(\w+)', _f.expression)
            if not _sp_match:
                continue
            _sp_trans = transform_map.get(_sp_match.group(1))
            if not (_sp_trans and _sp_trans.table_attributes):
                continue
            _sp_full = _sp_trans.table_attributes.get("Stored Procedure Name", "")
            if not _sp_full:
                continue
            _parts = _sp_full.split('.')
            if len(_parts) >= 3:
                _sp_call, _sp_schema = '.'.join(_parts[1:]), _parts[0]
            else:
                _sp_call, _sp_schema = _sp_full, ""
            _args = [a.strip() for a in
                     _f.expression.split('(', 1)[1].rsplit(')', 1)[0].split(',')]
            _sp_calls.append({"col": _f.name, "sp_call": _sp_call,
                              "sp_schema": _sp_schema, "args": _args})
            _sp_cols.add(_f.name)
    _lib_cfg = {
        "rename_columns": list(step.params.get("rename_columns") or []),
        "computed_columns": [
            {"name": _cc.name, "expr": _cc.expression}
            for _cc in computed_columns
            if _cc.name not in _sp_cols
            and (_cc.expression or "").strip() != _cc.name
        ],
        "pass_through_cols": [
            _c for _c in output_columns
            if _c not in {_cc.name for _cc in computed_columns}
        ],
    }
    _all_exprs = " ".join(_cc.expression or "" for _cc in computed_columns)
    _subs = {}
    if '$$' in _all_exprs and plan and plan.mapping_variables:
        for _var, _val in plan.mapping_variables.items():
            if _var in _all_exprs:
                _subs[_var] = _val.replace('$', '')
    if _subs:
        _lib_cfg["substitutions"] = _subs
    if inline_lkp_info:
        _lib_cfg["inline_lookup_joins"] = list(inline_lkp_info.values())
    if _sp_calls:
        _lib_cfg["sp_calls"] = _sp_calls
    return _lib_cfg
```

1b. In `_handle_expression`: set the cfg after the existing param stores (after the `params["inline_lookup_joins"]` / SP-params block, right before the multi-upstream branch at ~line 1570):

```python
        step.params["expression_cfg"] = _build_expression_cfg(
            step, computed_columns, output_columns, transform, plan,
            inline_lkp_info, _re, self.transform_map)
```

1c. The two mapplet rename-only step builders (handlers.py:4161 and :4421, where `_rename_step.params["rename_columns"] = ...` is set on an `ApplyExpressionStep`) get the same assignment after their `rename_columns` set:

```python
        _rename_step.params["expression_cfg"] = _build_expression_cfg(
            _rename_step, [], [], None, None, {}, _re, self.transform_map)
```

(rename-only steps have no computed columns; their `computed_columns`/`output_columns`/`transform` are empty/None. Verify with `grep -n "params\[.rename_columns.\] =" handlers.py` that both sites are covered.)

- Existing `params["sp_call_text"]`/`params["sp_schema"]`/`params["window_imports"]`/`params["inline_lookup_joins"]`/`params["output_columns"]` sets stay (harmless once the template block is replaced).
- `Dict`/`Any` annotations are optional here — plain dicts keep it simple.

- [ ] **Step 2: Replace the APPLY_EXPRESSION template block** — `templates/mapping.py.j2`, replace lines 313-413 (from `{% elif step.step_type == IRStepType.APPLY_EXPRESSION %}` through the `ctx.register_df` line) with:

```jinja2
        {% elif step.step_type == IRStepType.APPLY_EXPRESSION %}
        # Expression: {{ step.step_name }}
        {% set ecfg = step.params.get('expression_cfg', {}) %}
        {{ step.df_output }} = lib.expression(
            spark=spark,
            input_df={{ step.df_input }},
            name='{{ step.step_name }}',
            {% if ecfg.get('rename_columns') %}
            rename_columns={{ ecfg['rename_columns'] | pyrepr }},
            {% endif %}
            {% if ecfg.get('computed_columns') %}
            computed_columns=[
                {{ ecfg['computed_columns'] | map('pyrepr') | join(',\n                ') }}
            ],
            {% endif %}
            {% if ecfg.get('pass_through_cols') %}
            pass_through_cols={{ ecfg['pass_through_cols'] | pyrepr }},
            {% endif %}
            {% if ecfg.get('substitutions') %}
            substitutions={% for _k, _v in ecfg['substitutions'].items() %}'{{ _k }}': {{ _v }}{% if not loop.last %}, {% endif %}{% endfor %},
            {% endif %}
            {% if ecfg.get('inline_lookup_joins') %}
            inline_lookup_joins=[
            {% for _j in ecfg['inline_lookup_joins'] %}
                {'lookup_df': {{ _j['lookup_df'] }}, 'join_predicates': {{ _j['join_predicates'] | pyrepr }}, 'return_port': {{ _j['return_port'] | pyrepr }}},
            {% endfor %}
            ],
            {% endif %}
            {% if ecfg.get('sp_calls') %}
            sp_calls=[
                {{ ecfg['sp_calls'] | map('pyrepr') | join(',\n                ') }}
            ],
            sp_conn=conn_oracle,
            {% endif %}
            config=config,
        )
        ctx.register_df("{{ step.df_output }}", {{ step.df_output }})
```

- [ ] **Step 3: Run the existing test suite**

Run: `python3.11 -m pytest tests/ -q 2>&1 | tail -10`
Expected: existing tests PASS. If any test asserts on the old inline expression rendering (e.g. `withColumn(...)` in a rendered mapping fixture), update that assertion to the `lib.expression(` call form.

- [ ] **Step 4: Commit**

```bash
git add informatica_sparker/handlers.py informatica_sparker/templates/mapping.py.j2
git commit -m "feat: generator emits lib.expression calls for APPLY_EXPRESSION steps"
```

---

### Task 5: Generator wiring — Filter (handler config + template block)

**Files:**
- Modify: `informatica_sparker/informatica_sparker/handlers.py` (`_handle_filter`, lines 1203-1265)
- Modify: `informatica_sparker/informatica_sparker/templates/mapping.py.j2` (APPLY_FILTER block, lines 274-311)

**Interfaces:**
- Consumes: `lib.filter` signature (Task 3); existing handler locals: `filter_inner` (raw translated condition text; bare-numeric rewrite already applied), `_filter_renames` (`[{'from', 'to'}]`), `plan.mapping_variables`, `self._sequence_attachments`.
- Produces: `step.params["lib_filter_cfg"]` dict with keys: `rename_columns` (`[(old, new)]`), `condition` (raw text, `"TRUE"` if empty; `$$` normalized via `_normalize_sql_text`), `substitutions` (optional, `{'$$v_x': 'v_x'}`), `sequence_attach` (optional).

- [ ] **Step 1: Replace the filter param stores**

In `_handle_filter`, replace lines 1247-1264 (the `if filter_inner and '$$' ...` block through `return step`) with:

```python
        # Component-method config: lib.filter owns the runtime semantics
        # (renames before condition, $$ substitution, sequence attach).
        _cond_text = filter_inner
        if _cond_text and '$$' in _cond_text and plan and plan.mapping_variables:
            _cond_text = self._normalize_sql_text(_cond_text, plan)
        _lib_cfg: Dict[str, Any] = {
            "rename_columns": [(r["from"], r["to"]) for r in _filter_renames],
            "condition": _cond_text or "TRUE",
        }
        _subs: Dict[str, str] = {}
        if '$$' in _cond_text and plan and plan.mapping_variables:
            for _var, _val in plan.mapping_variables.items():
                if _var in _cond_text:
                    _subs[_var] = _val.replace('$', '')
        if _subs:
            _lib_cfg["substitutions"] = _subs
        if self._sequence_attachments.get(instance.name):
            _lib_cfg["sequence_attach"] = self._sequence_attachments[instance.name]
        step.params["lib_filter_cfg"] = _lib_cfg
        return step
```

- [ ] **Step 2: Replace the APPLY_FILTER template block** — `templates/mapping.py.j2`, replace lines 274-311 with:

```jinja2
        {% elif step.step_type == IRStepType.APPLY_FILTER %}
        # Filter: {{ step.step_name }}
        {% set fcfg = step.params.get('lib_filter_cfg', {}) %}
        {{ step.df_output }} = lib.filter(
            spark=spark,
            input_df={{ step.df_input }},
            name='{{ step.step_name }}',
            {% if fcfg.get('rename_columns') %}
            rename_columns={{ fcfg['rename_columns'] | pyrepr }},
            {% endif %}
            condition={{ fcfg.get('condition', 'TRUE') | pyrepr }},
            {% if fcfg.get('substitutions') %}
            substitutions={% for _k, _v in fcfg['substitutions'].items() %}'{{ _k }}': {{ _v }}{% if not loop.last %}, {% endif %}{% endfor %},
            {% endif %}
            {% if fcfg.get('sequence_attach') %}
            sequence_attach={{ fcfg['sequence_attach'] | pyrepr }},
            {% endif %}
            config=config,
        )
        ctx.register_df("{{ step.df_output }}", {{ step.df_output }})
```

- [ ] **Step 3: Run the existing test suite**

Run: `python3.11 -m pytest tests/ -q 2>&1 | tail -10`
Expected: existing tests PASS (fix any inline-filter-rendering assertions as in Task 4 Step 3).

- [ ] **Step 4: Commit**

```bash
git add informatica_sparker/handlers.py informatica_sparker/templates/mapping.py.j2
git commit -m "feat: generator emits lib.filter calls for APPLY_FILTER steps"
```

---

### Task 6: Reconvert WF_EMS_DDS_APLY_MTH and verify (Phase 1 gate)

**Files:**
- Run: `informatica_sparker` build + CLI convert; backup dir `PySpark_workflows/dds/_pre_refactor_WF_EMS_DDS_APLY_MTH/`
- Test: reconverted `PySpark_workflows/dds/WF_EMS_DDS_APLY_MTH/`

- [ ] **Step 1: Backup the current generated output** (before any reconversion touches it)

```bash
cd /var/lib/airflow/dags/adam/informatica
cp -r PySpark_workflows/dds/WF_EMS_DDS_APLY_MTH PySpark_workflows/dds/_pre_refactor_WF_EMS_DDS_APLY_MTH
ls PySpark_workflows/dds/_pre_refactor_WF_EMS_DDS_APLY_MTH/m_*.py | wc -l   # expect 49
```

- [ ] **Step 2: Rebuild and reinstall the wheel** (CLI reads site-packages, not repo source)

```bash
cd /var/lib/airflow/dags/adam/informatica/informatica_sparker
python3.11 -m build 2>&1 | tail -5
python3.11 -m pip install --user --force-reinstall dist/informatica_sparker-*.whl 2>&1 | tail -3
```

- [ ] **Step 3: Reconvert the test workflow**

```bash
OUT_ROOT=/var/lib/airflow/dags/adam/informatica/PySpark_workflows
informatica-sparker convert /var/lib/airflow/dags/adam/informatica/PowerCenter_workflows/dds/WF_EMS_DDS_APLY_MTH.XML -o $OUT_ROOT/dds/WF_EMS_DDS_APLY_MTH 2>&1 | tail -5
```
Expected: **0 warnings / 0 errors** (the established gate).

- [ ] **Step 4: Static checks on the reconverted output**

```bash
cd /var/lib/airflow/dags/adam/informatica/PySpark_workflows/dds/WF_EMS_DDS_APLY_MTH
grep -c "lib.expression(" m_*.py | grep -v ":0" | wc -l          # expression steps → lib.expression
grep -c "lib.filter(" m_*.py | grep -v ":0" | wc -l              # filter steps → lib.filter
grep -rn "\.filter(expr(" m_*.py | wc -l                          # expect 0 inline filter blocks
grep -rn "withColumn(" m_*.py | wc -l                             # expect only pre-steps/target remnants
grep -rn "lib.expression(" m_*.py | head -2                       # spot-check the call form
```
Also spot-check one mapping against its backup: the expression steps in the new file must carry the same computed columns/renames as the backup's inline form (e.g. compare `grep -c "withColumn("` per file between `_pre_refactor_...` and the new output — every old inline expression column must appear as a `{'name': ..., 'expr': ...}` entry in the new file; count via `grep -o "'name': '[A-Z_]*'"` per expression step). Report the per-file counts in the task summary.

- [ ] **Step 5: Full test suite + reconverted-workflow smoke**

```bash
cd /var/lib/airflow/dags/adam/informatica/informatica_sparker
python3.11 -m pytest tests/ -q 2>&1 | tail -5
```
Expected: all PASS.

- [ ] **Step 6: Runtime verification (user checkpoint — cluster access required)**

Pick 3-5 mappings covering the phase's semantics (expression chains, filter with `$$` variables, filter feeding lookup, connected-sequence attach — check `grep -l "sequence_attach\|NEXTVAL" m_*.py` for sequence candidates, `grep -l '\$\$' m_*.py` for substitution candidates). Run each on the cluster with the default python:

```bash
cd /var/lib/airflow/dags/adam/informatica/PySpark_workflows/dds/WF_EMS_DDS_APLY_MTH
python m_<mapping>.py 2>&1 | tail -20
```
Expected per mapping: `Mapping ... completed: SUCCESS`; target tables get the same deltas as the verified v2026.08.03 round. If a run fails, the difference is attributable: either a refactor regression (template move) or pre-existing converter evolution — compare against `_pre_refactor_.../m_<mapping>.py` behavior (Gate 4 optional old/new equivalence).

- [ ] **Step 7: Update CLAUDE.md**

Add a "Component Methods" section to `/var/lib/airflow/dags/adam/informatica/CLAUDE.md` (English, matching existing style): document the three-layer separation, the kwargs call convention, and the Phase-1 method list (`lib.expression`, `lib.filter`) with the shared helpers. Note: CLAUDE.md lives **outside** the informatica_sparker repo (`/var/lib/airflow/dags` is not a git repo) — edit it in place, no commit.

---

## Out of scope (this plan)

- Static lookup, joiner, aggregator, router, union, sorter, sequence, SQ, update strategy, write target — Phases 2-4, each gets its own plan after this one delivers.
- `lib.dynamic_lookup` — unchanged.
- Any expression-translation changes (`expr_translator` untouched).
- No regeneration of other workflows.

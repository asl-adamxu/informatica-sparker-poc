# Component Methods Phase 2 Implementation Plan — static_lookup + joiner + aggregator + stored_procedure

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend the component-method pattern (Phase 1 delivered `lib.expression` / `lib.filter`) to the four remaining data-flow components of this phase: `lib.static_lookup` (all 4 join kinds + dedup policies + NewLookupRow probe), `lib.joiner` (master/detail select+alias, 3 join shapes), `lib.aggregator` (groupBy/agg incl. literal GROUPBY + DISTINCT path), and `lib.stored_procedure` (user-requested: reusable SP components as a method, with the expression `sp_calls` path sharing its implementation).

**Architecture:** Same three-layer separation as Phase 1 (design doc `docs/specs/2026-08-12-component-methods-design.md`): generator = syntax conversion (translation stays at codegen), runtime_lib methods = runtime semantics, generated files = declarative kwargs calls. Every template block moves verbatim — test locks behavior first, then the template code is deleted.

**Tech Stack:** Jinja2 templates (mapping.py.j2, runtime_lib.py.j2), Python 3.11 generator build, PySpark 3.5.4, pytest with local SparkSession + conftest fixtures (tests/conftest.py from Phase 1), informatica-sparker CLI.

## Global Constraints

- Translation stays at codegen time: `lib.*` methods receive **translated** results only.
- Verbatim move: no behavior changes; a test locks behavior before the template block is deleted.
- kwargs call form: `lib.xxx(spark=spark, input_df=..., <keys>, config=config)`; `name` is NOT rendered (Phase 1 decision — the Step log locates the step; runtime signature keeps `name=None` for error messages only).
- Complex lists render one dict per line via `map('pyrepr') | join(',\n<indent>')`; scalar values via `pyrepr`; `rename_columns` renders one tuple per line (Phase 1 pattern).
- `substitutions` values render as identifiers (never quoted); dict braces via `{{ '{' }}...{{ '}' }}`.
- No Chinese comments in generated code. Zero import side effects for mapping modules.
- **Phase 1 lessons (binding)**: (1) mapplet-internal construction paths must be wired too — the template reads ONLY the new cfg key; (2) add wiring regression tests parsing real XML (every step of the type must carry the cfg); (3) the reconversion gate must compare per-step semantics against the backup (join conditions, agg expressions, lookup predicates), not just count calls; (4) render smoke tests ast.parse the real template blocks.
- Test workflow: WF_EMS_DDS_APLY_MTH (49 mappings; coverage: Lookup 48, Aggregator 38, Joiner 2 — verified at runtime in Phase 1).
- Build/convert cycle: `python3.11 -m build` → `python3.11 -m pip install --user --force-reinstall dist/informatica_sparker-*.whl` → `informatica-sparker convert <XML> -o <OUT_ROOT>`; reconversion gate 0 warnings / 0 errors; every generated file passes `python3.11 -m py_compile`.
- Commit per task in the git repo (branch `enhance`); run pytest from the repo root with `python3.11 -m pytest tests/ -q`.
- Runtime-lib tests render `runtime_lib.py.j2` via Jinja2 and import as a module; SparkSession fixture `local[2]` from tests/conftest.py. Watch partition-scoping: `monotonically_increasing_id` values are per-partition — tests needing exact values coalesce/repartition the INPUT frame (Phase 1 precedent).

## MOVE PROTOCOL (apply to every method task)

1. The source of truth is the template block in `mapping.py.j2` (line ranges given per task).
2. `{{ step.df_output }}` → `df`; `{{ step.df_input }}` → `input_df`; Jinja control flow → Python control flow over config lists.
3. Template-time decisions that depend on the **translated string** (expr-vs-API markers) move into lib verbatim (shared helper `_with_column` / eval-namespace pattern from Phase 1).
4. Template-time decisions that depend on **XML metadata** (port classification, policies, dedup) stay in the generator and become config keys.
5. The generated `logger.info("Step: ...")` stays in the generated file; methods log at debug only.

---

### Task 1: `lib.static_lookup` (flagship of this phase)

**Files:**
- Modify: `informatica_sparker/informatica_sparker/templates/runtime_lib.py.j2`
- Create: `informatica_sparker/tests/test_lib_static_lookup.py`

**Interfaces:**
- Consumes: `_rename_columns`? No — lookups use `lkp_field_remap` (withColumn, not drop+rename). Consumes nothing else.
- Produces: `static_lookup(spark, input_df, lookup_df, name=None, join_spec=None, lookup_type='left', broadcast=True, lkp_field_remap=None, dedup=None, new_lookup_row_key=None, new_lookup_row_col='NewLookupRow', config=None, **kwargs)` → DataFrame.
  - `join_spec`: `{'kind': 'predicates', 'predicates': [{'source_col', 'lookup_col'}]}` | `{'kind': 'common_cols'}` | `{'kind': 'expr', 'expr': '<translated join condition>'}` | `{'kind': 'cross'}`.
  - `dedup`: `{'policy': 'report_error'|'last'|'first', 'keys': [str]}` — applied to the LOOKUP DataFrame before joining.
  - `new_lookup_row_key` (probe preference) + `new_lookup_row_col` (emitted column name, default 'NewLookupRow'): the NewLookupRow column is derived at runtime — probe = first lookup column that survived the merge select (case-insensitive exclusion, the key preference first); `expr("CASE WHEN `<probe>` IS NULL THEN 0 ELSE 1 END")`; `lit(1)` fallback when no probe survives.
  - Execution order: lkp port pre-rename → dedup → join (broadcast) → merge select (case-insensitive exclusion) → probe column.
  - Common-cols kind (`__common_cols__`): the `_cc`/`__lkp_dup`/`__rhs` attribute-lineage-break logic from the template moves verbatim (including the synthetic-key `lit(1)` fallback when no common columns exist).

- [ ] **Step 1: Write the failing test** — `tests/test_lib_static_lookup.py`

```python
"""Tests for lib.static_lookup. Fixtures (runtime_lib, spark) come from conftest.py."""


def test_predicates_join_and_merge_exclusion(runtime_lib, spark):
    main = spark.createDataFrame([(1, "A"), (2, "B")], ["KEY", "IN_VAL"])
    lkp = spark.createDataFrame([(1, "X"), (2, "Y")], ["KEY", "LKP_VAL"])
    out = runtime_lib.static_lookup(
        spark=spark, input_df=main, lookup_df=lkp, name="LKP",
        join_spec={"kind": "predicates",
                   "predicates": [{"source_col": "KEY", "lookup_col": "KEY"}]},
    )
    # lookup KEY excluded (same name as input), LKP_VAL merged
    assert out.columns == ["KEY", "IN_VAL", "LKP_VAL"]
    assert {r["KEY"]: r["LKP_VAL"] for r in out.collect()} == {1: "X", 2: "Y"}


def test_common_cols_kind(runtime_lib, spark):
    main = spark.createDataFrame([(1, "A")], ["KEY", "V"])
    lkp = spark.createDataFrame([(1, "X")], ["KEY", "LKP_VAL"])
    out = runtime_lib.static_lookup(
        spark=spark, input_df=main, lookup_df=lkp, name="LKP",
        join_spec={"kind": "common_cols"},
    )
    assert out.columns == ["KEY", "V", "LKP_VAL"]
    assert out.collect()[0]["LKP_VAL"] == "X"


def test_common_cols_synthetic_key_fallback(runtime_lib, spark):
    main = spark.createDataFrame([(1,)], ["A"])
    lkp = spark.createDataFrame([("x",)], ["B"])
    out = runtime_lib.static_lookup(
        spark=spark, input_df=main, lookup_df=lkp, name="LKP",
        join_spec={"kind": "common_cols"},
    )
    assert out.columns == ["A", "B"] and out.count() == 1


def test_expr_kind(runtime_lib, spark):
    main = spark.createDataFrame([(1,)], ["A"])
    lkp = spark.createDataFrame([(1,)], ["B"])
    out = runtime_lib.static_lookup(
        spark=spark, input_df=main, lookup_df=lkp, name="LKP",
        join_spec={"kind": "expr", "expr": "A = B"},
    )
    assert out.count() == 1


def test_cross_kind(runtime_lib, spark):
    main = spark.createDataFrame([(1,)], ["A"])
    lkp = spark.createDataFrame([("x",), ("y",)], ["B"])
    out = runtime_lib.static_lookup(
        spark=spark, input_df=main, lookup_df=lkp, name="LKP",
        join_spec={"kind": "cross"},
    )
    assert out.count() == 2


def test_lkp_field_remap(runtime_lib, spark):
    main = spark.createDataFrame([(1,)], ["ACTUAL"])
    lkp = spark.createDataFrame([(1, "X")], ["KEY", "LKP_VAL"])
    out = runtime_lib.static_lookup(
        spark=spark, input_df=main, lookup_df=lkp, name="LKP",
        join_spec={"kind": "predicates",
                   "predicates": [{"source_col": "IN_KEY", "lookup_col": "KEY"}]},
        lkp_field_remap={"IN_KEY": "ACTUAL"},
    )
    assert out.collect()[0]["LKP_VAL"] == "X"


def test_dedup_report_error(runtime_lib, spark):
    main = spark.createDataFrame([(1,)], ["KEY"])
    lkp = spark.createDataFrame([(1, "X"), (1, "Y")], ["KEY", "V"])
    try:
        runtime_lib.static_lookup(
            spark=spark, input_df=main, lookup_df=lkp, name="LKP",
            join_spec={"kind": "predicates",
                       "predicates": [{"source_col": "KEY", "lookup_col": "KEY"}]},
            dedup={"policy": "report_error", "keys": ["KEY"]},
        )
        assert False, "expected RuntimeError"
    except RuntimeError:
        pass


def test_dedup_last(runtime_lib, spark):
    main = spark.createDataFrame([(1,)], ["KEY"])
    lkp = spark.createDataFrame([(1, "X"), (1, "Y")], ["KEY", "V"])
    out = runtime_lib.static_lookup(
        spark=spark, input_df=main, lookup_df=lkp, name="LKP",
        join_spec={"kind": "predicates",
                   "predicates": [{"source_col": "KEY", "lookup_col": "KEY"}]},
        dedup={"policy": "last", "keys": ["KEY"]},
    )
    assert out.collect()[0]["V"] == "Y"


def test_dedup_first(runtime_lib, spark):
    main = spark.createDataFrame([(1,)], ["KEY"])
    lkp = spark.createDataFrame([(1, "X"), (1, "Y")], ["KEY", "V"])
    out = runtime_lib.static_lookup(
        spark=spark, input_df=main, lookup_df=lkp, name="LKP",
        join_spec={"kind": "predicates",
                   "predicates": [{"source_col": "KEY", "lookup_col": "KEY"}]},
        dedup={"policy": "first", "keys": ["KEY"]},
    )
    assert out.collect()[0]["V"] == "X"


def test_new_lookup_row_key(runtime_lib, spark):
    main = spark.createDataFrame([(1,), (3,)], ["KEY"])
    lkp = spark.createDataFrame([(1, "X")], ["KEY", "LKP_VAL"])
    out = runtime_lib.static_lookup(
        spark=spark, input_df=main, lookup_df=lkp, name="LKP",
        join_spec={"kind": "predicates",
                   "predicates": [{"source_col": "KEY", "lookup_col": "KEY"}]},
        new_lookup_row_key="NewLookupRow",
    )
    rows = {r["KEY"]: r["NewLookupRow"] for r in out.collect()}
    assert rows == {1: 1, 3: 0}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /var/lib/airflow/dags/adam/informatica/informatica_sparker && python3.11 -m pytest tests/test_lib_static_lookup.py -v 2>&1 | tail -5`
Expected: FAIL — `AttributeError: 'module' object has no attribute 'static_lookup'`.

- [ ] **Step 3: Implement `static_lookup`** — append to `templates/runtime_lib.py.j2` after `filter`:

```python
def static_lookup(spark, input_df, lookup_df, name=None, join_spec=None,
                  lookup_type="left", broadcast=True, lkp_field_remap=None,
                  dedup=None, new_lookup_row_key=None,
                  new_lookup_row_col="NewLookupRow", config=None, **kwargs):
    """Convert one static (non-dynamic) Informatica Lookup.

    Semantics:
      1. lkp_field_remap: upstream columns renamed to the lookup's INPUT port
         names before the join.
      2. dedup: Lookup policy on multiple match — report_error raises on
         duplicate join keys (CMN_1650-style), last keeps the last row per key
         (row_number window), first/any keeps the first (dropDuplicates).
      3. join: broadcast left join by join_spec kind — 'predicates'
         (source_col == lookup_col pairs), 'common_cols' (merge on common
         columns with an attribute-lineage break and a synthetic-key fallback),
         'expr' (translated condition), 'cross' (lit(True)).
      4. merge select: lookup columns that case-insensitively duplicate input
         columns are excluded.
      5. new_lookup_row_key: NewLookupRow = 1 when the lookup matched, 0 when
         it missed — probed at runtime on the first surviving lookup column
         (lit(1) fallback when none survive).
    """
    df = input_df
    for _port, _col in (lkp_field_remap or {}).items():
        df = df.withColumn(_port, col(_col))
    _lkp_df = lookup_df
    if dedup:
        _policy = (dedup.get("policy") or "").lower()
        _keys = dedup.get("keys") or []
        if _policy == "report_error":
            _dup = _lkp_df.groupBy(*[col(_k) for _k in _keys]).count() \
                .filter(col("count") > 1).count()
            if _dup:
                raise RuntimeError(
                    "Lookup %s: %d duplicate keys found — Report Error policy"
                    % (name or "?", _dup))
        elif _policy == "last":
            from pyspark.sql.window import Window as _LkpWindow
            _w = _LkpWindow.partitionBy(*[col(_k) for _k in _keys]) \
                .orderBy(lit(0).desc())
            _lkp_df = _lkp_df.withColumn("_rn", row_number().over(_w)) \
                .filter(col("_rn") == 1).drop("_rn")
        else:
            _lkp_df = _lkp_df.dropDuplicates(subset=_keys)
    _kind = (join_spec or {}).get("kind")
    if _kind == "predicates" and not (join_spec or {}).get("predicates"):
        raise ValueError(
            "Lookup %s: kind=predicates requires non-empty predicates"
            % (name or "?"))
    if _kind == "common_cols":
        _cc = list(dict.fromkeys(
            c for c in df.columns
            if c.lower() in [x.lower() for x in _lkp_df.columns]))
        if _cc:
            __lkp_dup = [c for c in _lkp_df.columns
                         if c.lower() in [x.lower() for x in df.columns]
                         and c.lower() not in [x.lower() for x in _cc]]
            __rhs = _lkp_df.drop(*__lkp_dup) if __lkp_dup else _lkp_df
            __rhs = __rhs.select(*[col(c).alias(c) for c in __rhs.columns])
            return df.join(__rhs, on=_cc, how=lookup_type)
        logging.warning(
            "Lookup %s: no common columns between input and lookup — using "
            "synthetic key join", name or "?")
        __rhs = _lkp_df.withColumn("_join_key", lit(1))
        __rhs = __rhs.select(*[col(c).alias(c) for c in __rhs.columns])
        return df.withColumn("_join_key", lit(1)).join(
            __rhs, on="_join_key", how=lookup_type).drop("_join_key")
    if _kind == "cross":
        return df.join(_lkp_df, lit(True), lookup_type)
    _predicates = (join_spec or {}).get("predicates") or []
    _cond = None
    for _jp in _predicates:
        _part = col("_main.%s" % _jp["source_col"]) == \
            col("_lkp.%s" % _jp["lookup_col"])
        _cond = _part if _cond is None else (_cond & _part)
    if _kind == "expr":
        _cond = expr((join_spec or {}).get("expr", "TRUE"))
    _joined = df.alias("_main").join(
        broadcast(_lkp_df).alias("_lkp") if broadcast else _lkp_df.alias("_lkp"),
        _cond,
        lookup_type,
    ).select(
        *[col("_main." + c).alias(c) for c in df.columns],
        *[_lkp_df[c] for c in _lkp_df.columns
          if c.lower() not in [x.lower() for x in df.columns]],
    )
    if new_lookup_row_key:
        _nlr_cols = [c for c in _lkp_df.columns
                     if c.lower() not in [x.lower() for x in df.columns]
                     and c.lower() != new_lookup_row_key.lower()]
        if (new_lookup_row_key.lower() not in [x.lower() for x in df.columns]
                and new_lookup_row_key.lower() in [x.lower() for x in _nlr_cols]):
            _nlr_cols = [c for c in _nlr_cols
                         if c.lower() != new_lookup_row_key.lower()]
            _nlr_cols.insert(0, new_lookup_row_key)
        if _nlr_cols:
            _joined = _joined.withColumn(
                new_lookup_row_col,
                expr("CASE WHEN `" + _nlr_cols[0] + "` IS NULL THEN 0 ELSE 1 END"))
        else:
            _joined = _joined.withColumn(new_lookup_row_col, lit(1))
    return _joined
```

Notes:
- The old template's predicates-join select used `*[_lkp_input[c] for c in _lkp_input.columns]` (qualified by the input df) plus `*[{{ lookup_df_name }}[c] ...]`. The `col("_main." + c).alias(c)` form is equivalent (same column identity, unambiguous) — the old rendered form relied on alias-qualified access too (`col("_main.{{ jp.source_col }}")` in the join condition). If a test shows column-name drift, fall back to the literal `*[df[c] for c in df.columns]` form.
- `row_number`/`lit`/`col`/`expr`/`broadcast` come from the module's `from pyspark.sql.functions import *`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `python3.11 -m pytest tests/test_lib_static_lookup.py -v 2>&1 | tail -6`
Expected: all PASS. If `test_predicates_join_and_merge_exclusion` fails on column order, adapt the expected column order to what Spark produces (order is not semantic).

- [ ] **Step 5: Commit**

```bash
git add informatica_sparker/templates/runtime_lib.py.j2 tests/test_lib_static_lookup.py
git commit -m "feat: lib.static_lookup component method (4 join kinds, dedup policies, NewLookupRow probe)"
```

---

### Task 2: `lib.joiner`

**Files:**
- Modify: `informatica_sparker/informatica_sparker/templates/runtime_lib.py.j2`
- Create: `informatica_sparker/tests/test_lib_joiner.py`

**Interfaces:**
- Produces: `joiner(spark, master_df, detail_df, name=None, join_type='inner', master_selects=None, detail_selects=None, on=None, missing_cols=None, config=None, **kwargs)` → DataFrame.
  - `master_selects`/`detail_selects`: `[{'from', 'to'}]` — select+alias intermediate frames (from==to renders `col("from")`).
  - `on`: `{'kind': 'cols', 'pairs': [{'master_col', 'detail_col'}]}` | `{'kind': 'expr', 'expr': '<translated>'}` | `{'kind': 'cross'}`.
  - `missing_cols`: `[str]` — lit(None) fill after join.

- [ ] **Step 1: Write the failing test** — `tests/test_lib_joiner.py`

```python
"""Tests for lib.joiner. Fixtures (runtime_lib, spark) come from conftest.py."""


def test_selects_and_cols_join(runtime_lib, spark):
    m = spark.createDataFrame([(1, "A")], ["M_KEY", "M_V"])
    d = spark.createDataFrame([(1, "X")], ["D_KEY", "D_V"])
    out = runtime_lib.joiner(
        spark=spark, master_df=m, detail_df=d, name="JNR",
        join_type="inner",
        master_selects=[{"from": "M_KEY", "to": "KEY"}, {"from": "M_V", "to": "M_V"}],
        detail_selects=[{"from": "D_KEY", "to": "KEY"}, {"from": "D_V", "to": "D_V"}],
        on={"kind": "cols", "pairs": [{"master_col": "KEY", "detail_col": "KEY"}]},
    )
    assert out.columns == ["KEY", "M_V", "D_V"]
    assert out.collect()[0]["D_V"] == "X"


def test_expr_join(runtime_lib, spark):
    m = spark.createDataFrame([(1,)], ["A"])
    d = spark.createDataFrame([(1,)], ["B"])
    out = runtime_lib.joiner(
        spark=spark, master_df=m, detail_df=d, name="JNR",
        on={"kind": "expr", "expr": "A = B"},
    )
    assert out.count() == 1


def test_cross_join(runtime_lib, spark):
    m = spark.createDataFrame([(1,)], ["A"])
    d = spark.createDataFrame([("x",), ("y",)], ["B"])
    out = runtime_lib.joiner(
        spark=spark, master_df=m, detail_df=d, name="JNR",
        on={"kind": "cross"},
    )
    assert out.count() == 2


def test_missing_cols_fill(runtime_lib, spark):
    m = spark.createDataFrame([(1,)], ["A"])
    d = spark.createDataFrame([(1,)], ["B"])
    out = runtime_lib.joiner(
        spark=spark, master_df=m, detail_df=d, name="JNR",
        on={"kind": "expr", "expr": "A = B"},
        missing_cols=["C"],
    )
    assert out.columns == ["A", "B", "C"]
    assert out.collect()[0]["C"] is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3.11 -m pytest tests/test_lib_joiner.py -v 2>&1 | tail -4`
Expected: FAIL — `AttributeError: ... no attribute 'joiner'`.

- [ ] **Step 3: Implement `joiner`** — append to `templates/runtime_lib.py.j2` after `static_lookup`:

```python
def joiner(spark, master_df, detail_df, name=None, join_type="inner",
           master_selects=None, detail_selects=None, on=None,
           missing_cols=None, config=None, **kwargs):
    """Convert one Informatica Joiner.

    Master/detail upstream columns are select+aliased to the Joiner's port
    names first (avoids column conflicts). The join condition is one of:
    'cols' (equi-join on master_col == detail_col pairs), 'expr' (translated
    condition), or 'cross'. Missing Joiner ports without upstream connectors
    are filled with lit(None).
    """
    _m_df = master_df
    if master_selects:
        _m_df = master_df.select(*[
            col(_s["from"]) if _s["from"] == _s["to"]
            else col(_s["from"]).alias(_s["to"])
            for _s in master_selects
        ])
    _d_df = detail_df
    if detail_selects:
        _d_df = detail_df.select(*[
            col(_s["from"]) if _s["from"] == _s["to"]
            else col(_s["from"]).alias(_s["to"])
            for _s in detail_selects
        ])
    _kind = (on or {}).get("kind")
    if _kind == "cols":
        _cond = None
        for _p in (on or {}).get("pairs") or []:
            _part = _m_df[_p["master_col"]] == _d_df[_p["detail_col"]]
            _cond = _part if _cond is None else (_cond & _part)
    elif _kind == "expr":
        _cond = expr((on or {}).get("expr", "TRUE"))
    else:
        _cond = lit(True)
    df = _m_df.join(_d_df, _cond, join_type)
    for _c in (missing_cols or []):
        if _c.lower() not in [x.lower() for x in df.columns]:
            df = df.withColumn(_c, lit(None))
    return df
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python3.11 -m pytest tests/test_lib_joiner.py tests/test_lib_static_lookup.py -v 2>&1 | tail -4`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add informatica_sparker/templates/runtime_lib.py.j2 tests/test_lib_joiner.py
git commit -m "feat: lib.joiner component method (master/detail selects, cols/expr/cross joins, missing fills)"
```

---

### Task 3: `lib.aggregator`

**Files:**
- Modify: `informatica_sparker/informatica_sparker/templates/runtime_lib.py.j2`
- Create: `informatica_sparker/tests/test_lib_aggregator.py`

**Interfaces:**
- Consumes: `_api_namespace()` (Phase 1 helper — the eval namespace for direct-API expressions).
- Produces: `aggregator(spark, input_df, name=None, group_by=None, aggs=None, agg_selects=None, agg_literals=None, distinct=False, missing_cols=None, config=None, **kwargs)` → DataFrame.
  - `aggs`: `[{'name', 'expr'}]` where `expr` is the translated Python-source form the old template rendered directly (`sum("col")` / `expr("""...""")` / `expr(f"""...{v}...""")`) — evaluated via `_api_namespace()`.
  - `agg_selects`: `[{'from', 'to'}]`; `from` in ('__null__', 'DUMMY') → `lit(None).alias(to)`.
  - `agg_literals`: `[{'name', 'expression'}]` → `expr(expression).alias(name)` (added to the input select AND groupBy).
  - `distinct=True` (GROUP BY with no aggregations): select agg_selects (or group_by columns) + `.distinct()`.
  - `missing_cols`: lit(None) fill after.

- [ ] **Step 1: Write the failing test** — `tests/test_lib_aggregator.py`

```python
"""Tests for lib.aggregator. Fixtures (runtime_lib, spark) come from conftest.py."""


def test_groupby_agg_api_form(runtime_lib, spark):
    df = spark.createDataFrame([("a", 1), ("a", 2), ("b", 3)], ["K", "V"])
    out = runtime_lib.aggregator(
        spark=spark, input_df=df, name="AGG",
        group_by=["K"],
        aggs=[{"name": "TOTAL", "expr": 'sum("V")'}],
    )
    rows = {r["K"]: r["TOTAL"] for r in out.collect()}
    assert rows == {"a": 3, "b": 3}


def test_agg_expr_wrapped_form(runtime_lib, spark):
    df = spark.createDataFrame([("a", 1), ("a", 2)], ["K", "V"])
    out = runtime_lib.aggregator(
        spark=spark, input_df=df, name="AGG",
        group_by=["K"],
        aggs=[{"name": "TOTAL", "expr": 'expr("sum(V)")'}],
    )
    assert out.collect()[0]["TOTAL"] == 3


def test_agg_selects_and_literals(runtime_lib, spark):
    df = spark.createDataFrame([("a", 1)], ["SRC", "V"])
    out = runtime_lib.aggregator(
        spark=spark, input_df=df, name="AGG",
        group_by=["K"],
        aggs=[{"name": "TOTAL", "expr": 'sum("V")'}],
        agg_selects=[{"from": "SRC", "to": "K"}, {"from": "__null__", "to": "D"}],
        agg_literals=[{"name": "DUMMY", "expression": "0"}],
    )
    assert out.columns == ["K", "DUMMY", "TOTAL"]
    assert out.collect()[0]["D"] is None


def test_distinct_path(runtime_lib, spark):
    df = spark.createDataFrame([("a", 1), ("a", 1), ("b", 2)], ["K", "V"])
    out = runtime_lib.aggregator(
        spark=spark, input_df=df, name="AGG",
        group_by=["K"], distinct=True,
        agg_selects=[{"from": "K", "to": "K"}],
    )
    assert sorted(r["K"] for r in out.collect()) == ["a", "b"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3.11 -m pytest tests/test_lib_aggregator.py -v 2>&1 | tail -4`
Expected: FAIL — `AttributeError: ... no attribute 'aggregator'`.

- [ ] **Step 3: Implement `aggregator`** — append to `templates/runtime_lib.py.j2` after `joiner`:

```python
def aggregator(spark, input_df, name=None, group_by=None, aggs=None,
               agg_selects=None, agg_literals=None, distinct=False,
               missing_cols=None, config=None, **kwargs):
    """Convert one Informatica Aggregator.

    GROUP BY with aggregations: upstream columns are select+aliased to the
    Aggregator's port names (unconnected INPUT ports become lit(None),
    literal GROUPBY fields become expr(...) columns), then groupBy + agg
    over the translated aggregation expressions (evaluated as Python source
    against the pyspark namespace — the old template rendered them directly).
    GROUP BY with no aggregations is equivalent to DISTINCT over the selected
    columns.
    """
    _group_by = group_by or []
    if distinct:
        if agg_selects:
            return input_df.select(*[
                lit(None).alias(_s["to"]) if _s["from"] in ("__null__", "DUMMY")
                else (col(_s["from"]) if _s["from"] == _s["to"]
                      else col(_s["from"]).alias(_s["to"]))
                for _s in agg_selects
            ]).distinct()
        return input_df.select(*_group_by).distinct()
    if not _group_by:
        return _fill_missing(input_df, missing_cols)
    _agg_input = input_df
    if agg_selects or agg_literals:
        _sel = []
        for _s in agg_selects or []:
            if _s["from"] in ("__null__", "DUMMY"):
                _sel.append(lit(None).alias(_s["to"]))
            elif _s["from"] == _s["to"]:
                _sel.append(col(_s["from"]))
            else:
                _sel.append(col(_s["from"]).alias(_s["to"]))
        for _l in agg_literals or []:
            _sel.append(expr(_l["expression"]).alias(_l["name"]))
        _agg_input = input_df.select(*_sel)
    _ns = _api_namespace()
    df = _agg_input.groupBy(*_group_by).agg(*[
        eval(_a["expr"], {"__builtins__": {}}, _ns).alias(_a["name"])
        for _a in (aggs or [])
    ])
    return _fill_missing(df, missing_cols)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python3.11 -m pytest tests/test_lib_aggregator.py tests/test_lib_joiner.py -v 2>&1 | tail -4`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add informatica_sparker/templates/runtime_lib.py.j2 tests/test_lib_aggregator.py
git commit -m "feat: lib.aggregator component method (groupBy/agg, literal GROUPBY, DISTINCT path)"
```

---

### Task 4: `lib.stored_procedure` (user-requested reusable SP method)

**Files:**
- Modify: `informatica_sparker/informatica_sparker/templates/runtime_lib.py.j2`
- Create: `informatica_sparker/tests/test_lib_stored_procedure.py`

**Interfaces:**
- Consumes: nothing new (uses `call_stored_procedure`, already in runtime_lib).
- Produces:
  - `stored_procedure(spark, input_df, name=None, sp_call=None, sp_schema=None, arg_cols=None, output_col='SUCCESS', sp_conn=None, config=None, **kwargs)` → DataFrame — the reusable-SP component method: collects `arg_cols` on the driver, calls `call_stored_procedure` per row (schema resolution `sp_conn.schema or sp_schema`), sets `output_col` to `lit("SUCCESS")`.
  - `_run_sp_call(spark, df, sp, sp_conn)` — the shared implementation (module-private). `sp` is the `{'col', 'sp_call', 'sp_schema', 'args'}` dict from `lib.expression`'s `sp_calls` config.
- `lib.expression`'s SP loop is REFACTORED to call `_run_sp_call` (behavior identical — the existing `test_expression_sp_calls` must keep passing unchanged).

- [ ] **Step 1: Write the failing test** — `tests/test_lib_stored_procedure.py`

```python
"""Tests for lib.stored_procedure. Fixtures (runtime_lib, spark) come from conftest.py."""


def test_stored_procedure_calls_per_row(runtime_lib, spark, monkeypatch):
    df = spark.createDataFrame([(1,), (2,)], ["COL"])
    calls = []
    monkeypatch.setattr(runtime_lib, "call_stored_procedure",
                        lambda s, c, sp, args: calls.append((sp, list(args))))
    out = runtime_lib.stored_procedure(
        spark=spark, input_df=df, name="SP_X",
        sp_call="PKG.SP_X", sp_schema="PDPA", arg_cols=["COL"],
        sp_conn={"schema": "PSOR"},
    )
    assert calls == [("PSOR.PKG.SP_X", [1]), ("PSOR.PKG.SP_X", [2])]
    assert out.collect()[0]["SUCCESS"] == "SUCCESS"


def test_stored_procedure_no_schema(runtime_lib, spark, monkeypatch):
    df = spark.createDataFrame([(1,)], ["COL"])
    calls = []
    monkeypatch.setattr(runtime_lib, "call_stored_procedure",
                        lambda s, c, sp, args: calls.append((sp, list(args))))
    out = runtime_lib.stored_procedure(
        spark=spark, input_df=df, name="SP_X",
        sp_call="SP_X", sp_schema="", arg_cols=["COL"],
        sp_conn=None,
    )
    assert calls == [("SP_X", [1])]
    assert out.columns == ["COL", "SUCCESS"]


def test_run_sp_call_shared_with_expression(runtime_lib, spark, monkeypatch):
    """The expression sp_calls path and stored_procedure share one impl."""
    df = spark.createDataFrame([(1,), (2,)], ["COL"])
    calls = []
    monkeypatch.setattr(runtime_lib, "call_stored_procedure",
                        lambda s, c, sp, args: calls.append((sp, list(args))))
    out = runtime_lib.expression(
        spark=spark, input_df=df, name="EXPR",
        sp_calls=[{"col": "OUT", "sp_call": "SP_X", "sp_schema": "", "args": ["COL"]}],
        sp_conn=None,
    )
    assert calls == [("SP_X", [1]), ("SP_X", [2])]
    assert out.collect()[0]["OUT"] == "SUCCESS"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3.11 -m pytest tests/test_lib_stored_procedure.py -v 2>&1 | tail -4`
Expected: FAIL — `AttributeError: ... no attribute 'stored_procedure'`.

- [ ] **Step 3: Implement `_run_sp_call` + `stored_procedure`** — append to `templates/runtime_lib.py.j2` after `aggregator`:

```python
def _run_sp_call(spark, df, sp, sp_conn):
    """Execute one stored-procedure call per row (shared by lib.expression's
    sp_calls and lib.stored_procedure). sp: {'col', 'sp_call', 'sp_schema',
    'args'}. Schema resolution: connection schema wins, XML owner falls back.
    """
    _sp_call = sp["sp_call"]
    if sp.get("sp_schema"):
        _schema = (sp_conn or {}).get("schema", "") or sp["sp_schema"]
        _sp_call = _schema + "." + _sp_call
    _args = sp["args"]
    if len(_args) == 1:
        _vals = [r[_args[0]] for r in df.select(_args[0]).collect()]
        for _v in _vals:
            call_stored_procedure(spark, sp_conn, _sp_call, [_v])
    else:
        _rows = [r for r in df.select(*_args).collect()]
        for _r in _rows:
            call_stored_procedure(spark, sp_conn, _sp_call, [_r[c] for c in _args])
    return df.withColumn(sp["col"], lit("SUCCESS"))


def stored_procedure(spark, input_df, name=None, sp_call=None, sp_schema=None,
                     arg_cols=None, output_col="SUCCESS", sp_conn=None,
                     config=None, **kwargs):
    """Convert one Informatica Stored Procedure component.

    Collects the argument columns on the driver and calls
    call_stored_procedure once per row (schema resolution: connection schema
    wins, the XML owner falls back), then sets the output column to 'SUCCESS'.
    Reusable SP components are referenced by name; the expression-level
    :SP.xxx() path shares this implementation via _run_sp_call.
    """
    return _run_sp_call(spark, input_df, {
        "col": output_col,
        "sp_call": sp_call,
        "sp_schema": sp_schema or "",
        "args": arg_cols or [],
    }, sp_conn)
```

- [ ] **Step 4: Refactor `expression`'s SP loop to use `_run_sp_call`**

In `expression()` (Phase 1), replace the SP loop body:

```python
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
```

with:

```python
    for _sp in (sp_calls or []):
        df = _run_sp_call(spark, df, _sp, sp_conn)
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `python3.11 -m pytest tests/test_lib_stored_procedure.py tests/test_lib_expression.py -v 2>&1 | tail -4`
Expected: all PASS (including the pre-existing `test_expression_sp_calls` — behavior unchanged).

- [ ] **Step 6: Commit**

```bash
git add informatica_sparker/templates/runtime_lib.py.j2 tests/test_lib_stored_procedure.py
git commit -m "feat: lib.stored_procedure component method; expression sp_calls shares _run_sp_call"
```

---

### Task 5: Generator wiring — static lookup (handler cfg + template + mapplet path)

**Files:**
- Modify: `informatica_sparker/informatica_sparker/handlers.py` (APPLY_LOOKUP static branch ~line 471-549 params already exist as step params: `join_predicates`, `join_expr`, `lookup_type`, `broadcast`, `dedup_lookup*`, `lkp_field_remap`, `new_lookup_row_key`, `new_lookup_row_col`, `lookup_df`; and the mapplet-internal lookup path ~line 4137)
- Modify: `informatica_sparker/informatica_sparker/templates/mapping.py.j2` (APPLY_LOOKUP static branches, lines 415-549 — the `join_expr == '__common_cols__'`, `join_predicates`, `join_expr`, and cross branches; the `dynamic_lookup` branch stays untouched)
- Create: `informatica_sparker/tests/test_lookup_wiring.py`

**Interfaces:**
- Consumes: `lib.static_lookup` signature (Task 1).
- Produces: `step.params["lookup_cfg"]` dict with keys: `join_spec` (`{'kind', ...}`), `lookup_type`, `broadcast` (bool, default True), `lkp_field_remap` (optional), `dedup` (`{'policy', 'keys'}`, optional), `new_lookup_row_key` (optional, the lookup's NewLookupRow **probe key** — see note).

Note on the NewLookupRow probe: the Phase-1-era template rendered `new_lookup_row_col` (the OUTPUT column name, e.g. `NewLookupRow_LKP_...`) and probed via `_nlr_lkp_cols` derived from the lookup's surviving columns. The runtime `static_lookup` derives the probe itself from the surviving lookup columns; the cfg must pass `new_lookup_row_key` = the probe PREFERENCE (the first lookup column to try) — the handler already computes this preference for the old template's `_nlr_key` logic. Simplest faithful mapping: pass `new_lookup_row_key` = the old `new_lookup_row_key` param value and `new_lookup_row_col` = the old `new_lookup_row_col` param; the method emits the OUTPUT column under `new_lookup_row_col` — adjust the Task-1 method accordingly: add a `new_lookup_row_col` parameter (default 'NewLookupRow') that names the emitted column, keeping the probe logic on `new_lookup_row_key`. Update Task 1's tests to pass `new_lookup_row_col='NewLookupRow'` (the current default) — no test change needed if the default matches. DO THIS: extend the Task 1 signature with `new_lookup_row_col='NewLookupRow'` and use it in the final withColumn instead of the bare `new_lookup_row_key`.

- [ ] **Step 1: Confirm `new_lookup_row_col` is already in place**

Task 1's `static_lookup` already has `new_lookup_row_col='NewLookupRow'` (default) and uses it for the emitted column. Run Task 1's tests — still green (the default matches the test).

- [ ] **Step 2: Build the cfg in the main-mapping lookup handler**

In the static-lookup portion of the lookup handler (the branch that sets `join_predicates`/`join_expr`/`dedup_lookup*`/`new_lookup_row_key`), assemble:

```python
        if join_expr == "__common_cols__":
            _join_spec: Dict[str, Any] = {"kind": "common_cols"}
        elif join_expr:
            _join_spec = {"kind": "expr", "expr": join_expr}
        elif join_predicates:
            _join_spec = {"kind": "predicates", "predicates": join_predicates}
        else:
            _join_spec = {"kind": "cross"}
        _lookup_cfg: Dict[str, Any] = {
            "join_spec": _join_spec,
            "lookup_type": step.params.get("lookup_type", "left"),
            "broadcast": step.params.get("broadcast", True),
        }
        if step.params.get("lkp_field_remap"):
            _lookup_cfg["lkp_field_remap"] = step.params["lkp_field_remap"]
        if step.params.get("dedup_lookup"):
            _policy = ("report_error" if step.params.get("dedup_lookup_error")
                       else ("last" if step.params.get("dedup_lookup_last") else "first"))
            _lookup_cfg["dedup"] = {
                "policy": _policy,
                "keys": step.params.get("dedup_lookup_keys", []),
            }
        if step.params.get("new_lookup_row_key"):
            _lookup_cfg["new_lookup_row_key"] = step.params["new_lookup_row_key"]
            _lookup_cfg["new_lookup_row_col"] = step.params.get(
                "new_lookup_row_col", "NewLookupRow")
        step.params["lookup_cfg"] = _lookup_cfg
```

(The exact branch structure may differ — locate the static-branch tail in the lookup handler and add this before the step is returned. Keep the existing params; the template stops reading them once replaced.)

- [ ] **Step 3: Wire the mapplet-internal lookup path**

At the mapplet lookup `ApplyLookupStep` construction (~line 4137), set the same `lookup_cfg`:

```python
                    _mpl_lkp_cfg = {
                        "join_spec": (
                            {"kind": "expr", "expr": join_expr}
                            if join_expr else
                            {"kind": "predicates", "predicates": join_predicates}
                            if join_predicates else {"kind": "cross"}
                        ),
                        "lookup_type": "left",
                        "broadcast": True,
                    }
                    steps[-1].params["lookup_cfg"] = _mpl_lkp_cfg
```

(The mapplet path also has dedup-lookup and dynamic-lookup handling after the construction — if `steps[-1]` is later mutated for dedup, extend `_lookup_cfg["dedup"]` there the same way as Step 2; dynamic lookup config leaves `lookup_cfg` absent and the template's dynamic branch is untouched.)

- [ ] **Step 4: Replace the APPLY_LOOKUP static template branches**

In `templates/mapping.py.j2`, replace the static branches (from `{% elif join_predicates %}` through the `lit(True)` cross branch, i.e. template lines ~471-549) with:

```jinja2
        {% elif step.params.get('lookup_cfg') %}
        # Lookup: {{ step.step_name }}
        {{ step.df_output }} = lib.static_lookup(
            spark=spark,
            input_df={{ step.df_input }},
            lookup_df={{ lookup_df_name }},
            {% set lkcfg = step.params['lookup_cfg'] %}
            join_spec={{ lkcfg['join_spec'] | pyrepr }},
            lookup_type={{ lkcfg.get('lookup_type', 'left') | pyrepr }},
            broadcast={{ lkcfg.get('broadcast', True) | pyrepr }},
            {% if lkcfg.get('lkp_field_remap') %}
            lkp_field_remap={{ lkcfg['lkp_field_remap'] | pyrepr }},
            {% endif %}
            {% if lkcfg.get('dedup') %}
            dedup={{ lkcfg['dedup'] | pyrepr }},
            {% endif %}
            {% if lkcfg.get('new_lookup_row_key') %}
            new_lookup_row_key={{ lkcfg['new_lookup_row_key'] | pyrepr }},
            new_lookup_row_col={{ lkcfg.get('new_lookup_row_col', 'NewLookupRow') | pyrepr }},
            {% endif %}
            config=config,
        )
        ctx.register_df("{{ step.df_output }}", {{ step.df_output }})
```

- [ ] **Step 5: Add the wiring regression test** — `tests/test_lookup_wiring.py` (pattern: tests/test_filter_mapplet_wiring.py)

Parse `WF_EMS_DDS_APLY_MTH.XML`, build plans for every mapping, and assert:

```python
def test_every_static_lookup_step_has_cfg(ems_xml):
    seen = 0
    for mapping in ems_xml:
        handlers = TransformHandlers(mapping, UserConfig())
        plan = handlers.build_ir_plan()
        for step in plan.steps:
            if not isinstance(step, ApplyLookupStep):
                continue
            # skip dynamic lookups (they carry dynamic_lookup config instead)
            if step.params.get("dynamic_lookup"):
                continue
            cfg = step.params.get("lookup_cfg")
            assert cfg is not None, (
                "%s: ApplyLookupStep %s has no lookup_cfg — the template "
                "renders no lookup at all" % (mapping.name, step.step_name))
            assert cfg["join_spec"].get("kind") in (
                "predicates", "common_cols", "expr", "cross")
            seen += 1
    assert seen > 0, "expected at least one static lookup step"
```

Plus render smoke test: render the APPLY_LOOKUP static block with a synthetic `lookup_cfg` (predicates + dedup + new_lookup_row_key) and ast.parse the output (pattern: tests/test_filter_template_render.py).

- [ ] **Step 6: Run tests**

Run: `python3.11 -m pytest tests/test_lookup_wiring.py tests/ -q 2>&1 | tail -3`
Expected: all PASS.

- [ ] **Step 7: Commit**

```bash
git add informatica_sparker/handlers.py informatica_sparker/templates/mapping.py.j2 tests/test_lookup_wiring.py
git commit -m "feat: generator emits lib.static_lookup calls (main + mapplet paths) + wiring regression test"
```

---

### Task 6: Generator wiring — joiner (handler cfg + template)

**Files:**
- Modify: `informatica_sparker/informatica_sparker/handlers.py` (`_handle_joiner`, ~line 2276)
- Modify: `informatica_sparker/informatica_sparker/templates/mapping.py.j2` (APPLY_JOINER block, lines 551-623)
- Modify: `informatica_sparker/tests/test_joiner_wiring.py` (create)

**Interfaces:**
- Consumes: `lib.joiner` signature (Task 2).
- Produces: `step.params["joiner_cfg"]` with keys: `join_type`, `master_selects`/`detail_selects` (optional), `on` (`{'kind': 'cols'|'expr'|'cross', ...}`), `missing_cols` (optional).

- [ ] **Step 1: Build the cfg in `_handle_joiner`**

The handler already computes `join_predicates`, `raw_condition`, `use_fallback`, `join_type`, `master_selects`, `detail_selects`, `joiner_missing_cols` (params). Add before the step is returned:

```python
        _preds = step.params.get("join_predicates") or []
        _raw = step.params.get("raw_condition") or ""
        if _preds and not step.params.get("use_fallback"):
            _on: Dict[str, Any] = {
                "kind": "cols",
                "pairs": [{"master_col": jp.get("master_col"),
                           "detail_col": jp.get("detail_col")}
                          for jp in _preds],
            }
        elif _raw:
            _on = {"kind": "expr", "expr": _raw}
        else:
            _on = {"kind": "cross"}
        _joiner_cfg: Dict[str, Any] = {
            "join_type": step.params.get("join_type", "inner"),
            "on": _on,
        }
        if step.params.get("master_selects"):
            _joiner_cfg["master_selects"] = step.params["master_selects"]
        if step.params.get("detail_selects"):
            _joiner_cfg["detail_selects"] = step.params["detail_selects"]
        if step.params.get("joiner_missing_cols"):
            _joiner_cfg["missing_cols"] = step.params["joiner_missing_cols"]
        step.params["joiner_cfg"] = _joiner_cfg
```

(Read the `_handle_joiner` body first: the template reads these keys from `step.params` — `join_predicates`, `raw_condition`, `use_fallback`, `join_type`, `master_selects`, `detail_selects`, `joiner_missing_cols`. Build the cfg from `step.params` exactly as above; the handler must already be storing them there — confirm with a grep, and if any key is stored under a different name, use the actual name.)

- [ ] **Step 2: Replace the APPLY_JOINER template block** (lines 551-623) with:

```jinja2
        {% elif step.step_type == IRStepType.APPLY_JOINER %}
        # Joiner: {{ step.step_name }}
        {% set jnrcfg = step.params['joiner_cfg'] %}
        {{ step.df_output }} = lib.joiner(
            spark=spark,
            master_df={{ step.params.get('df_master', step.df_input) }},
            detail_df={{ step.params.get('df_detail', 'df_detail') }},
            {% if jnrcfg.get('master_selects') %}
            master_selects=[
                {{ jnrcfg['master_selects'] | map('pyrepr') | join(',\n                ') }}
            ],
            {% endif %}
            {% if jnrcfg.get('detail_selects') %}
            detail_selects=[
                {{ jnrcfg['detail_selects'] | map('pyrepr') | join(',\n                ') }}
            ],
            {% endif %}
            join_type={{ jnrcfg.get('join_type', 'inner') | pyrepr }},
            on={{ jnrcfg['on'] | pyrepr }},
            {% if jnrcfg.get('missing_cols') %}
            missing_cols={{ jnrcfg['missing_cols'] | pyrepr }},
            {% endif %}
            config=config,
        )
        ctx.register_df("{{ step.df_output }}", {{ step.df_output }})
```

- [ ] **Step 3: Add the wiring regression test** — `tests/test_joiner_wiring.py`

Parse `WF_EMS_DDS_APLY_MTH.XML` (2 joiner mappings) — assert every `ApplyJoinerStep` carries `joiner_cfg` with a valid `on.kind`; assert the two joiner mappings actually appear (non-vacuity: `seen >= 1`). Plus a render smoke test with ast.parse (synthetic step with cols-kind on).

- [ ] **Step 4: Run tests**

Run: `python3.11 -m pytest tests/test_joiner_wiring.py tests/ -q 2>&1 | tail -3`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add informatica_sparker/handlers.py informatica_sparker/templates/mapping.py.j2 tests/test_joiner_wiring.py
git commit -m "feat: generator emits lib.joiner calls + wiring regression test"
```

---

### Task 7: Generator wiring — aggregator (handler cfg + template)

**Files:**
- Modify: `informatica_sparker/informatica_sparker/handlers.py` (`_handle_aggregator`, ~line 2264)
- Modify: `informatica_sparker/informatica_sparker/templates/mapping.py.j2` (APPLY_AGGREGATOR block, lines 625-678)
- Create: `informatica_sparker/tests/test_aggregator_wiring.py`

**Interfaces:**
- Consumes: `lib.aggregator` signature (Task 3); the handler's existing params: `group_by`, `aggregations` (`{col_name: python-source agg expr}`), `agg_selects`, `agg_literals`, `distinct` (template's `distinct` param).
- Produces: `step.params["aggregator_cfg"]` with keys: `group_by`, `aggs` (`[{'name', 'expr'}]`), `agg_selects`, `agg_literals` (optional), `distinct` (optional bool), `missing_cols` (optional).

- [ ] **Step 1: Build the cfg in `_handle_aggregator`**

```python
        _agg_cfg: Dict[str, Any] = {
            "group_by": group_by,
            "aggs": [{"name": _n, "expr": _e}
                     for _n, _e in step.params.get("aggregations", {}).items()],
        }
        if step.params.get("agg_selects"):
            _agg_cfg["agg_selects"] = step.params["agg_selects"]
        if step.params.get("agg_literals"):
            _agg_cfg["agg_literals"] = step.params["agg_literals"]
        if step.params.get("distinct"):
            _agg_cfg["distinct"] = True
        step.params["aggregator_cfg"] = _agg_cfg
```

(Verify local/param names against the actual handler — the template reads `group_by`, `aggregations`, `agg_selects`, `agg_literals`, `distinct` from params.)

- [ ] **Step 2: Replace the APPLY_AGGREGATOR template block** (lines 625-678) with:

```jinja2
        {% elif step.step_type == IRStepType.APPLY_AGGREGATOR %}
        # Aggregator: {{ step.step_name }}
        {% set aggcfg = step.params['aggregator_cfg'] %}
        {{ step.df_output }} = lib.aggregator(
            spark=spark,
            input_df={{ step.df_input }},
            {% if aggcfg.get('agg_selects') %}
            agg_selects=[
                {{ aggcfg['agg_selects'] | map('pyrepr') | join(',\n                ') }}
            ],
            {% endif %}
            {% if aggcfg.get('agg_literals') %}
            agg_literals=[
                {{ aggcfg['agg_literals'] | map('pyrepr') | join(',\n                ') }}
            ],
            {% endif %}
            {% if aggcfg.get('distinct') %}
            distinct=True,
            {% else %}
            group_by={{ aggcfg['group_by'] | pyrepr }},
            aggs=[
                {{ aggcfg['aggs'] | map('pyrepr') | join(',\n                ') }}
            ],
            {% endif %}
            config=config,
        )
        ctx.register_df("{{ step.df_output }}", {{ step.df_output }})
```

Note: when `distinct` is set, `group_by`/`aggs` are omitted — `lib.aggregator`'s DISTINCT path uses only `agg_selects` (or falls back to `group_by`). To keep the DISTINCT-without-selects path working, ALSO render `group_by` for distinct: change the template to always render `group_by`, and render `aggs` only when present:

```jinja2
            group_by={{ aggcfg['group_by'] | pyrepr }},
            {% if aggcfg.get('aggs') %}
            aggs=[
                {{ aggcfg['aggs'] | map('pyrepr') | join(',\n                ') }}
            ],
            {% endif %}
            {% if aggcfg.get('distinct') %}
            distinct=True,
            {% endif %}
```

- [ ] **Step 3: Add the wiring regression test** — `tests/test_aggregator_wiring.py`

Parse `WF_EMS_DDS_APLY_MTH.XML` (38 aggregator mappings) — assert every `ApplyAggregatorStep` carries `aggregator_cfg`; assert `aggs`/`agg_selects` presence matches params; non-vacuity `seen >= 20`. Plus a render smoke test with ast.parse (synthetic step with aggs incl. an f-string-form expr, asserting valid Python).

- [ ] **Step 4: Run tests**

Run: `python3.11 -m pytest tests/test_aggregator_wiring.py tests/ -q 2>&1 | tail -3`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add informatica_sparker/handlers.py informatica_sparker/templates/mapping.py.j2 tests/test_aggregator_wiring.py
git commit -m "feat: generator emits lib.aggregator calls + wiring regression test"
```

---

### Task 8: Generator wiring — standalone stored procedure

**Files:**
- Modify: `informatica_sparker/informatica_sparker/handlers.py` (Stored Procedure instance dispatch, ~line 777)
- Modify: `informatica_sparker/informatica_sparker/templates/mapping.py.j2` (no template change expected — see step 1)
- Modify: `informatica_sparker/tests/test_joiner_wiring.py`? No — create `informatica_sparker/tests/test_stored_procedure_wiring.py`

**Interfaces:**
- Consumes: `lib.stored_procedure` signature (Task 4).
- Produces: nothing new — the standalone SP instance stays a no-op in the data flow (Informatica semantics: the CALL happens via the expression's `:SP.` reference; the component instance itself is a pass-through). The wiring work: the dispatch comment is updated to point at `lib.stored_procedure`, and the reusable-SP definitions are made discoverable by the expression path.

- [ ] **Step 1: Confirm no template change is needed**

The standalone SP instance path (handlers.py ~777) is a no-op — verify that no generated step exists for SP instances in the current WF_EMS_DDS_APLY_MTH output (`grep -rn "Stored Procedure:" m_*.py` → expect none, since the instances are logged-not-converted). The data-flow call happens only through `lib.expression`'s `sp_calls` (Phase 1) — already wired.

- [ ] **Step 2: Update the dispatch comment + make reusable SPs resolvable by the expression path**

In the Stored Procedure dispatch branch, update the comment to document the component-method form:

```python
            elif inst_type == "Stored Procedure":
                # Stored procedures are called via `lib.stored_procedure(...)`
                # (the reusable-SP component method) through `:SP.xxx()`
                # references in expression transforms; the instance itself is
                # a no-op in the data flow.
                self.logger.log_transformation(inst_name, "StoredProcedure",
                    "Handled via expression reference (lib.stored_procedure)", LogLevel.INFO)
```

- [ ] **Step 3: Add the regression test** — `tests/test_stored_procedure_wiring.py`

Parse `WF_EMS_DDS_APLY_MTH.XML`; assert that for the adtn_del mapping, the mapplet-internal expression step `apply_MPLT_DDS_APPLY_DELETE_AFFECT_RECORD_EXP_SP_DELETE` carries `sp_calls` with `sp_call == 'SP_DELETE_DDS_FACT'` (the reusable SP — the Phase 1 fallback path), and that no `ApplyExpressionStep` computed column carries raw `:SP.` text (reuse the assertion from tests/test_sp_calls_fallback.py).

- [ ] **Step 4: Run tests**

Run: `python3.11 -m pytest tests/test_stored_procedure_wiring.py tests/ -q 2>&1 | tail -3`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add informatica_sparker/handlers.py tests/test_stored_procedure_wiring.py
git commit -m "feat: standalone SP components documented as lib.stored_procedure + wiring regression test"
```

---

### Task 9: `lib.load_mapping_variables` (user-requested mapping-init wrapper)

**Files:**
- Modify: `informatica_sparker/informatica_sparker/templates/runtime_lib.py.j2`
- Modify: `informatica_sparker/informatica_sparker/templates/mapping.py.j2` (the mapping-variable loading header block, lines 68-90)
- Create: `informatica_sparker/tests/test_lib_load_mapping_variables.py`

**Interfaces:**
- Produces: `load_mapping_variables(config, var_names, logger=None)` → `Dict[str, str]` keyed by clean names (no `$$` prefix).
  - Reads the UTL_JOB_PARAM file: `config["objects"]["UTL_JOB_PARAM"]["path"]` resolved via `_resolve_path`.
  - Parses lines `$$var=value`; only listed var_names collected; keys are `var.replace('$','')`.
  - Missing file → module-level `logging.warning("UTL_JOB_PARAM not found, using default values")` once (behavior parity with the old template's warning), returns `{}`.
  - Per-variable "Loaded %s=%s from %s" lines move to module-level debug (the old template logged them at INFO on the mapping logger; they are low-value detail — the warning is the visible signal).
- Template: the generated header becomes
  ```python
  _vars = lib.load_mapping_variables(config, ["$$v_x", ...])
  v_x = _vars.get("v_x", "<default>")
  ```
  per variable — defaults from `var_default` exactly as the old `{{ clean_name }} = "{{ var_default }}"` lines.

- [ ] **Step 1: Write the failing test** — `tests/test_lib_load_mapping_variables.py` (conftest fixtures)

```python
"""Tests for lib.load_mapping_variables. Fixtures come from conftest.py."""


def test_loads_and_cleans_names(runtime_lib, tmp_path, monkeypatch):
    param_file = tmp_path / "UTL_JOB_PARAM"
    param_file.write_text("$$v_snsh_date=202606\n$$v_init_flag=Y\n$$v_unlisted=Z\n")
    config = {"objects": {"UTL_JOB_PARAM": {"path": str(param_file)}}}
    out = runtime_lib.load_mapping_variables(
        config, ["$$v_snsh_date", "$$v_init_flag"])
    assert out == {"v_snsh_date": "202606", "v_init_flag": "Y"}


def test_missing_file_returns_empty_and_warns(runtime_lib, tmp_path, caplog):
    config = {"objects": {"UTL_JOB_PARAM": {"path": str(tmp_path / "nope")}}}
    out = runtime_lib.load_mapping_variables(config, ["$$v_x"])
    assert out == {}
    # warning parity with the old template
    assert any("UTL_JOB_PARAM" in r.message for r in caplog.records)


def test_missing_variable_absent_from_result(runtime_lib, tmp_path):
    param_file = tmp_path / "UTL_JOB_PARAM"
    param_file.write_text("$$v_a=1\n")
    config = {"objects": {"UTL_JOB_PARAM": {"path": str(param_file)}}}
    out = runtime_lib.load_mapping_variables(config, ["$$v_a", "$$v_b"])
    assert out == {"v_a": "1"}  # v_b absent → caller .get() default


def test_no_utl_job_param_object(runtime_lib):
    out = runtime_lib.load_mapping_variables({}, ["$$v_x"])
    assert out == {}
```

- [ ] **Step 2: Run test to verify it fails** — `AttributeError: ... no attribute 'load_mapping_variables'`

- [ ] **Step 3: Implement** — append to `templates/runtime_lib.py.j2` after `sequence`:

```python
def load_mapping_variables(config, var_names, logger=None):
    """Read $$ mapping variables from the UTL_JOB_PARAM file (mapping-level
    init — the wrapper for the per-mapping parameter-loading step).

    Returns {clean_name: value} (no $$ prefix). Missing file → warning + {};
    variables not listed in the file are absent from the result, so callers
    .get() their declared defaults.
    """
    _out = {}
    _obj = ((config or {}).get("objects") or {}).get("UTL_JOB_PARAM", {})
    if not isinstance(_obj, dict):
        return _out
    _path = _resolve_path(_obj.get("path"))
    try:
        with open(_path, "r") as _f:
            for _line in _f:
                _line = _line.strip()
                for _var in (var_names or []):
                    if _line.startswith(_var + "="):
                        _val = _line.split("=", 1)[1]
                        _out[_var.replace("$", "")] = _val
                        logging.getLogger(__name__).debug(
                            "Loaded %s=%s from %s", _var, _val, _path)
    except Exception:
        logging.getLogger(__name__).warning(
            "UTL_JOB_PARAM not found, using default values")
    return _out
```

- [ ] **Step 4: Replace the template header block** (mapping.py.j2 lines 68-90) with:

```jinja2
    {% if mapping_variables %}
    _vars = lib.load_mapping_variables(config, [{% for var_name, var_default in mapping_variables.items() %} "{{ var_name }}", {% endfor %}])
    {% for var_name, var_default in mapping_variables.items() %}
    {% set clean_name = var_name | replace('$', '') %}
    {{ clean_name }} = _vars.get("{{ clean_name }}", "{{ var_default }}")
    {% endfor %}
    {% endif %}
```

- [ ] **Step 5: Run tests** — unit tests + full suite (must stay green).

- [ ] **Step 6: Commit**

```bash
git add informatica_sparker/templates/runtime_lib.py.j2 informatica_sparker/templates/mapping.py.j2 tests/test_lib_load_mapping_variables.py
git commit -m "feat: lib.load_mapping_variables wrapper for the per-mapping UTL_JOB_PARAM init"
```

### Task 10: Reconvert WF_EMS_DDS_APLY_MTH and verify (Phase 2 gate)

**Files:**
- Run: build + CLI convert; backup already exists at `PySpark_workflows/dds/_pre_refactor_WF_EMS_DDS_APLY_MTH/` (Phase 1)
- Test: reconverted `PySpark_workflows/dds/WF_EMS_DDS_APLY_MTH/`

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
grep -c "lib.static_lookup(" m_*.py | grep -v ":0" | wc -l
grep -c "lib.aggregator(" m_*.py | grep -v ":0" | wc -l
grep -c "lib.joiner(" m_*.py | grep -v ":0" | wc -l
grep -rn "\.join(\|\.agg(" m_*.py | wc -l    # expect only pre-step remnants
```

- [ ] **Step 4: Per-step semantic comparison vs the backup (Phase 1 lesson)**

For every new `lib.static_lookup` call, extract `join_spec.predicates` and compare to the backup's `_main.` join predicates of the same-named step (same `source_col == lookup_col` pairs, case-insensitive). For every `lib.aggregator` call, extract the `aggs` exprs and compare to the backup's `.agg(` block expressions (same column → same expression text). For every `lib.joiner` call, compare the `on.pairs`/`expr` to the backup's join condition. Report counts: `compared`, `mismatches` (must be 0). Follow the line-based approach of `/tmp/compare_filter_conditions.py` (extend that script or write `/tmp/compare_phase2_steps.py`).

- [ ] **Step 5: Full test suite**

```bash
cd /var/lib/airflow/dags/adam/informatica/informatica_sparker
python3.11 -m pytest tests/ -q 2>&1 | tail -2
```
Expected: all PASS.

- [ ] **Step 6: Runtime verification (user checkpoint — cluster access required)**

User runs the full workflow (as in Phase 1): `cd PySpark_workflows/dds/WF_EMS_DDS_APLY_MTH && python wf_ems_dds_aply_mth.py` — expect all mappings SUCCESS, no errors. Report candidate coverage: 48 lookup mappings (static + dynamic), 38 aggregators, 2 joiners.

- [ ] **Step 7: Update CLAUDE.md**

Append a Phase 2 subsection to the "Component Methods" section in `/var/lib/airflow/dags/adam/informatica/CLAUDE.md`: `lib.static_lookup` (4 join kinds, dedup policies, runtime probe), `lib.joiner`, `lib.aggregator` (eval-namespace agg exprs, literal GROUPBY, DISTINCT), `lib.stored_procedure` (+ `_run_sp_call` shared with expression sp_calls). Edit in place, no commit (outside the informatica_sparker repo).

---

## Out of scope (this plan)

- Dynamic lookup (`lib.dynamic_lookup`) — already in lib form, untouched.
- Router, union, sorter, sequence, SQ port handling, update strategy, write target — delivered in Phase 3/4 + the API-polish round (all complete).
- **Mapplet independent-reuse refactor — Phase 5** (user decision 2026-08-13): mapplets become reusable mini-mappings invoked from mappings (lib-level component). Explicitly deferred until after Phase 2 completes and the user verifies.
- No expression-translation changes (`expr_translator` untouched).
- No regeneration of other workflows.

## Phase 2 additions over the original plan (user decision 2026-08-13)

- Task 9 (new): `lib.load_mapping_variables` — the per-mapping UTL_JOB_PARAM parameter-loading step is wrapped into a lib method; the generated header becomes `_vars = lib.load_mapping_variables(config, [...])` + per-variable `.get()` defaults.
- All Phase 2 methods adopt the API-polish conventions from the start: optional `spark`/`config` (rendered only when used — stored_procedure needs spark; static_lookup/joiner/aggregator need neither), field-heavy params one entry per line, no `name=` in generated calls.

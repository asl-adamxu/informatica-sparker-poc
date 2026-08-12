# Component Methods Phase 3 Implementation Plan — router + union + sorter + sequence

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend the component-method pattern to the low-risk group: `lib.router` (multi-output split with multi-feed union input + per-group filters/renames + DEFAULT negation), `lib.union` (per-group select+alias then unionByName), `lib.sorter` (connector renames + orderBy), `lib.sequence` (standalone NEXTVAL attachment).

**Architecture:** Same three-layer separation (design doc `docs/specs/2026-08-12-component-methods-design.md`): generator = syntax conversion, runtime_lib methods = runtime semantics, generated files = declarative kwargs calls. Template blocks move verbatim — test locks behavior first, then the template code is deleted.

**Tech Stack:** Jinja2 templates, Python 3.11 generator build, PySpark 3.5.4, pytest with conftest fixtures (tests/conftest.py), informatica-sparker CLI.

**Execution order note:** Phase 3 and Phase 4 run BEFORE Phase 2 (user decision — Phase 2's static_lookup/joiner/aggregator/stored_procedure are the most complex and go last). This plan is executed after Phase 4's plan is written; the reconversion gate at the end covers all Phase 3+4 changes in one round.

## Global Constraints

- Translation stays at codegen time: `lib.*` methods receive **translated** results only.
- Verbatim move: no behavior changes; a test locks behavior before the template block is deleted.
- kwargs call form: `lib.xxx(spark=spark, input_df=..., <keys>, config=config)`; `name` is NOT rendered (Phase 1 decision; runtime keeps `name=None` for errors).
- Complex lists one dict per line via `map('pyrepr') | join(',\n<indent>')`; scalars via `pyrepr`; `rename_columns` one tuple per line.
- `substitutions` values render as identifiers with dict braces via `{{ '{' }}...{{ '}' }}`.
- No Chinese comments in generated code. Zero import side effects for mapping modules.
- **Phase 1 lessons (binding)**: (1) mapplet-internal construction paths wired too — the template reads ONLY the new cfg key; (2) wiring regression tests parse real XML (every step of the type carries the cfg); (3) reconversion gate compares per-step semantics vs the backup, not just call counts; (4) render smoke tests ast.parse the real template blocks.
- **Known dead path**: the union template's `flag_column`/`normalize_flag_column` branch has never rendered in any generated output (function undefined in both templates — it would NameError if ever hit). `lib.union` accepts `flag_column` and raises a clear ValueError if non-empty (old behavior: NameError crash); the handler keeps passing it.
- **Router `$$` substitution rule**: the old template replaced `$$var` with the bare variable (a numeric value would TypeError on `str.replace`). `lib.router` uses the plain `str(v)` rule (same as expression) — strictly more robust, no generated case currently triggers `$$` in router conditions (verify in Task 5 with a grep).
- Test workflow: WF_EMS_DDS_APLY_MTH (coverage: Router 6, Union 19, Sorter 1, Sequence 0 — sequence is unit-tested only, deferred runtime coverage to a later workflow).
- Build/convert cycle: `python3.11 -m build` → `python3.11 -m pip install --user --force-reinstall dist/informatica_sparker-*.whl` → `informatica-sparker convert <XML> -o <OUT_ROOT>`; reconversion gate 0 warnings / 0 errors; `python3.11 -m py_compile m_*.py`.
- Commit per task in the git repo (branch `enhance`); pytest from the repo root.
- Runtime-lib tests render `runtime_lib.py.j2` via Jinja2 (conftest); SparkSession `local[2]`. `monotonically_increasing_id` is per-partition — tests needing exact values coalesce/repartition the INPUT frame.

## MOVE PROTOCOL

1. Source of truth = the template block (line ranges per task).
2. `{{ step.df_output }}` → `df`; `{{ step.df_input }}` → `input_df`; Jinja control flow → Python control flow over config lists.
3. Translated-string decisions (expr-vs-API markers, `$$` presence) move into lib verbatim (`_with_column`, `_substitute` from Phase 1).
4. XML-metadata decisions stay in the generator as config keys.
5. `logger.info("Step: ...")` stays in the generated file; methods log at debug only.

---

### Task 1: `lib.router` (multi-output component)

**Files:**
- Modify: `informatica_sparker/informatica_sparker/templates/runtime_lib.py.j2`
- Create: `informatica_sparker/tests/test_lib_router.py`

**Interfaces:**
- Consumes: `_rename_columns`, `_substitute` (Phase 1).
- Produces: `router(spark, input_df, name=None, groups=None, multi_feed=False, feeds=None, substitutions=None, config=None, **kwargs)` → `Dict[str, DataFrame]` keyed by each group's `df_output`.
  - `groups`: `[{'name', 'df_output', 'condition' (raw translated text; optional), 'filter_inner' (raw text with $$; optional), 'default_negated' (list of group names whose conditions are negated, optional), 'renames' ([(from, to)], optional)}]`.
  - `multi_feed=True`: builds `df_rtr_input` as a UNION of all feeds first — `feeds`: `[(df, aliases_dict)]` where aliases maps upstream column → Router INPUT port; every Router input port is select+aliased per feed (`lit(None)` fill for absent), then `unionByName` — then the ordered group split runs on the union input.
  - `substitutions`: `{'$$v_x': v_x}` — plain `str(v)` rule; applied to `filter_inner` texts.
  - Condition precedence per group (matches the template's ordered branches): `filter_inner` with `$$` → substituted text via expr; `default_negated` → chain of `~expr(...)` per named group's condition; `condition` → expr(text); none → pass-through (`df`).
  - `renames` applied after the filter (drop target first).

- [ ] **Step 1: Write the failing test** — `tests/test_lib_router.py`

```python
"""Tests for lib.router. Fixtures (runtime_lib, spark) come from conftest.py."""


def test_group_split_and_renames(runtime_lib, spark):
    df = spark.createDataFrame([("A", 1), ("B", 2), ("A", 3)], ["GRP", "V"])
    out = runtime_lib.router(
        spark=spark, input_df=df, name="RTR",
        groups=[
            {"name": "G1", "df_output": "df_rtr_G1",
             "condition": "GRP = 'A'", "renames": [("GRP", "GRP1")]},
            {"name": "G2", "df_output": "df_rtr_G2",
             "condition": "GRP = 'B'"},
        ],
    )
    g1 = sorted(r["V"] for r in out["df_rtr_G1"].collect())
    g2 = sorted(r["V"] for r in out["df_rtr_G2"].collect())
    assert g1 == [1, 3] and g2 == [2]
    assert out["df_rtr_G1"].columns == ["GRP1", "V"]


def test_default_negated_group(runtime_lib, spark):
    df = spark.createDataFrame([("A", 1), ("B", 2)], ["GRP", "V"])
    out = runtime_lib.router(
        spark=spark, input_df=df, name="RTR",
        groups=[
            {"name": "G1", "df_output": "df_rtr_G1", "condition": "GRP = 'A'"},
            {"name": "G2", "df_output": "df_rtr_G2",
             "default_negated": ["G1"]},
        ],
    )
    assert [r["GRP"] for r in out["df_rtr_G2"].collect()] == ["B"]


def test_pass_through_group(runtime_lib, spark):
    df = spark.createDataFrame([(1,)], ["V"])
    out = runtime_lib.router(
        spark=spark, input_df=df, name="RTR",
        groups=[{"name": "G1", "df_output": "df_rtr_G1"}],
    )
    assert out["df_rtr_G1"].count() == 1


def test_substitution(runtime_lib, spark):
    df = spark.createDataFrame([(1,), (5,)], ["V"])
    out = runtime_lib.router(
        spark=spark, input_df=df, name="RTR",
        groups=[{"name": "G1", "df_output": "df_rtr_G1",
                 "filter_inner": "V > $$v_min"}],
        substitutions={"$$v_min": "2"},
    )
    assert sorted(r["V"] for r in out["df_rtr_G1"].collect()) == [5]


def test_multi_feed_union_input(runtime_lib, spark):
    df1 = spark.createDataFrame([("A", 1)], ["GRP", "V"])
    df2 = spark.createDataFrame([("B", 2)], ["GRP", "V"])
    out = runtime_lib.router(
        spark=spark, input_df=df1, name="RTR",
        multi_feed=True,
        feeds=[(df1, {}), (df2, {})],
        groups=[
            {"name": "G1", "df_output": "df_rtr_G1", "condition": "GRP = 'A'"},
            {"name": "G2", "df_output": "df_rtr_G2", "condition": "GRP = 'B'"},
        ],
    )
    assert out["df_rtr_G1"].count() == 1 and out["df_rtr_G2"].count() == 1


def test_multi_feed_aliases_fill(runtime_lib, spark):
    df1 = spark.createDataFrame([("A", 1)], ["SRC", "V"])
    df2 = spark.createDataFrame([("B", 2)], ["SRC", "V"])
    out = runtime_lib.router(
        spark=spark, input_df=df1, name="RTR",
        multi_feed=True,
        feeds=[(df1, {"SRC": "PORT1"}), (df2, {"SRC": "PORT1"})],
        groups=[{"name": "G1", "df_output": "df_rtr_G1",
                 "condition": "PORT1 = 'A'"}],
    )
    assert out["df_rtr_G1"].collect()[0]["PORT1"] == "A"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /var/lib/airflow/dags/adam/informatica/informatica_sparker && python3.11 -m pytest tests/test_lib_router.py -v 2>&1 | tail -4`
Expected: FAIL — `AttributeError: ... no attribute 'router'`.

- [ ] **Step 3: Implement `router`** — append to `templates/runtime_lib.py.j2` after `filter`:

```python
def router(spark, input_df, name=None, groups=None, multi_feed=False,
           feeds=None, substitutions=None, config=None, **kwargs):
    """Convert one Informatica Router into its output-group DataFrames.

    Returns {df_output: DataFrame}. Multi-input routers first build the input
    as a UNION of all feeds (NULL-filled per Router INPUT port, port aliases
    from the XML connectors), then the ORDERED group split runs (first match
    wins). Each group's condition is: filter_inner ($$ substituted) >
    default_negated (negated chain of named groups' conditions) > condition >
    pass-through. Connector renames apply after the filter, drop-first.
    """
    if multi_feed:
        _feeds = feeds or []
        _rtr_ports = []
        for _df, _aliases in _feeds:
            for _c in _df.columns:
                _p = _aliases.get(_c, _c)
                if _p.lower() not in [x.lower() for x in _rtr_ports]:
                    _rtr_ports.append(_p)
        _feed_views = []
        for _df, _aliases in _feeds:
            _rev = {_v: _k for _k, _v in _aliases.items()}
            _sel = []
            for _p in _rtr_ports:
                if _p in _rev:
                    if _rev[_p].lower() in [x.lower() for x in _df.columns]:
                        _sel.append(col(_rev[_p]).alias(_p))
                    else:
                        _sel.append(lit(None).alias(_p))
                elif _p.lower() in [x.lower() for x in _df.columns] and _p not in _aliases:
                    _sel.append(col(_p))
                else:
                    _sel.append(lit(None).alias(_p))
            _feed_views.append(_df.select(*_sel))
        _rtr_in = _feed_views[0]
        for _v in _feed_views[1:]:
            _rtr_in = _rtr_in.unionByName(_v)
    else:
        _rtr_in = input_df
    # Pre-substitute $$ filter texts once (the template's first-pass vars)
    _prepared = {}
    for _g in groups or []:
        _f_inner = _g.get("filter_inner") or ""
        if _f_inner and "$$" in _f_inner:
            _f_inner = _substitute(_f_inner, substitutions)
        _prepared[_g["name"]] = _f_inner
    _conds = {}
    for _g in groups or []:
        _f_inner = _prepared.get(_g["name"]) or ""
        if _f_inner:
            _conds[_g["name"]] = expr(_f_inner)
        elif _g.get("condition"):
            _conds[_g["name"]] = expr(_g["condition"])
    _out = {}
    for _g in groups or []:
        _name, _df_out = _g["name"], _g["df_output"]
        if _prepared.get(_name):
            _df = _rtr_in.filter(expr(_prepared[_name]))
        elif _g.get("default_negated"):
            _df = _rtr_in
            for _neg in _g["default_negated"]:
                _df = _df.filter(~_conds[_neg])
        elif _g.get("condition"):
            _df = _rtr_in.filter(expr(_g["condition"]))
        else:
            _df = _rtr_in
        _df = _rename_columns(_df, _g.get("renames") or [])
        _out[_df_out] = _df
    return _out
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python3.11 -m pytest tests/test_lib_router.py -v 2>&1 | tail -4`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add informatica_sparker/templates/runtime_lib.py.j2 tests/test_lib_router.py
git commit -m "feat: lib.router component method (multi-feed union input, ordered group split, DEFAULT negation)"
```

---

### Task 2: `lib.union`

**Files:**
- Modify: `informatica_sparker/informatica_sparker/templates/runtime_lib.py.j2`
- Create: `informatica_sparker/tests/test_lib_union.py`

**Interfaces:**
- Produces: `union(spark, input_df, name=None, inputs=None, union_selects=None, flag_column="", output_columns=None, config=None, **kwargs)` → DataFrame.
  - `inputs`: `[str df names]` — resolved via the generated code? No — the METHOD receives DataFrame OBJECTS: the generated call passes `inputs=[df_A, df_B]` (unquoted names, like `inline_lookup_joins.lookup_df`).
  - `union_selects`: `[{'df_input': DataFrame, 'selects': [{'from', 'to'}]}]` — each input is select+aliased (case-insensitive same-name → `col(from)`) then `unionByName(allowMissingColumns=True)`.
  - `flag_column`: non-empty raises ValueError (dead path — old template NameError'd on the undefined `normalize_flag_column`; no generated case exists).
  - `output_columns`: after union, `_fill_missing` then `select(*output_columns)`.

- [ ] **Step 1: Write the failing test** — `tests/test_lib_union.py`

```python
"""Tests for lib.union. Fixtures (runtime_lib, spark) come from conftest.py."""


def test_union_selects(runtime_lib, spark):
    a = spark.createDataFrame([(1, "x")], ["K", "A"])
    b = spark.createDataFrame([(2, "y")], ["K", "B"])
    out = runtime_lib.union(
        spark=spark, input_df=a, name="UN",
        union_selects=[
            {"df_input": a, "selects": [{"from": "K", "to": "K"}, {"from": "A", "to": "V"}]},
            {"df_input": b, "selects": [{"from": "K", "to": "K"}, {"from": "B", "to": "V"}]},
        ],
    )
    assert sorted((r["K"], r["V"]) for r in out.collect()) == [(1, "x"), (2, "y")]


def test_union_simple_inputs(runtime_lib, spark):
    a = spark.createDataFrame([(1,)], ["K"])
    b = spark.createDataFrame([(2,)], ["K"])
    out = runtime_lib.union(
        spark=spark, input_df=a, name="UN",
        inputs=[a, b],
    )
    assert sorted(r["K"] for r in out.collect()) == [1, 2]


def test_union_output_columns_fill(runtime_lib, spark):
    a = spark.createDataFrame([(1,)], ["K"])
    b = spark.createDataFrame([(1,)], ["K"])
    out = runtime_lib.union(
        spark=spark, input_df=a, name="UN",
        inputs=[a, b],
        output_columns=["K", "MISSING"],
    )
    assert out.columns == ["K", "MISSING"]
    assert out.collect()[0]["MISSING"] is None


def test_union_flag_column_raises(runtime_lib, spark):
    a = spark.createDataFrame([(1,)], ["K"])
    try:
        runtime_lib.union(
            spark=spark, input_df=a, name="UN",
            inputs=[a], flag_column="FLAG",
        )
        assert False, "expected ValueError"
    except ValueError:
        pass
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3.11 -m pytest tests/test_lib_union.py -v 2>&1 | tail -4`
Expected: FAIL — `AttributeError: ... no attribute 'union'`.

- [ ] **Step 3: Implement `union`** — append to `templates/runtime_lib.py.j2` after `router`:

```python
def union(spark, input_df, name=None, inputs=None, union_selects=None,
          flag_column="", output_columns=None, config=None, **kwargs):
    """Convert one Informatica Union.

    Per-input groups are select+aliased to the union's output ports, then
    unionByName(allowMissingColumns=True). Output ports absent from the union
    are filled with lit(None) and the frame is re-selected to the output
    column list.
    """
    if flag_column:
        raise ValueError(
            "Union %s: flag_column unions are not supported" % (name or "?"))
    _sels = union_selects or []
    if _sels:
        _views = []
        for _us in _sels:
            _views.append(_us["df_input"].select(*[
                col(_s["from"]) if _s["from"].lower() == _s["to"].lower()
                else col(_s["from"]).alias(_s["to"])
                for _s in _us["selects"]
            ]))
        df = _views[0]
        for _v in _views[1:]:
            df = df.unionByName(_v, allowMissingColumns=True)
    else:
        df = (inputs or [input_df])[0]
        for _v in (inputs or [input_df])[1:]:
            df = df.unionByName(_v, allowMissingColumns=True)
    if output_columns:
        df = _fill_missing(df, output_columns)
        df = df.select(*output_columns)
    return df
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python3.11 -m pytest tests/test_lib_union.py tests/test_lib_router.py -v 2>&1 | tail -4`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add informatica_sparker/templates/runtime_lib.py.j2 tests/test_lib_union.py
git commit -m "feat: lib.union component method (per-group selects, unionByName, output fills)"
```

---

### Task 3: `lib.sorter`

**Files:**
- Modify: `informatica_sparker/informatica_sparker/templates/runtime_lib.py.j2`
- Create: `informatica_sparker/tests/test_lib_sorter.py`

**Interfaces:**
- Consumes: `_rename_columns` (Phase 1).
- Produces: `sorter(spark, input_df, name=None, rename_columns=None, sort_columns=None, config=None, **kwargs)` → DataFrame.
  - `sort_columns`: `[{'column', 'direction'}]` — `direction.upper() == 'DESC'` → `desc(column)` else `asc(column)`.

- [ ] **Step 1: Write the failing test** — `tests/test_lib_sorter.py`

```python
"""Tests for lib.sorter. Fixtures (runtime_lib, spark) come from conftest.py."""


def test_sorter_renames_and_order(runtime_lib, spark):
    df = spark.createDataFrame([(2,), (1,)], ["A"])
    out = runtime_lib.sorter(
        spark=spark, input_df=df, name="SRT",
        rename_columns=[("A", "B")],
        sort_columns=[{"column": "B", "direction": "ASC"}],
    )
    assert out.columns == ["B"]
    assert [r["B"] for r in out.collect()] == [1, 2]


def test_sorter_desc(runtime_lib, spark):
    df = spark.createDataFrame([(1,), (2,)], ["A"])
    out = runtime_lib.sorter(
        spark=spark, input_df=df, name="SRT",
        sort_columns=[{"column": "A", "direction": "DESC"}],
    )
    assert [r["A"] for r in out.collect()] == [2, 1]


def test_sorter_no_sort_passthrough(runtime_lib, spark):
    df = spark.createDataFrame([(1,)], ["A"])
    out = runtime_lib.sorter(spark=spark, input_df=df, name="SRT")
    assert out.count() == 1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3.11 -m pytest tests/test_lib_sorter.py -v 2>&1 | tail -4`
Expected: FAIL — `AttributeError: ... no attribute 'sorter'`.

- [ ] **Step 3: Implement `sorter`** — append to `templates/runtime_lib.py.j2` after `union`:

```python
def sorter(spark, input_df, name=None, rename_columns=None,
           sort_columns=None, config=None, **kwargs):
    """Convert one Informatica Sorter.

    Connector renames apply first, then orderBy over the sort keys (ASC by
    default, DESC when the direction says so).
    """
    df = _rename_columns(input_df, rename_columns or [])
    if sort_columns:
        _sorts = []
        for _sc in sort_columns:
            if (_sc.get("direction") or "ASC").upper() == "DESC":
                _sorts.append(desc(_sc["column"]))
            else:
                _sorts.append(asc(_sc["column"]))
        df = df.orderBy(*_sorts)
    return df
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python3.11 -m pytest tests/test_lib_sorter.py -v 2>&1 | tail -4`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add informatica_sparker/templates/runtime_lib.py.j2 tests/test_lib_sorter.py
git commit -m "feat: lib.sorter component method (renames + orderBy)"
```

---

### Task 4: `lib.sequence`

**Files:**
- Modify: `informatica_sparker/informatica_sparker/templates/runtime_lib.py.j2`
- Create: `informatica_sparker/tests/test_lib_sequence.py`

**Interfaces:**
- Produces: `sequence(spark, input_df, name=None, output_col='NEXTVAL', start=1, config=None, **kwargs)` → DataFrame.
  - `withColumn(output_col, monotonically_increasing_id() + start)`. Connected sequences are already handled by `lib.filter`'s `sequence_attach` (Phase 1); this method serves standalone `ApplySequenceStep`s.

- [ ] **Step 1: Write the failing test** — `tests/test_lib_sequence.py`

```python
"""Tests for lib.sequence. Fixtures (runtime_lib, spark) come from conftest.py."""


def test_sequence_attaches_nexval(runtime_lib, spark):
    df = spark.createDataFrame([(1,), (2,)], ["A"]).repartition(1)
    out = runtime_lib.sequence(
        spark=spark, input_df=df, name="SEQ",
        output_col="NEXTVAL", start=100,
    )
    assert sorted(r["NEXTVAL"] for r in out.collect()) == [100, 101]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3.11 -m pytest tests/test_lib_sequence.py -v 2>&1 | tail -4`
Expected: FAIL — `AttributeError: ... no attribute 'sequence'`.

- [ ] **Step 3: Implement `sequence`** — append to `templates/runtime_lib.py.j2` after `sorter`:

```python
def sequence(spark, input_df, name=None, output_col="NEXTVAL", start=1,
             config=None, **kwargs):
    """Convert one standalone Informatica Sequence Generator.

    Attaches the next-value column as monotonically_increasing_id() + start.
    (Connected sequences attach via lib.filter's sequence_attach.)
    """
    return input_df.withColumn(
        output_col, monotonically_increasing_id() + int(start))
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python3.11 -m pytest tests/test_lib_sequence.py -v 2>&1 | tail -4`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add informatica_sparker/templates/runtime_lib.py.j2 tests/test_lib_sequence.py
git commit -m "feat: lib.sequence component method (standalone NEXTVAL)"
```

---

### Task 5: Generator wiring — router (handler cfg + template)

**Files:**
- Modify: `informatica_sparker/informatica_sparker/handlers.py` (`_handle_router`, ~line 2686)
- Modify: `informatica_sparker/informatica_sparker/templates/mapping.py.j2` (APPLY_ROUTER block, lines 760-852)
- Create: `informatica_sparker/tests/test_router_wiring.py`

**Interfaces:**
- Consumes: `lib.router` signature (Task 1).
- Produces: `step.params["router_cfg"]` with keys: `groups` (list of per-group dicts with `df_output`, `condition` raw text, `filter_inner`, `default_negated`, `renames` as tuples), `multi_feed` (bool), `feeds` (optional, list of `(df_name, aliases)` — df names rendered UNQUOTED), `substitutions` (optional).
- The template must render the call and UNPACK the returned dict: `_rtr = lib.router(...)` then `df_rtr_G1 = _rtr['df_rtr_G1']` per group (the group's `df_output` name keys the dict).

- [ ] **Step 1: Build the cfg in `_handle_router`**

The handler already builds `groups` (each with `name`, `df_output`, `condition`, `renames`, `filter_inner`, `default_negated`) and `_feed_specs`/`_multi_feed` for the multi-feed path. Convert to the cfg form (conditions to RAW text — the template currently renders `{{ group.get('condition') }}` as Python source like `expr("...")`; strip the wrapper with the same `expr("(.*)")$` regex used in `_handle_filter`; `renames` to `(from, to)` tuples):

```python
        _rtr_groups = []
        for _g in step.params.get("groups", []):
            _cond = _g.get("condition") or ""
            _m = re.match(r'expr\("(.*)"\)$', _cond)
            _rtr_groups.append({
                "name": _g["name"],
                "df_output": _g.get("df_output", "df_group"),
                "condition": _m.group(1) if _m else _cond,
                "filter_inner": _g.get("filter_inner", ""),
                "default_negated": _g.get("default_negated", []),
                "renames": [tuple(_r) for _r in _g.get("renames", [])],
            })
        _rtr_cfg: Dict[str, Any] = {"groups": _rtr_groups}
        if step.params.get("multi_feed"):
            _rtr_cfg["multi_feed"] = True
            _rtr_cfg["feeds"] = step.params.get("feeds", [])
        step.params["router_cfg"] = _rtr_cfg
```

(Read `_handle_router` first: verify the group dict keys (`name`/`df_output`/`condition`/`renames`/`filter_inner`/`default_negated`) and the `feeds`/`multi_feed` params — adapt to the actual key names.)

- [ ] **Step 2: Replace the APPLY_ROUTER template block** (lines 760-852) with:

```jinja2
        {% elif step.step_type == IRStepType.APPLY_ROUTER %}
        # Router: {{ step.step_name }} - splits into multiple output groups
        {% set rtrcfg = step.params['router_cfg'] %}
        _rtr = lib.router(
            spark=spark,
            input_df={{ step.df_input }},
            {% if rtrcfg.get('multi_feed') %}
            multi_feed=True,
            feeds=[
            {% for _df, _aliases in rtrcfg['feeds'] %}
                ({{ _df }}, {{ _aliases | pyrepr }}),
            {% endfor %}
            ],
            {% endif %}
            groups=[
            {% for _g in rtrcfg['groups'] %}
                {{ _g | pyrepr }},
            {% endfor %}
            ],
            {% if rtrcfg.get('substitutions') %}
            substitutions={{ '{' }}{% for _k, _v in rtrcfg['substitutions'].items() %}'{{ _k }}': {{ _v }}{% if not loop.last %}, {% endif %}{% endfor %}{{ '}' }},
            {% endif %}
            config=config,
        )
        {% for _g in rtrcfg['groups'] %}
        {{ _g['df_output'] }} = _rtr[{{ _g['df_output'] | pyrepr }}]
        ctx.register_df("{{ _g['df_output'] }}", {{ _g['df_output'] }})
        {% endfor %}
```

Note: `{{ _g | pyrepr }}` renders the whole group dict one per line (includes `filter_inner` raw text with `$$` markers — fine, they are strings; `condition` raw text; `renames` tuples). The `df_output` values inside the group dict are plain strings — pyrepr quotes them correctly; only the top-level `feeds` df names render unquoted.

- [ ] **Step 3: Add the wiring regression test** — `tests/test_router_wiring.py`

Parse `WF_EMS_DDS_APLY_MTH.XML` (6 router mappings): assert every `ApplyRouterStep` carries `router_cfg` with non-empty `groups`; `seen >= 1`; every group has `df_output` and (condition XOR default_negated XOR pass-through). Plus a render smoke test: render the APPLY_ROUTER block with a synthetic cfg (2 groups + multi_feed feeds) and ast.parse the output.

- [ ] **Step 4: Run tests**

Run: `python3.11 -m pytest tests/test_router_wiring.py tests/ -q 2>&1 | tail -3`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add informatica_sparker/handlers.py informatica_sparker/templates/mapping.py.j2 tests/test_router_wiring.py
git commit -m "feat: generator emits lib.router calls (dict unpack per group) + wiring regression test"
```

---

### Task 6: Generator wiring — union + sorter + sequence (handler cfgs + template)

**Files:**
- Modify: `informatica_sparker/informatica_sparker/handlers.py` (`_handle_union` ~2552, `_handle_sorter` ~2509, `_handle_sequence` ~3045)
- Modify: `informatica_sparker/informatica_sparker/templates/mapping.py.j2` (APPLY_UNION lines 713-758, APPLY_SORTER lines 680-711, APPLY_SEQUENCE lines 854-860)
- Create: `informatica_sparker/tests/test_union_sorter_wiring.py`

**Interfaces:**
- Consumes: `lib.union`/`lib.sorter`/`lib.sequence` signatures (Tasks 2-4).
- Produces: `step.params["union_cfg"]`, `step.params["sorter_cfg"]`, `step.params["sequence_cfg"]`.

- [ ] **Step 1: Build the cfgs**

Union (handler locals: `inputs`, `flag_column`, `output_columns`, `_union_selects` — each select carries `df_input` as a df NAME string; the template renders `{{ us.df_input }}` unquoted, so the cfg must keep names and the TEMPLATE renders them unquoted inside the dicts):

```python
        _union_cfg: Dict[str, Any] = {
            "inputs": step.params.get("df_inputs", []),
            "flag_column": step.params.get("flag_column", ""),
            "output_columns": step.params.get("output_columns", []),
        }
        if step.params.get("union_selects"):
            _union_cfg["union_selects"] = step.params["union_selects"]
        step.params["union_cfg"] = _union_cfg
```

Sorter (handler stores `sorter_renames` as dicts with `from`/`to`? — the template reads `sorter_renames` and `sort_columns` from params; convert renames to tuples):

```python
        _sorter_cfg: Dict[str, Any] = {
            "rename_columns": [
                (_r.get("from"), _r.get("to"))
                for _r in step.params.get("sorter_renames", [])
                if (_r.get("from") or "").lower() != (_r.get("to") or "").lower()
            ],
            "sort_columns": step.params.get("sort_columns", []),
        }
        step.params["sorter_cfg"] = _sorter_cfg
```

Sequence (handler stores `sequence_name`/`start_value` in params):

```python
        step.params["sequence_cfg"] = {
            "output_col": step.params.get("sequence_name", "NEXTVAL"),
            "start": step.params.get("start_value", 1),
        }
```

(Verify actual key names against the handlers — the template reads `sorter_renames`, `sort_columns`, `sequence_name`, `start_value`, `df_inputs`, `flag_column`, `union_selects` from params.)

- [ ] **Step 2: Replace the template blocks**

APPLY_UNION (lines 713-758) — the flag_column branch is removed from the template (dead path; `lib.union` raises on non-empty flag_column; the handler still passes it):

```jinja2
        {% elif step.step_type == IRStepType.APPLY_UNION %}
        # Union: {{ step.step_name }}
        {% set uncfg = step.params['union_cfg'] %}
        {{ step.df_output }} = lib.union(
            spark=spark,
            input_df={{ step.df_input }},
            {% if uncfg.get('union_selects') %}
            union_selects=[
            {% for _us in uncfg['union_selects'] %}
                {'df_input': {{ _us['df_input'] }}, 'selects': {{ _us['selects'] | pyrepr }}},
            {% endfor %}
            ],
            {% else %}
            inputs=[{% for _df in uncfg.get('inputs', []) %}{{ _df }}{% if not loop.last %}, {% endif %}{% endfor %}],
            {% endif %}
            {% if uncfg.get('flag_column') %}
            flag_column={{ uncfg['flag_column'] | pyrepr }},
            {% endif %}
            {% if uncfg.get('output_columns') %}
            output_columns={{ uncfg['output_columns'] | pyrepr }},
            {% endif %}
            config=config,
        )
        ctx.register_df("{{ step.df_output }}", {{ step.df_output }})
```

APPLY_SORTER (lines 680-711):

```jinja2
        {% elif step.step_type == IRStepType.APPLY_SORTER %}
        # Sorter: {{ step.step_name }}
        {% set srtcfg = step.params['sorter_cfg'] %}
        {{ step.df_output }} = lib.sorter(
            spark=spark,
            input_df={{ step.df_input }},
            {% if srtcfg.get('rename_columns') %}
            rename_columns=[
                {{ srtcfg['rename_columns'] | map('pyrepr') | join(',\n                ') }}
            ],
            {% endif %}
            {% if srtcfg.get('sort_columns') %}
            sort_columns=[
                {{ srtcfg['sort_columns'] | map('pyrepr') | join(',\n                ') }}
            ],
            {% endif %}
            config=config,
        )
        ctx.register_df("{{ step.df_output }}", {{ step.df_output }})
```

APPLY_SEQUENCE (lines 854-860):

```jinja2
        {% elif step.step_type == IRStepType.APPLY_SEQUENCE %}
        # Sequence Generator: {{ step.step_name }}
        {% set seqcfg = step.params['sequence_cfg'] %}
        {{ step.df_output }} = lib.sequence(
            spark=spark,
            input_df={{ step.df_input }},
            output_col={{ seqcfg.get('output_col', 'NEXTVAL') | pyrepr }},
            start={{ seqcfg.get('start', 1) | pyrepr }},
            config=config,
        )
        ctx.register_df("{{ step.df_output }}", {{ step.df_output }})
```

- [ ] **Step 3: Add the wiring regression test** — `tests/test_union_sorter_wiring.py`

Parse `WF_EMS_DDS_APLY_MTH.XML`: every `ApplyUnionStep` carries `union_cfg` (`seen >= 1`); every `ApplySorterStep` carries `sorter_cfg`; no `ApplySequenceStep` in this workflow (assert 0 — connected sequences attach via filter; if one appears, assert it carries `sequence_cfg`). Plus render smoke tests for the union block (selects path with unquoted df names + ast.parse) and sorter block.

- [ ] **Step 4: Run tests**

Run: `python3.11 -m pytest tests/test_union_sorter_wiring.py tests/ -q 2>&1 | tail -3`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add informatica_sparker/handlers.py informatica_sparker/templates/mapping.py.j2 tests/test_union_sorter_wiring.py
git commit -m "feat: generator emits lib.union/lib.sorter/lib.sequence calls + wiring regression test"
```

---

### Task 7: Reconvert WF_EMS_DDS_APLY_MTH and verify (Phase 3 gate — runs with Phase 4's gate)

**Files:**
- Run: build + CLI convert (backup already exists from Phase 1)

**NOTE:** this gate runs ONCE, AFTER Phase 4's tasks are committed (the user's execution order is Phase 3 → Phase 4 → Phase 2; both Phase 3 and Phase 4 change the same template file, so a single reconversion at the end of Phase 4 covers both — see Phase 4's plan, Task 6 "Reconvert and verify". If Phase 4's plan is not yet written when this task is reached, run this gate now; otherwise skip to Phase 4.)

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
grep -c "lib.router(" m_*.py | grep -v ":0" | wc -l      # 6 expected
grep -c "lib.union(" m_*.py | grep -v ":0" | wc -l       # 19 expected
grep -c "lib.sorter(" m_*.py | grep -v ":0" | wc -l      # 1 expected
grep -rn "_rtr = lib.router" m_*.py | wc -l              # multi-output unpack present
grep -rn "\.filter(expr(" m_*.py | wc -l                 # must be 0 (router conditions now in lib)
```

- [ ] **Step 4: Per-step semantic comparison vs the backup (Phase 1 lesson)**

For every new `lib.router` call, extract each group's `condition`/`filter_inner` and compare to the backup's same-named group filters (`df_rtr_<group>` steps): same condition texts, same `default_negated` group structure. For every `lib.union` call, compare the per-group selects (`from`→`to` pairs) to the backup's select+alias lines. For the sorter, compare sort columns. Follow the line-based approach of `/tmp/compare_filter_conditions.py` (write `/tmp/compare_phase3_steps.py`). Report counts: `compared`, `mismatches` (must be 0).

- [ ] **Step 5: Full test suite**

```bash
cd /var/lib/airflow/dags/adam/informatica/informatica_sparker
python3.11 -m pytest tests/ -q 2>&1 | tail -2
```
Expected: all PASS.

- [ ] **Step 6: Runtime verification (user checkpoint)**

After Phase 4's gate reconversion: user runs the full workflow on the cluster (`cd PySpark_workflows/dds/WF_EMS_DDS_APLY_MTH && python wf_ems_dds_aply_mth.py`) — expect all mappings SUCCESS. Report coverage: 6 routers, 19 unions, 1 sorter. (Sequence: unit-tested only; no runtime coverage in this workflow.)

- [ ] **Step 7: Update CLAUDE.md**

Append the Phase 3 methods to the "Component Methods" section in `/var/lib/airflow/dags/adam/informatica/CLAUDE.md`: `lib.router` (multi-feed union input, ordered group split, DEFAULT negation, dict return), `lib.union` (per-group selects, flag_column unsupported), `lib.sorter`, `lib.sequence` (standalone). Edit in place, no commit.

---

## Out of scope (this plan)

- static_lookup, joiner, aggregator, stored_procedure — Phase 2, executed LAST (user decision), plan at `docs/superpowers/plans/2026-08-12-component-methods-phase-2.md`.
- Source Qualifier port handling, update strategy, write target — Phase 4 (plan written alongside this one).
- Dynamic lookup — already in lib form, untouched.
- No expression-translation changes (`expr_translator` untouched).
- No regeneration of other workflows.

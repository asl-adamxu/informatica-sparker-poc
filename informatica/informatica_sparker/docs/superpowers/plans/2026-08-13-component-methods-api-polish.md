# Component Methods API Polish Plan — defaults, dict-forms, positional mapping (2026-08-13)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Apply five user-specified API optimizations across all component methods (Phase 1/3/4 deliverables, and as the convention Phase 2 must adopt): (1) `spark`/`config` become optional so generated calls omit them when unused; field-heavy params render multi-line; (2) `lib.sq_output` merges `port_cols` + `column_types` into one dict `{'NAME': 'type'}`; (3) `lib.router` merges `condition`/`filter_inner` into one `condition`, and `renames` render one tuple per line; (4) `lib.union` `selects` become from-only position lists with `output_columns` kept; (5) `lib.write_target` replaces `target_columns`/`field_map`/`unmapped_columns` with positional `source_columns` + `target_columns` (unconnected fields = None at the aligned source position).

**Architecture:** unchanged three-layer separation; this plan changes the runtime signatures + the generator cfg shapes + the template renderings together, then re-runs the full gate.

## Global Constraints

- Behavior preserved: these are pure API-shape changes; the runtime semantics of every method stay identical to the current verified state (semantic comparison gate enforces it).
- kwargs call form; `name` NOT rendered.
- Multi-line rendering for field-heavy params: one entry per line via `map('pyrepr') | join(',\n<indent>')`; scalars via `pyrepr`.
- `substitutions` identifiers unquoted with `{{ '{' }}...{{ '}' }}` braces.
- No Chinese comments in generated code. Zero import side effects.
- Commit per task (branch `enhance`); pytest from repo root; build/convert gate at the end.
- Test workflow: WF_EMS_DDS_APLY_MTH; backup `_pre_refactor_WF_EMS_DDS_APLY_MTH` unchanged.

## The five optimizations (exact spec)

### Opt 1: optional `spark`/`config`
- ALL component methods get `spark=None, config=None` defaults; methods that don't use them simply ignore them.
- Generated calls render `spark=spark` / `config=config` ONLY when the method actually needs them:
  - `lib.expression`: render `spark=spark` only when `sp_calls` is present (SP path needs it); never render `config`.
  - `lib.filter`, `lib.router`, `lib.union`, `lib.sorter`, `lib.sequence`, `lib.sq_output`, `lib.update_strategy`: render neither.
  - `lib.write_target`: render both (batch_update/delete + csv objects need them).
  - `lib.dynamic_lookup`: unchanged (both rendered — executor config).
- Field-heavy params (`rename_columns`, `computed_columns`, `sp_calls`, `inline_lookup_joins`, `union_selects`, `groups`' `renames`) render one entry per line.

### Opt 2: `lib.sq_output` `port_cols` dict form
- Signature: `sq_output(spark=None, input_df=None, name=None, port_cols=None, filter_condition=None, substitutions=None, distinct=False, config=None, **kwargs)`.
- `port_cols`: ordered dict `{'NAME': 'string', 'EST_CODE': 'string', ...}` — order drives the two-pass rename AND the final select (dict insertion order = port order); the value is the cast type (None/'' → no cast). `column_types` param is REMOVED.
- **Python 3.6 caveat (runtime runs on system python 3.6)**: dict insertion order is an implementation detail of CPython 3.6 (language-guaranteed only from 3.7). Mitigation: (a) the name-match-first pass is order-independent and consumes nearly all columns; (b) the positional fallback only applies to unaliased SQL-expression columns, which are rare and come LAST in real queries; (c) CPython 3.6 preserves insertion order in practice (verified by the semantic-comparison gate on the reconverted workflow). If order ever breaks, the symptom is a misaligned positional rename — loud and visible in the comparison gate.
- Runtime: `_port_names = list(port_cols)`; cast loop reads `port_cols[_cname]`.
- Template renders one entry per line:
  ```
  port_cols={
      'TIME_DMNS_KEY': 'decimal',
      'LAST_REC_TXN_DATE': 'date/time',
  },
  ```
- Handler cfg: `"port_cols": {name: ctype}` built from `output_columns` + `output_column_types` (missing type → `''`).

### Opt 3: `lib.router` `condition` merge + multi-line renames
- `filter_inner` REMOVED; one `condition` per group (raw translated text; may contain `$$`).
- Runtime: `_cond_text = _substitute(condition, substitutions)` when `$$` present, then `expr(...)`; the `_prepared`/`_conds` pre-pass collapses to a single `_conds` build.
- Group rendering in the template becomes structured (NOT whole-dict pyrepr): name/df_output/condition/default_negated per line, `renames` one tuple per line.

### Opt 4: `lib.union` `selects` from-only
- `union_selects`: `[{'df_input': df, 'selects': ['from_a', 'from_b']}]` — positional; selects[j] aliases to `output_columns[j]` (the group's select list is in output-column order; shorter than output_columns → those columns stay absent, filled lit(None) post-union).
- `output_columns` kept and still drives `_fill_missing` + final select.
- Runtime: `col(_s).alias(_port)` for i, _s in enumerate(selects) against output_columns[i]; if output_columns is missing, alias stays `_s` (defensive).
- Handler cfg: `"union_selects": [{"df_input": ..., "selects": [froms...]}]` from the existing `_selects` (they already carry from+to; take `from` in to-order).

### Opt 5: `lib.write_target` positional source/target columns
- REMOVED params: `target_columns`, `field_map`, `unmapped_columns`.
- ADDED params: `source_columns` (list, aligned positionally to `target_columns`; `None` entry = unconnected target → `lit(None).cast(StringType())`), `target_columns` (kept as the positional target list — the final select order).
- Runtime: for i in range(len(target_columns)): src = source_columns[i] if i < len else None; if src is None → fill; elif src.lower() != tgt.lower() → drop-first rename; else keep. Then the final select of target_columns.
- The `src_rowid` exclusion is preserved: unconnected `SRC_ROWID` entries render as None in source_columns but the fill is SKIPPED at runtime (same as today).
- Handler cfg: build `source_columns` aligned to `target_columns` from `field_map` + `unmapped_columns`: for each target col, source = field_map.get(tgt) or None (identity-mapped columns → the col name itself).

## Tasks

### Task 1: Runtime signatures — optional spark/config + omit-rendering (Opt 1)

- Modify runtime_lib.py.j2: give `spark=None, config=None` defaults to expression/filter/router/union/sorter/sequence/sq_output/update_strategy (write_target keeps spark positional-first since it's always needed; expression's spark stays but defaults None).
- Modify mapping.py.j2: conditional `spark=spark` for expression (only with sp_calls); remove `spark=spark`/`config=config` from filter/router/union/sorter/sequence/sq_output/update_strategy blocks; write_target keeps both.
- Update ALL render smoke tests: assert `spark=`/`config=` ABSENT where omitted, present where kept.
- Update lib tests that call with `spark=spark` — they may keep passing spark (signature accepts it); only render tests assert omission.
- Full suite green (167 base).

### Task 2: sq_output port_cols dict form (Opt 2)

- Runtime method + unit tests (dict order drives rename/select; value drives cast; None value no cast).
- Handler cfg (merge into one dict; missing type → '').
- Template block (one entry per line).
- Render smoke test + wiring test updates (cfg key shape changed: `port_cols` is now a dict; `column_types` gone).

### Task 3: router condition merge + structured group rendering (Opt 3)

- Runtime method + unit tests (single condition field; $$ substitution via `_substitute`).
- Handler cfg (drop filter_inner; condition carries the raw text incl. $$).
- Template block (structured per-group rendering; renames one tuple per line).
- Render smoke test + wiring test updates.

### Task 4: union selects from-only (Opt 4)

- Runtime method + unit tests (positional alias against output_columns).
- Handler cfg (selects from-only lists).
- Template block (selects render as `['a', 'b']` lists).
- Render smoke test + wiring test updates.

### Task 5: write_target source/target positional (Opt 5)

- Runtime method + unit tests (None entry → lit(None).cast(StringType()); src_rowid skip; identity keep; rename drop-first; final select).
- Handler cfg (aligned source_columns).
- Template block (source_columns + target_columns one entry per line).
- Render smoke test + wiring test updates.

### Task 6: Rebuild + reconvert + full gate

- Build wheel, reconvert WF_EMS_DDS_APLY_MTH (0w/0e).
- py_compile + static gates.
- Update /tmp/compare_phase4_steps.py for the new param shapes (port_cols dict, union selects from-only, write_target source/target positional) and re-run: must be 0 mismatches vs backup.
- Re-run /tmp/compare_filter_conditions.py (26/26).
- Full pytest green.
- CLAUDE.md: update the Component Methods section for the new call forms.
- Runtime verification: user checkpoint.

## Out of scope

- Phase 2 methods (static_lookup/joiner/aggregator/stored_procedure) — they adopt these conventions when built; this plan only touches the existing Phase 1/3/4 methods.
- lib.dynamic_lookup — unchanged.

# Dynamic Lookup Keyword-Argument Call (2026-08-11)

## Goal

Improve readability of the generated `lib.dynamic_lookup(...)` call in mapping
files: pass the lookup configuration as explicit keyword arguments instead of
one inline dict.

## Design

1. **Runtime signature** (`runtime_lib.py.j2` `dynamic_lookup`):

   ```python
   def dynamic_lookup(spark, input_df, lookup_df, cfg=None, config=None, **dl_kwargs):
       cfg = dict(cfg or {})
       cfg.update(dl_kwargs)
   ```

   Backward compatible: old positional-dict callers (existing generated
   mappings, tests) keep working.

2. **Template** (`mapping.py.j2` dynamic_lookup branch): render the call as
   `spark=spark, input_df=_lkp_input, lookup_df=<df>, <each config key as a
   named kwarg>, config=config`. Values render in Python single-quote style via
   a new `pyrepr` filter (codegen.py). `lookup_output_fields` renders one dict
   per line (a plain repr of the whole list would exceed line width and break
   indentation on continuation lines).

3. **codegen.py**: register `pyrepr = repr` filter on the Jinja environment.

4. **Tests** (`test_dynamic_lookup.py`): add a kwargs-style call path test and
   extend the template-rendering assertion to check `spark=spark` kwargs form.

5. **Regeneration**: rebuild + reinstall the `informatica-sparker` package
   (the installed copy is a stale pyc-only 2026.8.4 build), regenerate
   WF_NHS_TL, then verify with pytest + a YARN mapping run.

## Out of scope

- No state-machine behavior changes (kwargs merge into the same cfg dict).
- No changes to static lookups or other steps.

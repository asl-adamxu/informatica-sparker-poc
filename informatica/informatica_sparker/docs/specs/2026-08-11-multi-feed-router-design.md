# Multi-Input Router Conversion (v2026.08.11)

## Problem

Generated code for multi-input Routers (WF_EMS_TL, all 9 `RTRTRANS` routers) is
broken in three ways:

1. **Single-input translation**: `_handle_router` picks ONE upstream DataFrame
   via `_get_input_df`. Group conditions referencing ports fed by *other*
   upstreams crash with `[UNRESOLVED_COLUMN]` (e.g. `OUT_DLPK_SOR_CACHE1`,
   `LAST_REC_TXN_TYPE_CODE`), or silently lose data.
2. **Group ORDER ignored**: groups are filtered independently; a `TRUE` group
   takes ALL rows, overlapping earlier groups (Informatica semantics: first
   match wins, evaluated in GROUP ORDER).
3. **Router-fed targets get no write steps**: `_all_upstreams_available` never
   sees a Router (only `RTRTRANS_<group>` suffixed keys are registered, never
   the bare instance name), so router-fed targets/transforms stay "deferred
   forever" and are dropped — `SSA_EMS_TAM_RAS1`, `SSA_EMS_CMM_HSE_SRVC_APLY1`,
   `EMS_TAM_RAS_L21` writes were missing entirely (silent data loss).

Two consumer shapes exist:
- **Type A** (6 mappings): router outputs feed transforms (EXPTRANS1/3,
  UPD_SSAL2_MSTR1, LKP_DYN_*, UPDTRANS) which reference **output port names**
  (REF_FIELD-suffixed).
- **Type B** (3 mappings): router outputs feed only targets
  (SSA_EMS_TAM_RAS1 / SSA_EMS_CMM_HSE_SRVC_APLY1 / EMS_TAM_RAS_L21 style).

## Design (approved: full scope — writes included, all 9 routers)

Approach A: a **multi-feed path in `_handle_router`** + ordered-group rendering
in `mapping.py.j2`; single-input routers (other workflows) are untouched.

### 1. `_handle_router` multi-feed detection & union construction

- Group connectors into the Router by `from_instance`; for each upstream resolve
  its df (same logic as `_get_all_input_dfs`). Aliases = `{from_field: to_field}`
  for connectors where `from_field != to_field`.
- `len(feeds) > 1` → multi-feed path (`step.params["multi_feed"] = True`,
  `step.params["feeds"] = [(df_name, aliases)]`). Any unresolved feed df falls
  back to the legacy single-input path with a warning.
- Template renders the union exactly like the validated manual fix: runtime
  port set (`_rtr_ports` from all feeds' alias-mapped columns), per-feed select
  with `lit(None)` fill (aliased source columns guarded too), `unionByName`
  chain, `ctx.register_df("df_rtr_input", ...)`.

### 2. Ordered group filtering (first match wins)

- Groups sorted by `TransformGroup.order` (parser already reads `ORDER`);
  DEFAULT last.
- Handler composes each group's filter: `G_i = ~c1 & ... & ~c_{i-1} & c_i`.
  A `TRUE`/empty condition group is the fallback: `~c1 & ... & ~c_{i-1}`
  (the `TRUE` term itself is dropped).
- DEFAULT: `lit(False)` when any prior condition was TRUE; otherwise the
  negated conjunction of all conditions. `$$`-variable conditions compose via
  the existing `_rtr_<group>_filter` runtime variables (`~(expr(var))`).
- `eqNullSafe` semantics preserved via translation (`NULL` never matches).

### 3. Group renames from input-port names

- Multi-feed group dfs carry **input port names** (the union's column names),
  so rename `from` = `field.ref_field` directly (legacy path keeps the
  `_rtr_field_remap` upstream-column lookup). Rename `to` = output port name,
  drop-first (unchanged). This keeps Type A consumers working (they reference
  output names) and lets Type B targets use the **standard write machinery**:
  after renames, target column names no longer collide with input-port names,
  so the existing "rename only if target name absent" guard maps every target
  column to the correct router port (verified column-by-column for all 3
  TAM_RAS targets, value-equal to the manual fix).

### 4. Downstream availability fix

- `_all_upstreams_available`: an upstream counts as available when the bare
  name is registered OR any `"<name>_"`-prefixed key exists (router group
  keys) in `current_df_map`/`_direct_df_map`. Router-fed targets/transforms
  then process normally; the existing router-aware `_get_input_df` block
  (`_tf.name == from_field` → `RTRTRANS_<group>` key) resolves the correct
  group df. `_handle_target` and the write template are unchanged.

### 5. Backward compatibility

- Single-input routers: byte-identical generated code (legacy branch).
- `plan.router_outputs`, BFS skip, `_get_input_df` preference chains unchanged.

## Verification (targeted first)

1. Existing pytest suite (`tests/`) on source tree.
2. Package-API regeneration of `M_S5_SSAL2_TRANSFORM_EMS_TAM_RAS` → diff router
   section against the validated manual fix (`grand/.../m_s5_ssal2...py`),
   `py_compile`, static port coverage, group-semantics simulation.
3. Same for `M_S5_SSAL2_TRANSFORM_EMS_TOW_TPS_AGRMT` (Type B) and
   `M_EMS_SSAL2_TRANS_RWV_RATE_CNCSN` (Type A — cross-feed condition +
   transform consumers).
4. Rebuild wheel (`python3.11 -m build` + `pip install --user
   --force-reinstall`) for CLI use; full WF_EMS_TL regeneration is the user's
   call afterwards.

## References

- Manual fix (validated): `grand/PowerCenter_workflows/WF_EMS_TL/m_s5_ssal2_transform_ems_tam_ras.py`
  router block; `docs/superpowers/plans/` design notes from 2026-08-11 session.

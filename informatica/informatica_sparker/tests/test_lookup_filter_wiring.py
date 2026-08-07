"""Regression tests for lookup/filter upstream wiring in NHS TL mappings.

Verifies that a filter fed by a Lookup Procedure consumes that lookup's
chain/merge output (not a stale pre-lookup DataFrame), e.g.:

    FILTRANS_STS  <- DLKP_SOR_STS + EXP_BK   (must see END_DATE / NewLookupRow)
    FILTRANS_MSTR <- DLKP_SOR_MSTR + EXP_BK  (must see MSTR NewLookupRow)

The source of truth is WF_NHS_TL.XML only; EMS TL is intentionally not used.
"""

import re
from pathlib import Path

from informatica_sparker.ir import ApplyExpressionStep, ApplyFilterStep, ApplyLookupStep
from informatica_sparker.handlers import TransformHandlers
from informatica_sparker.models import UserConfig
from informatica_sparker.parser import InfaXMLParser


NHS_TL_XML = (
    Path(__file__).resolve().parents[2]
    / "PowerCenter_workflows"
    / "transform_and_load"
    / "WF_NHS_TL.XML"
)


def _load_nhs_mappings():
    parser = InfaXMLParser(NHS_TL_XML.read_bytes())
    assert parser.parse()
    return [
        m
        for m in parser.get_mappings()
        if m.name.startswith("M_NHS_SSAL2_TRAN_NHS_")
    ]


def test_lookup_fed_filters_consume_lookup_merge_output():
    mappings = _load_nhs_mappings()
    assert len(mappings) >= 40, "expected the NHS SSAL2 mapping set"

    checked = 0
    for mapping in mappings:
        handlers = TransformHandlers(mapping, UserConfig())
        plan = handlers.build_ir_plan()
        assert plan is not None, mapping.name

        steps = list(plan.steps)
        lookup_steps = {
            step.step_name[6:]: step
            for step in steps
            if isinstance(step, ApplyLookupStep)
        }

        for step in steps:
            if not isinstance(step, ApplyFilterStep):
                continue
            inst_name = (
                step.step_name[6:]
                if step.step_name.startswith("apply_")
                else step.step_name
            )
            # Upstream Lookup Procedure instances for this filter (graph-based,
            # so it also covers LKP_DYN_SOR_* lookups inside AGMT mapplets).
            lookup_upstreams = []
            for connector in mapping.connectors:
                if connector.to_instance != inst_name:
                    continue
                up = handlers.instance_map.get(connector.from_instance)
                if up is not None and handlers._resolve_transformation_type(up) == "Lookup Procedure":
                    lookup_upstreams.append(connector.from_instance)
            if not lookup_upstreams:
                continue

            for up_name in dict.fromkeys(lookup_upstreams):
                lk_step = lookup_steps.get(up_name)
                assert lk_step is not None, (
                    f"{mapping.name}: lookup step for {up_name} not found"
                )
                assert lk_step.df_output == step.df_input, (
                    f"{mapping.name}: {step.step_name} input {step.df_input} "
                    f"!= {up_name} output {lk_step.df_output}"
                )
                assert steps.index(lk_step) < steps.index(step), (
                    f"{mapping.name}: {step.step_name} emitted before {up_name}"
                )
                assert step.df_input.startswith(("df_lkp_merge", "df_merge")), (
                    f"{mapping.name}: {step.step_name} input {step.df_input} "
                    "is not a lookup merge output"
                )
                checked += 1

    assert checked >= 80, f"expected many lookup-fed filters, checked {checked}"


def test_filtrans_sts_and_mstr_use_their_own_lookup_merge():
    mappings = _load_nhs_mappings()
    checked = 0
    for mapping in mappings:
        handlers = TransformHandlers(mapping, UserConfig())
        plan = handlers.build_ir_plan()
        steps = list(plan.steps)
        by_name = {step.step_name: step for step in steps}

        sts_filter = by_name.get("apply_FILTRANS_STS")
        sts_lookup = by_name.get("apply_DLKP_SOR_STS")
        assert sts_filter is not None and sts_lookup is not None, mapping.name
        assert sts_filter.df_input == sts_lookup.df_output, (
            f"{mapping.name}: FILTRANS_STS input {sts_filter.df_input} "
            f"!= DLKP_SOR_STS output {sts_lookup.df_output}"
        )
        assert steps.index(sts_lookup) < steps.index(sts_filter), mapping.name

        mstr_filter = by_name.get("apply_FILTRANS_MSTR")
        assert mstr_filter is not None, mapping.name
        mstr_lookups = [
            step
            for step in steps
            if isinstance(step, ApplyLookupStep)
            and step.df_output == mstr_filter.df_input
            and steps.index(step) < steps.index(mstr_filter)
        ]
        assert mstr_lookups, (
            f"{mapping.name}: no lookup step before FILTRANS_MSTR "
            f"produced input {mstr_filter.df_input}"
        )
        checked += 1

    assert checked == len(mappings)


def _rename_steps(plan):
    return [
        step
        for step in plan.steps
        if isinstance(step, ApplyExpressionStep)
        and step.step_name.startswith("rename_")
        and step.params.get("rename_columns")
    ]


def test_mapplet_internal_rename_pairs_are_not_repeated():
    """External input-port renames are applied once at the mapplet entry.
    Internal expression rename steps must not repeat them (e.g. renaming
    INIT_FLAG to IN_V_INIT_IND again after it was already renamed), which
    produced unresolved columns downstream (see EXP_CDC INIT_FLAG failure).
    """
    mappings = _load_nhs_mappings()
    checked = 0
    for mapping in mappings:
        handlers = TransformHandlers(mapping, UserConfig())
        plan = handlers.build_ir_plan()
        seen = {}
        for step in _rename_steps(plan):
            for pair in step.params["rename_columns"]:
                key = tuple(pair)
                if key in seen:
                    # A reusable mapplet instantiated twice (MSTR/STS) produces
                    # identical internal rename steps; that is fine. Repeating
                    # a pair across DIFFERENT rename steps is the bug (e.g.
                    # INIT_FLAG → IN_V_INIT_IND in EXP_UPD_STRATEGY, EXP_CDC
                    # and EXP_OUTPUT).
                    assert seen[key] == step.step_name, (
                        f"{mapping.name}: rename pair {key} repeated in "
                        f"{seen[key]} and {step.step_name}"
                    )
                else:
                    seen[key] = step.step_name
                checked += 1
    assert checked > 0


def test_dlpk_cache_status_exp_cdc_rename_is_internal_only():
    """EXP_CDC consumes the mapplet input ports directly (IN_V_INIT_IND,
    IN_V_LAST_UPDATE_DATE, IN_SOR_DATE); it must NOT re-apply the external
    forward renames that were already done at the mapplet entry.
    """
    mappings = _load_nhs_mappings()
    phase_asp = next(
        m for m in mappings if m.name == "M_NHS_SSAL2_TRAN_NHS_PHASE_ASP"
    )
    handlers = TransformHandlers(phase_asp, UserConfig())
    plan = handlers.build_ir_plan()
    cdc_steps = [
        step
        for step in _rename_steps(plan)
        if step.step_name == "rename_EXP_CDC"
    ]
    assert cdc_steps, "rename_EXP_CDC steps not found"
    for step in cdc_steps:
        pairs = {tuple(p) for p in step.params["rename_columns"]}
        assert ("INIT_FLAG", "IN_V_INIT_IND") not in pairs, (
            "external forward rename repeated inside rename_EXP_CDC"
        )
        assert ("IN_V_INIT_IND", "INIT_FLAG") in pairs
        assert ("IN_V_LAST_UPDATE_DATE", "LAST_UPDATE_DATE") in pairs
        assert ("IN_SOR_DATE", "SOR_DATE") in pairs


def _df_parent_map(plan):
    """Map each generated df name to its primary lineage input (expression,
    rename, lookup merge and common-columns merge steps)."""
    parent = {}
    for step in plan.steps:
        if step.df_input and step.df_output and step.df_input != step.df_output:
            parent[step.df_output] = step.df_input
    return parent


def _is_descendant(parent, df, ancestor):
    seen = set()
    cur = df
    while cur and cur in parent:
        if cur in seen:
            return False
        seen.add(cur)
        cur = parent[cur]
        if cur == ancestor:
            return True
    return False


def _path_breaks_preservation(parent, step_map, descendant, ancestor):
    """True when the primary lineage from `ancestor` to `descendant` contains
    a rename step or a step type that does not preserve every column name.
    Those are the reasons a parent-child common-columns merge is kept: the
    descendant no longer provably carries a needed column (withColumnRenamed
    is a silent no-op when its source is missing, so removing the merge would
    lose data without an error).
    """
    cur = descendant
    while cur and cur != ancestor:
        st = step_map.get(cur)
        if st is None:
            return True
        if not isinstance(st, (ApplyExpressionStep, ApplyFilterStep, ApplyLookupStep)):
            return True
        if st.params.get("rename_columns"):
            return True
        if isinstance(st, ApplyExpressionStep) and st.params.get("computed_columns"):
            # A computed column can overwrite a source column with a new value
            # (e.g. mapplet EXP_OUTPUT sets LAST_REC_TXN_TYPE_CODE = NULL),
            # so the descendant no longer provably carries the ancestor's
            # value even though the column name survives.
            return True
        cur = parent.get(cur)
    return cur != ancestor


def test_parent_child_common_column_joins_kept_only_when_needed():
    """A common-columns merge may join a df with its own descendant ONLY when
    the descendant no longer provably carries a needed column (a rename or a
    non-column-preserving step lies on the lineage path). Purely redundant
    parent-child merges (e.g. df_EXPTRANS_STS ⋈ df_lkp_merge_11,
    EXP_UPD_STRATEGY ⋈ EXP_CDC) are removed — joining them made the Spark
    analyzer loop with "Max iterations (100) reached for batch Resolution"
    (M_NHS_SSAL2_TRAN_NHS_HOS_APLY, join_..._STS_0).
    """
    mappings = _load_nhs_mappings()
    checked = 0
    kept = 0
    for mapping in mappings:
        handlers = TransformHandlers(mapping, UserConfig())
        plan = handlers.build_ir_plan()
        parent = _df_parent_map(plan)
        step_map = {s.df_output: s for s in plan.steps}
        for step in plan.steps:
            if not (
                isinstance(step, ApplyLookupStep)
                and step.params.get("join_expr") == "__common_cols__"
            ):
                continue
            left = step.df_input
            right = step.params.get("lookup_df")
            checked += 1
            if _is_descendant(parent, right, left):
                descendant, ancestor = right, left
            elif _is_descendant(parent, left, right):
                descendant, ancestor = left, right
            else:
                continue
            kept += 1
            assert _path_breaks_preservation(
                parent, step_map, descendant, ancestor
            ), (
                f"{mapping.name}: kept parent-child merge {step.step_name} "
                f"joins {left} with descendant {right} but the descendant "
                "preserves every column name — the merge is redundant"
            )
    assert checked > 0
    assert kept > 0, "EXP_OPR_IND-style needed merges must be kept"


def test_hos_aply_sts_cache_input_uses_lookup_merge_directly():
    """The failing step in the reported log must no longer exist at all: the
    STS cache-status mapplet input should consume the SSA_STS lookup merge df
    directly, with no redundant join or pass-through step in between."""
    mappings = _load_nhs_mappings()
    mapping = next(
        m for m in mappings if m.name == "M_NHS_SSAL2_TRAN_NHS_HOS_APLY"
    )
    handlers = TransformHandlers(mapping, UserConfig())
    plan = handlers.build_ir_plan()
    by_name = {step.step_name: step for step in plan.steps}
    assert "join_MPLT_DLKP_CACHE_STATUS_STS_0" not in by_name, (
        "redundant STS cache-status input merge step must be removed"
    )
    assert "join_output_MPLT_DLKP_CACHE_STATUS_STS_0" not in by_name, (
        "redundant STS cache-status output merge step must be removed"
    )
    input_step = by_name["input_MPLT_DLKP_CACHE_STATUS_STS"]
    assert isinstance(input_step, ApplyExpressionStep)
    assert input_step.df_input == by_name["apply_DLKP_SSA_STS"].df_output
    output_step = by_name["apply_MPLT_DLKP_CACHE_STATUS_STS"]
    assert isinstance(output_step, ApplyExpressionStep)
    output_join = by_name["join_output_MPLT_DLKP_CACHE_STATUS_STS_1"]
    assert isinstance(output_join, ApplyLookupStep)
    assert output_step.df_input == output_join.df_output


def test_hos_aply_exp_opr_ind_merge_restored():
    """EXP_OPR_IND reads three upstream groups: LAST_REC_TXN_TYPE_CODE from
    FILTRANS_MSTR, NewLookupRow from DLKP_SSA_MSTR and OUT_V_OPR_IND from the
    cache-status mapplet. The mapplet output renames LAST_REC_TXN_TYPE_CODE →
    OUT_V_LAST_REC_TXN_TYPE_CODE, so the original column is only available on
    df_lkp_merge_2 — the merge must be generated, otherwise the expression
    fails with [UNRESOLVED_COLUMN] LAST_REC_TXN_TYPE_CODE.
    """
    mappings = _load_nhs_mappings()
    mapping = next(
        m for m in mappings if m.name == "M_NHS_SSAL2_TRAN_NHS_HOS_APLY"
    )
    handlers = TransformHandlers(mapping, UserConfig())
    plan = handlers.build_ir_plan()
    by_name = {step.step_name: step for step in plan.steps}

    merge_step = by_name["merge_EXP_OPR_IND_0"]
    assert isinstance(merge_step, ApplyLookupStep)
    assert merge_step.params.get("join_expr") == "__common_cols__"
    assert merge_step.df_input == by_name[
        "apply_MPLT_DLKP_CACHE_STATUS_MSTR"
    ].df_output
    assert merge_step.params.get("lookup_df") == by_name[
        "apply_DLKP_SSA_MSTR"
    ].df_output
    assert by_name["apply_EXP_OPR_IND"].df_input == merge_step.df_output


def test_hos_aply_cache_status_output_merge_keeps_rename_sources():
    """The cache-status mapplet OUTPUT is fed by EXP_UPD_STRATEGY, EXP_CDC and
    EXP_OUTPUT. EXP_OUTPUT renames AGMT_IND → IN_AGMT_IND internally, so using
    it alone would make the output rename AGMT_IND → OUT_AGMT_IND a silent
    no-op (data loss). The final stage must therefore merge EXP_CDC with
    EXP_OUTPUT so AGMT_IND (from EXP_CDC) survives for the output renames.
    """
    mappings = _load_nhs_mappings()
    mapping = next(
        m for m in mappings if m.name == "M_NHS_SSAL2_TRAN_NHS_HOS_APLY"
    )
    handlers = TransformHandlers(mapping, UserConfig())
    plan = handlers.build_ir_plan()
    by_name = {step.step_name: step for step in plan.steps}

    for suffix in ("MSTR", "STS"):
        join_name = f"join_output_MPLT_DLKP_CACHE_STATUS_{suffix}_1"
        join_step = by_name[join_name]
        assert isinstance(join_step, ApplyLookupStep)
        assert join_step.params.get("join_expr") == "__common_cols__"
        assert join_step.df_input.endswith("EXP_CDC")
        assert join_step.params.get("lookup_df", "").endswith("EXP_OUTPUT")
        output_step = by_name[f"apply_MPLT_DLKP_CACHE_STATUS_{suffix}"]
        assert output_step.df_input == join_step.df_output


def test_mapplet_dynamic_lookups_compute_unique_newlookuprow():
    """Mapplet-internal Dynamic Lookups (e.g. LKP_DYN_SOR / LKP_DYN_SSA in
    MPLT_AGMT_*) must compute their own NewLookupRow 0/1 hit indicator with a
    per-instance unique column name. Without it, the second lookup's
    NewLookupRow clobbers the first before the renames map them to
    SOR_CACHE_STATUS / SSA_CACHE_STATUS, leaving one NULL and never writing
    the target (or crashing with UNRESOLVED_COLUMN SOR_CACHE_STATUS).
    """
    mappings = _load_nhs_mappings()
    checked = 0
    for mapping in mappings:
        handlers = TransformHandlers(mapping, UserConfig())
        plan = handlers.build_ir_plan()
        used_cols = {}
        for step in plan.steps:
            if not (
                isinstance(step, ApplyLookupStep)
                and step.step_name.startswith("apply_MPLT_")
                and "_LKP_DYN_" in step.step_name
            ):
                continue
            assert step.params.get("new_lookup_row_key"), (
                f"{mapping.name}: {step.step_name} has no NewLookupRow judge key"
            )
            nlr_col = step.params.get("new_lookup_row_col")
            assert nlr_col and nlr_col.startswith("NewLookupRow_"), (
                f"{mapping.name}: {step.step_name} NewLookupRow column "
                f"not unique: {nlr_col}"
            )
            assert nlr_col not in used_cols, (
                f"{mapping.name}: {step.step_name} reuses NewLookupRow "
                f"column {nlr_col} of {used_cols.get(nlr_col)}"
            )
            used_cols[nlr_col] = step.step_name
            checked += 1
    assert checked >= 80, f"expected many mapplet dynamic lookups, got {checked}"


def test_mapplet_sor_ssa_cache_status_renames_have_distinct_sources():
    """MPLT_AGMT_* EXP_OUTPUT rename steps map the SOR lookup's NewLookupRow
    to SOR_CACHE_STATUS and the SSA lookup's NewLookupRow to SSA_CACHE_STATUS.
    Both renames must use distinct source columns — renaming the same
    NewLookupRow twice is a silent no-op for the second (withColumnRenamed
    ignores missing columns) and leaves SSA_CACHE_STATUS NULL.
    """
    mappings = _load_nhs_mappings()
    checked = 0
    for mapping in mappings:
        handlers = TransformHandlers(mapping, UserConfig())
        plan = handlers.build_ir_plan()
        for step in plan.steps:
            if not (
                isinstance(step, ApplyExpressionStep)
                and step.step_name == "rename_EXP_OUTPUT"
            ):
                continue
            pairs = {tuple(p) for p in step.params.get("rename_columns", [])}
            sor = [src for src, tgt in pairs if tgt == "SOR_CACHE_STATUS"]
            ssa = [src for src, tgt in pairs if tgt == "SSA_CACHE_STATUS"]
            if not sor or not ssa:
                continue
            assert sor[0] != ssa[0], (
                f"{mapping.name}: SOR_CACHE_STATUS and SSA_CACHE_STATUS both "
                f"rename from {sor[0]}"
            )
            checked += 1
    assert checked >= 40, f"expected many SOR/SSA rename steps, got {checked}"


def test_lookup_with_mapplet_upstream_merges_extra_branch():
    """DLKP_SOR_STS in M_NHS_SSAL2_TRAN_NHS_EST_BANK_ITEM is fed by the
    EXP_BK / DLKP_SOR_MSTR main chain AND by MPLT_AGMT_NHS_RVN_CLCT_TRML,
    which provides IN_RVN_CLCT_TRML_KEY. The mapplet output must be merged
    into the lookup input before the join, otherwise the generated code fails
    with UNRESOLVED_COLUMN RVN_CLCT_TRML_KEY.
    """
    mappings = _load_nhs_mappings()
    mapping = next(
        m for m in mappings if m.name == "M_NHS_SSAL2_TRAN_NHS_EST_BANK_ITEM"
    )
    handlers = TransformHandlers(mapping, UserConfig())
    plan = handlers.build_ir_plan()
    by_name = {step.step_name: step for step in plan.steps}

    merge_step = by_name["merge_DLKP_SOR_STS_0"]
    assert isinstance(merge_step, ApplyLookupStep)
    assert merge_step.params.get("join_expr") == "__common_cols__"
    assert merge_step.df_input == by_name["apply_DLKP_SOR_MSTR"].df_output
    assert merge_step.params.get("lookup_df") == by_name[
        "apply_MPLT_AGMT_NHS_RVN_CLCT_TRML"
    ].df_output

    lookup_step = by_name["apply_DLKP_SOR_STS"]
    assert isinstance(lookup_step, ApplyLookupStep)
    assert lookup_step.df_input == merge_step.df_output
    assert lookup_step.df_output == by_name["apply_DLKP_SOR_MSTR"].df_output


def test_lookup_input_merges_never_self_reference_chain_output():
    """A lookup-input merge must never join against the lookup's own
    accumulating chain output (e.g. df_lkp_merge_13), which would create a
    self-referential plan and crash codegen/runtime."""
    mappings = _load_nhs_mappings()
    checked = 0
    for mapping in mappings:
        handlers = TransformHandlers(mapping, UserConfig())
        plan = handlers.build_ir_plan()
        by_name = {step.step_name: step for step in plan.steps}
        for step in plan.steps:
            if not (
                isinstance(step, ApplyLookupStep)
                and step.step_name.startswith("merge_")
            ):
                continue
            target = step.step_name[len("merge_"):].rsplit("_", 1)[0]
            apply_step = by_name.get(f"apply_{target}")
            if not isinstance(apply_step, ApplyLookupStep):
                continue
            chain_out = apply_step.df_output
            merge_idx = plan.steps.index(step)
            prior_chain = any(
                s.df_output == chain_out and plan.steps.index(s) < merge_idx
                for s in plan.steps
            )
            for side in (step.df_input, step.params.get("lookup_df")):
                if side != chain_out:
                    continue
                # Chained lookups legitimately re-read the accumulating chain
                # df (produced by an earlier lookup). Referencing the chain
                # output before any step created it is the self-reference bug
                # (the merge would read a variable that only exists after this
                # lookup's own apply step runs).
                assert prior_chain, (
                    f"{mapping.name}: {step.step_name} references its own "
                    f"chain output {chain_out} before any step produced it"
                )
            checked += 1
    assert checked > 0


def test_lookup_chain_base_prefers_mapplet_external_input():
    """When a lookup's chain input is a mapplet output, the chain must be
    based on the mapplet's external input stream (df_EXP_BK) so source
    columns like LAST_REC_TXN_TYPE_CODE survive — the mapplet output sets
    them to NULL, which would silently disable all delete handling
    (DELETE_IND/DEL_FLAG/EXP_OPR_IND never see the source 'D' value).
    """
    mappings = _load_nhs_mappings()
    mapping = next(
        m for m in mappings if m.name == "M_NHS_SSAL2_TRAN_NHS_FLAT_SLCT_STMT"
    )
    handlers = TransformHandlers(mapping, UserConfig())
    plan = handlers.build_ir_plan()
    by_name = {step.step_name: step for step in plan.steps}

    merge_step = by_name["merge_DLKP_SOR_MSTR_0"]
    assert isinstance(merge_step, ApplyLookupStep)
    assert merge_step.params.get("join_expr") == "__common_cols__"
    assert merge_step.df_input == "df_EXP_BK"
    assert merge_step.params.get("lookup_df") == by_name[
        "apply_MPLT_AGMT_NHS_FLAT_SLCT"
    ].df_output

    lookup_step = by_name["apply_DLKP_SOR_MSTR"]
    assert lookup_step.df_input == merge_step.df_output
    filter_step = by_name["apply_FILTRANS_MSTR"]
    assert filter_step.df_input == lookup_step.df_output


def test_decode_null_search_translates_to_is_null():
    """Informatica DECODE(x, NULL, a, b) matches NULL rows; Spark needs
    `IS NULL`, not `= NULL` (which is always NULL/false and silently takes
    the ELSE branch, e.g. END_DATE in EXP_CDC)."""
    from informatica_sparker.expr_translator import ExpressionTranslator

    translator = ExpressionTranslator(mapping_name="test")
    out = translator.translate(
        "DECODE(LAST_UPDATE_DATE, NULL, "
        "ADD_TO_DATE(SNAPSHOT_DATE,'D',-1), LAST_UPDATE_DATE)",
        "column",
        "END_DATE",
    )
    assert "IS NULL" in out
    assert "= NULL" not in out


def test_mapplet_no_empty_rename_steps():
    """Mapplet internal expressions must not emit a rename step when there is
    nothing to rename (e.g. df_MPLT_AGMT_NHS_HOS_APLY_rename_2 was a pure
    alias of its input with no withColumnRenamed calls)."""
    mappings = _load_nhs_mappings()
    for mapping in mappings:
        handlers = TransformHandlers(mapping, UserConfig())
        plan = handlers.build_ir_plan()
        for step in plan.steps:
            if not (
                isinstance(step, ApplyExpressionStep)
                and step.step_name.startswith("rename_")
            ):
                continue
            assert step.params.get("rename_columns"), (
                f"{mapping.name}: empty rename step {step.step_name}"
            )


def test_mapplet_lookup_report_error_policy_converted():
    """Mapplet-internal lookups with 'Lookup policy on multiple match' =
    'Report Error' must generate the duplicate-key check (groupBy join keys,
    raise RuntimeError), same as main-mapping lookups."""
    parser = InfaXMLParser(NHS_TL_XML.read_bytes())
    assert parser.parse()
    mapplets = parser.get_mapplets()
    mappings = _load_nhs_mappings()
    checked = 0
    for mapping in mappings:
        handlers = TransformHandlers(mapping, UserConfig())
        plan = handlers.build_ir_plan()
        for step in plan.steps:
            if not (
                isinstance(step, ApplyLookupStep)
                and step.step_name.startswith("apply_MPLT_")
            ):
                continue
            body = step.step_name[len("apply_"):]
            for mpl_name, mpl_def in mapplets.items():
                if not body.startswith(mpl_name + "_"):
                    continue
                lkp_name = body[len(mpl_name) + 1:]
                if lkp_name not in {i.name for i in mpl_def.get("instances", [])}:
                    continue
                transforms = {t.name: t for t in mpl_def.get("transformations", [])}
                tr = transforms.get(lkp_name)
                if tr is None or getattr(tr, "type", "") != "Lookup Procedure":
                    break
                policy = tr.table_attributes.get("Lookup policy on multiple match", "")
                if policy.upper() != "REPORT ERROR":
                    break
                assert step.params.get("dedup_lookup_error") is True, (
                    f"{mapping.name}: mapplet lookup {lkp_name} lost its "
                    "Report Error duplicate-key check"
                )
                assert step.params.get("dedup_lookup_keys"), (
                    f"{mapping.name}: mapplet lookup {lkp_name} has no "
                    "dedup keys for Report Error"
                )
                checked += 1
                break
    assert checked >= 40, f"expected many mapplet Report Error lookups, got {checked}"


def test_stable_semantic_df_names():
    """Generated df names must be derived from instance+role semantics, not a
    global counter, so adding/removing an unrelated step does not renumber
    every DataFrame. Mapplet-internal names are scoped to the mapplet INSTANCE
    (MSTR vs STS) and therefore unique and stable."""
    mappings = _load_nhs_mappings()
    old_counter_pattern = re.compile(
        r"(?:^df_(?:lkp_)?merge_\d+$|^df_mplt_lkp_chain_\d+$|"
        r"^df_(?:nrm|rank|tc)_\d+$|^df_rtr_.*_\d+$|"
        r"_(?:rename|nullinput)_\d+$|(?<!_merge)_input_\d+$|_merge_\d+$)"
    )
    for mapping in mappings:
        handlers = TransformHandlers(mapping, UserConfig())
        plan = handlers.build_ir_plan()
        for step in plan.steps:
            out = step.df_output or ""
            assert not old_counter_pattern.search(out), (
                f"{mapping.name}: counter-based df name {out} in "
                f"{step.step_name}"
            )

    blt = next(
        m for m in mappings if m.name == "M_NHS_SSAL2_TRAN_NHS_HOS_APLY_BLT"
    )
    handlers = TransformHandlers(blt, UserConfig())
    plan = handlers.build_ir_plan()
    by_name = {step.step_name: step for step in plan.steps}
    assert by_name["input_MPLT_AGMT_NHS_HOS_APLY"].df_output == (
        "df_MPLT_AGMT_NHS_HOS_APLY_input"
    )
    assert by_name[
        "apply_MPLT_AGMT_NHS_HOS_APLY_LKP_DYN_SOR_NHS_HOS_APLY"
    ].df_output == "df_mplt_lkp_chain_MPLT_AGMT_NHS_HOS_APLY_EXP_NULL_BKEY"

    item = next(
        m for m in mappings if m.name == "M_NHS_SSAL2_TRAN_NHS_EST_BANK_ITEM"
    )
    handlers = TransformHandlers(item, UserConfig())
    plan = handlers.build_ir_plan()
    cdc_steps = [
        s for s in plan.steps
        if s.step_name == "apply_MPLT_DLKP_CACHE_STATUS_EXP_CDC"
    ]
    assert len(cdc_steps) == 2
    cdc_outputs = {s.df_output for s in cdc_steps}
    assert len(cdc_outputs) == 2
    assert any("_MSTR_" in o for o in cdc_outputs)
    assert any("_STS_" in o for o in cdc_outputs)


def test_independent_branch_merges_are_preserved():
    """Merges between independent branches must NOT be removed: when the
    already-merged df does not contain another input branch's fields, the
    common-columns merge is required."""
    mappings = _load_nhs_mappings()
    expected = {
        "M_NHS_SSAL2_TRAN_NHS_REF_CODE": "merge_EXP_OPR_IND_0",
        "M_NHS_SSAL2_TRAN_NHS_FLAT_SLCT_SSN_ASGN": "join_MPLT_AGMT_NHS_INTVW_SCHD_0",
    }
    independent_only = {
        "M_NHS_SSAL2_TRAN_NHS_FLAT_SLCT_SSN_ASGN": "join_MPLT_AGMT_NHS_INTVW_SCHD_0",
    }
    for mapping in mappings:
        if mapping.name not in expected:
            continue
        handlers = TransformHandlers(mapping, UserConfig())
        plan = handlers.build_ir_plan()
        step = next(
            (s for s in plan.steps if s.step_name == expected[mapping.name]),
            None,
        )
        assert step is not None, (
            f"{mapping.name}: independent-branch merge "
            f"{expected[mapping.name]} must be preserved"
        )
        assert isinstance(step, ApplyLookupStep)
        assert step.params.get("join_expr") == "__common_cols__"
        if mapping.name not in independent_only:
            continue
        parent = _df_parent_map(plan)
        left = step.df_input
        right = step.params.get("lookup_df")
        assert not (
            _is_descendant(parent, left, right)
            or _is_descendant(parent, right, left)
        ), f"{mapping.name}: {step.step_name} should be independent branches"

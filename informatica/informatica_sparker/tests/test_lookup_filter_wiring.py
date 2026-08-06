"""Regression tests for lookup/filter upstream wiring in NHS TL mappings.

Verifies that a filter fed by a Lookup Procedure consumes that lookup's
chain/merge output (not a stale pre-lookup DataFrame), e.g.:

    FILTRANS_STS  <- DLKP_SOR_STS + EXP_BK   (must see END_DATE / NewLookupRow)
    FILTRANS_MSTR <- DLKP_SOR_MSTR + EXP_BK  (must see MSTR NewLookupRow)

The source of truth is WF_NHS_TL.XML only; EMS TL is intentionally not used.
"""

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


def test_independent_branch_merges_are_preserved():
    """Merges between independent branches must NOT be removed: when the
    already-merged df does not contain another input branch's fields, the
    common-columns merge is required."""
    mappings = _load_nhs_mappings()
    expected = {
        "M_NHS_SSAL2_TRAN_NHS_REF_CODE": "merge_EXP_OPR_IND_0",
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
        parent = _df_parent_map(plan)
        left = step.df_input
        right = step.params.get("lookup_df")
        assert not (
            _is_descendant(parent, left, right)
            or _is_descendant(parent, right, left)
        ), f"{mapping.name}: {step.step_name} should be independent branches"

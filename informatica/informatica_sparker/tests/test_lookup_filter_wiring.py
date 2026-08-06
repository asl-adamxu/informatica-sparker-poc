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

"""Regression tests for deferred-instance input resolution.

EMS TL mappings can contain a SOURCE and TARGET with the same name, which
makes the graph fall back to XML instance order.  In that fallback a Mapplet /
Router / Expression can be visited before its upstream has been generated.
The converter now defers those instances and, while processing them, prefers
each upstream's direct output over the lookup chain/merge name that a later
lookup registered over it.
"""

from informatica_sparker.handlers import TransformHandlers
from informatica_sparker.models import Connector, Instance, MappingDefinition, UserConfig


def _handler():
    mapping = MappingDefinition(
        name="m_deferred",
        instances=[
            Instance(name="SQ", type="TRANSFORMATION",
                     transformation_type="Source Qualifier"),
            Instance(name="EXP", type="TRANSFORMATION",
                     transformation_type="Expression"),
            Instance(name="LKP", type="TRANSFORMATION",
                     transformation_type="Lookup Procedure"),
            Instance(name="RTR", type="TRANSFORMATION",
                     transformation_type="Router"),
        ],
        connectors=[
            Connector(from_instance="SQ", from_field="K",
                      to_instance="EXP", to_field="K"),
            Connector(from_instance="EXP", from_field="K",
                      to_instance="LKP", to_field="IN_K"),
            Connector(from_instance="EXP", from_field="K",
                      to_instance="RTR", to_field="K"),
            Connector(from_instance="LKP", from_field="NEW_FLAG",
                      to_instance="RTR", to_field="NEW_FLAG"),
        ],
    )
    return TransformHandlers(mapping, UserConfig())


def test_deferred_expression_prefers_direct_upstream_after_lookup_chain_walk():
    h = _handler()
    h.current_df_map.update({
        "SQ": "df_lkp_merge_LKP",
        "EXP": "df_lkp_merge_LKP",
        "LKP": "df_lkp_merge_LKP",
    })
    h._direct_df_map.update({
        "SQ": "df_SQ",
        "EXP": "df_EXP",
        "LKP": "df_lkp_merge_LKP",
    })
    h._lookup_order.append("LKP")

    # Normal (non-deferred) resolution keeps the chain df for downstream nodes.
    h._prefer_direct_input = False
    assert h._get_input_df("EXP") == "df_lkp_merge_LKP"
    assert h._get_input_df("RTR") == "df_lkp_merge_LKP"

    # Deferred processing must use the SQ's own output for EXP, otherwise the
    # generated code creates a circular dependency (EXP built from a merge that
    # itself needs EXP).
    h._prefer_direct_input = True
    assert h._get_input_df("EXP") == "df_SQ"
    assert h._get_all_input_dfs("EXP") == ["df_SQ"]

    # A lookup-fed Router must still see the lookup chain, even when deferred.
    assert h._get_input_df("RTR") == "df_lkp_merge_LKP"


def test_router_waits_until_all_upstreams_are_available():
    h = _handler()
    h.current_df_map.update({"EXP": "df_EXP"})
    h._direct_df_map.update({"EXP": "df_EXP"})

    # LKP has not been processed yet, so the Router must stay deferred even
    # though EXPTRANS2 is already available.
    assert h._all_upstreams_available("RTR") is False

    h.current_df_map["LKP"] = "df_lkp_merge_LKP"
    h._direct_df_map["LKP"] = "df_lkp_merge_LKP"
    assert h._all_upstreams_available("RTR") is True

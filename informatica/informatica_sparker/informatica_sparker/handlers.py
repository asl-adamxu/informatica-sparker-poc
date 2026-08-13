import re
import networkx as nx
from typing import Dict, List, Optional, Any, Set, Tuple
from .models import (
    MappingDefinition, Transformation, Instance, Connector,
    SourceDefinition, TargetDefinition, UserConfig, SourceConfig, TargetConfig,
    normalize_db_type
)
from .ir import (
    IRPlan, IRStep, ReadSQLStep, ReadFileStep, ApplySourceQualifierStep,
    ApplyFilterStep, ApplyExpressionStep, ApplyLookupStep, ApplyJoinerStep,
    ApplyAggregatorStep, ApplySorterStep, ApplyUnionStep, ApplyRouterStep,
    ApplyUpdateStrategyStep, WriteTargetStep, MergeDeltaStep, ExecuteSQLStep,
    ApplySequenceStep, ApplyNormalizerStep, ApplyRankStep,
    ApplyTransactionControlStep, ComputedColumn
)
from .expr_translator import ExpressionTranslator, sanitize_for_expr
from .graph_builder import GraphBuilder
from .logger import ConversionLogger, LogLevel, LogStage

# Informatica Filter conditions may be a bare numeric port (non-zero = TRUE,
# zero = FALSE). Spark 3.5 filter() requires a boolean, so bare numeric
# conditions must become explicit comparisons (FILTER_NOT_BOOLEAN fix).
_NUMERIC_DATATYPES = {
    "integer", "smallint", "bigint", "decimal", "number", "float",
    "double", "real", "numeric", "money",
}
_BARE_IDENTIFIER_RE = re.compile(r'^[A-Za-z_$][A-Za-z0-9_$]*$')


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
            _sp_full = ""
            if _sp_trans and _sp_trans.table_attributes:
                _sp_full = _sp_trans.table_attributes.get("Stored Procedure Name", "") or ""
            if not _sp_full:
                # Fallback: reusable / mapplet-local Stored Procedure components
                # are not in transform_map — call the procedure name extracted
                # from the :SP. expression without a schema prefix (matches the
                # old template's sp_call_text fallback).
                _sp_call, _sp_schema = _sp_match.group(1), ""
            else:
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
        "rename_columns": [tuple(_p) for _p in (step.params.get("rename_columns") or [])],
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
                # Values are DEFAULT VALUES (e.g. '202606'), not names; the
                # template renders the value unquoted as the runtime variable
                # identifier (loaded override-aware from UTL_JOB_PARAM), so
                # derive it from the KEY ($$v_x → v_x).
                _subs[_var] = _var.replace('$', '')
    if _subs:
        _lib_cfg["substitutions"] = _subs
    if inline_lkp_info:
        _lib_cfg["inline_lookup_joins"] = list(inline_lkp_info.values())
    if _sp_calls:
        _lib_cfg["sp_calls"] = _sp_calls
    return _lib_cfg


class TransformHandlers:

    def __init__(self, mapping: MappingDefinition, user_config: UserConfig,
                 logger: Optional[ConversionLogger] = None,
                 session_file_sources: Optional[Dict[str, Any]] = None):
        self.mapping = mapping
        self.user_config = user_config
        self.logger = logger or ConversionLogger()
        self.logger.set_current_mapping(mapping.name)
        self.session_file_sources = session_file_sources or {}
        self.expr_translator = ExpressionTranslator(mapping_name=mapping.name, logger=self.logger)
        self._plan = None  # built in build_ir_plan
        self.transform_map: Dict[str, Transformation] = {}
        self.instance_map: Dict[str, Instance] = {}
        self.source_map: Dict[str, SourceDefinition] = {}
        self.target_map: Dict[str, TargetDefinition] = {}
        self.df_counter = 0
        self.current_df_map: Dict[str, str] = {}
        # Tracks chain-join DataFrames: maps upstream df name → chain df name.
        # Lookups sharing the same upstream chain-join onto one accumulating df.
        self._chain_df_map: Dict[str, str] = {}
        # Tracks the most recent chain merge output so subsequent lookups chain
        # sequentially instead of branching in parallel (prevents column loss
        # when a downstream lookup starts from a stale intermediate result).
        self._last_chain_output: Optional[str] = None
        # Order in which lookup instances registered their chain/merge output.
        # Used by _get_input_df to prefer the correct lookup when a component
        # is fed by one or more lookup procedures.
        self._lookup_order: List[str] = []
        # Connected sequence generators (no upstream) attach their NEXTVAL
        # column to the downstream consumer's step. Maps consumer instance
        # name → list of {"col": ..., "start": ...} attachments.
        self._sequence_attachments: Dict[str, list] = {}
        # Union step output df names — targets fed directly by a union get the
        # NullType→StringType cast (unionByName allowMissingColumns can leave
        # missing-side / lit(None)-filled columns as NullType, which JDBC
        # cannot map to Oracle: "Can't get JDBC type for void").
        self._union_output_dfs: set = set()
        # Mapplet external input/output df names, used to keep the source row
        # stream as the base of downstream lookup chains (mapplet outputs may
        # overwrite source columns such as LAST_REC_TXN_TYPE_CODE with NULL).
        self._mapplet_input_df: Dict[str, str] = {}
        self._mapplet_output_df: Dict[str, str] = {}
        # Direct (non-chain) DataFrame registered for each processed instance.
        # Lookup chain-walk may overwrite current_df_map for upstream instances
        # so downstream components see the accumulated lookup merge; this map
        # preserves each instance's own output for deferred processing.
        self._direct_df_map: Dict[str, str] = {}
        # While processing a deferred instance, prefer its direct upstream
        # outputs instead of a chain/merge df that a lookup may have registered
        # under the upstream name. Lookup-fed filters/routers still get the
        # lookup chain via the lookup preference above.
        self._prefer_direct_input = False

        self._build_maps()

    def _build_maps(self):
        for transform in self.mapping.transformations:
            self.transform_map[transform.name] = transform

        for instance in self.mapping.instances:
            self.instance_map[instance.name] = instance

        for source in self.mapping.sources:
            self.source_map[source.name] = source

        for target in self.mapping.targets:
            self.target_map[target.name] = target

    def _resolve_transformation_type(self, instance: Instance) -> str:
        inst_type = instance.type
        trans_type = instance.transformation_type
        trans_name = instance.transformation_name or instance.name

        if inst_type and inst_type.upper() == "TRANSFORMATION":
            if trans_type:
                return trans_type
            transform = self.transform_map.get(trans_name)
            if transform and transform.type:
                return transform.type
            return self._infer_type_from_name(trans_name)

        if inst_type and inst_type.upper() == "MAPPLET":
            return "MAPPLET"

        if trans_type and trans_type not in ("", "TRANSFORMATION"):
            return trans_type

        if inst_type and inst_type not in ("", "TRANSFORMATION"):
            return inst_type

        return self._infer_type_from_name(trans_name)

    def _infer_type_from_name(self, name: str) -> str:
        name_upper = name.upper()

        prefix_mapping = {
            "SQ_": "Source Qualifier",
            "ASQ_": "Source Qualifier",
            "EXPTRANS": "Expression",
            "EXP_": "Expression",
            "FILTRANS": "Filter",
            "FIL_": "Filter",
            "AGGTRANS": "Aggregator",
            "AGG_": "Aggregator",
            "LKP_": "Lookup Procedure",
            "LKPTRANS": "Lookup Procedure",
            "JNRTRANS": "Joiner",
            "JNR_": "Joiner",
            "SRTTRANS": "Sorter",
            "SRT_": "Sorter",
            "SORTRANS": "Sorter",
            "RTRTRANS": "Router",
            "RTR_": "Router",
            "UNTRANS": "Union",
            "UN_": "Union",
            "SEQTRANS": "Sequence Generator",
            "SEQ_": "Sequence Generator",
            "UPDTRANS": "Update Strategy",
            "UPD_": "Update Strategy",
            "MPLT_": "MAPPLET",
            "NRM_": "Normalizer",
            "NRMTRANS": "Normalizer",
            "RANK": "Rank",
            "RKT": "Rank",
            "TCRTRANS": "Transaction Control",
            "TCTRANS": "Transaction Control",
        }

        suffix_mapping = {
            "_XFORM": "Expression",
            "_EXP": "Expression",
            "_FIL": "Filter",
            "_AGG": "Aggregator",
            "_LKP": "Lookup Procedure",
            "_JNR": "Joiner",
            "_SRT": "Sorter",
            "_RTR": "Router",
            "_UPD": "Update Strategy",
            "_NRM": "Normalizer",
            "_RANK": "Rank",
            "_TCR": "Transaction Control",
        }

        for prefix, trans_type in prefix_mapping.items():
            if name_upper.startswith(prefix):
                return trans_type

        for suffix, trans_type in suffix_mapping.items():
            if name_upper.endswith(suffix):
                return trans_type

        if "LOOKUP" in name_upper or "LKP" in name_upper:
            return "Lookup Procedure"
        if "EXPRESSION" in name_upper or "XFORM" in name_upper:
            return "Expression"
        if "FILTER" in name_upper:
            return "Filter"
        if "AGGREGAT" in name_upper:
            return "Aggregator"
        if "JOINER" in name_upper:
            return "Joiner"
        if "SORT" in name_upper:
            return "Sorter"
        if "ROUTER" in name_upper:
            return "Router"
        if "UNION" in name_upper:
            return "Union"
        if "SEQUENCE" in name_upper:
            return "Sequence Generator"
        if "UPDATE" in name_upper:
            return "Update Strategy"
        if "NORMALIZ" in name_upper:
            return "Normalizer"
        if "RANK" in name_upper:
            return "Rank"
        if "TRANSACTION" in name_upper:
            return "Transaction Control"

        return "UNKNOWN"

    def _get_df_name(self, prefix: str = "df", instance: Optional[Instance] = None) -> str:
        if instance and instance.name:
            _safe = re.sub(r'[^a-zA-Z0-9_]', '_', instance.name)
            _name = f"df_{_safe}"
            # Avoid collisions: if name already used, append counter
            if _name in self.current_df_map.values():
                self.df_counter += 1
                return f"{_name}_{self.df_counter}"
            return _name
        self.df_counter += 1
        return f"{prefix}_{self.df_counter}"

    @staticmethod
    def _df_name(*parts: str) -> str:
        """Build a stable, readable df variable name from semantic parts
        instead of a global counter (e.g. _df_name("merge", "DLKP_SOR_STS", 0)
        -> df_merge_DLKP_SOR_STS_0). Adding/removing an unrelated step no
        longer renumbers every downstream DataFrame."""
        _clean = []
        for _p in parts:
            _s = str(_p)
            if _s.startswith("df_"):
                _s = _s[3:]
            _s = re.sub(r'[^a-zA-Z0-9_]', '_', _s)
            if _s:
                _clean.append(_s)
        return "df_" + "_".join(_clean)

    def _register_df(self, instance: Instance, df_name: str):
        self.current_df_map[instance.name] = df_name
        self._direct_df_map[instance.name] = df_name
        if instance.transformation_name and instance.transformation_name != instance.name:
            self.current_df_map[instance.transformation_name] = df_name
            self._direct_df_map[instance.transformation_name] = df_name

    def _get_source_config(self, source_name: str) -> Optional[SourceConfig]:
        for config in self.user_config.sources:
            if config.source_name == source_name:
                return config
        return None

    def _get_target_config(self, target_name: str) -> Optional[TargetConfig]:
        for config in self.user_config.targets:
            if config.target_name == target_name:
                return config
        return None

    def _find_lookup_connection(self, lookup_name: str) -> str:
        # Check user config first
        for config in self.user_config.sources:
            if config.source_name == lookup_name:
                return config.connection_alias or "lookup_conn"

        lookup_conn = getattr(self.user_config, 'lookup_connection', None)
        if lookup_conn:
            return lookup_conn

        for config in self.user_config.sources:
            if config.connection_alias:
                return config.connection_alias

        # Fall back to source definition's db_name from XML
        for src in self.mapping.sources:
            if src.name == lookup_name or lookup_name.startswith(src.name):
                return src.db_name or src.name

        # Try to find any source that might contain this lookup table
        for src in self.mapping.sources:
            if src.db_name:
                return src.db_name

        return "source"

    @staticmethod
    def _normalize_var_case(text: str, plan: IRPlan) -> str:
        """Normalize $$ mapping variable references in text to match declared case.

        SQL queries and filter expressions may reference $$ variables in any case
        (e.g. $$V_SNSH_DATE vs $$v_snsh_date), but the code generator uses the declared
        variable name's case for .replace(). This function case-insensitively normalizes
        all $$ references to the declared case so .replace() always matches.

        NOTE: does NOT add TO_DATE quotes — use _normalize_sql_text for SQL texts.
        Expression texts go through expr_translator._replace_pm_variables which
        already wraps $$ variables in quotes; adding quotes here would double them.
        """
        if not text or not plan or not plan.mapping_variables:
            return text
        for var_name in plan.mapping_variables:
            if not isinstance(var_name, str) or not var_name.startswith('$$'):
                continue
            text = re.sub(re.escape(var_name), var_name, text, flags=re.IGNORECASE)
        return text

    @staticmethod
    def _normalize_sql_text(text: str, plan: IRPlan) -> str:
        """Normalize $$ variables in SQL pushdown / filter texts and quote TO_DATE args.

        Same case normalization as _normalize_var_case, plus wraps unquoted $$ variables
        inside TO_DATE() with single quotes (TO_DATE($$v_rpt_mth, 'YYYYMM') →
        TO_DATE('$$v_rpt_mth', 'YYYYMM')). Bare $$ variables become bare numeric values
        after template .replace(), causing ORA-00936 in Oracle.

        Use ONLY for texts that keep $$ markers until template .replace() — SQL
        pushdown queries, lookup SQL, and filter conditions (translate_for_filter
        strips $$ before translation). Expression texts must use _normalize_var_case.
        """
        text = TransformHandlers._normalize_var_case(text, plan)
        if not text:
            return text
        # Wrap unquoted $$ variables inside TO_DATE() with quotes
        if 'TO_DATE' in text.upper():
            text = re.sub(
                r'(TO_DATE\s*\(\s*)\$\$(\w+)(\s*,)',
                r"\1'$$\2'\3",
                text,
                flags=re.IGNORECASE
            )
        return text

    def _process_deferred_instance(self, instance: Instance, plan: IRPlan) -> bool:
        """Process one instance whose input was not ready in the main pass.

        While a deferred instance runs, upstream resolution prefers each
        upstream's direct output (not a lookup chain df that may have been
        registered under that name). Lookup-fed filters/routers still receive
        the lookup chain through the lookup preference in `_get_input_df`.
        Returns True when the instance was successfully processed.
        """
        _d_type = self._resolve_transformation_type(instance)
        self._prefer_direct_input = True
        try:
            if not self._all_upstreams_available(instance.name):
                return False
            if _d_type == "Joiner":
                _all_inputs = self._get_all_input_dfs(instance.name)
                _expected = len(set(
                    c.from_instance for c in self.mapping.connectors
                    if c.to_instance == instance.name))
                if len(_all_inputs) < min(_expected, 4) or len(_all_inputs) < 2:
                    return False
            elif not self._get_input_df(instance.name):
                return False

            if _d_type in ("TARGET", "Target Definition"):
                self.logger.log_transformation(instance.name, "Target",
                    "Processing target (deferred)", LogLevel.INFO)
                _steps = self._handle_target(instance, plan)
                for _s in _steps:
                    plan.add_step(_s)
                return bool(_steps)

            if _d_type == "Expression":
                self.logger.log_transformation(instance.name, "Expression",
                    "Processing expression (deferred)", LogLevel.INFO)
                _result = self._handle_expression(instance, plan)
                if isinstance(_result, list):
                    for _s in _result:
                        if _s:
                            plan.add_step(_s)
                elif _result:
                    plan.add_step(_result)
                return bool(_result)

            if _d_type == "Filter":
                self.logger.log_transformation(instance.name, "Filter",
                    "Processing filter (deferred)", LogLevel.INFO)
                _step = self._handle_filter(instance, plan)
                if _step:
                    plan.add_step(_step)
                return bool(_step)

            if _d_type in ("Union", "Custom Transformation"):
                self.logger.log_transformation(instance.name, "Union",
                    "Processing union (deferred)", LogLevel.INFO)
                _result = self._handle_union(instance, plan)
                if _result:
                    plan.add_step(_result)
                return bool(_result)

            if _d_type == "Joiner":
                self.logger.log_transformation(instance.name, "Joiner",
                    "Processing joiner (deferred)", LogLevel.INFO)
                _step = self._handle_joiner(instance, plan)
                if _step:
                    plan.add_step(_step)
                return bool(_step)

            if _d_type == "Lookup Procedure":
                self.logger.log_transformation(instance.name, "Lookup",
                    "Processing lookup (deferred)", LogLevel.INFO)
                _steps = self._handle_lookup(instance, plan)
                for _s in _steps:
                    plan.add_step(_s)
                return bool(_steps)

            if _d_type == "Aggregator":
                self.logger.log_transformation(instance.name, "Aggregator",
                    "Processing aggregator (deferred)", LogLevel.INFO)
                _result = self._handle_aggregator(instance, plan)
                if isinstance(_result, list):
                    for _s in _result:
                        if _s:
                            plan.add_step(_s)
                elif _result:
                    plan.add_step(_result)
                return bool(_result)

            if _d_type == "Sorter":
                self.logger.log_transformation(instance.name, "Sorter",
                    "Processing sorter (deferred)", LogLevel.INFO)
                _step = self._handle_sorter(instance, plan)
                if _step:
                    plan.add_step(_step)
                return bool(_step)

            if _d_type == "Normalizer":
                self.logger.log_transformation(instance.name, "Normalizer",
                    "Processing normalizer (deferred)", LogLevel.INFO)
                _step = self._handle_normalizer(instance, plan)
                if _step:
                    plan.add_step(_step)
                return bool(_step)

            if _d_type == "Rank":
                self.logger.log_transformation(instance.name, "Rank",
                    "Processing rank (deferred)", LogLevel.INFO)
                _step = self._handle_rank(instance, plan)
                if _step:
                    plan.add_step(_step)
                return bool(_step)

            if _d_type == "Router":
                self.logger.log_transformation(instance.name, "Router",
                    "Processing router (deferred)", LogLevel.INFO)
                _steps = self._handle_router(instance, plan)
                for _s in _steps:
                    plan.add_step(_s)
                return bool(_steps)

            if _d_type == "Update Strategy":
                self.logger.log_transformation(instance.name, "UpdateStrategy",
                    "Processing update strategy (deferred)", LogLevel.INFO)
                _steps = self._handle_update_strategy(instance, plan)
                for _s in _steps:
                    plan.add_step(_s)
                return bool(_steps)

            if _d_type == "Transaction Control":
                self.logger.log_transformation(instance.name, "TransactionControl",
                    "Processing transaction control (deferred)", LogLevel.INFO)
                _step = self._handle_transaction_control(instance, plan)
                if _step:
                    plan.add_step(_step)
                return bool(_step)

            if _d_type == "MAPPLET":
                self.logger.log_mapplet(instance.name,
                    "Processing mapplet (deferred)", LogLevel.INFO)
                _steps = self._handle_mapplet(instance, plan)
                for _s in _steps:
                    plan.add_step(_s)
                return bool(_steps)

            return False
        finally:
            self._prefer_direct_input = False

    def build_ir_plan(self) -> IRPlan:
        # Collect mapping variables from XML definition
        mvars = {}
        for v in getattr(self.mapping, 'mapping_variables', []):
            name = v.name if hasattr(v, 'name') else v
            mvars[name] = v.default_value if hasattr(v, 'default_value') else ""
        plan = IRPlan(
            mapping_name=self.mapping.name,
            mapping_variables=mvars
        )
        # Feed mapping variables to the expression translator so $$ placeholders
        # are resolved in aggregation and other non-template expression contexts.
        self.expr_translator.pm_variables = mvars
        self._plan = plan

        self.logger.log(LogStage.GRAPH, "Mapping", self.mapping.name, "Building transformation graph", LogLevel.INFO)

        graph_builder = GraphBuilder(self.mapping)
        graph_builder.build()
        ordered_instances = graph_builder.get_topological_order()

        self.logger.log(LogStage.GRAPH, "Mapping", self.mapping.name, f"Graph built with {len(ordered_instances)} nodes", LogLevel.SUCCESS)

        # Deferred instances buffer — SSAL1 extract mappings have SOURCE+TARGET
        # sharing a name, creating a graph cycle that breaks topological sort.
        # The fallback order is wrong, so instances that can't find their input
        # are deferred and processed at the end when all upstream DFs are ready.
        _deferred_insts: List[Instance] = []
        _processed_inst_names: Set[str] = set()

        for inst_name in ordered_instances:
            instance = self.instance_map.get(inst_name)
            if not instance:
                self.logger.log(LogStage.TRANSFORM, "Instance", inst_name, "Instance not found in map", LogLevel.WARNING)
                continue

            inst_type = self._resolve_transformation_type(instance)

            # Defer if the upstream DataFrame isn't available yet (handles both
            # duplicate-name collision and wrong-fallback-order cases).
            _has_upstream = any(
                c.to_instance == instance.name for c in self.mapping.connectors)
            if inst_type in ("TARGET", "Target Definition", "Filter", "Expression",
                             "Union", "Custom Transformation", "MAPPLET", "Router",
                             "Aggregator", "Sorter", "Update Strategy", "Normalizer",
                             "Rank", "Transaction Control", "Lookup Procedure"):
                if _has_upstream:
                    if not self._all_upstreams_available(instance.name):
                        _deferred_insts.append(instance)
                        continue
            elif inst_type in ("Joiner",):
                # Joiner needs ALL upstream DFs to be available, not just one
                _all_inputs = self._get_all_input_dfs(instance.name)
                _expected = len(set(
                    c.from_instance for c in self.mapping.connectors
                    if c.to_instance == instance.name))
                if (not self._all_upstreams_available(instance.name)
                        or len(_all_inputs) < min(_expected, 4)
                        or len(_all_inputs) < 2):
                    _deferred_insts.append(instance)
                    continue

            # Track this instance as processed so Phase 3 (post-cycle-resolution)
            # does not re-process it and create duplicate steps.
            _processed_inst_names.add(inst_name)

            if inst_type in ("SOURCE", "Source Definition"):
                # Skip source read when ALL downstream SQs use SQL pushdown
                # (reads DB directly). If ANY SQ has no pushdown, the Source
                # must be kept for that SQ (e.g. SQ_SP_DELETE sharing a Source
                # with a pushdown SQ like SQ_DPA_FACT_EMS_EST_PLT).
                _sq_pushdown = False
                _all_pushdown = True
                for _conn in self.mapping.connectors:
                    if _conn.from_instance == inst_name:
                        _to_inst = self.instance_map.get(_conn.to_instance)
                        if _to_inst:
                            _to_type = self._resolve_transformation_type(_to_inst)
                            if _to_type == "Source Qualifier":
                                _sq_trans = self.transform_map.get(
                                    _to_inst.transformation_name or _to_inst.name
                                )
                                if _sq_trans:
                                    _udj = _sq_trans.table_attributes.get("User Defined Join", "")
                                    _sqo = _sq_trans.table_attributes.get("Sql Query", "")
                                    if _udj.strip() or _sqo.strip():
                                        _sq_pushdown = True
                                    else:
                                        _all_pushdown = False
                                        break
                if _sq_pushdown and _all_pushdown:
                    self.logger.log_transformation(inst_name, "Source",
                        "Skipped (SQ uses SQL pushdown — read handled by SQ)",
                        LogLevel.INFO)
                else:
                    self.logger.log_transformation(inst_name, "Source", "Processing source definition", LogLevel.INFO)
                    step = self._handle_source(instance, plan)
                    if step:
                        plan.add_step(step)
                        self.logger.log_transformation(inst_name, "Source", "Source converted", LogLevel.SUCCESS)

            elif inst_type in ("Source Qualifier", "Application Source Qualifier"):
                is_app = "Application" in inst_type
                self.logger.log_transformation(inst_name, "SourceQualifier",
                    f"{'Application ' if is_app else ''}Source qualifier", LogLevel.INFO)
                step = self._handle_source_qualifier(instance, plan)
                if step:
                    if is_app:
                        step.comments.append(
                            "Application Source Qualifier — source-specific connection parameters "
                            "may be needed in config.yml"
                        )
                    plan.add_step(step)
                    self.logger.log_transformation(inst_name, "SourceQualifier",
                        f"{'Application ' if is_app else ''}Source qualifier converted", LogLevel.SUCCESS)

            elif inst_type == "Filter":
                self.logger.log_transformation(inst_name, "Filter", "Processing filter transformation", LogLevel.INFO)
                step = self._handle_filter(instance, plan)
                if step:
                    plan.add_step(step)
                    self.logger.log_transformation(inst_name, "Filter", "Filter converted", LogLevel.SUCCESS)

            elif inst_type == "Expression":
                self.logger.log_transformation(inst_name, "Expression", "Processing expression transformation", LogLevel.INFO)
                result = self._handle_expression(instance, plan)
                if isinstance(result, list):
                    for s in result:
                        if s:
                            plan.add_step(s)
                elif result:
                    plan.add_step(result)
                if result:
                    self.logger.log_transformation(inst_name, "Expression", "Expression converted", LogLevel.SUCCESS)

            elif inst_type == "Lookup Procedure":
                self.logger.log_transformation(inst_name, "Lookup", "Processing lookup transformation", LogLevel.INFO)
                steps = self._handle_lookup(instance, plan)
                for step in steps:
                    plan.add_step(step)
                self.logger.log_transformation(inst_name, "Lookup", f"Lookup converted ({len(steps)} steps)", LogLevel.SUCCESS)

            elif inst_type == "Joiner":
                self.logger.log_transformation(inst_name, "Joiner", "Processing joiner transformation", LogLevel.INFO)
                step = self._handle_joiner(instance, plan)
                if step:
                    plan.add_step(step)
                    self.logger.log_transformation(inst_name, "Joiner", "Joiner converted", LogLevel.SUCCESS)

            elif inst_type == "Aggregator":
                self.logger.log_transformation(inst_name, "Aggregator", "Processing aggregator transformation", LogLevel.INFO)
                result = self._handle_aggregator(instance, plan)
                if isinstance(result, list):
                    for s in result:
                        if s:
                            plan.add_step(s)
                elif result:
                    plan.add_step(result)
                if result:
                    self.logger.log_transformation(inst_name, "Aggregator", "Aggregator converted", LogLevel.SUCCESS)

            elif inst_type == "Sorter":
                self.logger.log_transformation(inst_name, "Sorter", "Processing sorter transformation", LogLevel.INFO)
                step = self._handle_sorter(instance, plan)
                if step:
                    plan.add_step(step)
                    self.logger.log_transformation(inst_name, "Sorter", "Sorter converted", LogLevel.SUCCESS)

            elif inst_type == "Normalizer":
                self.logger.log_transformation(inst_name, "Normalizer", "Processing normalizer transformation", LogLevel.INFO)
                step = self._handle_normalizer(instance, plan)
                if step:
                    plan.add_step(step)
                    self.logger.log_transformation(inst_name, "Normalizer", "Normalizer converted", LogLevel.SUCCESS)

            elif inst_type == "Rank":
                self.logger.log_transformation(inst_name, "Rank", "Processing rank transformation", LogLevel.INFO)
                step = self._handle_rank(instance, plan)
                if step:
                    plan.add_step(step)
                    self.logger.log_transformation(inst_name, "Rank", "Rank converted", LogLevel.SUCCESS)

            elif inst_type in ("Union", "Custom Transformation"):
                self.logger.log_transformation(inst_name, "Union", "Processing union transformation", LogLevel.INFO)
                step = self._handle_union(instance, plan)
                if step:
                    plan.add_step(step)
                    self.logger.log_transformation(inst_name, "Union", "Union converted", LogLevel.SUCCESS)

            elif inst_type == "Router":
                self.logger.log_transformation(inst_name, "Router", "Processing router transformation", LogLevel.INFO)
                steps = self._handle_router(instance, plan)
                for step in steps:
                    plan.add_step(step)
                self.logger.log_transformation(inst_name, "Router", f"Router converted ({len(steps)} steps)", LogLevel.SUCCESS)

            elif inst_type in ("Sequence Generator", "Sequence"):
                self.logger.log_transformation(inst_name, "Sequence", "Processing sequence generator", LogLevel.INFO)
                step = self._handle_sequence(instance, plan)
                if step:
                    plan.add_step(step)
                    self.logger.log_transformation(inst_name, "Sequence", "Sequence generator converted", LogLevel.SUCCESS)

            elif inst_type == "Update Strategy":
                self.logger.log_transformation(inst_name, "UpdateStrategy", "Processing update strategy", LogLevel.INFO)
                steps = self._handle_update_strategy(instance, plan)
                for step in steps:
                    plan.add_step(step)
                    self.logger.log_transformation(inst_name, "UpdateStrategy", "Update strategy converted", LogLevel.SUCCESS)

            elif inst_type in ("TARGET", "Target Definition"):
                self.logger.log_transformation(inst_name, "Target", "Processing target definition", LogLevel.INFO)
                steps = self._handle_target(instance, plan)
                for step in steps:
                    plan.add_step(step)
                if steps:
                    self.logger.log_transformation(inst_name, "Target", f"Target converted ({len(steps)} steps)", LogLevel.SUCCESS)

            elif inst_type == "MAPPLET":
                self.logger.log_mapplet(inst_name, "Processing mapplet", LogLevel.INFO)
                steps = self._handle_mapplet(instance, plan)
                for step in steps:
                    plan.add_step(step)
                if steps:
                    self.logger.log_mapplet(inst_name, f"Mapplet converted ({len(steps)} steps)", LogLevel.SUCCESS)

            elif inst_type == "Stored Procedure":
                # Stored procedures are handled via `:SP.xxx()` references in expression
                # transforms; the instance itself is a no-op in the data flow.
                self.logger.log_transformation(inst_name, "StoredProcedure",
                    "Handled via expression reference", LogLevel.INFO)

            elif inst_type == "Transaction Control":
                self.logger.log_transformation(inst_name, "TransactionControl",
                    "Processing transaction control", LogLevel.INFO)
                step = self._handle_transaction_control(instance, plan)
                if step:
                    plan.add_step(step)
                    self.logger.log_transformation(inst_name, "TransactionControl",
                        "Transaction control converted (no-op in PySpark)", LogLevel.SUCCESS)

            elif inst_type == "UNKNOWN":
                transform = self.transform_map.get(instance.transformation_name or instance.name)
                if transform:
                    inferred = self._infer_type_from_transform(transform)
                    if inferred != "UNKNOWN":
                        self.logger.log_transformation(inst_name, "Inferred", f"Inferred type '{inferred}'", LogLevel.INFO)
                        plan.add_warning(f"Inferred type '{inferred}' for {inst_name}")
                        self._handle_with_type(instance, inferred, plan)
                    else:
                        self.logger.log_transformation(inst_name, "Unknown", f"Unknown transformation type: {inst_type}", LogLevel.WARNING)
                        plan.add_warning(f"Unknown transformation type: {inst_type} ({inst_name})")
                else:
                    self.logger.log_transformation(inst_name, "Unknown", f"Unknown transformation type: {inst_type}", LogLevel.WARNING)
                    plan.add_warning(f"Unknown transformation type: {inst_type} ({inst_name})")

            else:
                self.logger.log_transformation(inst_name, inst_type, f"Unsupported transformation type: {inst_type}", LogLevel.WARNING)
                plan.add_warning(f"Unsupported transformation type: {inst_type} ({inst_name})")

        # Process deferred instances in dependency order. Because the fallback
        # graph order can put several dependent transforms ahead of their
        # upstreams (e.g. duplicate SOURCE/TARGET names create a cycle), keep
        # retrying until no instance can make progress.
        import logging as _logging
        if _deferred_insts:
            _logging.warning(f"DEFERRED instances: {[(d.name, type(d).__name__) for d in _deferred_insts]}")
            _logging.warning(f"current_df_map keys: {list(self.current_df_map.keys())}")
        _pending = list(_deferred_insts)
        _remaining = []
        while _pending:
            _remaining = []
            _progress = False
            for _d_inst in _pending:
                if self._process_deferred_instance(_d_inst, plan):
                    _progress = True
                else:
                    _remaining.append(_d_inst)
            if not _progress:
                break
            _pending = _remaining
        for _d_inst in _remaining:
            _logging.warning(
                f"Deferred instance still missing input: {_d_inst.name} "
                f"({self._resolve_transformation_type(_d_inst)})")

        # Also catch any instances that were overwritten in instance_map by a
        # SOURCE with the same name (SSAL1 pattern). These weren't found by
        # _get_input_df above because the map returned the SOURCE, not them.
        _handled_names = {_d.name for _d in _deferred_insts}
        for _extra_inst in self.mapping.instances:
            _extra_type = self._resolve_transformation_type(_extra_inst)
            if _extra_inst.name not in _handled_names and _extra_inst.name not in _processed_inst_names:
                if _extra_type in ("TARGET", "Target Definition"):
                    self.logger.log_transformation(_extra_inst.name, "Target",
                        "Processing target (post-cycle-resolution)", LogLevel.INFO)
                    _steps = self._handle_target(_extra_inst, plan)
                    for _s in _steps:
                        plan.add_step(_s)
                    if _steps:
                        self.logger.log_transformation(_extra_inst.name, "Target",
                            f"Target converted ({len(_steps)} steps)", LogLevel.SUCCESS)
                elif _extra_type in ("Union", "Custom Transformation"):
                    self.logger.log_transformation(_extra_inst.name, "Union",
                        "Processing union (post-cycle-resolution)", LogLevel.INFO)
                    _result = self._handle_union(_extra_inst, plan)
                    if _result:
                        plan.add_step(_result)
                elif _extra_type == "Expression":
                    self.logger.log_transformation(_extra_inst.name, "Expression",
                        "Processing expression (post-cycle-resolution)", LogLevel.INFO)
                    _result = self._handle_expression(_extra_inst, plan)
                    if isinstance(_result, list):
                        for _s in _result:
                            if _s:
                                plan.add_step(_s)
                    elif _result:
                        plan.add_step(_result)

        # Post-process: upgrade filter steps that still reference a chain/merge
        # df_input (e.g. df_lkp_merge_1) to use the downstream non-chain
        # DataFrame that directly consumes that chain (e.g. df_EXPTRANS1
        # whose df_input IS df_lkp_merge_1). Scan plan steps backward from
        # the filter to find the last step that consumes the same chain DF.
        # This avoids picking up unrelated steps from other pipelines.
        for _sp in plan.steps:
            if isinstance(_sp, ApplyFilterStep) and _sp.df_input and _sp.df_input.startswith(('df_lkp_merge', 'df_merge', 'df_sq_')):
                # Filters fed by a Lookup Procedure must consume the lookup's
                # chain/merge output (e.g. FILTRANS_STS ← DLKP_SOR_STS needs
                # END_DATE/NewLookupRow). Rewriting them to an earlier non-chain
                # consumer of the same chain df breaks column resolution, so
                # skip the upgrade for lookup-fed filters.
                _fil_inst = _sp.step_name[6:] if _sp.step_name.startswith('apply_') else _sp.step_name
                _fed_by_lookup = False
                for _c in self.mapping.connectors:
                    if _c.to_instance == _fil_inst:
                        _up_inst = self.instance_map.get(_c.from_instance)
                        if _up_inst is not None and self._resolve_transformation_type(_up_inst) == "Lookup Procedure":
                            _fed_by_lookup = True
                            break
                if _fed_by_lookup:
                    continue
                _chain = _sp.df_input
                _sp_idx = plan.steps.index(_sp)
                for _p in reversed(plan.steps[:_sp_idx]):
                    if _p.df_input == _chain and _p.df_output and not _p.df_output.startswith(('df_lkp_merge', 'df_merge', 'df_sq_')):
                        _sp.df_input = _p.df_output
                        break

        return plan

    def _normalize_instance_to_object_name(self, name: str) -> str:
        return re.sub(r'\d+$', '', name)

    def _handle_source(self, instance: Instance, plan: IRPlan) -> Optional[IRStep]:
        source_name = instance.transformation_name or self._normalize_instance_to_object_name(instance.name)
        source = None

        for s in self.mapping.sources:
            if s.name == source_name or source_name.startswith(s.name):
                source = s
                break

        if not source:
            plan.add_warning(f"Source definition not found: {source_name}")
            return None

        df_name = self._get_df_name("df_src", instance)
        self._register_df(instance, df_name)

        source_config = self._get_source_config(source.name)

        from .models import SourceType, normalize_db_type
        if source.source_type == SourceType.SQL:
            conn_alias = (source_config.connection_alias if source_config and source_config.connection_alias
                         else source.db_name or "default_conn")
            db_type = normalize_db_type(source.database_type)
            is_odbc = "odbc" in (source.database_type or "").lower()
            if is_odbc:
                self.logger.log_transformation(instance.name, "Source",
                    "ODBC source — converted to JDBC (configure JDBC driver in config.yml)",
                    LogLevel.INFO)
            return ReadSQLStep(
                step_name=f"read_{instance.name}",
                df_output=df_name,
                connection_alias=conn_alias,
                table_name=source.name,
                db_type=db_type
            )
        else:
            file_format = source_config.file_format.value if source_config and source_config.file_format else "csv"
            file_path = (source_config.file_path if source_config and source_config.file_path
                        else f"/tmp/{source.name}")
            options = {}
            if source_config:
                options = {
                    "delimiter": source_config.delimiter,
                    "header": str(source_config.header).lower(),
                    "quote": source_config.quote_char
                }
            # Collect source field names for flat files (used to rename CSV columns by position)
            _src_field_names = [f.name for f in source.fields] if source.fields else []
            _rf_step = ReadFileStep(
                step_name=f"read_{instance.name}",
                df_output=df_name,
                file_format=file_format,
                file_path=file_path,
                options=options,
                table_name=source.name
            )
            if _src_field_names:
                _rf_step.params["source_field_names"] = _src_field_names
            return _rf_step

    def _handle_source_qualifier(self, instance: Instance, plan: IRPlan) -> Optional[IRStep]:
        input_df = self._get_input_df(instance.name)
        if not input_df:
            # Check if this SQ has any upstream connectors — if not, it's a file-based
            # or standalone source qualifier (no warning needed, reads from file directly).
            # Also skip warning if the SQ uses SQL pushdown (User Defined Join or Sql Query),
            # because the corresponding Source Definition was skipped as unnecessary.
            _transform = self.transform_map.get(instance.transformation_name or instance.name)
            _has_sql_override = False
            if _transform:
                _udj = _transform.table_attributes.get("User Defined Join", "")
                _sql = _transform.table_attributes.get("Sql Query", "")
                _sqo = _transform.table_attributes.get("SQL Override", "") or _transform.table_attributes.get("Sql Override", "")
                _has_sql_override = bool(_udj.strip() or _sql.strip() or _sqo.strip())
            _has_upstream = any(
                c.to_instance == instance.name
                and c.from_instance != c.to_instance  # skip self-loops (Source Def → same-name SQ)
                for c in self.mapping.connectors
            )
            if _has_upstream and not _has_sql_override:
                plan.add_warning(f"No input DataFrame found for {instance.name}")

            # When the Source Definition and Source Qualifier share the same
            # instance name (e.g. HA_PRH_RNTL_UNIT), the Source is silently
            # overwritten in instance_map and never dispatched. Detect this by
            # checking for a same-named Source Definition in mapping.sources
            # and generate the read step here on the fly.
            if not _has_sql_override:
                _src_def = None
                for _s in self.mapping.sources:
                    if _s.name == instance.name or instance.name.startswith(_s.name):
                        _src_def = _s
                        break
                if _src_def:
                    # Build a minimal source instance so we can call _handle_source
                    _src_instance = Instance(
                        name=instance.name,
                        type="SOURCE",
                        transformation_type="Source Definition",
                        transformation_name=instance.name,
                    )
                    _src_step = self._handle_source(_src_instance, plan)
                    if _src_step:
                        _src_df = _src_step.df_output
                        plan.add_step(_src_step)
                        input_df = _src_df
                        self.logger.log_transformation(
                            instance.name, "SourceQualifier",
                            f"Generated inline source read step for same-named Source {_src_def.name}",
                            LogLevel.INFO,
                        )
            if not input_df:
                input_df = "df_source"

        df_output = self._get_df_name("df_sq", instance)
        self._register_df(instance, df_output)

        transform = self.transform_map.get(instance.transformation_name or instance.name)

        sql_query = ""
        sql_override = ""
        filter_cond = ""
        distinct = False
        output_columns = []

        if transform:
            sql_query = transform.table_attributes.get("Sql Query", "")
            sql_override = transform.table_attributes.get("User Defined Join", "")
            if not sql_override:
                sql_override = transform.table_attributes.get("SQL Override", "")
            if not sql_override:
                sql_override = transform.table_attributes.get("Sql Override", "")
            filter_cond = transform.table_attributes.get("Source Filter", "")
            distinct = transform.table_attributes.get("Select Distinct", "NO") == "YES"

            for field in transform.fields:
                if "OUTPUT" in field.port_type:
                    output_columns.append(field.name)

        final_sql = sql_override.strip() if sql_override.strip() else sql_query.strip()
        use_sql_pushdown = bool(final_sql)

        translated_filter = ""
        filter_inner = ""
        if filter_cond:
            # Strip table name prefixes from column references (e.g. "TBL.COL" -> "COL")
            # since the filter is applied on the DataFrame after SQL execution, where
            # column aliases no longer apply.
            _filter_cond = re.sub(r'\b(\w+)\.(\w+)\b', r'\2', filter_cond)
            _tf = self.expr_translator.translate_for_filter(_filter_cond, "source_filter")
            translated_filter = _tf
            # Extract inner text from the translated result for filter_inner.
            # translate_for_filter now skips $$ variable replacement (prevents
            # double-quoting), so the result has functions translated (TO_NUMBER
            # → cast(... as decimal)) while $$ variables are preserved for
            # runtime .replace() in the template.
            import re as _re
            _m = _re.match(r'expr\("(.*)"\)$', _tf)
            filter_inner = _m.group(1) if _m else _filter_cond

        source_inputs = self._get_source_inputs_for_sq(instance.name)

        # If the SQL override is just a User Defined Join (not a complete SELECT),
        # construct a full SQL query: SELECT <cols> FROM <tables> WHERE <join> [AND <filter>]
        if final_sql and not re.match(r'\s*(SELECT|WITH)\b', final_sql, re.IGNORECASE):
            # Extract unique table names from the join condition and source filter
            _all_sql_text = final_sql
            if filter_cond:
                _all_sql_text += " " + filter_cond
            _tables = set()
            for _m in re.finditer(r'\b(\w+)\.\w+\b', _all_sql_text):
                _tables.add(_m.group(1))
            _from_clause = ', '.join(sorted(_tables)) if _tables else (
                source_inputs[0].get("name", "DUAL") if source_inputs else "DUAL"
            )
            # Build column→table mapping from connectors feeding this Source Qualifier.
            # Each connector links a source definition's field to the SQ's input port.
            _field_table: Dict[str, str] = {}
            for _c in self.mapping.connectors:
                if _c.to_instance == instance.name:
                    _field_table[_c.to_field] = _c.from_instance
            # Prefix each output column with its source table name.
            # Columns that cannot be mapped (no connector) are left bare.
            _qualified_cols = []
            for _c in output_columns:
                _t = _field_table.get(_c, "")
                if _t:
                    _qualified_cols.append(f"{_t}.{_c}")
                else:
                    _qualified_cols.append(_c)
            _select_cols = ', '.join(_qualified_cols) if _qualified_cols else '*'
            _where_parts = [final_sql]
            if filter_cond:
                _where_parts.append(filter_cond)
            _where_clause = ' AND '.join(_where_parts)
            final_sql = f"SELECT {_select_cols} FROM {_from_clause} WHERE {_where_clause}"

        source_name = source_inputs[0].get("name", "") if source_inputs else ""
        conn_alias = self._resolve_connection_alias(
            instance_name=instance.name,
            target_name=source_name,
            plan=plan
        )

        # Determine source database type for potential SQL translation
        source_db_type = "oracle"
        if source_inputs:
            raw_type = source_inputs[0].get("database_type", "")
            if raw_type:
                from .models import normalize_db_type
                source_db_type = normalize_db_type(raw_type)

        # When the SQL override is a complete SELECT, alias columns to match
        # output port names by position.  This prevents ORA-00918 (ambiguous
        # column) when the SELECT has duplicate bare column names (e.g. two
        # APLY_KEY columns from different tables) — the aliases make them unique.
        if use_sql_pushdown and final_sql and output_columns:
            _aliased_sql = self._alias_sql_columns(final_sql, output_columns)
            if _aliased_sql and _aliased_sql != final_sql:
                self.logger.log_transformation(
                    instance.name, "Source Qualifier",
                    f"Aliased {len(output_columns)} columns in SQL pushdown query",
                    LogLevel.INFO,
                )
                final_sql = _aliased_sql

        if use_sql_pushdown:
            # SQL Pushdown: execute native SQL on source database — no dialect translation
            # (Oracle (+) outer-join syntax, DECODE, etc. must be preserved as-is)

            step = ApplySourceQualifierStep(
                step_name=f"apply_{instance.name}",
                df_input=input_df,
                df_output=df_output,
                sql_query=final_sql,
                filter_condition="",
                distinct=False
            )
            step.params["use_sql_override"] = True
            step.params["sql_query"] = self._normalize_sql_text(final_sql, plan)
            step.params["filter_condition"] = ""
            step.params["distinct"] = False
            step.params["db_type"] = source_db_type
            if source_inputs:
                step.params["source_schema"] = source_inputs[0].get("owner", "")
            # Port datatypes drive the runtime type casts in lib.sq_output
            # (pushdown only — the non-pushdown path applies no casts).
            step.params["output_column_types"] = {
                _f.name: _f.datatype
                for _f in (transform.fields if transform else [])
                if "OUTPUT" in _f.port_type
            }
            step.comments.append(f"SQL Pushdown: Executing SQ SQL on source database ({source_db_type})")
        else:
            step = ApplySourceQualifierStep(
                step_name=f"apply_{instance.name}",
                df_input=input_df,
                df_output=df_output,
                sql_query="",
                filter_condition=translated_filter,
                distinct=distinct
            )
            step.params["use_sql_override"] = False
            step.params["sql_query"] = ""
            step.params["filter_condition"] = translated_filter
            if filter_inner:
                step.params["filter_inner"] = self._normalize_sql_text(filter_inner, plan)
            step.params["distinct"] = distinct
            step.params["db_type"] = source_db_type

        step.params["connection_alias"] = conn_alias
        step.params["output_columns"] = output_columns

        # Component-method config: lib.sq_output owns the runtime semantics of
        # SQ port handling (two-pass rename, port select, type casts, filter,
        # distinct). The template renders ONE lib.sq_output call from this cfg.
        _sq_cfg: Dict[str, Any] = {"port_cols": step.params.get("output_columns", [])}
        if step.params.get("output_column_types"):
            _sq_cfg["column_types"] = step.params["output_column_types"]
        if step.params.get("filter_inner") and '$$' not in step.params.get("filter_inner", ""):
            _sq_cfg["filter_condition"] = step.params["filter_inner"]
        elif step.params.get("filter_inner"):
            _sq_cfg["filter_condition"] = step.params["filter_inner"]
            _sq_cfg["substitutions"] = {
                # Values are DEFAULT VALUES (e.g. '202606'), not names; the
                # template renders the value unquoted as the runtime variable
                # identifier (loaded override-aware from UTL_JOB_PARAM), so
                # derive it from the KEY ($$v_x → v_x) — see _handle_filter.
                _var: _var.replace('$', '')
                for _var, _val in (plan.mapping_variables or {}).items()
                if _var in step.params["filter_inner"]
            }
        if step.params.get("distinct"):
            _sq_cfg["distinct"] = True
        step.params["sq_output_cfg"] = _sq_cfg

        return step

    def _get_source_inputs_for_sq(self, sq_name: str) -> List[Dict]:
        sources = []
        for conn in self.mapping.connectors:
            if conn.to_instance == sq_name:
                source = self.source_map.get(conn.from_instance)
                if source:
                    xml_db_name = source.db_name or ""
                    conn_alias = source.db_name or source.name or "source_db"
                    resolved_db = xml_db_name or conn_alias
                    sources.append({
                        "name": source.name,
                        "connection": conn_alias,
                        "resolved_database": resolved_db,
                        "owner": source.owner_name,
                        "database_type": source.database_type
                    })
        return sources

    def _alias_sql_columns(self, sql: str, output_columns: List[str]) -> str:
        """Add inline aliases only to duplicate columns in the SQL SELECT.
        Preserves original SQL formatting — only inserts ' AS alias' after
        duplicate column expressions without touching the rest of the text.
        """
        import re as _re
        _sel_match = _re.match(
            r'^\s*(SELECT\s+(?:DISTINCT\s+)?)(.*?)\bFROM\b',
            sql, _re.IGNORECASE | _re.DOTALL
        )
        if not _sel_match:
            return sql
        _sel_prefix = _sel_match.group(1)
        _col_list_str = _sel_match.group(2)
        # Preserve the original spacing before FROM by using the raw match end
        _from_part = sql[_sel_match.start(2) + len(_col_list_str):]
        # Parse column list into (start, end, text) tuples, preserving original positions
        _col_spans = []
        _depth = 0
        _start = 0
        for _i, _ch in enumerate(_col_list_str):
            if _ch == '(':
                _depth += 1
            elif _ch == ')':
                _depth -= 1
            elif _ch == ',' and _depth == 0:
                _col_spans.append((_start, _i, _col_list_str[_start:_i]))
                _start = _i + 1
        _col_spans.append((_start, len(_col_list_str), _col_list_str[_start:]))

        if len(_col_spans) != len(output_columns):
            return sql

        # Extract bare column names per span
        _bare_names = []
        for _s, _e, _txt in _col_spans:
            _clean = _txt.strip()
            _as_match = _re.search(r'\bAS\s+(\w+)', _clean, _re.IGNORECASE)
            if _as_match:
                _bare_names.append(_as_match.group(1))
            else:
                _dot_split = _clean.split('.')
                _bare_names.append(_dot_split[-1].strip())

        if len(_bare_names) == len(set(n.lower() for n in _bare_names)):
            return sql

        # Build result by inserting ' AS alias' at the end of duplicate columns only
        _seen: Dict[str, int] = {}
        _parts = []
        _prev_end = 0
        for _i, (_s, _e, _txt) in enumerate(_col_spans):
            _bare = _bare_names[_i].lower()
            _part = _col_list_str[_prev_end:_e]
            if _bare in _seen:
                # Strip trailing whitespace before appending alias, then restore it
                _stripped = _part.rstrip()
                _ws = _part[len(_stripped):]
                _part = f"{_stripped} AS {output_columns[_i]}{_ws}"
            _seen.setdefault(_bare, _i)
            _parts.append(_part)
            _prev_end = _e
        _parts.append(_col_list_str[_prev_end:])

        return f"{_sel_prefix}{''.join(_parts)}{_from_part}"

    def _normalize_bare_numeric_condition(self, inner_text: str,
                                          transform: Optional[Transformation]) -> str:
        """Rewrite a bare numeric port used as a Filter condition into an
        explicit boolean comparison (`COL != 0`) for Spark 3.5. Informatica
        accepts a bare numeric port (non-zero = TRUE); Spark rejects it with
        [DATATYPE_MISMATCH.FILTER_NOT_BOOLEAN]. Non-identifier conditions and
        non-numeric ports are left untouched."""
        if not inner_text or not transform:
            return inner_text
        if not _BARE_IDENTIFIER_RE.match(inner_text):
            return inner_text
        for field in transform.fields:
            if (field.name.upper() == inner_text.upper()
                    and (field.datatype or "").lower() in _NUMERIC_DATATYPES):
                return f"{inner_text} != 0"
        return inner_text

    def _handle_filter(self, instance: Instance, plan: IRPlan) -> Optional[IRStep]:
        input_df = self._get_input_df(instance.name)
        if not input_df:
            plan.add_warning(f"No input DataFrame found for {instance.name}")
            input_df = "df_input"

        df_output = self._get_df_name("df_fil", instance)
        self._register_df(instance, df_output)

        transform = self.transform_map.get(instance.transformation_name or instance.name)
        original_condition = ""
        if transform:
            original_condition = transform.table_attributes.get("Filter Condition", "")
            if not original_condition:
                for field in transform.fields:
                    if field.expression and field.name.upper() == "FILTER_CONDITION":
                        original_condition = field.expression
                        break

        if original_condition:
            condition = self.expr_translator.translate_for_filter(original_condition, "filter_condition")
            # Extract inner text for runtime $$ replacement in the template
            _m = re.match(r'expr\("(.*)"\)$', condition)
            filter_inner = _m.group(1) if _m else original_condition
        else:
            condition = "True"
            original_condition = ""
            filter_inner = ""
            plan.add_warning(f"No filter condition found for {instance.name}")

        # Bare numeric port as condition (e.g. VALUE="OUT_DLPK_SOR_CACHE"):
        # Informatica treats non-zero as TRUE; Spark 3.5 requires a boolean.
        _rewritten = self._normalize_bare_numeric_condition(filter_inner, transform)
        if _rewritten != filter_inner:
            filter_inner = _rewritten
            condition = f'expr("{_rewritten}")'

        step = ApplyFilterStep(
            step_name=f"apply_{instance.name}",
            df_input=input_df,
            df_output=df_output,
            condition=condition,
            original_condition=original_condition
        )
        # Collect connector field renames: upstream column → Filter input port name
        # (e.g. CUST_TNT_CODE_OUT → CUST_TNT_CODE) so the filter's condition
        # references the correct columns.
        _filter_renames = []
        for conn in self.mapping.connectors:
            if conn.to_instance == instance.name and conn.from_field.lower() != conn.to_field.lower():
                _filter_renames.append({
                    "from": conn.from_field,
                    "to": conn.to_field,
                })

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
                    _subs[_var] = _var.replace('$', '')
        if _subs:
            _lib_cfg["substitutions"] = _subs
        if self._sequence_attachments.get(instance.name):
            _lib_cfg["sequence_attach"] = self._sequence_attachments[instance.name]
        step.params["lib_filter_cfg"] = _lib_cfg
        return step

    def _handle_expression(self, instance: Instance, plan: IRPlan) -> Optional[IRStep]:
        import re as _re

        input_df = self._get_input_df(instance.name)
        if not input_df:
            plan.add_warning(f"No input DataFrame found for {instance.name}")
            input_df = "df_input"

        # When an expression has multiple upstream DataFrames (e.g. EXPTRANS1
        # receives columns from 10+ mapplets), return a list with join pre-steps
        # so the template joins them all before processing expressions.
        all_inputs = self._get_all_input_dfs(instance.name)
        extra_inputs = [df for df in all_inputs if df != input_df]
        _multistep = bool(extra_inputs and input_df != "df_input")

        df_output = self._get_df_name("df_exp", instance)
        self._register_df(instance, df_output)

        transform = self.transform_map.get(instance.transformation_name or instance.name)
        computed_columns = []
        output_columns = []

        # --- Build field remap from main mapping connectors -----------------
        # When the expression's INPUT ports differ from upstream column names
        # (e.g. IN_HSHLD_SIZE ← HSHLD_SIZE), remap them in expressions.
        _expr_field_remap: Dict[str, str] = {}
        for conn in self.mapping.connectors:
            if conn.to_instance == instance.name and conn.from_field != conn.to_field:
                _expr_field_remap[conn.to_field] = conn.from_field

        # --- Pre-process: resolve inline lookup calls (:LKP.xxx()) ---
        # Scan all fields for :LKP.lkp_name(args) patterns, look up each
        # referenced lookup transformation, extract its INPUT/LOOKUP/RETURN
        # ports and condition, then build join predicates so the template
        # can join the lookup DataFrame into the expression upstream.
        _expr_window_imports = False
        inline_lkp_info = {}  # lkp_name -> {lookup_name, lookup_df, return_port, join_predicates}
        _lkp_pattern = _re.compile(r':LKP\.(\w+)\s*\(([^)]+)\)', _re.IGNORECASE)

        if transform:
            for field in transform.fields:
                if not field.expression or ':LKP.' not in field.expression:
                    continue
                for match in _lkp_pattern.finditer(field.expression):
                    lkp_name = match.group(1)
                    if lkp_name in inline_lkp_info:
                        continue  # already resolved

                    lkp_args = [a.strip() for a in match.group(2).split(',')]
                    lkp_transform = self.transform_map.get(lkp_name)
                    if not lkp_transform:
                        plan.add_warning(f"Inline lookup '{lkp_name}' not found in transform_map for {instance.name}")
                        continue

                    # Extract INPUT, LOOKUP, RETURN ports from lookup definition
                    input_ports = []
                    return_port = None
                    for _f in lkp_transform.fields:
                        pt = _f.port_type.upper()
                        if pt == 'INPUT':
                            input_ports.append(_f.name)
                        elif 'RETURN' in pt:
                            return_port = _f.name

                    if not return_port:
                        plan.add_warning(f"Inline lookup '{lkp_name}' has no RETURN port")
                        continue
                    if not input_ports:
                        plan.add_warning(f"Inline lookup '{lkp_name}' has no INPUT ports")
                        continue

                    # Parse lookup condition to map INPUT ports → LOOKUP columns
                    condition = lkp_transform.table_attributes.get("Lookup condition", "")
                    join_predicates = []
                    for part in _re.split(r'\s+AND\s+', condition, flags=_re.IGNORECASE):
                        part_match = _re.match(r'(\w+(?:\.\w+)?)\s*=\s*(\w+(?:\.\w+)?)', part.strip())
                        if part_match:
                            lookup_col = part_match.group(1)
                            input_port = part_match.group(2)
                            # Strip any table prefix (e.g. "T1.COL" → "COL")
                            input_simple = input_port.rsplit('.', 1)[-1] if '.' in input_port else input_port
                            lookup_simple = lookup_col.rsplit('.', 1)[-1] if '.' in lookup_col else lookup_col
                            try:
                                input_idx = input_ports.index(input_simple)
                                source_col = lkp_args[input_idx]
                                join_predicates.append({
                                    "source_col": source_col,
                                    "lookup_col": lookup_simple,
                                })
                            except (ValueError, IndexError):
                                pass

                    if not join_predicates:
                        plan.add_warning(f"Inline lookup '{lkp_name}' condition could not be parsed")
                        continue

                    # Get lookup DataFrame name (populated by _handle_lookup)
                    lookup_df = plan.lookup_dfs.get(lkp_name)
                    if not lookup_df:
                        plan.add_warning(f"Inline lookup '{lkp_name}' DF not found in plan.lookup_dfs")
                        continue

                    inline_lkp_info[lkp_name] = {
                        'lookup_name': lkp_name,
                        'lookup_df': lookup_df,
                        'return_port': return_port,
                        'join_predicates': join_predicates,
                    }

        # --- Unconnected INPUT ports → NULL (Informatica semantics) ----------
        # Input ports declared on the transformation but fed by NO connector
        # (e.g. columns added after the upstream SQ was refreshed) hold NULL
        # at runtime in Informatica. Replace their references with NULL (or
        # the port's DEFAULTVALUE) so expressions don't crash with
        # UNRESOLVED_COLUMN.
        _unconnected_inputs = []
        if transform:
            _connected_inputs = {
                conn.to_field.lower()
                for conn in self.mapping.connectors
                if conn.to_instance == instance.name
            }
            _unconnected_inputs = [
                f for f in transform.fields
                if "INPUT" in (f.port_type or "").upper()
                and f.name.lower() not in _connected_inputs
            ]

            for field in transform.fields:
                expr_text = field.expression or ""

                # Unconnected input ports referenced in this expression become
                # NULL (or DEFAULTVALUE) — Informatica treats them as NULL.
                for _uc in _unconnected_inputs:
                    if _uc.name.lower() in expr_text.lower():
                        _replacement = (
                            "'" + _uc.default_value.replace("'", "''") + "'"
                            if _uc.default_value else "NULL"
                        )
                        expr_text = re.sub(
                            r'\b' + re.escape(_uc.name) + r'\b',
                            _replacement,
                            expr_text,
                            flags=re.IGNORECASE
                        )

                # Replace :LKP.xxx() calls with the lookup's RETURN port column
                if ':LKP.' in expr_text:
                    for lkp_name, info in inline_lkp_info.items():
                        expr_text = _re.sub(
                            r':LKP\.' + _re.escape(lkp_name) + r'\s*\([^)]+\)',
                            info['return_port'],
                            expr_text,
                            flags=_re.IGNORECASE
                        )

                # Remap INPUT port names to actual upstream column names
                # (e.g. IN_HSHLD_SIZE → HSHLD_SIZE) based on main mapping connectors.
                # Use word boundaries to avoid substring corruption (e.g. replacing
                # OFR_BFR_SUCC_RLET_CNT inside TOT_OFR_BFR_SUCC_RLET_CNT).
                for _port, _col in _expr_field_remap.items():
                    if _port != _col:
                        expr_text = re.sub(r'\b' + re.escape(_port) + r'\b', _col, expr_text)

                # Normalize $$ mapping variable case before translation (Informatica
                # is case-insensitive for variable names, e.g. $$V_SNSH_DATE vs $$v_snsh_date).
                if plan and plan.mapping_variables and '$$' in expr_text:
                    expr_text = self._normalize_var_case(expr_text, plan)

                if "OUTPUT" in field.port_type:
                    output_columns.append(field.name)
                    if expr_text:
                        translated = self.expr_translator.translate(expr_text, "column", field.name)
                        sanitized = sanitize_for_expr(translated)
                        computed_columns.append(ComputedColumn(
                            name=field.name,
                            expression=sanitized,
                            datatype=field.datatype
                        ))
                elif "LOCAL VARIABLE" in field.port_type.upper():
                    # LOCAL VARIABLE with expression — promote to a computed column
                    # so downstream expressions can reference it.
                    if expr_text:
                        # Detect self-referencing LOCAL VARIABLEs that implement a
                        # row-counter pattern (e.g. IIF(ISNULL(X),1,X+1)). In
                        # Informatica these are processed row-by-row and increment
                        # per row. In PySpark, convert to row_number() instead.
                        _counter_ref = bool(re.search(
                            r'\b' + re.escape(field.name) + r'\s*\+?\s*1\b',
                            expr_text
                        ))
                        _retain_ref = not _counter_ref and bool(re.search(
                            r'\b' + re.escape(field.name) + r'\b',
                            expr_text
                        ))
                        if _counter_ref:
                            sanitized = 'monotonically_increasing_id() + 1'
                        elif _retain_ref:
                            # Retain pattern: IIF(cond, value, self_ref) local
                            # variable that keeps its value across rows.
                            # Convert to last(when(cond, value), True) window.
                            import re as _re2
                            _iif = _re2.search(r'IIF\s*\(', expr_text, _re2.IGNORECASE)
                            if _iif:
                                _args = self.expr_translator._extract_function_args(expr_text, _iif.end() - 1)
                            if _iif and len(_args) == 3 and _args[2].strip() == field.name:
                                _ct = self.expr_translator.translate(_args[0], "column", field.name)
                                _vt = self.expr_translator.translate(_args[1], "column", field.name)
                                _sc = sanitize_for_expr(_ct)
                                _sv = sanitize_for_expr(_vt)
                                sanitized = f'last(when(expr("{_sc}"), expr("{_sv}")), True).over(Window.orderBy(lit(1)))'
                            else:
                                _tt = self.expr_translator.translate(expr_text, "column", field.name)
                                _st = sanitize_for_expr(_tt)
                                sanitized = f'last(when(lit(True), expr("{_st}")), True).over(Window.orderBy(lit(1)))'
                            _expr_window_imports = True
                        else:
                            translated = self.expr_translator.translate(expr_text, "column", field.name)
                            sanitized = sanitize_for_expr(translated)
                        computed_columns.append(ComputedColumn(
                            name=field.name,
                            expression=sanitized,
                            datatype=field.datatype
                        ))

        # Sort computed columns by dependency order — if column A's expression
        # references column B, B must be computed first (topological order).
        if len(computed_columns) > 1:
            _dep_graph = nx.DiGraph()
            # Iterate dependency names deterministically: a plain set makes the
            # topological order (and therefore generated column order) depend
            # on the process hash seed, so the same mapping can produce
            # different output files between runs.
            _all_names = sorted({c.name for c in computed_columns})
            for _cc in computed_columns:
                _dep_graph.add_node(_cc.name)
                for _dep_name in _all_names:
                    if _dep_name != _cc.name and re.search(
                        r'\b' + re.escape(_dep_name) + r'\b',
                        _cc.expression or ""
                    ):
                        _dep_graph.add_edge(_dep_name, _cc.name)
            try:
                _sorted = list(nx.topological_sort(_dep_graph))
                _col_map = {c.name: c for c in computed_columns}
                computed_columns = [_col_map[n] for n in _sorted if n in _col_map]
            except nx.NetworkXUnfeasible:
                # Cycle detected — keep original order
                pass

        step = ApplyExpressionStep(
            step_name=f"apply_{instance.name}",
            df_input=input_df,
            df_output=df_output,
            computed_columns=computed_columns
        )
        step.params["output_columns"] = output_columns
        # Detect columns referenced in expressions but not defined as ports —
        # these are external references that should get NULL at runtime.
        if transform:
            _known_cols = set(output_columns) | {f.name for f in transform.fields}
            _expr_refs: Set[str] = set()
            for _f in transform.fields:
                if _f.expression and _f.expression.strip():
                    # Find words that look like column names (ALL_CAPS_UNDERSCORE)
                    for _m in re.finditer(r'\b[A-Z][A-Z0-9_]{2,}(?:\s*\.\s*[A-Z][A-Z0-9_]{2,})?\b', _f.expression):
                        _ref = _m.group(0).strip()
                        # Skip SQL keywords / common operators
                        if _ref.upper() in ('AND', 'OR', 'NOT', 'IS', 'NULL', 'TRUE', 'FALSE', 'IN', 'IIF', 'DECODE', 'NVL', 'NVL2', 'COALESCE', 'NULLIF', 'SUBSTR', 'SUBSTRING', 'COUNT', 'SUM', 'MIN', 'MAX', 'AVG', 'FIRST', 'LAST', 'UPPER', 'LOWER', 'TRIM', 'LTRIM', 'RTRIM', 'LENGTH', 'INSTR', 'LPAD', 'RPAD', 'REPLACE', 'REG_REPLACE', 'TO_DATE', 'TO_CHAR', 'TO_NUMBER', 'TO_DECIMAL', 'TO_INTEGER', 'TO_FLOAT', 'TO_BIGINT', 'SYSDATE', 'CURRENT_TIMESTAMP', 'ROUND', 'TRUNC', 'ABS', 'MOD', 'POWER', 'SQRT', 'FLOOR', 'CEIL', 'SIGN', 'GREATEST', 'LEAST', 'ADD_TO_DATE', 'DATE_DIFF', 'GET_DATE_PART', 'SET_DATE_PART', 'LAST_DAY', 'NEXT_DAY', 'MONTHS_BETWEEN', 'ADD_MONTHS', 'MAKE_DATE_TIME', 'ERROR', 'ABORT', 'WHEN', 'THEN', 'ELSE', 'CASE', 'END'):
                            continue
                        if _ref not in _known_cols:
                            _expr_refs.add(_ref)
            if _expr_refs:
                step.params["expression_external_refs"] = sorted(_expr_refs)
        if _expr_window_imports:
            step.params["window_imports"] = True

        # Attach inline lookup join info so the template generates the join code
        if inline_lkp_info:
            step.params["inline_lookup_joins"] = list(inline_lkp_info.values())

        # Attach stored-procedure call text to step params when applicable
        if transform:
            for field in transform.fields:
                if field.expression and ':SP.' in field.expression:
                    _sp_match = _re.search(r':SP\.(\w+)', field.expression)
                    if _sp_match:
                        _sp_trans_name = _sp_match.group(1)
                        _sp_transform = self.transform_map.get(_sp_trans_name)
                        if _sp_transform and _sp_transform.table_attributes:
                            _sp_full = _sp_transform.table_attributes.get("Stored Procedure Name", "")
                            if _sp_full:
                                _parts = _sp_full.split('.')
                                if len(_parts) >= 3:
                                    _sp_call = '.'.join(_parts[1:])
                                    # Keep the owner schema for {_schema} parameterization
                                    # (replaces the hardcoded prefix at runtime, like SQL)
                                    step.params["sp_schema"] = _parts[0]
                                else:
                                    _sp_call = _sp_full
                                step.params["sp_call_text"] = _sp_call
                                break

        step.params["expression_cfg"] = _build_expression_cfg(
            step, computed_columns, output_columns, transform, plan,
            inline_lkp_info, _re, self.transform_map)

        # Multi-upstream: merge extra DFs into the main input so that
        # lookups and other pipeline branches contribute their columns.
        # (e.g. EXPTRANS1 receiving columns from both AGGTRANS2 chain
        #  and separate lookups like LKP_UAO_FEE_ADV_AMT).
        if _multistep and extra_inputs:
            _cur_df = input_df
            _df_parent = self._build_df_parent_map(plan.steps)
            _step_map = {s.df_output: s for s in plan.steps if s.df_output}
            _needed = set()
            for _c in self.mapping.connectors:
                if _c.to_instance == instance.name and _c.from_instance in self.current_df_map:
                    _needed.add(_c.from_field)
            for _i, _extra_df in enumerate(extra_inputs):
                _redundant_to = self._redundant_merge_df(
                    _df_parent, _step_map, _cur_df, _extra_df, _needed)
                if _redundant_to is not None:
                    # Lookup-merge descendant of the current df (or vice versa):
                    # joining them is redundant and can make the analyzer loop;
                    # the descendant already has all columns — use it directly.
                    _cur_df = _redundant_to
                    continue
                _merge_df = self._df_name("df_merge", instance.name, _i)
                _merge_step = ApplyLookupStep(
                    step_name=f"merge_{instance.name}_{_i}",
                    df_input=_cur_df,
                    df_output=_merge_df,
                    lookup_df=_extra_df,
                    join_predicates=[],
                    join_expr="__common_cols__",
                    output_columns=[],
                    lookup_type="left",
                )
                plan.add_step(_merge_step)
                _df_parent[_merge_df] = _cur_df
                _step_map[_merge_df] = _merge_step
                _cur_df = _merge_df
                self.logger.log_transformation(instance.name, "Expression",
                    f"Merge extra df {_extra_df} into {_cur_df} via common columns",
                    LogLevel.INFO)
            input_df = _cur_df
            step.df_input = input_df
        elif _multistep:
            self.logger.log_transformation(instance.name, "Expression",
                "Extra inputs exist but none to merge — all columns already available",
                LogLevel.INFO)

        return step

    def _build_dynamic_lookup_params(
        self,
        transform: Transformation,
        join_predicates: List[Dict[str, str]],
        instance_name: str,
        new_lookup_row_col: str = "NewLookupRow",
        plan: Optional[IRPlan] = None,
        ref_field_remap: Optional[Dict[str, str]] = None,
    ) -> Optional[Dict[str, Any]]:
        """Build the full dynamic-lookup configuration carried on an
        ApplyLookupStep.

        Dynamic Lookup Cache=YES is the trigger. Informatica restricts the
        multiple-match policy for dynamic caches to Report Error; the
        converter refuses any other policy instead of silently deduplicating
        the lookup source.
        """
        if not transform or not join_predicates:
            return None
        attrs = transform.table_attributes or {}
        if str(attrs.get("Dynamic Lookup Cache", "NO")).upper() != "YES":
            return None

        policy = str(attrs.get("Lookup policy on multiple match", "")).strip()
        if policy.upper() != "REPORT ERROR":
            msg = (
                f"Dynamic lookup {instance_name}: policy '{policy or 'empty'}' "
                "is not supported — dynamic cache requires Report Error"
            )
            if plan is not None:
                plan.add_error(msg)
            else:
                self.logger.log_transformation(
                    instance_name, "Lookup", msg, LogLevel.ERROR)
            return None

        output_fields = []
        sequence_config = None
        for field in transform.fields:
            port_type = (field.port_type or "").upper()
            if ("DYNLOOKUP" in port_type
                    or "LOOKUP/OUTPUT" not in port_type
                    or "RETURN" in port_type):
                continue
            ref_field = (field.ref_field or "").strip()
            if ref_field_remap and ref_field in ref_field_remap:
                ref_field = ref_field_remap[ref_field]
            output_fields.append({
                "name": field.name,
                "ref_field": ref_field,
                "ignore_in_compare": str(field.ignore_in_compare or "NO").upper() == "YES",
                "ignore_null_inputs": str(field.ignore_null_inputs or "NO").upper() == "YES",
                "datatype": field.datatype or "string",
            })
            if ref_field.upper() == "SEQUENCE-ID" and sequence_config is None:
                sequence_config = {"output_col": field.name}

        if not output_fields:
            plan.add_warning(
                f"Dynamic lookup {instance_name}: no LOOKUP/OUTPUT ports found; "
                "only NewLookupRow will be emitted"
            )

        params = {
            "name": instance_name,
            "join_predicates": [
                {
                    "source_col": jp.get("source_col", ""),
                    "lookup_col": jp.get("lookup_col", ""),
                }
                for jp in join_predicates
                if isinstance(jp, dict)
            ],
            "output_columns": [f["name"] for f in output_fields],
            "lookup_output_fields": output_fields,
            "new_lookup_row_col": new_lookup_row_col,
            "sequence_config": sequence_config,
            "insert_else_update": str(attrs.get("Insert Else Update", "NO")).upper() == "YES",
            "update_else_insert": str(attrs.get("Update Else Insert", "NO")).upper() == "YES",
            "update_condition": str(attrs.get("Update Dynamic Cache Condition", "TRUE") or "TRUE"),
            "output_old_value_on_update": str(
                attrs.get("Output Old Value On Update", "NO")).upper() == "YES",
            "case_sensitive_string_comparison": str(
                attrs.get("Case Sensitive String Comparison", "NO")).upper() == "YES",
            "lookup_policy": "Report Error",
            "order_by_columns": [],
        }

        if str(attrs.get("Synchronize Dynamic Cache", "NO")).upper() == "YES":
            plan.add_warning(
                f"Dynamic lookup {instance_name}: Synchronize Dynamic Cache=YES "
                "is not emulated; each lookup keeps an independent cache"
            )
        if params["update_else_insert"]:
            plan.add_warning(
                f"Dynamic lookup {instance_name}: Update Else Insert=YES is "
                "not supported for this converter; insert-row semantics are used"
            )
        if str(attrs.get("Lookup cache initialize", "NO")).upper() == "NO" and str(
                attrs.get("Lookup cache persistent", "YES")).upper() == "YES":
            self.logger.log_transformation(
                instance_name, "Lookup",
                "Persistent dynamic cache is seeded from the lookup source on "
                "every run (no cross-session reuse)",
                LogLevel.INFO)
        return params

    def _handle_lookup(self, instance: Instance, plan: IRPlan) -> List[IRStep]:
        steps = []

        input_df = self._get_input_df(instance.name)
        # Capture the upstream df set BEFORE chain registration overwrites
        # current_df_map entries with the accumulating chain output name;
        # otherwise a multi-upstream merge would see the lookup's own output
        # (e.g. df_lkp_merge_13) as an "extra" input and self-reference it.
        _lookup_upstream_dfs = self._get_all_input_dfs(instance.name)
        # Fields each upstream branch provides to this lookup (used to decide
        # whether an ancestor extra can be skipped safely).
        _lookup_needed_by_df: Dict[str, Set[str]] = {}
        for _c in self.mapping.connectors:
            if _c.to_instance == instance.name and _c.from_instance in self.current_df_map:
                _lookup_needed_by_df.setdefault(
                    self.current_df_map[_c.from_instance], set()).add(_c.from_field)

        transform = self.transform_map.get(instance.transformation_name or instance.name)
        if not transform:
            plan.add_warning(f"Lookup transformation not found: {instance.name}")
            return steps

        lookup_df = self._get_df_name("df_lkp", instance)

        lookup_sql = transform.table_attributes.get("Lookup Sql Override", "")
        lookup_sql = self._normalize_sql_text(lookup_sql, plan)
        lookup_table = transform.table_attributes.get("Lookup table name", "")

        lookup_conn = self._find_lookup_connection(lookup_table or instance.name)

        if lookup_sql:
            # Extract schema prefix from the SQL (e.g. "PSOR" from "FROM PSOR.SOR_SYS_PRPTY")
            # so the template can parameterize it with the connection's configured schema.
            lookup_schema = ""
            schema_source_found = None
            import re as _re
            _sql_schema_match = _re.search(r'\bFROM\s+(\w+)\.', lookup_sql, _re.IGNORECASE)
            if _sql_schema_match:
                _sql_schema = _sql_schema_match.group(1)
                # Confirm this is a known schema prefix — look for a source definition
                # with matching owner_name, and update lookup_conn if found.
                for src in self.mapping.sources:
                    if src.owner_name and src.owner_name.upper() == _sql_schema.upper():
                        lookup_schema = src.owner_name
                        schema_source_found = src
                        break
                if not schema_source_found:
                    # Schema prefix seen in SQL but no source definition matched it.
                    # Still record it so the template parameterizes the query with the
                    # connection's configured schema (or falls back to the original).
                    lookup_schema = _sql_schema

            # If we found a source with matching owner_name, use its db_name
            # as the connection alias (handles $Source lookups in flat-file mappings).
            if lookup_schema and schema_source_found and schema_source_found.db_name:
                if lookup_conn.lower() != schema_source_found.db_name.lower():
                    lookup_conn = schema_source_found.db_name


                    
            rs = ReadSQLStep(
                step_name=f"read_{instance.name}",
                df_output=lookup_df,
                connection_alias=lookup_conn,
                query=lookup_sql,
                is_lookup=True
            )
            rs.params["source_schema"] = lookup_schema
            steps.append(rs)
        elif lookup_table:
            steps.append(ReadSQLStep(
                step_name=f"read_{instance.name}",
                df_output=lookup_df,
                connection_alias=lookup_conn,
                table_name=lookup_table,
                is_lookup=True
            ))
        elif self.session_file_sources:
            # Flat file lookup — read from file using session file source info
            _lkp_file_info = self.session_file_sources.get(instance.name)
            if _lkp_file_info and _lkp_file_info.get("filename"):
                _lkp_file_dir = _lkp_file_info.get("file directory", "")
                _lkp_file_name = _lkp_file_info.get("filename", "")
                if _lkp_file_dir:
                    _lkp_file_dir = _lkp_file_dir.replace("$PMLookupFileDir", "__SOURCE_FILE_DIR__")
                _lkp_path = f"{_lkp_file_dir}/{_lkp_file_name}" if _lkp_file_dir else _lkp_file_name
                # Collect field names from lookup transformation to rename CSV columns
                # by position (the template's source_field_names mechanism).
                _lkp_field_names = []
                for _f in transform.fields:
                    _pt = (_f.port_type or "").upper()
                    if "OUTPUT" in _pt or "LOOKUP" in _pt or "INPUT/OUTPUT" in _pt:
                        if "RETURN" not in _pt:
                            _lkp_field_names.append(_f.name)
                _rf_step = ReadFileStep(
                    step_name=f"read_{instance.name}",
                    df_output=lookup_df,
                    file_path=_lkp_path,
                    file_format="csv",
                    is_lookup=True,
                    table_name=instance.name,
                )
                if _lkp_field_names:
                    _rf_step.params["source_field_names"] = _lkp_field_names
                steps.append(_rf_step)
                self.logger.log_transformation(
                    instance.name, "Lookup",
                    f"Flat file lookup: {_lkp_path} ({len(_lkp_field_names)} fields)",
                    LogLevel.INFO,
                )

        plan.lookup_dfs[instance.name] = lookup_df

        output_columns = []
        for field in transform.fields:
            if "OUTPUT" in field.port_type.upper() and "RETURN" not in field.port_type.upper():
                continue
            if "RETURN" in field.port_type.upper() or (field.expression and "OUTPUT" in field.port_type.upper()):
                output_columns.append(field.name)

        plan.lookup_return_ports[instance.name] = output_columns

        if input_df:
            # Chain lookups that share the same upstream into one accumulating df.
            # First lookup on this input → start chain; subsequent → chain onto it.
            # Determine the upstream instance name (for chain key).
            # All lookups feeding from the same upstream instance share one chain df.
            _upstream_name = ""
            for _c in self.mapping.connectors:
                if _c.to_instance == instance.name:
                    _upstream_name = _c.from_instance
                    break
            if _upstream_name in self._chain_df_map:
                # Chain onto existing accumulated df
                df_output = self._chain_df_map[_upstream_name]
                _chain_input = df_output
            else:
                # First lookup for this upstream — create chain df.
                # Chain onto the previous merge output only when the resolved
                # input is itself a raw chain/merge/SQ result. If the lookup is
                # fed by a downstream transformation (e.g. EXPTRANS after an
                # Aggregator), that DF already carries all columns — use it
                # directly instead of a stale merge.
                df_output = self._df_name("df_lkp_merge", _upstream_name)
                if _upstream_name:
                    self._chain_df_map[_upstream_name] = df_output
                if not input_df or input_df.startswith(('df_lkp_merge', 'df_merge', 'df_sq_')):
                    _chain_input = self._last_chain_output or input_df
                else:
                    _chain_input = input_df
                self._last_chain_output = df_output
            # Register lookup instance → chain df
            self._register_df(instance, df_output)
            self._lookup_order.append(instance.name)
            # Walk upstream from this lookup's connector chain and register the
            # chain df for every instance in the same pipeline segment. This
            # ensures that downstream components (e.g. EXPTRANS4 connected to
            # DRP_EXCP) see the chain df instead of the raw SQ result.
            # NOTE: Skip Router instances — they have multiple output groups
            # registered under suffixed keys (RTRTRANS_VALID_TYPE, etc.) and
            # overwriting their map entry would break downstream lookup.
            # Also skip Lookup Procedure instances: a later lookup chained onto
            # the same pipeline must not clobber an earlier lookup's registered
            # merge output (FILTRANS_MSTR/STS resolve their input from the
            # lookup instance that actually feeds them).
            _visited = set()
            _queue = [_upstream_name] if _upstream_name else []
            while _queue:
                _inst = _queue.pop(0)
                if _inst in _visited:
                    continue
                _visited.add(_inst)
                _skip = False
                _ii = self.instance_map.get(_inst)
                if _ii:
                    if _ii.transformation_type == 'Router':
                        _skip = True
                    elif self._resolve_transformation_type(_ii) == "Lookup Procedure":
                        _skip = True
                if not _skip:
                    self.current_df_map[_inst] = df_output
                # Find what feeds this instance (walk upstream regardless)
                for _c in self.mapping.connectors:
                    if _c.to_instance == _inst:
                        _queue.append(_c.from_instance)

            # Build field remap from connectors: lookup INPUT port → upstream column
            # (Informatica adds numeric suffixes like TNCY_AGRMT_BK1 when multiple
            # connectors feed the same lookup port name; the actual column has no suffix.)
            # Only include INPUT/OUTPUT ports (not pure LOOKUP/OUTPUT return columns).
            _lkp_input_ports: Set[str] = set()
            if transform:
                for _f in transform.fields:
                    if _f.port_type and 'INPUT' in _f.port_type.upper():
                        _lkp_input_ports.add(_f.name)
            _lkp_field_remap: Dict[str, str] = {}
            for conn in self.mapping.connectors:
                if (conn.to_instance == instance.name
                    and conn.from_field != conn.to_field
                    and conn.to_field in _lkp_input_ports):
                    _lkp_field_remap[conn.to_field] = conn.from_field

            lookup_cond = transform.table_attributes.get("Lookup condition", "")
            parsed_condition = self._parse_lookup_condition(lookup_cond, lookup_df)

            join_predicates = parsed_condition.get("join_columns", [])
            join_expr = parsed_condition.get("condition_expr") or ""

            # Also remap column references in complex join expressions
            if join_expr:
                for _port, _col in _lkp_field_remap.items():
                    if _port != _col:
                        join_expr = re.sub(r'\b' + re.escape(_port) + r'\b', _col, join_expr)

            # Multi-upstream lookups: the lookup's input ports may come from
            # several independent branches (e.g. DLKP_SOR_STS.IN_RVN_CLCT_TRML_KEY
            # comes from MPLT_AGMT_NHS_RVN_CLCT_TRML while its other ports come
            # from the EXP_BK / DLKP_SOR_MSTR chain). Merge the extra upstream
            # dfs into the chain input so every remapped source column exists
            # on the df that feeds the lookup join.
            _extra_lookup_inputs = [
                _d for _d in _lookup_upstream_dfs
                if _d != _chain_input
                and not str(_chain_input).startswith("df_input")
            ]
            # When the lookup chain input is a mapplet's output, base the
            # chain on the mapplet's external input stream instead — the
            # mapplet may have overwritten source columns (e.g.
            # LAST_REC_TXN_TYPE_CODE = NULL) that downstream filters and
            # expressions still need (FILTRANS_MSTR/STS, EXPTRANS_*, EXP_OPR_IND).
            _mpl_base = None
            for _c in self.mapping.connectors:
                if _c.to_instance == instance.name:
                    _up = _c.from_instance
                    if (_up in self._mapplet_output_df
                            and _chain_input == self._mapplet_output_df[_up]):
                        # Walk nested mapplets up to the ultimate external
                        # input (e.g. PYMT mapplet ← TXN mapplet ← EXP_BK).
                        _walk = _up
                        _visited = set()
                        while _walk in self._mapplet_input_df and _walk not in _visited:
                            _visited.add(_walk)
                            _in_df = self._mapplet_input_df[_walk]
                            if _in_df and _in_df != _chain_input:
                                _mpl_base = _in_df
                            _next = next(
                                (k for k, v in self._mapplet_output_df.items()
                                 if v == _in_df and k not in _visited),
                                None)
                            if _next is None:
                                break
                            _walk = _next
                        break
            if _extra_lookup_inputs or _mpl_base is not None:
                _df_parent = self._build_df_parent_map(plan.steps)
                _step_map = {s.df_output: s for s in plan.steps if s.df_output}
                # Prefer the most ancestral upstream df as the base so the
                # source stream's column values survive name collisions with
                # mapplet outputs that overwrote them. Lookup chain outputs are
                # consumed by downstream filters/expressions, so a descendant
                # branch must never silently replace base columns.
                _cur_lookup_df = _mpl_base if _mpl_base is not None else _chain_input
                _base_df = _cur_lookup_df
                for _i, _extra_df in enumerate(
                        [d for d in _lookup_upstream_dfs if d != _base_df]):
                    # Only an extra that is a pure ancestor of the current base
                    # may be skipped (the base already carries every column the
                    # ancestor provides). A descendant extra may have overwritten
                    # source columns (e.g. a mapplet output sets
                    # LAST_REC_TXN_TYPE_CODE = NULL) and must always be merged
                    # so the base's source values survive name collisions.
                    if (self._is_df_descendant(_df_parent, _cur_lookup_df, _extra_df)
                            and self._lineage_preserves_columns(
                                _df_parent, _step_map, _cur_lookup_df, _extra_df,
                                _lookup_needed_by_df.get(_extra_df, set()))):
                        continue
                    _merge_df = self._df_name("df_merge", instance.name, _i)
                    _merge_step = ApplyLookupStep(
                        step_name=f"merge_{instance.name}_{_i}",
                        df_input=_cur_lookup_df,
                        df_output=_merge_df,
                        lookup_df=_extra_df,
                        join_predicates=[],
                        join_expr="__common_cols__",
                        output_columns=[],
                        lookup_type="left",
                    )
                    steps.append(_merge_step)
                    _df_parent[_merge_df] = _cur_lookup_df
                    _step_map[_merge_df] = _merge_step
                    _cur_lookup_df = _merge_df
                _chain_input = _cur_lookup_df

            steps.append(ApplyLookupStep(
                step_name=f"apply_{instance.name}",
                df_input=_chain_input,
                df_output=df_output,
                lookup_df=lookup_df,
                join_predicates=join_predicates,
                join_expr=join_expr,
                output_columns=output_columns,
                lookup_type="left"
            ))
            if _lkp_field_remap:
                steps[-1].params["lkp_field_remap"] = _lkp_field_remap
            # Dynamic Lookup: carry the full dynamic-cache configuration so
            # the template emits the precise applyInPandas state machine
            # (NewLookupRow 0 = hit/no change, 1 = insert, 2 = update).
            _dyn_lkp_params = self._build_dynamic_lookup_params(
                transform, join_predicates, instance.name,
                new_lookup_row_col="NewLookupRow", plan=plan,
                ref_field_remap=_lkp_field_remap)
            if _dyn_lkp_params is not None:
                steps[-1].params["dynamic_lookup"] = _dyn_lkp_params
                # Keep the legacy params for IR consumers/tests; the template's
                # dynamic_lookup branch ignores them and uses the full config.
                steps[-1].params["new_lookup_row_key"] = join_predicates[0].get(
                    "lookup_col", "")
                steps[-1].params["dedup_lookup"] = True
                steps[-1].params["dedup_lookup_error"] = True
                steps[-1].params["dedup_lookup_keys"] = [
                    jp.get("lookup_col", "") for jp in join_predicates
                ]
            # Handle "Lookup policy on multiple match" — dedup the lookup
            # DataFrame by join keys when there could be multiple matches.
            # Options: Use First Value, Use Last Value, Use Any Value, Report Error.
            if _dyn_lkp_params is None and join_predicates:
                try:
                    _lkp_policy = transform.table_attributes.get(
                        "Lookup policy on multiple match", "")
                except (AttributeError, TypeError):
                    _lkp_policy = ""
                _dedup_keys = [jp.get("lookup_col", "") for jp in join_predicates]
                if _lkp_policy.upper() in ("USE FIRST VALUE", "USE ANY VALUE", ""):
                    steps[-1].params["dedup_lookup"] = True
                    steps[-1].params["dedup_lookup_keys"] = _dedup_keys
                elif _lkp_policy.upper() == "USE LAST VALUE":
                    steps[-1].params["dedup_lookup"] = True
                    steps[-1].params["dedup_lookup_keys"] = _dedup_keys
                    steps[-1].params["dedup_lookup_last"] = True
                elif _lkp_policy.upper() == "REPORT ERROR":
                    steps[-1].params["dedup_lookup"] = True
                    steps[-1].params["dedup_lookup_error"] = True
                    steps[-1].params["dedup_lookup_keys"] = _dedup_keys

        return steps

    def _parse_lookup_condition(self, condition: str, lookup_df: str) -> Dict[str, Any]:
        result = {
            "join_columns": [],
            "condition_expr": None,
            "raw_condition": condition
        }

        if not condition:
            return result

        parts = re.split(r'\s+AND\s+', condition, flags=re.IGNORECASE)

        for part in parts:
            part = part.strip()
            match = re.match(r'(\w+(?:\.\w+)?)\s*=\s*(\w+(?:\.\w+)?)', part)
            if match:
                left_col = match.group(1).split('.')[-1]
                right_col = match.group(2).split('.')[-1]
                # Informatica lookups use format: LOOKUP_COL = SOURCE_COL
                result["join_columns"].append({
                    "source_col": right_col,
                    "lookup_col": left_col
                })

        if not result["join_columns"]:
            sanitized = sanitize_for_expr(condition)
            result["condition_expr"] = sanitized

        return result

    def _joiner_pick_master_detail(self, joiner_instance: Instance, inputs: List[str]) -> tuple:
        transform = self.transform_map.get(joiner_instance.transformation_name or joiner_instance.name)
        if not transform:
            if len(inputs) < 2:
                return inputs[0], inputs[0], None, None
            return inputs[0], inputs[1], None, None

        port_role = {f.name: (f.port_type or "").upper() for f in transform.fields}
        master_from = None
        detail_from = None

        for c in self.mapping.connectors:
            if c.to_instance != joiner_instance.name:
                continue
            role = port_role.get(c.to_field, "")
            if "MASTER" in role:
                master_from = c.from_instance
            elif "DETAIL" in role:
                detail_from = c.from_instance

        # If only MASTER is marked (no DETAIL marker), derive detail via exclusion
        if master_from and not detail_from:
            for c in self.mapping.connectors:
                if c.to_instance == joiner_instance.name and c.from_instance != master_from:
                    detail_from = c.from_instance
                    break

        if master_from and master_from in self.current_df_map:
            df_master = self.current_df_map[master_from]
        else:
            df_master = inputs[0] if inputs else "df_input"

        if detail_from and detail_from in self.current_df_map:
            df_detail = self.current_df_map[detail_from]
        else:
            df_detail = inputs[1] if len(inputs) > 1 else (inputs[0] if inputs else "df_detail")

        return df_master, df_detail, master_from, detail_from

    def _parse_joiner_condition(self, cond: str) -> dict:
        result = {
            "join_predicates": [],
            "raw_condition": cond,
            "use_fallback": False
        }

        if not cond or not cond.strip():
            return result

        cond_stripped = cond.strip()

        if re.search(r'\bOR\b', cond_stripped, re.IGNORECASE):
            result["use_fallback"] = True
            return result

        parts = re.split(r"\s+AND\s+", cond_stripped, flags=re.IGNORECASE)
        predicates = []

        equi_pattern = re.compile(
            r'^(MASTER|DETAIL)\.(\w+)\s*=\s*(MASTER|DETAIL)\.(\w+)$',
            re.IGNORECASE
        )

        for p in parts:
            p = p.strip()
            m = equi_pattern.match(p)
            if not m:
                result["use_fallback"] = True
                result["join_predicates"] = []
                return result

            prefix1, col1 = m.group(1).upper(), m.group(2)
            prefix2, col2 = m.group(3).upper(), m.group(4)

            if prefix1 == prefix2:
                result["use_fallback"] = True
                result["join_predicates"] = []
                return result

            if prefix1 == "MASTER":
                predicates.append({"master_col": col1, "detail_col": col2})
            else:
                predicates.append({"master_col": col2, "detail_col": col1})

        result["join_predicates"] = predicates
        return result

    def _handle_joiner(self, instance: Instance, plan: IRPlan) -> Optional[IRStep]:
        transform = self.transform_map.get(instance.transformation_name or instance.name)

        inputs = self._get_all_input_dfs(instance.name)
        if len(inputs) < 2:
            plan.add_warning(f"Joiner {instance.name} needs 2 inputs, found {len(inputs)}")
            return None

        df_output = self._get_df_name("df_jnr", instance)
        self._register_df(instance, df_output)

        join_condition = ""
        join_type = "inner"

        if transform:
            join_condition = transform.table_attributes.get("Join Condition", "")
            join_type_attr = transform.table_attributes.get("Join Type", "Normal")
            if "Master Outer" in join_type_attr:
                join_type = "right"
            elif "Detail Outer" in join_type_attr:
                join_type = "left"
            elif "Full Outer" in join_type_attr:
                join_type = "full"

        df_master, df_detail, master_from, detail_from = self._joiner_pick_master_detail(instance, inputs)
        parsed_condition = self._parse_joiner_condition(join_condition)

        step = ApplyJoinerStep(
            step_name=f"apply_{instance.name}",
            df_master=df_master,
            df_detail=df_detail,
            df_output=df_output,
            join_condition=join_condition,
            join_type=join_type
        )
        step.params["join_predicates"] = parsed_condition["join_predicates"]
        step.params["raw_condition"] = parsed_condition["raw_condition"]
        step.params["use_fallback"] = parsed_condition["use_fallback"]

        # Build select maps for master/detail: upstream column → Joiner port name.
        # Collect all upstream instances that feed into this Joiner.
        _master_selects: List[Dict] = []
        _detail_selects: List[Dict] = []
        _seen_master = set()
        _seen_detail = set()
        for conn in self.mapping.connectors:
            if conn.to_instance == instance.name:
                _entry = {"from": conn.from_field, "to": conn.to_field}
                # Assign to master or detail based on which df the instance maps to
                _inst_df = self.current_df_map.get(conn.from_instance)
                if _inst_df == df_master and conn.from_field not in _seen_master:
                    _master_selects.append(_entry)
                    _seen_master.add(conn.from_field)
                elif _inst_df == df_detail and conn.from_field not in _seen_detail:
                    _detail_selects.append(_entry)
                    _seen_detail.add(conn.from_field)
        if _master_selects:
            step.params["master_selects"] = _master_selects
        if _detail_selects:
            step.params["detail_selects"] = _detail_selects

        # Detect Joiner output ports without upstream connectors — add NULL
        if transform:
            _mapped_to = {s["to"] for s in _master_selects} | {s["to"] for s in _detail_selects}
            _missing = []
            for _f in transform.fields:
                _pt = (_f.port_type or "").upper()
                if "OUTPUT" in _pt or "INPUT/OUTPUT" in _pt:
                    if _f.name not in _mapped_to:
                        _missing.append(_f.name)
            if _missing:
                step.params["joiner_missing_cols"] = _missing

        return step

    def _handle_aggregator(self, instance: Instance, plan: IRPlan) -> Optional[IRStep]:
        input_df = self._get_input_df(instance.name)
        if not input_df:
            input_df = "df_input"

        # Build select mappings: upstream column → Aggregator port name.
        # Use select(col("x").alias("y")) instead of withColumnRenamed to
        # avoid duplicate columns when an upstream column already has the
        # target name.  Only mapped columns are carried forward.
        _agg_selects: List[Dict] = []
        for conn in self.mapping.connectors:
            if conn.to_instance == instance.name:
                _agg_selects.append({
                    "from": conn.from_field,
                    "to": conn.to_field
                })

        # When the Aggregator has multiple upstream DataFrames (e.g. columns
        # spread across EXPTRANS5 + EXPTRANS6 + JNRTRANS), merge extra inputs
        # onto the primary input via common-column join (same as Expression handler).
        all_inputs = self._get_all_input_dfs(instance.name)
        extra_inputs = [df for df in all_inputs if df != input_df]
        _pre_steps: List[IRStep] = []
        if extra_inputs and input_df != "df_input":
            _cur_df = input_df
            _df_parent = self._build_df_parent_map(plan.steps)
            _step_map = {s.df_output: s for s in plan.steps if s.df_output}
            _needed = set()
            for _c in self.mapping.connectors:
                if _c.to_instance == instance.name and _c.from_instance in self.current_df_map:
                    _needed.add(_c.from_field)
            for _i, _extra_df in enumerate(extra_inputs):
                _redundant_to = self._redundant_merge_df(
                    _df_parent, _step_map, _cur_df, _extra_df, _needed)
                if _redundant_to is not None:
                    # Lookup-merge descendant of the current df (or vice versa):
                    # joining them is redundant and can make the analyzer loop;
                    # the descendant already has all columns — use it directly.
                    _cur_df = _redundant_to
                    continue
                _merge_df = self._df_name("df_merge", instance.name, _i)
                _pre_steps.append(ApplyLookupStep(
                    step_name=f"merge_{instance.name}_{_i}",
                    df_input=_cur_df,
                    df_output=_merge_df,
                    lookup_df=_extra_df,
                    join_predicates=[],
                    join_expr="__common_cols__",
                    output_columns=[],
                    lookup_type="left",
                ))
                _df_parent[_merge_df] = _cur_df
                _step_map[_merge_df] = _pre_steps[-1]
                _cur_df = _merge_df
            input_df = _cur_df

        df_output = self._get_df_name("df_agg", instance)
        self._register_df(instance, df_output)

        transform = self.transform_map.get(instance.transformation_name or instance.name)

        group_by = []
        aggregations = {}
        _agg_literals: List[Dict] = []

        if transform:
            self.logger.log_transformation(instance.name, "Aggregator", f"Processing aggregator with {len(transform.fields)} fields", LogLevel.INFO)

            for field in transform.fields:
                is_literal = bool(
                    field.expression
                    and (re.match(r"^'[^']*'$", field.expression)
                         or re.match(r"^\d+(\.\d+)?$", field.expression))
                )
                if field.is_group_by or "GROUP BY" in field.port_type.upper():
                    group_by.append(field.name)
                    self.logger.log_transformation(instance.name, "Aggregator", f"GROUP BY: {field.name}", LogLevel.INFO)
                    # Literal GROUPBY fields (e.g. 0 as DUMMY, 'U' as HSHLD_AEM_IND)
                    # are kept in groupBy (so they survive agg()) AND added to the
                    # select below so the column exists in _agg_input.
                    if is_literal and "OUTPUT" in (field.port_type or "").upper():
                        _agg_literals.append({
                            "name": field.name,
                            "expression": field.expression,
                            "datatype": field.datatype,
                        })
                        self.logger.log_transformation(instance.name, "Aggregator", f"LITERAL: {field.name} = {field.expression}", LogLevel.INFO)
                elif field.expression and "OUTPUT" in field.port_type.upper():
                    # Literal constant (e.g. 0 as DUMMY, 'N' as FLAG) not marked as
                    # GROUP BY — treat as literal: add to select + groupBy so it
                    # survives agg(), rather than rendering 0.alias("DUMMY") in agg().
                    if is_literal:
                        _agg_literals.append({
                            "name": field.name,
                            "expression": field.expression,
                            "datatype": field.datatype,
                        })
                        group_by.append(field.name)
                        self.logger.log_transformation(instance.name, "Aggregator", f"LITERAL (non-GROUPBY): {field.name} = {field.expression}", LogLevel.INFO)
                    else:
                        # Replace $$ mapping variables with f-string placeholders
                        # (e.g. $$v_rpt_mth M-bM-^FM-^R {v_rpt_mth}) so they resolve at runtime
                        # from the Python variables loaded via job_params.
                        expr_text = field.expression
                        has_agg_dollar = False
                        for _mv_name in plan.mapping_variables:
                            if isinstance(_mv_name, str) and re.search(re.escape(_mv_name), expr_text, re.IGNORECASE):
                                _py_name = _mv_name.replace('$', '')
                                expr_text = re.sub(re.escape(_mv_name), '{' + _py_name + '}', expr_text, flags=re.IGNORECASE)
                                has_agg_dollar = True
                        # Signal to _translate_aggregation_expr to use f-string for expr()
                        pyspark_agg = self._translate_aggregation_expr(expr_text, field.name, use_fstr=has_agg_dollar)
                        if pyspark_agg:
                            aggregations[field.name] = pyspark_agg
                            self.logger.log_transformation(instance.name, "Aggregator", f"Aggregation: {field.name} = {pyspark_agg}", LogLevel.INFO)
                        elif "INPUT" in (field.port_type or "").upper():
                            # INPUT/OUTPUT field with pass-through expression that isn't
                            # an aggregation. Check that it is NOT used as an argument to
                            # any aggregate function before adding to groupBy (e.g.
                            # TIME_DMNS_KEY in MAX(TIME_DMNS_KEY) should NOT be grouped).
                            _agg_args = set()
                            for _af in transform.fields:
                                if _af.expression and "OUTPUT" in (_af.port_type or "").upper():
                                    _agg_match = re.search(
                                        r'\b(MAX|MIN|SUM|COUNT|AVG|FIRST|LAST|MEDIAN|STDDEV|VARIANCE)\s*\(\s*(\w+)\s*\)',
                                        _af.expression, re.IGNORECASE
                                    )
                                    if _agg_match:
                                        _agg_args.add(_agg_match.group(2))
                            if field.name not in _agg_args:
                                group_by.append(field.name)
                                self.logger.log_transformation(instance.name, "Aggregator", f"GROUP BY (implicit): {field.name}", LogLevel.INFO)
                            else:
                                self.logger.log_transformation(instance.name, "Aggregator", f"SKIP GROUP BY (agg arg): {field.name}", LogLevel.INFO)

            self.logger.log_transformation(instance.name, "Aggregator", f"Found {len(group_by)} GROUP BY keys, {len(aggregations)} aggregations", LogLevel.INFO)

        step = ApplyAggregatorStep(
            step_name=f"apply_{instance.name}",
            df_input=input_df,
            df_output=df_output,
            group_by=group_by,
            aggregations=aggregations
        )
        if _agg_selects:
            step.params["agg_selects"] = _agg_selects
        if _agg_literals:
            step.params["agg_literals"] = _agg_literals
        # Pass mapping variables so template can do runtime $$ substitution
        if plan.mapping_variables:
            step.params["mapping_variables"] = plan.mapping_variables
        if _pre_steps:
            _pre_steps.append(step)
            return _pre_steps
        return step

    def _translate_aggregation_expr(self, expr: str, field_name: str, use_fstr: bool = False) -> str:
        expr_upper = expr.upper().strip()

        # Handle simple pass-through (GROUP BY ports)
        if expr_upper == field_name.upper():
            return ""

        # Match outer aggregate function: FUNC_NAME(inner_expr)
        agg_funcs = {
            'MIN': 'min', 'MAX': 'max', 'SUM': 'sum',
            'COUNT': 'count', 'AVG': 'avg',
            'FIRST': 'first', 'LAST': 'last',
            'MEDIAN': 'percentile_approx', 'STDDEV': 'stddev',
            'VARIANCE': 'variance',
        }

        for func_name, pyspark_func in agg_funcs.items():
            _pat = r'^' + func_name + r'\s*\(\s*(.+)\s*\)\s*$'
            match = re.match(_pat, expr, re.IGNORECASE | re.DOTALL)
            if match:
                inner_expr = match.group(1).strip()

                # Detect Informatica conditional-aggregate syntax:
                #   COUNT(column, condition) / SUM(column, condition)
                # Split at the first top-level comma (not inside parentheses).
                _col_expr = inner_expr
                _cond_expr = ""
                _depth = 0
                for _i, _ch in enumerate(inner_expr):
                    if _ch == '(':
                        _depth += 1
                    elif _ch == ')':
                        _depth -= 1
                    elif _ch == ',' and _depth == 0:
                        _col_expr = inner_expr[:_i].strip()
                        _cond_expr = inner_expr[_i + 1:].strip()
                        break

                if _cond_expr and func_name in ('COUNT', 'SUM', 'MIN', 'MAX'):
                    # Translate the condition part separately
                    _translated_cond = self.expr_translator.translate(
                        _cond_expr, "aggregation", field_name
                    )
                    # Translate the column part
                    _translated_col = self.expr_translator.translate(
                        _col_expr, "aggregation", field_name
                    )
                    if re.match(r'^[A-Za-z_]\w*$', _translated_col):
                        _col_ref = f'col("{_translated_col}")'
                    else:
                        # Complex expression (e.g. "MTH_RENT_AMT_IN - VOID_RENT_AMT_IN")
                        # Use the translated expression with expr() instead of col()
                        _col_ref = f'expr("{_translated_col}")'
                    if use_fstr:
                        return f'{pyspark_func}(when(expr(f"""{_translated_cond}"""), {_col_ref}))'
                    else:
                        return f'{pyspark_func}(when(expr("""{_translated_cond}"""), {_col_ref}))'

                # Translate the inner expression (e.g. DECODE → CASE WHEN)
                translated_inner = self.expr_translator.translate(inner_expr, "aggregation", field_name)
                # Simple column reference → use string arg; complex expression → use expr()
                if re.match(r'^[A-Za-z_]\w*$', translated_inner):
                    _inner = f'"{translated_inner}"'
                else:
                    if use_fstr:
                        _inner = f'expr(f"""{translated_inner}""")'
                    else:
                        _inner = f'expr("""{translated_inner}""")'
                if func_name == 'MEDIAN':
                    return f'{pyspark_func}({_inner}, 0.5)'
                return f'{pyspark_func}({_inner})'

        # Fallback: try general translation
        translated = self.expr_translator.translate(expr, "aggregation", field_name)
        if use_fstr and '{' in translated:
            # Bare mapping variable placeholder like {v_REC_RLS_IND} would be
            # interpreted as a set literal in Python — wrap with lit() so the
            # runtime Python variable resolves as a Column expression.
            _mv_match = re.match(r'^\s*\{(\w+)\}\s*$', translated)
            if _mv_match:
                return f'lit({_mv_match.group(1)})'
            # Complex expression with embedded mapping variable — use f-string
            # expr() so {varname} is resolved at runtime via Python f-string.
            return f'expr(f"""{translated}""")'
        # Complex expression (e.g. CASE WHEN ... END) — always wrap with expr()
        # so the template's agg() renders valid Python. Returning the raw text
        # would produce `CASE WHEN ... END END.alias(...)` → SyntaxError.
        return f'expr("""{translated}""")'

    def _handle_sorter(self, instance: Instance, plan: IRPlan) -> Optional[IRStep]:
        input_df = self._get_input_df(instance.name)
        if not input_df:
            input_df = "df_input"

        df_output = self._get_df_name("df_srt", instance)
        self._register_df(instance, df_output)

        transform = self.transform_map.get(instance.transformation_name or instance.name)
        sort_columns = []

        if transform:
            for field in transform.fields:
                if not field.is_sort_key:
                    continue
                _dir = field.sort_direction.upper()
                if "DESC" in _dir:
                    _dir = "DESC"
                else:
                    _dir = "ASC"
                sort_columns.append({
                    "column": field.name,
                    "direction": _dir
                })
        # Collect connector field renames: upstream column -> Sorter port name
        _sorter_renames = []
        for conn in self.mapping.connectors:
            if conn.to_instance == instance.name and conn.from_field.lower() != conn.to_field.lower():
                _sorter_renames.append({
                    "from": conn.from_field,
                    "to": conn.to_field,
                })

        step = ApplySorterStep(
            step_name=f"apply_{instance.name}",
            df_input=input_df,
            df_output=df_output,
            sort_columns=sort_columns
        )
        if _sorter_renames:
            step.params["sorter_renames"] = _sorter_renames
        _sorter_cfg: Dict[str, Any] = {
            "rename_columns": [
                (_r.get("from"), _r.get("to"))
                for _r in step.params.get("sorter_renames", [])
                if (_r.get("from") or "").lower() != (_r.get("to") or "").lower()
            ],
            "sort_columns": step.params.get("sort_columns", []),
        }
        step.params["sorter_cfg"] = _sorter_cfg
        return step

    def _handle_union(self, instance: Instance, plan: IRPlan) -> Optional[IRStep]:
        inputs = self._get_all_input_dfs(instance.name)

        if not inputs:
            plan.add_warning(f"No inputs found for Union {instance.name}")
            return None

        df_output = self._get_df_name("df_un", instance)
        self._register_df(instance, df_output)

        transform = self.transform_map.get(instance.transformation_name or instance.name)
        output_columns = []
        flag_column = ""

        if transform:
            for field in transform.fields:
                if "OUTPUT" in field.port_type:
                    output_columns.append(field.name)
                    lower_name = field.name.lower()
                    if 'del' in lower_name and ('ins' in lower_name or 'upd' in lower_name) and 'flag' in lower_name:
                        flag_column = field.name

        # Build per-upstream-df select maps for the Union.
        # For each upstream DataFrame that has connectors to this Union,
        # determine which output columns it contributes and how to map its
        # column names → output column names via select+alias.
        _union_selects: List[Dict] = []
        if transform:
            # Collect output ports (ordered by definition order = position)
            _output_ports = [f.name for f in transform.fields
                           if f.group_name and f.group_name.upper() == 'OUTPUT']
            # Collect input ports per group, preserving position order
            _group_ports: Dict[str, List[str]] = {}
            for f in transform.fields:
                if f.group_name and f.group_name.upper() != 'OUTPUT' and 'INPUT' in (f.port_type or '').upper():
                    _grp = f.group_name.upper()
                    _group_ports.setdefault(_grp, []).append(f.name)
            # Build input_port → output_port map per group (by position)
            _grp_port_map: Dict[str, Dict[str, str]] = {}
            for _grp, _ports in _group_ports.items():
                _mapping = {}
                for _i, _in_port in enumerate(_ports):
                    if _i < len(_output_ports):
                        _mapping[_in_port] = _output_ports[_i]
                if _mapping:
                    _grp_port_map[_grp] = _mapping

            # For each input group, build ONE intermediate DF that selects+aliases
            # the needed columns from the best available upstream DataFrame.
            # Prefer downstream transformations (EXPTRANS, EXP_) over raw Router
            # outputs, since they inherit all upstream columns plus computed ones.
            _union_selects: List[Dict] = []
            for _grp, _port_map in _grp_port_map.items():
                # Collect connectors that feed this group
                _grp_connectors = []
                for conn in self.mapping.connectors:
                    if conn.to_instance == instance.name and conn.to_field in _port_map:
                        _grp_connectors.append(conn)
                if not _grp_connectors:
                    continue

                # Group connectors by upstream DataFrame, with Router resolution
                _up_entries: Dict[str, List[Dict]] = {}
                for conn in _grp_connectors:
                    _df = self.current_df_map.get(conn.from_instance)
                    if not _df:
                        _ii = self.instance_map.get(conn.from_instance)
                        if _ii and _ii.transformation_type == 'Router':
                            _rtr = self.transform_map.get(
                                _ii.transformation_name or conn.from_instance)
                            if _rtr:
                                for _tf in _rtr.fields:
                                    if _tf.name == conn.from_field and _tf.group_name:
                                        _key = f"{conn.from_instance}_{_tf.group_name}"
                                        _df = self.current_df_map.get(_key)
                                        break
                    if not _df:
                        continue
                    _up_entries.setdefault(_df, [])
                    if not any(e["from"] == conn.from_field and e["to"] == _port_map[conn.to_field]
                               for e in _up_entries[_df]):
                        _up_entries[_df].append({
                            "from": conn.from_field,
                            "to": _port_map[conn.to_field]
                        })

                if not _up_entries:
                    continue

                # Pick the "best" upstream df: prefer EXPTRANS/EXP_ (downstream
                # transformations that inherit all upstream columns), otherwise
                # the one with the most connector mappings.
                def _prefer_order(_name: str) -> int:
                    return 0 if 'exptrans' in _name.lower() or '_exp_' in _name.lower() else 1
                _best_df = max(_up_entries, key=lambda k: (-_prefer_order(k), -len(_up_entries[k])))

                # Build the select from ALL connector mappings for this group
                _selects = []
                for _out_port in _output_ports:
                    for _in_grp in _grp_connectors:
                        _out = _port_map.get(_in_grp.to_field)
                        if _out == _out_port:
                            _selects.append({"from": _in_grp.from_field, "to": _out_port})
                            break

                if _selects:
                    _safe_inst = re.sub(r'[^a-zA-Z0-9_]', '_', instance.name)
                    _intermediate_var = f"df_{_safe_inst}_{_grp.lower()}"
                    _union_selects.append({
                        "group_name": _grp,
                        "df_input": _best_df,
                        "selects": _selects,
                        "intermediate_var": _intermediate_var
                    })

        step = ApplyUnionStep(
            step_name=f"apply_{instance.name}",
            df_inputs=inputs,
            df_output=df_output,
            union_all=True
        )

        step.params["output_columns"] = output_columns
        step.params["flag_column"] = flag_column
        if _union_selects:
            step.params["union_selects"] = _union_selects

        self._union_output_dfs.add(df_output)

        if flag_column:
            step.comments.append(f"Normalizing flag column to: {flag_column}")

        _union_cfg: Dict[str, Any] = {
            "inputs": step.params.get("df_inputs", []),
            "flag_column": step.params.get("flag_column", ""),
            "output_columns": step.params.get("output_columns", []),
        }
        if step.params.get("union_selects"):
            _union_cfg["union_selects"] = step.params["union_selects"]
        step.params["union_cfg"] = _union_cfg

        return step

    def _handle_router(self, instance: Instance, plan: IRPlan) -> List[IRStep]:
        steps = []

        input_df = self._get_input_df(instance.name)
        if not input_df:
            input_df = "df_input"

        transform = self.transform_map.get(instance.transformation_name or instance.name)
        if not transform:
            plan.add_warning(f"Router transformation not found: {instance.name}")
            return steps

        # Build condition map from parsed GROUP elements (EXPRESSION attribute)
        group_condition_map = {}
        for g in transform.groups:
            gtype = g.type.upper()
            if gtype in ("OUTPUT", "OUTPUT/DEFAULT") and g.name.upper() != "INPUT":
                group_condition_map[g.name] = g.expression or ""

        groups = []
        group_conditions = {}
        discovered = set()
        trans_name = instance.transformation_name or instance.name

        for field in transform.fields:
            if "OUTPUT" in field.port_type.upper():
                group_name = field.group_name or "DEFAULT"
                if group_name in discovered:
                    continue
                discovered.add(group_name)

                # Get condition from parsed GROUP expressions first
                condition = group_condition_map.get(group_name, "")
                if not condition:
                    # Fallback: existing table_attributes patterns
                    cond_patterns = [
                        f"{group_name} Group Filter Condition",
                        f"Router Group {group_name}",
                        f"GROUP_FILTER_CONDITION_{group_name}",
                        f"{group_name}_GROUP_FILTER_CONDITION",
                        f"Filter Condition {group_name}",
                        f"{group_name}",
                    ]
                    for pattern in cond_patterns:
                        condition = transform.table_attributes.get(pattern, "")
                        if condition:
                            break
                    if not condition and field.expression:
                        condition = field.expression

                group_conditions[group_name] = condition

        # Build connector field remap: upstream column name → Router input port name.
        # The actual DataFrame has upstream column names; REF_FIELD uses Router port names.
        _rtr_field_remap = {}
        for conn in self.mapping.connectors:
            if conn.to_instance == instance.name:
                _rtr_field_remap[conn.to_field] = conn.from_field

        # Multi-input detection (v2026.08.11): group the connectors feeding this
        # router by upstream instance. Each feed maps upstream column names to
        # Router INPUT port names (connector to_field); aliases only record
        # renames (from_field != to_field). Multi-input routers are translated
        # as a UNION of all feeds (missing ports -> NULL) with ORDERED group
        # conditions (first match wins, per XML GROUP ORDER); single-input
        # routers keep the legacy translation unchanged.
        _feed_conns = {}
        for conn in self.mapping.connectors:
            if conn.to_instance == instance.name:
                _feed_conns.setdefault(conn.from_instance, []).append(conn)
        _feed_specs = []  # [(df_name, {from_field: to_field, ...}), ...]
        for _uinst in _feed_conns:
            _df = None
            if _uinst in self._direct_df_map:
                _df = self._direct_df_map[_uinst]
            elif _uinst in self.current_df_map:
                _df = self.current_df_map[_uinst]
            if not _df:
                plan.add_warning(
                    f"Router {instance.name}: upstream {_uinst} DataFrame unresolved; "
                    "falling back to legacy single-input translation")
                _feed_specs = []
                break
            _aliases = {}
            for _c in _feed_conns[_uinst]:
                if _c.from_field != _c.to_field:
                    _aliases[_c.from_field] = _c.to_field
            _feed_specs.append((_df, _aliases))
        _multi_feed = len(_feed_specs) > 1

        # Build column rename map per group: in Informatica, Router output fields
        # get suffixed names (e.g. PRPL_CSSA_APLY_ID_TYPE_CODE1 / _CODE2) via REF_FIELD.
        # PySpark .filter() preserves input column names, so we must rename after filter.
        group_renames = {}  # group_name -> [{"from": actual_col_name, "to": output_name}, ...]
        for field in transform.fields:
            if "OUTPUT" in field.port_type.upper():
                gname = field.group_name or "DEFAULT"
                if gname not in group_renames:
                    group_renames[gname] = []
                if field.ref_field and field.name.lower() != field.ref_field.lower():
                    # REF_FIELD is the Router's input port name. The union
                    # (multi-feed) translation carries INPUT PORT names in the
                    # group df, so rename from the input port directly; the
                    # legacy single-input path maps it to the actual upstream
                    # column name.
                    if _multi_feed:
                        actual_col = field.ref_field
                    else:
                        actual_col = _rtr_field_remap.get(field.ref_field, field.ref_field)
                    # Skip renames where the resolved source equals the output
                    # name (case-insensitive — Spark resolution is case-insensitive)
                    if actual_col.lower() != field.name.lower():
                        group_renames[gname].append({
                            "from": actual_col,
                            "to": field.name
                        })

        all_conditions = []

        for group_name, condition in group_conditions.items():
            df_output = self._df_name("df_rtr", instance.name, group_name)

            self.current_df_map[f"{instance.name}_{group_name}"] = df_output
            self.current_df_map[f"{trans_name}_{group_name}"] = df_output
            self.current_df_map[f"{instance.name}{group_name}"] = df_output
            self.current_df_map[f"{trans_name}{group_name}"] = df_output
            self.current_df_map[f"{instance.name}.{group_name}"] = df_output
            self.current_df_map[f"{trans_name}.{group_name}"] = df_output
            self._direct_df_map[f"{instance.name}_{group_name}"] = df_output
            self._direct_df_map[f"{trans_name}_{group_name}"] = df_output

            if condition:
                translated = self.expr_translator.translate_for_filter(condition, f"router_{group_name}")
                all_conditions.append(translated)
                # For $$ mapping variables: extract inner text for runtime .replace()
                _filter_inner = ""
                if '$$' in translated:
                    _m_i = re.match(r'expr\(["\'](.+)["\']\)', translated)
                    if _m_i:
                        _filter_inner = _m_i.group(1)
            else:
                translated = ""
                _filter_inner = ""

            _grp = {
                "name": group_name,
                "df_output": df_output,
                "condition": translated,
                "renames": group_renames.get(group_name, [])
            }
            if _filter_inner:
                _grp["filter_inner"] = self._normalize_sql_text(_filter_inner, plan)
            groups.append(_grp)

        if groups and not _multi_feed:
            default_group = next((g for g in groups if g["name"] == "DEFAULT"), None)
            if default_group and all_conditions:
                _valid = [c for c in all_conditions if c]
                negated = " & ".join([f"~({c})" for c in _valid])
                default_group["condition"] = negated
                # DEFAULT group: build list of negated conditions for chained filter
                # (PySpark ~ inside expr() is bitwise NOT, not logical NOT)
                if any('$$' in c for c in _valid):
                    _d_negated = []
                    for _g in groups:
                        if _g.get("filter_inner") and '$$' in _g["filter_inner"]:
                            _d_negated.append({"name": _g["name"], "inner": _g["filter_inner"]})
                    if _d_negated:
                        default_group["default_negated"] = _d_negated

        if _multi_feed:
            # ORDERED group split (first match wins): sort output groups by the
            # XML GROUP ORDER attribute; DEFAULT is evaluated last. Each group's
            # filter is the negated conjunction of all PRIOR conditions AND its
            # own condition; a TRUE/empty condition group is the fallback for
            # everything not yet matched, so DEFAULT becomes lit(False) whenever
            # a TRUE group exists. NULL never matches (translate_for_filter
            # renders '= 1' comparisons; rows with NULL fall through to later
            # groups / DEFAULT). $$ variable conditions compose through the
            # _rtr_<group>_filter runtime variables defined in the template.
            _g_order = {g.name: g.order for g in transform.groups}
            _ordered = sorted(
                [g for g in groups if g["name"].upper() != "DEFAULT"],
                key=lambda g: _g_order.get(g["name"], 999))
            _default_g = next((g for g in groups if g["name"].upper() == "DEFAULT"), None)
            if _default_g:
                _ordered.append(_default_g)

            _neg_parts = []
            _had_true = False
            for _g in _ordered:
                _c = _g.get("condition") or ""
                _is_true = (not _c) or _c.strip().upper() in ("EXPR(\"TRUE\")", "TRUE")
                _expr_c = f"expr(_rtr_{_g['name'].lower()}_filter)" if _g.get("filter_inner") else _c
                if _g["name"].upper() == "DEFAULT":
                    if _had_true:
                        _g["condition"] = "lit(False)"
                    else:
                        _g["condition"] = " & ".join(_neg_parts) if _neg_parts else "expr(\"FALSE\")"
                    continue
                if _is_true:
                    _had_true = True
                    _g["condition"] = " & ".join(_neg_parts) if _neg_parts else "expr(\"TRUE\")"
                elif _had_true:
                    # A prior TRUE/fallback group already consumed every
                    # remaining row; later groups are always empty.
                    _g["condition"] = "lit(False)"
                else:
                    _g["condition"] = " & ".join(_neg_parts + [_expr_c]) if _neg_parts else _expr_c
                    _neg_parts.append(f"~({_expr_c})")

        plan.router_outputs[instance.name] = [g["df_output"] for g in groups]

        steps.append(ApplyRouterStep(
            step_name=f"apply_{instance.name}",
            df_input=("df_rtr_input" if _multi_feed else input_df),
            groups=groups
        ))
        if _multi_feed:
            steps[-1].params["multi_feed"] = True
            steps[-1].params["feeds"] = _feed_specs

        # Component-method config: lib.router owns the runtime semantics
        # (multi-feed union input, per-group filters, connector renames, $$
        # substitution via filter_inner). The template reads ONLY this key.
        _rtr_step = steps[-1]
        _rtr_groups = []
        for _g in _rtr_step.params.get("groups", []):
            _cond = _g.get("condition") or ""
            # Conditions arrive as translated Python source (`expr("...")`);
            # lib.router wraps its own condition text in expr() at runtime,
            # so the cfg stores the RAW inner text (same regex as _handle_filter).
            _m = re.match(r'expr\("(.*)"\)$', _cond)
            _rtr_group = {
                "name": _g["name"],
                "df_output": _g.get("df_output", "df_group"),
                "condition": _m.group(1) if _m else _cond,
                "filter_inner": _g.get("filter_inner", ""),
                # lib.router's default_negated branch chains ~_conds[name] —
                # it keys conditions by GROUP NAME (the handler's dict list
                # carries name + inner; only the name is needed).
                "default_negated": [
                    _d["name"] for _d in _g.get("default_negated", [])
                ],
                "renames": [
                    (_r["from"], _r["to"]) for _r in _g.get("renames", [])
                ],
            }
            _is_default = _g["name"].upper() == "DEFAULT"
            if _is_default and _rtr_group["default_negated"]:
                # $$-case DEFAULT negation chain: names only, drop the
                # composed condition text (branch is exclusive in lib.router).
                _rtr_group["condition"] = ""
            elif _is_default and _rtr_group["condition"] and "~(" in _rtr_group["condition"]:
                # Composed ~(expr("...")) & ... negation chain (no $$): express
                # via the runtime's default_negated branch (chained
                # filter(~_conds[name])) — same rows as the old single
                # filter(~expr(...) & ...) call. Names = every non-default
                # group that carries a condition / filter_inner.
                _rtr_group["default_negated"] = [
                    _x["name"] for _x in _rtr_step.params.get("groups", [])
                    if _x["name"].upper() != "DEFAULT"
                    and (_x.get("condition") or _x.get("filter_inner"))
                ]
                _rtr_group["condition"] = ""
            elif _rtr_group["condition"] == "lit(False)":
                # multi-feed groups after a TRUE/fallback group are always
                # empty; lib.router wraps conditions in expr(), so SQL FALSE.
                _rtr_group["condition"] = "FALSE"
            elif _rtr_group["condition"] == "lit(True)":
                _rtr_group["condition"] = "TRUE"
            elif not _m and _cond and "expr(" in _cond:
                # multi-feed ORDERED split composition: ~(expr("a")) & expr("b")
                # → raw SQL `NOT (a) AND (b)` (lib.router wraps in expr()).
                _t = re.sub(r'expr\("([^"]*)"\)', r"\1", _cond)
                _t = _t.replace("~(", "NOT (").replace(" & ", " AND ")
                _rtr_group["condition"] = _t
            _rtr_groups.append(_rtr_group)
        _rtr_cfg: Dict[str, Any] = {"groups": _rtr_groups}
        if _rtr_step.params.get("multi_feed"):
            _rtr_cfg["multi_feed"] = True
            _rtr_cfg["feeds"] = _rtr_step.params.get("feeds", [])
        # $$ mapping variables inside group filter_inner texts: map the key to
        # the RUNTIME IDENTIFIER ($$v_x → v_x) — the template renders the
        # value unquoted (loaded override-aware from UTL_JOB_PARAM). Same rule
        # as lib.filter's cfg.
        _rtr_subs: Dict[str, str] = {}
        for _g in _rtr_step.params.get("groups", []):
            _fi = _g.get("filter_inner") or ""
            if '$$' in _fi and plan and plan.mapping_variables:
                for _var, _val in plan.mapping_variables.items():
                    if _var in _fi:
                        _rtr_subs[_var] = _var.replace('$', '')
        if _rtr_subs:
            _rtr_cfg["substitutions"] = _rtr_subs
        _rtr_step.params["router_cfg"] = _rtr_cfg

        return steps

    def _handle_normalizer(self, instance: Instance, plan: IRPlan) -> Optional[IRStep]:
        input_df = self._get_input_df(instance.name)
        if not input_df:
            input_df = "df_input"

        df_output = self._get_df_name("df_nrm", instance)
        self._register_df(instance, df_output)

        transform = self.transform_map.get(instance.transformation_name or instance.name)

        # Normalizer: identify GENERATED_KEY, non-repeating passthrough
        # columns, and repeating field groups (e.g. DIAG_1, DIAG_2, DIAG_3).
        key_name = "GENERATED_KEY"
        passthrough_cols = []
        repeating_groups = {}  # base_name -> [field1_name, field2_name, ...]

        if transform:
            self.logger.log_transformation(instance.name, "Normalizer",
                f"Processing normalizer with {len(transform.fields)} fields", LogLevel.INFO)

            for field in transform.fields:
                name = field.name
                # GENERATED_KEY is the sequence counter
                if name.upper() == "GENERATED_KEY":
                    key_name = name
                    continue
                # GCID_ fields are group occurrence IDs — pass through
                if name.upper().startswith("GCID_"):
                    passthrough_cols.append({"name": name, "datatype": field.datatype})
                    continue
                # Check if this is a repeating field (_N suffix)
                match = re.search(r'^(.+)_(\d+)$', name)
                if match:
                    base = match.group(1)
                    if base not in repeating_groups:
                        repeating_groups[base] = []
                    repeating_groups[base].append(name)
                else:
                    passthrough_cols.append({"name": name, "datatype": field.datatype})

            # Build the grouped structure: for each repeating group,
            # create an entry with base_name and the ordered variant names.
            grouped = []
            for base_name in sorted(repeating_groups.keys()):
                members = sorted(repeating_groups[base_name],
                                 key=lambda x: int(re.search(r'_(\d+)$', x).group(1)))
                grouped.append({
                    "base_name": base_name,
                    "members": members,
                })
        else:
            grouped = []

        step = ApplyNormalizerStep(
            step_name=f"apply_{instance.name}",
            df_input=input_df,
            df_output=df_output,
            key_name=key_name,
            normalizer_fields=grouped,
        )
        step.params["passthrough_cols"] = passthrough_cols
        return step

    def _handle_rank(self, instance: Instance, plan: IRPlan) -> Optional[IRStep]:
        input_df = self._get_input_df(instance.name)
        if not input_df:
            input_df = "df_input"

        df_output = self._get_df_name("df_rank", instance)
        self._register_df(instance, df_output)

        transform = self.transform_map.get(instance.transformation_name or instance.name)

        group_by = []
        rank_expression = ""
        rank_port_name = "RANKINDEX"
        top_n = 1
        sort_direction = "DESC"
        rank_function = "dense_rank"  # Informatica default: tied values share rank

        if transform:
            self.logger.log_transformation(instance.name, "Rank",
                f"Processing rank with {len(transform.fields)} fields", LogLevel.INFO)

            for field in transform.fields:
                # GROUP BY ports determine partition columns
                if field.is_group_by or "GROUP BY" in field.port_type.upper():
                    group_by.append(field.name)
                # The Rank port holds the ranking expression/value
                elif field.name.upper() in ("RANK", "RANKINDEX", "RANKINDEX_1"):
                    rank_port_name = field.name
                    if field.expression:
                        rank_expression = field.expression
                        # Translate the expression
                        translated = self.expr_translator.translate(
                            rank_expression, "rank", field.name
                        )
                        if translated:
                            rank_expression = translated

            # Read NumberOfRanks / TopN from table attributes
            n_attr = transform.table_attributes.get("Number of Ranks", "")
            if n_attr and n_attr.isdigit():
                top_n = int(n_attr)

            # Determine sort direction — Informatica Rank is typically
            # TOP (DESC) or BOTTOM (ASC)
            rank_dir = transform.table_attributes.get("Rank Direction", "").upper()
            if rank_dir in ("ASC", "ASCENDING", "BOTTOM"):
                sort_direction = "ASC"

            # Detect tie handling — Informatica Rank assigns tied values
            # the same rank by default. When "Ties" is absent or "Yes",
            # use dense_rank. When "No", use row_number.
            ties = transform.table_attributes.get("Ties", "").upper()
            if ties in ("NO", "FALSE", "NONE"):
                rank_function = "row_number"

        step = ApplyRankStep(
            step_name=f"apply_{instance.name}",
            df_input=input_df,
            df_output=df_output,
            group_by=group_by,
            rank_expression=rank_expression,
            rank_port_name=rank_port_name,
            top_n=top_n,
            sort_direction=sort_direction,
            rank_function=rank_function,
        )
        if plan.mapping_variables:
            step.params["mapping_variables"] = plan.mapping_variables
        if group_by:
            plan.needs_window_import = True
        return step

    def _handle_sequence(self, instance: Instance, plan: IRPlan) -> Optional[IRStep]:
        input_df = self._get_input_df(instance.name)

        transform = self.transform_map.get(instance.transformation_name or instance.name)
        seq_name = "NEXTVAL"
        start_value = 1

        if transform:
            for field in transform.fields:
                if field.name == "NEXTVAL":
                    seq_name = field.name
                    break
            start_value = int(transform.table_attributes.get("Start Value", 1))

        if not input_df:
            # Connected sequence generator (no upstream): it only provides the
            # NEXTVAL port to downstream transformations (e.g. a FILTER).
            # Emitting a standalone step with an undefined df_input produced
            # `df_input.withColumn(...)` → NameError, and registering df_SEQ_*
            # made the downstream filter resolve its input from the sequence
            # instead of the real data path. Instead, record the attachment so
            # the consumer's step adds the sequence column on its output
            # (post-filter), and skip registration entirely.
            for _conn in self.mapping.connectors:
                if _conn.from_instance == instance.name:
                    self._sequence_attachments.setdefault(_conn.to_instance, []).append(
                        {"col": _conn.to_field or seq_name, "start": start_value})
            return None

        df_output = self._get_df_name("df_seq", instance)
        self._register_df(instance, df_output)

        step = ApplySequenceStep(
            step_name=f"apply_{instance.name}",
            df_input=input_df,
            df_output=df_output,
            sequence_name=seq_name,
            start_value=start_value
        )
        step.params["sequence_cfg"] = {
            "output_col": step.params.get("sequence_name", "NEXTVAL"),
            "start": step.params.get("start_value", 1),
        }
        return step

    def _handle_update_strategy(self, instance: Instance, plan: IRPlan) -> List[IRStep]:
        input_df = self._get_input_df(instance.name)
        if not input_df:
            input_df = "df_input"

        # Update Strategy can be fed by a data stream (e.g. FILTRANS1) AND a
        # mapplet output that supplies the strategy field (e.g. MPLT_DLKP_CACHE_STATUS
        # with OUT_V_UPD_STRATEGY_STATUS). Joining all upstreams is required;
        # otherwise the generated _update_flag references a column the primary
        # input does not carry (UNRESOLVED_COLUMN).
        all_inputs = self._get_all_input_dfs(instance.name)
        extra_inputs = [df for df in all_inputs if df != input_df]
        pre_steps: List[IRStep] = []
        if extra_inputs and input_df != "df_input":
            _cur_df = input_df
            _df_parent = self._build_df_parent_map(list(plan.steps))
            _step_map = {s.df_output: s for s in plan.steps if s.df_output}
            _needed = set()
            for _c in self.mapping.connectors:
                if _c.to_instance == instance.name and _c.from_instance in self.current_df_map:
                    _needed.add(_c.from_field)
            for _i, _extra_df in enumerate(extra_inputs):
                _redundant_to = self._redundant_merge_df(
                    _df_parent, _step_map, _cur_df, _extra_df, _needed)
                if _redundant_to is not None:
                    _cur_df = _redundant_to
                    continue
                _merge_df = self._df_name("df_merge", instance.name, _i)
                _merge_step = ApplyLookupStep(
                    step_name=f"merge_{instance.name}_{_i}",
                    df_input=_cur_df,
                    df_output=_merge_df,
                    lookup_df=_extra_df,
                    join_predicates=[],
                    join_expr="__common_cols__",
                    output_columns=[],
                    lookup_type="left",
                )
                pre_steps.append(_merge_step)
                _df_parent[_merge_df] = _cur_df
                _step_map[_merge_df] = _merge_step
                _cur_df = _merge_df
            input_df = _cur_df

        df_output = self._get_df_name("df_upd", instance)
        self._register_df(instance, df_output)

        transform = self.transform_map.get(instance.transformation_name or instance.name)
        strategy_expr = "DD_INSERT"

        if transform:
            strategy_expr = transform.table_attributes.get("Update Strategy Expression", "DD_INSERT")

        has_update = "DD_UPDATE" in strategy_expr.upper()
        has_delete = "DD_DELETE" in strategy_expr.upper()

        step = ApplyUpdateStrategyStep(
            step_name=f"apply_{instance.name}",
            df_input=input_df,
            df_output=df_output,
            strategy_expression=strategy_expr
        )

        step.params["has_update"] = has_update
        step.params["has_delete"] = has_delete
        step.params["needs_merge"] = has_update or has_delete

        # Determine if strategy_expr is a field reference (e.g. "UPDATE_FLAG")
        # or a static DD_* value. Field references need dynamic when() logic.
        _is_field_ref = bool(
            strategy_expr
            and "DD_" not in strategy_expr.upper()
            and re.match(r'^[A-Za-z_]\w*$', strategy_expr)
        )
        if _is_field_ref:
            step.params["strategy_field"] = strategy_expr
            step.comments.append(f"Strategy from field '{strategy_expr}' — dynamic DD_INSERT/UPDATE/DELETE")
        elif has_delete:
            # Static DD_DELETE — no _update_flag split needed; the write step
            # deletes all rows by primary key directly.
            step.params["static_dd"] = "DD_DELETE"
            step.comments.append("DD_DELETE: all rows deleted by primary key")
        elif has_update:
            # Static DD_UPDATE — no _update_flag split needed; the write step
            # batch-updates all rows by primary key directly.
            step.params["static_dd"] = "DD_UPDATE"
            step.comments.append("DD_UPDATE: all rows updated by primary key")
        else:
            # Static DD_INSERT — pass-through; the write step appends directly.
            step.params["static_dd"] = "DD_INSERT"
            step.comments.append("DD_INSERT: all rows appended directly")

        return pre_steps + [step]

    def _handle_transaction_control(self, instance: Instance, plan: IRPlan) -> Optional[IRStep]:
        input_df = self._get_input_df(instance.name)
        if not input_df:
            input_df = "df_input"

        df_output = self._get_df_name("df_tc", instance)
        self._register_df(instance, df_output)

        transform = self.transform_map.get(instance.transformation_name or instance.name)
        control_expr = ""

        if transform:
            control_expr = transform.table_attributes.get("Transaction Control", "")
            if not control_expr:
                # Fallback: look for expression in field definitions
                for field in transform.fields:
                    if field.expression and "OUTPUT" in field.port_type.upper():
                        control_expr = field.expression
                        break

        step = ApplyTransactionControlStep(
            step_name=f"apply_{instance.name}",
            df_input=input_df,
            df_output=df_output,
            control_expression=control_expr,
        )
        step.comments.append(
            "Transaction Control is a no-op in PySpark — Spark manages "
            "transactions at the batch level"
        )
        if control_expr:
            step.comments.append(f"Original control expression: {control_expr}")
        return step

    def _handle_target(self, instance: Instance, plan: IRPlan) -> List[IRStep]:
        steps = []

        input_df = self._get_input_df(instance.name)
        if not input_df:
            plan.add_warning(f"No input DataFrame found for target {instance.name}")
            input_df = "df_final"
        else:
            # Extra upstream DataFrames are already available via the chain.
            pass

        target_name = instance.transformation_name or self._normalize_instance_to_object_name(instance.name)
        target = None

        for t in self.mapping.targets:
            if t.name == target_name or target_name.startswith(t.name):
                target = t
                break

        target_config = self._get_target_config(target.name if target else target_name)

        target_transform = self.transform_map.get(instance.transformation_name or instance.name)
        if not target_transform:
            target_transform = self.transform_map.get(target_name)
        if not target_transform and target:
            target_transform = self.transform_map.get(target.name)
        for trans in self.mapping.transformations:
            if trans.type == "Target Definition" and trans.name == target_name:
                target_transform = trans
                break

        pre_sql = ""
        post_sql = ""

        if target_transform:
            pre_sql = target_transform.table_attributes.get("Pre SQL", "")
            if not pre_sql:
                pre_sql = target_transform.table_attributes.get("pre sql", "")
            post_sql = target_transform.table_attributes.get("Post SQL", "")
            if not post_sql:
                post_sql = target_transform.table_attributes.get("post sql", "")

        if target and not pre_sql:
            for k, v in getattr(target, 'table_attributes', {}).items():
                if k.lower() == 'pre sql' and v:
                    pre_sql = v
                    break

        conn_alias = self._resolve_connection_alias(
            instance_name=instance.name,
            target_name=target.name if target else target_name,
            target_config=target_config,
            plan=plan,
            is_target=True
        )

        if pre_sql:
            steps.append(ExecuteSQLStep(
                step_name=f"pre_sql_{instance.name}",
                sql_statement=pre_sql,
                sql_type="pre",
                connection_alias=conn_alias
            ))

        mapped_columns = set()
        field_map = {}  # target_field -> source_field for connectors to this target
        for conn in self.mapping.connectors:
            if conn.to_instance == instance.name:
                mapped_columns.add(conn.to_field)
                # Skip identity entries (source == target, case-insensitive) —
                # no rename needed and drop("X").withColumnRenamed("X", "X")
                # would silently drop the column.
                if conn.to_field.lower() != conn.from_field.lower():
                    field_map[conn.to_field] = conn.from_field

        unmapped_columns = []
        if target:
            for field in target.fields:
                if field.name not in mapped_columns:
                    unmapped_columns.append(field.name)

        sink_type = "delta"
        path = ""
        table_name = target.name if target else target_name
        mode = "append"

        # Detect flat file targets — use csv sink, path comes from objects.yml
        if target and target.database_type and "flat" in target.database_type.lower():
            sink_type = "csv"
            table_name = target.name

        if target_config:
            if target_config.output_format:
                sink_type = target_config.output_format
            if target_config.destination_path:
                path = target_config.destination_path
            if target_config.table_name:
                table_name = target_config.table_name
            if target_config.write_mode:
                mode = target_config.write_mode

        target_columns = []
        target_column_types = {}
        if target:
            target_columns = [f.name for f in target.fields]
            for f in target.fields:
                target_column_types[f.name] = f.datatype

        control_columns = ["_update_flag", "_update_strategy", "del_ins_upd_flag",
                          "del_upd_ins_flag", "ins_upd_del_flag", "DEL_INS_UPD_FLAG",
                          "DEL_UPD_INS_FLAG", "INS_UPD_DEL_FLAG"]
        columns_to_drop = [c for c in control_columns if c.lower() not in [tc.lower() for tc in target_columns]]

        # Detect if upstream is an Update Strategy (static DD_* or dynamic field)
        is_delete = False
        delete_keys = []
        has_update_flag = False
        static_dd = ""
        # Primary keys from the target definition (KEYTYPE contains PRIMARY) —
        # used as the UPDATE/DELETE key columns, matching Informatica semantics
        # (DD_UPDATE updates by key, DD_DELETE deletes by key).
        target_keys = [f.name for f in (target.fields if target else [])
                       if "PRIMARY" in (f.key_type or "").upper()]
        for connector in self.mapping.connectors:
            if connector.to_instance == instance.name:
                from_trans = self.transform_map.get(connector.from_instance)
                if from_trans:
                    strategy_expr = from_trans.table_attributes.get("Update Strategy Expression", "")
                    if "DD_DELETE" in strategy_expr.upper():
                        is_delete = True
                        static_dd = "DD_DELETE"
                        if target_keys:
                            delete_keys = list(target_keys)
                        elif connector.from_field and connector.from_field not in delete_keys:
                            delete_keys.append(connector.from_field)
                        break
                    # Static DD_UPDATE: mark all rows for update (by target primary key)
                    elif "DD_UPDATE" in strategy_expr.upper():
                        has_update_flag = True
                        static_dd = "DD_UPDATE"
                        if target_keys:
                            delete_keys = list(target_keys)
                        elif connector.from_field and connector.from_field not in delete_keys:
                            delete_keys.append(connector.from_field)
                        break
                    # Dynamic strategy from field (e.g. UPDATE_FLAG column)
                    elif strategy_expr and "DD_" not in strategy_expr.upper():
                        has_update_flag = True
                        if target_keys and not delete_keys:
                            delete_keys = list(target_keys)

        write_step = WriteTargetStep(
            step_name=f"write_{instance.name}",
            df_input=input_df,
            sink_type=sink_type,
            path=path,
            table_name=table_name,
            mode=mode,
            format=sink_type
        )

        write_step.params["connection_alias"] = conn_alias
        # Targets fed directly by a union may carry NullType columns (from
        # unionByName allowMissingColumns or lit(None) fills upstream) that
        # JDBC cannot map — cast them to StringType at write time only here.
        if input_df in self._union_output_dfs:
            write_step.params["cast_nulltype"] = True
        if static_dd:
            write_step.params["static_dd"] = static_dd
        if is_delete:
            write_step.params["is_delete"] = True
            write_step.params["delete_keys"] = delete_keys
            write_step.comments.append("DD_DELETE strategy detected — will DELETE matching rows instead of INSERT")
        if has_update_flag:
            write_step.params["has_update_flag"] = True
            write_step.params["delete_keys"] = delete_keys or [c.lower() for c in target_columns]
            if static_dd == "DD_UPDATE":
                write_step.comments.append("Static DD_UPDATE detected — batch UPDATE all rows by primary key")
            else:
                write_step.comments.append("Dynamic Update Strategy detected — will INSERT/UPDATE/DELETE based on _update_flag")

        if columns_to_drop:
            write_step.params["drop_columns"] = columns_to_drop
            write_step.comments.append(f"Dropping control columns before write: {', '.join(columns_to_drop)}")

        if target_columns:
            write_step.params["target_columns"] = target_columns
            write_step.params["target_column_types"] = target_column_types
            if field_map:
                write_step.params["field_map"] = field_map
            write_step.comments.append("Selecting only target-mapped columns with correct casing and data types")

        if unmapped_columns:
            write_step.params["unmapped_columns"] = unmapped_columns
            write_step.comments.append(f"Adding NULL for unmapped columns: {', '.join(unmapped_columns)}")

        if post_sql:
            write_step.params["post_sql"] = post_sql

        steps.append(write_step)

        return steps

    def _get_input_df(self, instance_name: str) -> Optional[str]:
        upstream_instances = []
        _upstream_fields: Dict[str, List[str]] = {}
        for connector in self.mapping.connectors:
            if connector.to_instance == instance_name:
                upstream_instances.append(connector.from_instance)
                _upstream_fields.setdefault(connector.from_instance, []).append(connector.from_field)

        _matched_df = None
        _first_match = None
        _matched_values = []
        for from_inst in upstream_instances:
            if from_inst in self.current_df_map:
                _v = self.current_df_map[from_inst]
                _matched_values.append(_v)
                if _first_match is None:
                    _first_match = _v
                _matched_df = _v
        if _matched_df:
            # When a FILTER's connector upstream is a Lookup Procedure, its
            # registered df IS the chain/merge output (upstream columns +
            # lookup columns). Prefer that over raw SQ/expression DFs or an
            # arbitrary first match, otherwise filters like FILTRANS_STS (fed
            # by DLKP_SOR_STS + EXP_BK) lose END_DATE/NewLookupRow. With
            # multiple lookup upstreams, the last-processed lookup carries all
            # previous merges. This preference is deliberately scoped to
            # filters/routers: applying it to expressions/mapplet inputs would
            # reorder which df is primary in multi-input merge pre-steps and
            # change left-join semantics.
            _target_inst = self.instance_map.get(instance_name)
            if _target_inst is not None and self._resolve_transformation_type(_target_inst) in ("Filter", "Router"):
                _lookup_ups = []
                for _u in dict.fromkeys(upstream_instances):
                    if _u in self._lookup_order and _u in self.current_df_map:
                        _lookup_ups.append(_u)
                if _lookup_ups:
                    _last_lkp = max(_lookup_ups, key=self._lookup_order.index)
                    return self.current_df_map[_last_lkp]
            # A deferred instance must see its direct upstream outputs, not the
            # chain/merge df that a lookup registered under the upstream name.
            # Lookup-fed filters/routers were already handled above.
            if self._prefer_direct_input:
                _direct_vals = []
                for _u in dict.fromkeys(upstream_instances):
                    if _u in self._direct_df_map:
                        _direct_vals.append(self._direct_df_map[_u])
                if _direct_vals:
                    if len(_direct_vals) == 1:
                        return _direct_vals[0]
                    _direct_non_chain = [v for v in _direct_vals
                                         if not v.startswith(('df_lkp_merge', 'df_merge', 'df_sq_'))]
                    if _direct_non_chain:
                        return _direct_non_chain[0]
                    return _direct_vals[0]
            if len(_matched_values) > 1:
                for _v in _matched_values:
                    if _v.startswith('df_lkp_result') or _v.startswith('df_jnr_result'):
                        return _v
                # When multiple upstreams exist (e.g. both LKP_DDS_DMNS_PRPTY_TYPE
                # and EXPTRANS1 feed into FILTRANS), prefer the most downstream
                # DataFrame (the full transformation, not a raw chain/merge).
                for _v in _matched_values:
                    if not _v.startswith(('df_lkp_merge', 'df_merge', 'df_sq_')):
                        # All matched non-chain values pass this check. If there's
                        # only one, return it. If multiple, prefer the one whose
                        # instance is fed by another upstream instance (downstream).
                        _non_chain = [v for v in _matched_values
                                      if not v.startswith(('df_lkp_merge', 'df_merge', 'df_sq_'))]
                        if len(_non_chain) == 1:
                            return _non_chain[0]
                        # Multiple non-chain matches: prefer the downstream instance.
                        # Check if any upstream instance feeds into another via connectors.
                        _up_names = {inst: df for inst, df in zip(upstream_instances, _matched_values)}
                        for _a in _up_names:
                            for _b in _up_names:
                                if _a != _b and any(
                                    c.from_instance == _a and c.to_instance == _b
                                    for c in self.mapping.connectors
                                ):
                                    return _up_names[_b]
                        return _first_match
                # Multiple non-special matches: return the first one (main pipeline)
                return _first_match
            return _matched_df

        # Router-aware resolution: if the upstream is a Router, use the
        # connector's from_field to determine which output group to use.
        for from_inst in upstream_instances:
            _ii = self.instance_map.get(from_inst)
            if _ii and _ii.transformation_type == 'Router':
                _transform = self.transform_map.get(
                    _ii.transformation_name or from_inst)
                if _transform:
                    for _fname in _upstream_fields.get(from_inst, []):
                        for _tf in _transform.fields:
                            if _tf.name == _fname and _tf.group_name:
                                _key = f"{from_inst}_{_tf.group_name}"
                                if _key in self.current_df_map:
                                    return self.current_df_map[_key]

        for from_inst in upstream_instances:
            upstream_instance = self.instance_map.get(from_inst)
            if upstream_instance:
                trans_name = upstream_instance.transformation_name
                if trans_name and trans_name in self.current_df_map:
                    return self.current_df_map[trans_name]
                for key in self.current_df_map:
                    if key == from_inst or from_inst == key:
                        return self.current_df_map[key]

        for from_inst in upstream_instances:
            for sep in [".", "_", ":", ""]:
                for key in self.current_df_map:
                    if key.startswith(from_inst.split(".")[0].split("_")[0].split(":")[0]):
                        if from_inst.replace(".", sep).replace("_", sep).replace(":", sep) in key.replace(".", sep).replace("_", sep).replace(":", sep):
                            return self.current_df_map[key]

            for key in self.current_df_map:
                from_norm = from_inst.lower().replace(".", "_").replace(":", "_")
                key_norm = key.lower().replace(".", "_").replace(":", "_")
                if from_norm == key_norm:
                    return self.current_df_map[key]

        return None
    def _get_all_input_dfs(self, instance_name: str) -> List[str]:
        inputs = []
        for connector in self.mapping.connectors:
            if connector.to_instance == instance_name:
                from_inst = connector.from_instance
                _df = None
                if self._prefer_direct_input and from_inst in self._direct_df_map:
                    _df = self._direct_df_map[from_inst]
                elif from_inst in self.current_df_map:
                    _df = self.current_df_map[from_inst]
                if _df:
                    df = _df
                    if df not in inputs:
                        inputs.append(df)
        return inputs

    def _all_upstreams_available(self, instance_name: str) -> bool:
        """True when every connector upstream has a registered DataFrame.

        Lookup chain-walk may map several upstream instances to the same merge
        df, so availability is counted by instance, not by unique df value.
        """
        _expected = {
            c.from_instance
            for c in self.mapping.connectors
            if c.to_instance == instance_name
        }
        if not _expected:
            return True
        # A Router registers only <instance>_<group> suffixed keys (never the
        # bare instance name); any group output satisfies availability for
        # downstream instances (targets/transforms fed by a router group).
        _available = {
            u for u in _expected
            if u in self._direct_df_map or u in self.current_df_map
            or any(k.startswith(u + "_") for k in self._direct_df_map)
            or any(k.startswith(u + "_") for k in self.current_df_map)
        }
        return len(_available) >= len(_expected)

    @staticmethod
    def _build_df_parent_map(steps: List[Any]) -> Dict[str, str]:
        """Map each generated df name to its primary lineage input.

        Expression/rename/pass-through steps and lookup/merge steps keep all
        input columns, so walking this map from a df gives its ancestors (the
        descendant carries every ancestor column). The map deliberately tracks
        only the PRIMARY input: a common-columns/lookup merge does NOT carry
        every column of its secondary side (colliding names are dropped), so
        the secondary side must never be treated as an ancestor.
        Iterating the steps in order makes the map reflect the latest variable
        binding.
        """
        parent: Dict[str, str] = {}
        for _s in steps:
            _out = getattr(_s, "df_output", None)
            _in = getattr(_s, "df_input", None)
            if _out and _in and _out != _in:
                parent[_out] = _in
        return parent

    @staticmethod
    def _is_df_descendant(parent: Dict[str, str], df: str, ancestor: str) -> bool:
        """True when `df` is (transitively) built from `ancestor`."""
        _seen = set()
        _cur = df
        while _cur and _cur in parent:
            if _cur in _seen:
                return False
            _seen.add(_cur)
            _cur = parent[_cur]
            if _cur == ancestor:
                return True
        return False

    @staticmethod
    def _lineage_preserves_columns(parent: Dict[str, str],
                                   step_map: Dict[str, Any],
                                   descendant: str, ancestor: str,
                                   needed: Set[str]) -> bool:
        """True when every needed column name survives (case-insensitive) on
        the primary lineage from `ancestor` to `descendant`.

        Expression/filter/lookup-merge steps keep all input column names, so
        the only way a needed name disappears is a rename step
        (drop(target).withColumnRenamed(source, target)) whose source is a
        needed column, or a step type that does not preserve columns (e.g.
        aggregator/joiner/union). Non-preserving steps make the check fail
        conservatively so a real merge is never skipped on a wrong assumption.
        """
        if not needed:
            return True
        _low_needed = {n.lower() for n in needed}
        _cur = descendant
        while _cur and _cur != ancestor:
            _st = step_map.get(_cur)
            if _st is None:
                return False
            if not isinstance(_st, (ApplyExpressionStep, ApplyFilterStep, ApplyLookupStep)):
                return False
            for _from, _to in (_st.params.get("rename_columns") or []):
                if _from.lower() in _low_needed:
                    return False
            _cur = parent.get(_cur)
        return _cur == ancestor

    def _redundant_merge_df(self, parent: Dict[str, str],
                            step_map: Dict[str, Any],
                            cur_df: str, extra_df: str,
                            needed: Set[str]) -> Optional[str]:
        """Return the df to use when one df is a primary-lineage descendant of
        the other AND the descendant provably still carries every column the
        downstream target needs.

        A common-columns merge of such a pair is redundant; the descendant can
        replace both inputs directly. This is deliberately stricter than plain
        lineage: a mapplet output rename (e.g. AGMT_IND → OUT_AGMT_IND) can
        remove a needed name, and `withColumnRenamed` is a silent no-op when
        the source is missing — skipping in that case would lose a column
        without any error. Returns None when a real merge is required (two
        independent branches, or the descendant no longer carries a needed
        column).
        """
        if self._is_df_descendant(parent, extra_df, cur_df):
            if self._lineage_preserves_columns(parent, step_map, extra_df, cur_df, needed):
                return extra_df
        if self._is_df_descendant(parent, cur_df, extra_df):
            if self._lineage_preserves_columns(parent, step_map, cur_df, extra_df, needed):
                return cur_df
        return None
    def _handle_mapplet(self, instance: Instance, plan: IRPlan) -> List[IRStep]:
        """Fully inline a mapplet by building a mini-DAG from its instances/connectors
        and dispatching each internal instance to the appropriate handler (Lookup,
        Expression, etc.) in topological order.

        Two mapplet structures are supported:
          Type A – instance-based: only INSTANCE + CONNECTOR children; transformation
                   definitions may be in the mapplet's own TRANSFORMATION children,
                   in the mapping's transform_map, or (for reusable lookups) absent.
          Type B – inline transformations: full TRANSFORMATION children for each step.
        """
        import re as _re
        steps: List[IRStep] = []

        # --- 1. Resolve mapplet identity ------------------------------------------------
        input_df = self._get_input_df(instance.name)
        if not input_df:
            plan.add_warning(f"No input DataFrame found for mapplet {instance.name}")
            input_df = "df_input"

        mapplet_name = instance.transformation_name or instance.name
        _mplt_prefix = "df_" + re.sub(r'[^a-zA-Z0-9_]', '_', mapplet_name)
        _mplt_step_prefix = _mplt_prefix[3:]  # safe name without df_ prefix, for step/hash keys
        # Instance-scoped df prefix: internal step df names stay stable and
        # unique per mapplet INSTANCE (e.g. MSTR vs STS), instead of a global
        # counter that renumbers every DataFrame on unrelated changes.
        _mpl_df_prefix = f"df_{instance.name}"

        # When a mapplet has multiple upstream DataFrames (e.g.
        # MPLT_GET_CMS_BLK_SCD_KEY receives columns from both
        # MPLT_LKP_EST_CODE and MPLT_LKP_BLK_CODE), merge them
        # before the entry-point remapping.
        all_inputs = self._get_all_input_dfs(instance.name)
        extra_inputs = [df for df in all_inputs if df != input_df]
        if extra_inputs and input_df != "df_input":
            _cur_df = input_df
            _all_steps = list(plan.steps) + steps
            _df_parent = self._build_df_parent_map(_all_steps)
            _step_map = {s.df_output: s for s in _all_steps if s.df_output}
            _needed = set()
            for _c in self.mapping.connectors:
                if _c.to_instance == instance.name and _c.from_instance in self.current_df_map:
                    _needed.add(_c.from_field)
            for _i, _extra_df in enumerate(extra_inputs):
                _redundant_to = self._redundant_merge_df(
                    _df_parent, _step_map, _cur_df, _extra_df, _needed)
                if _redundant_to is not None:
                    # One df is a lookup-merge descendant of the other, so the
                    # common-columns join would be redundant (and can make the
                    # Spark analyzer loop). The descendant already carries all
                    # needed columns — use it directly, no join or pass-through
                    # step required.
                    self.logger.log_transformation(
                        instance.name, "Mapplet",
                        f"Redundant input merge skipped: using {_redundant_to} directly",
                        LogLevel.INFO)
                    _cur_df = _redundant_to
                    continue
                _join_df = self._df_name(_mpl_df_prefix, "merge_input", _i)
                steps.append(ApplyLookupStep(
                    step_name=f"join_{instance.name}_{_i}",
                    df_input=_cur_df,
                    df_output=_join_df,
                    lookup_df=_extra_df,
                    join_predicates=[],
                    join_expr="__common_cols__",
                    output_columns=[],
                    lookup_type="left",
                ))
                _df_parent[_join_df] = _cur_df
                _step_map[_join_df] = steps[-1]
                _cur_df = _join_df
            input_df = _cur_df
        if input_df and input_df != "df_input":
            self._mapplet_input_df[instance.name] = input_df
        mapplet_def = self.mapping.mapplets.get(mapplet_name) if hasattr(self.mapping, 'mapplets') else None
        if not mapplet_def:
            self.logger.log_mapplet(instance.name, f"Mapplet definition not found: {mapplet_name}", LogLevel.WARNING)
            plan.add_warning(f"Mapplet definition not found: {mapplet_name}")
            self.current_df_map[instance.name] = input_df
            return steps

        # --- 2. Build local maps -------------------------------------------------------
        mpl_instances: Dict[str, Instance] = {}
        for i in mapplet_def.get("instances", []):
            mpl_instances[i.name] = i

        # Combine mapplet-local transformations with the mapping-level transform_map
        # so that reusable Lookup / Expression definitions in the folder scope are found.
        # IMPORTANT: mapplet-local transformations take precedence — they must NOT be
        # silently overwritten by same-named mapping-level ones (which may have
        # different LOCAL VARIABLES that don't exist in the mapplet's input).
        mpl_transforms: Dict[str, Transformation] = {}
        for t in mapplet_def.get("transformations", []):
            mpl_transforms[t.name] = t
        for tname, txf in self.transform_map.items():
            if tname not in mpl_transforms:
                mpl_transforms[tname] = txf

        mpl_connectors: List[Connector] = mapplet_def.get("connectors", [])

        if not mpl_instances:
            self.logger.log_mapplet(instance.name, "Mapplet has no instances", LogLevel.WARNING)
            plan.add_warning(f"Mapplet {mapplet_name} has no instances")
            self.current_df_map[instance.name] = input_df
            return steps

        # --- 3. Build mini-DAG ---------------------------------------------------------
        mpl_graph = nx.DiGraph()
        for inst_name, inst_obj in mpl_instances.items():
            mpl_graph.add_node(inst_name)
        for conn in mpl_connectors:
            if conn.from_instance in mpl_instances and conn.to_instance in mpl_instances:
                mpl_graph.add_edge(conn.from_instance, conn.to_instance)

        # Topological sort with cycle fallback
        try:
            mpl_order = list(nx.topological_sort(mpl_graph))
        except nx.NetworkXUnfeasible:
            self.logger.log_mapplet(instance.name, "Cycle detected in mapplet graph, using raw order", LogLevel.WARNING)
            plan.add_warning(f"Mapplet {mapplet_name} has cyclic dependencies; order may be incorrect")
            mpl_order = list(mpl_instances.keys())

        # Identify INPUT / OUTPUT boundary instances.
        # When multiple Output Transformation instances exist (e.g. OUTPUT_RLS_CNTL
        # and OUTPUT_DUMMY), pick the one with the most upstream connectors as the
        # "main" output — not the last one found in iteration order.
        input_inst = None
        output_inst = None
        _all_output_insts: List[Instance] = []
        for inst_obj in mpl_instances.values():
            itype = inst_obj.transformation_type or ""
            if "Input Transformation" in itype:
                input_inst = inst_obj
            elif "Output Transformation" in itype:
                _all_output_insts.append(inst_obj)
        if _all_output_insts:
            # Count connectors feeding each output instance; pick the one with
            # the most (the "main" data path, vs. e.g. OUTPUT_DUMMY for SP calls).
            _output_conn_counts: Dict[str, int] = {}
            for _conn in mpl_connectors:
                if _conn.to_instance in [o.name for o in _all_output_insts]:
                    _output_conn_counts[_conn.to_instance] = \
                        _output_conn_counts.get(_conn.to_instance, 0) + 1
            if _output_conn_counts:
                _main_output_name = max(_output_conn_counts, key=_output_conn_counts.get)
                for _oi in _all_output_insts:
                    if _oi.name == _main_output_name:
                        output_inst = _oi
                        break
            if not output_inst:
                output_inst = _all_output_insts[0]

        # --- 4. Map INPUT ports to upstream columns ------------------------------------
        input_field_map: Dict[str, str] = {}
        for conn in self.mapping.connectors:
            if conn.to_instance == instance.name:
                input_field_map[conn.to_field] = conn.from_field

        # --- 5. Handle mapplet INPUT port alignment ------------------------------------
        # Detect INPUT ports with no external connector — add as NULL
        _input_output_ports: Set[str] = set()
        if input_inst:
            _input_txf = mpl_transforms.get(input_inst.transformation_name or input_inst.name)
            if _input_txf:
                for _f in _input_txf.fields:
                    if "OUTPUT" in (_f.port_type or "").upper():
                        _input_output_ports.add(_f.name)
        _unconnected_inputs = _input_output_ports - set(input_field_map.keys())
        if _unconnected_inputs:
            _null_cols = []
            for _c in sorted(_unconnected_inputs):
                if _c not in input_df:
                    _null_cols.append({"name": _c, "expression": "NULL"})
            if _null_cols:
                _null_input_df = self._df_name(_mpl_df_prefix, "nullinput")
                _null_ccs = [ComputedColumn(name=n["name"], expression=n["expression"], datatype="string") for n in _null_cols]
                _null_step = ApplyExpressionStep(
                    step_name=f"nullinput_{instance.name}",
                    df_input=input_df,
                    df_output=_null_input_df,
                    computed_columns=_null_ccs,
                )
                _null_step.params["expression_cfg"] = _build_expression_cfg(
                    _null_step, _null_ccs, [], None, plan, {}, _re, self.transform_map)
                steps.append(_null_step)
                input_df = _null_input_df
                self.logger.log_mapplet(instance.name,
                    f"Added NULL columns for {len(_null_cols)} unconnected INPUT ports: {_unconnected_inputs}",
                    LogLevel.INFO)

        # The external upstream DataFrame uses its own column names (e.g.
        # ACTL_CMS_HSE_UNIT_KEY), but the mapplet's internal transformations
        # reference the mapplet's INPUT port names (e.g. IN_CMS_HSE_UNIT_KEY).
        # Rename mismatched columns at the entry point so all downstream steps
        # see the correct names.
        if input_field_map and input_inst:
            mismatches = {to_f: from_f for to_f, from_f in input_field_map.items()
                          if to_f != from_f}
            if mismatches:
                remap_cols = []
                for to_field, from_field in mismatches.items():
                    remap_cols.append(ComputedColumn(
                        name=to_field,
                        expression=from_field,
                        datatype="string",
                    ))
                remap_input_df = self._df_name(_mpl_df_prefix, "input")
                _remap_step = ApplyExpressionStep(
                    step_name=f"input_{instance.name}",
                    df_input=input_df,
                    df_output=remap_input_df,
                    computed_columns=remap_cols,
                )
                _remap_step.params["expression_cfg"] = _build_expression_cfg(
                    _remap_step, remap_cols, [], None, plan, {}, _re, self.transform_map)
                steps.append(_remap_step)
                input_df = remap_input_df

        # --- 6. Track DataFrames within mapplet ----------------------------------------
        mpl_df_map: Dict[str, str] = {}
        if input_inst:
            mpl_df_map[input_inst.name] = input_df
        # Unique per-instance output column for Dynamic Lookup NewLookupRow
        # (0/1 hit indicator). Multiple dynamic lookups in one mapplet (e.g.
        # LKP_DYN_SOR + LKP_DYN_SSA) each emit NewLookupRow; a unique name
        # prevents the second lookup from clobbering the first before the
        # downstream rename steps map them to SOR/SSA_CACHE_STATUS.
        mpl_nlr_cols: Dict[str, str] = {}

        # --- 7. Process internal instances in topological order ------------------------
        for mpl_inst_name in mpl_order:
            mpl_inst = mpl_instances.get(mpl_inst_name)
            if not mpl_inst:
                continue

            mpl_inst_type = (mpl_inst.transformation_type or "").strip()

            # Build field remap for this instance from both external and internal
            # connectors, so that column names fed into this transformation match
            # what its expressions/lookups expect.
            inst_field_remap: Dict[str, str] = dict(input_field_map)
            # Internal-only remap (no external input_field_map). External port
            # mappings are already handled by the input rename step, so they
            # must NOT be applied to lookup/filter predicates — the DataFrame
            # already uses the mapplet INPUT port names (e.g. IN_CASE_TYPE_KEY).
            _internal_remap: Dict[str, str] = {}
            for conn in mpl_connectors:
                if conn.to_instance == mpl_inst_name:
                    _actual = conn.from_field
                    if (conn.from_instance in mpl_nlr_cols
                            and conn.from_field.upper() == "NEWLOOKUPROW"):
                        _actual = mpl_nlr_cols[conn.from_instance]
                    if _actual != conn.to_field:
                        inst_field_remap[conn.to_field] = _actual
                        _internal_remap[conn.to_field] = _actual

            if "Input Transformation" in mpl_inst_type:
                # Input is already mapped to input_df
                continue

            elif "Output Transformation" in mpl_inst_type:
                # Handled after the loop — collect which instance feeds OUTPUT
                continue

            elif "Lookup Procedure" in mpl_inst_type:
                # Resolve the transformation definition
                lookup_transform = mpl_transforms.get(
                    mpl_inst.transformation_name or mpl_inst.name
                )
                if not lookup_transform:
                    # Try fuzzy matching against transform_map keys
                    for tname, txf in mpl_transforms.items():
                        if txf.type and "Lookup" in txf.type and (
                            mpl_inst_name in tname or tname in mpl_inst_name
                        ):
                            lookup_transform = txf
                            break

                if not lookup_transform:
                    self.logger.log_mapplet(
                        instance.name,
                        f"Lookup transform not found for {mpl_inst_name}",
                        LogLevel.WARNING,
                    )
                    plan.add_warning(
                        f"Mapplet {mapplet_name}: lookup {mpl_inst_name} skipped "
                        f"(transformation definition not found)"
                    )
                    continue

                lookup_sql = lookup_transform.table_attributes.get("Lookup Sql Override", "")
                lookup_sql = self._normalize_sql_text(lookup_sql, plan)
                lookup_table = lookup_transform.table_attributes.get("Lookup table name", "")
                lookup_cond = lookup_transform.table_attributes.get("Lookup condition", "")

                # Create lookup read step when we have SQL or table.
                # Use the lookup instance name (e.g. df_mplt_LKP_RENT_ADV_AMT)
                # instead of a numeric counter for readability.
                lookup_df_name = self._df_name(_mpl_df_prefix, mpl_inst.name)
                if lookup_sql:
                    lookup_conn = self._resolve_connection_alias(lookup_table or mpl_inst.name)
                    steps.append(ReadSQLStep(
                        step_name=f"read_{_mplt_step_prefix}_{mpl_inst.name}",
                        df_output=lookup_df_name,
                        connection_alias=lookup_conn,
                        query=lookup_sql,
                        is_lookup=True,
                    ))
                elif lookup_table:
                    lookup_conn = self._resolve_connection_alias(lookup_table)
                    steps.append(ReadSQLStep(
                        step_name=f"read_{_mplt_step_prefix}_{mpl_inst.name}",
                        df_output=lookup_df_name,
                        connection_alias=lookup_conn,
                        table_name=lookup_table,
                        is_lookup=True,
                    ))
                else:
                    # Skip — no SQL or table to read
                    self.logger.log_mapplet(
                        instance.name,
                        f"Lookup {mpl_inst_name} has no SQL override or table name",
                        LogLevel.WARNING,
                    )
                    continue

                plan.lookup_dfs[f"{_mplt_step_prefix}_{mpl_inst.name}"] = lookup_df_name

                # Find which DataFrame feeds this lookup within the mapplet
                mpl_input_df = None
                for conn in mpl_connectors:
                    if conn.to_instance == mpl_inst_name:
                        mpl_input_df = mpl_df_map.get(conn.from_instance)
                        if mpl_input_df:
                            break

                if mpl_input_df and lookup_df_name:
                    parsed = self._parse_lookup_condition(lookup_cond, lookup_df_name)
                    # Chain lookups that share the same mapplet input into one
                    # accumulating DataFrame, avoiding extra merge steps later.
                    _mpl_chain_key = f"mpl_{mapplet_name}_{mpl_input_df}"
                    if _mpl_chain_key in self._chain_df_map:
                        join_result_df = self._chain_df_map[_mpl_chain_key]
                        mpl_chain_input = join_result_df
                    else:
                        _chain_in_name = mpl_input_df
                        if _chain_in_name.startswith(f"df_{instance.name}_"):
                            _chain_in_name = _chain_in_name[len(f"df_{instance.name}_"):]
                        join_result_df = self._df_name(
                            "mplt_lkp_chain", instance.name, _chain_in_name)
                        self._chain_df_map[_mpl_chain_key] = join_result_df
                        mpl_chain_input = mpl_input_df
                    mpl_df_map[mpl_inst_name] = join_result_df

                    # Remap source columns using only internal connector remaps.
                    # External input_field_map remaps are excluded — the input
                    # rename step already aligned those column names.
                    join_predicates = parsed.get("join_columns", [])
                    for jc in join_predicates:
                        if jc.get("source_col") in _internal_remap:
                            jc["source_col"] = _internal_remap[jc["source_col"]]

                    # Also remap column names in complex join expressions
                    join_expr = parsed.get("condition_expr") or ""
                    if join_expr:
                        for _port, _col in _internal_remap.items():
                            if _port != _col:
                                join_expr = re.sub(r'\b' + re.escape(_port) + r'\b', _col, join_expr)

                    # Collect output columns from the lookup
                    output_cols = []
                    for field in lookup_transform.fields:
                        port_type = (field.port_type or "").upper()
                        if "OUTPUT" in port_type and "RETURN" not in port_type:
                            output_cols.append(field.name)

                    plan.lookup_return_ports[f"{_mplt_step_prefix}_{mpl_inst.name}"] = output_cols

                    steps.append(ApplyLookupStep(
                        step_name=f"apply_{_mplt_step_prefix}_{mpl_inst.name}",
                        df_input=mpl_chain_input,
                        df_output=join_result_df,
                        lookup_df=lookup_df_name,
                        join_predicates=join_predicates,
                        join_expr=join_expr,
                        output_columns=output_cols,
                        lookup_type="left",
                    ))
                    # Dynamic Lookup: carry the full dynamic-cache config and
                    # use a per-instance unique NewLookupRow column so multiple
                    # dynamic lookups in one mapplet (SOR + SSA) keep their own
                    # hit indicator until the downstream renames map them to
                    # SOR_CACHE_STATUS / SSA_CACHE_STATUS.
                    _dyn_lkp_params = self._build_dynamic_lookup_params(
                        lookup_transform, join_predicates,
                        f"{_mplt_step_prefix}_{mpl_inst.name}",
                        new_lookup_row_col="NewLookupRow", plan=plan,
                        ref_field_remap=_internal_remap)
                    if _dyn_lkp_params is not None:
                        _nlr_col = f"NewLookupRow_{re.sub(r'[^a-zA-Z0-9_]', '_', mpl_inst_name)}"
                        mpl_nlr_cols[mpl_inst_name] = _nlr_col
                        _dyn_lkp_params["new_lookup_row_col"] = _nlr_col
                        steps[-1].params["dynamic_lookup"] = _dyn_lkp_params
                        steps[-1].params["new_lookup_row_key"] = join_predicates[0].get(
                            "lookup_col", "")
                        steps[-1].params["new_lookup_row_col"] = _nlr_col
                        steps[-1].params["dedup_lookup"] = True
                        steps[-1].params["dedup_lookup_error"] = True
                        steps[-1].params["dedup_lookup_keys"] = [
                            jp.get("lookup_col", "") for jp in join_predicates
                        ]
                    # Handle "Lookup policy on multiple match" — same semantics
                    # as main-mapping lookups: Report Error raises on duplicate
                    # join keys, Use First/Any/Last dedup the lookup DataFrame.
                    if _dyn_lkp_params is None and join_predicates:
                        try:
                            _lkp_policy = lookup_transform.table_attributes.get(
                                "Lookup policy on multiple match", "")
                        except (AttributeError, TypeError):
                            _lkp_policy = ""
                        _dedup_keys = [jp.get("lookup_col", "") for jp in join_predicates]
                        if _lkp_policy.upper() in ("USE FIRST VALUE", "USE ANY VALUE", ""):
                            steps[-1].params["dedup_lookup"] = True
                            steps[-1].params["dedup_lookup_keys"] = _dedup_keys
                        elif _lkp_policy.upper() == "USE LAST VALUE":
                            steps[-1].params["dedup_lookup"] = True
                            steps[-1].params["dedup_lookup_keys"] = _dedup_keys
                            steps[-1].params["dedup_lookup_last"] = True
                        elif _lkp_policy.upper() == "REPORT ERROR":
                            steps[-1].params["dedup_lookup"] = True
                            steps[-1].params["dedup_lookup_error"] = True
                            steps[-1].params["dedup_lookup_keys"] = _dedup_keys
                elif lookup_df_name:
                    # Standalone lookup read (no join) — still register in mpl_df_map
                    # so downstream expressions can reference its columns.
                    mpl_df_map[mpl_inst_name] = lookup_df_name

            elif "Expression" in mpl_inst_type:
                transform = mpl_transforms.get(
                    mpl_inst.transformation_name or mpl_inst.name
                )
                if not transform:
                    continue

                # Find ALL input DataFrames for this expression, uniquely by
                # upstream instance. Use the LAST one (most complete — lookups
                # chain-join onto prior results and already contain all upstream
                # columns). Do NOT create extra merge steps; they would be
                # redundant (e.g. merging AGGTRANS into LKPTRANS when LKPTRANS
                # was left-joined from AGGTRANS).
                mpl_input_df = None
                mpl_upstream_dfs = {}
                for conn in mpl_connectors:
                    if conn.to_instance == mpl_inst_name:
                        _df = mpl_df_map.get(conn.from_instance)
                        if _df:
                            mpl_upstream_dfs[conn.from_instance] = _df
                if mpl_upstream_dfs:
                    # Pick the most recently processed upstream by checking the
                    # topological order in reverse — the last-processed instance
                    # (e.g. a lookup chain-joined from earlier steps) has all
                    # prior columns and is the most complete input.
                    for _rev_name in reversed(mpl_order):
                        if _rev_name in mpl_upstream_dfs:
                            mpl_input_df = mpl_upstream_dfs[_rev_name]
                            break
                if not mpl_input_df:
                    mpl_input_df = input_df
                # mpl_extra_inputs intentionally skipped — no redundant merges

                # Rename upstream columns to match Expression port names using
                # connector mappings (e.g. TBL_NAME → FACT_TABL_NAME) so that
                # expression port references resolve to the actual DataFrame columns.
                # When the mapplet has an Input Transformation, the external
                # input_field_map renames were already applied once at the
                # mapplet entry (input_<mapplet> step). Re-applying them on
                # every internal expression renames columns that no longer
                # exist (e.g. INIT_FLAG was already renamed to IN_V_INIT_IND),
                # producing unresolved columns downstream. In that case apply
                # only the internal connector renames.
                if input_inst is not None:
                    _expr_renames = [(actual_col, mpl_port)
                                     for mpl_port, actual_col in _internal_remap.items()
                                     if mpl_port.lower() != actual_col.lower()]
                else:
                    _expr_renames = [(actual_col, mpl_port)
                                     for mpl_port, actual_col in inst_field_remap.items()
                                     if mpl_port.lower() != actual_col.lower()]
                if _expr_renames:
                    # Rename upstream columns to match expression port names via
                    # a for loop of drop + withColumnRenamed (order-preserving).
                    _rename_df = self._df_name(_mpl_df_prefix, "rename", mpl_inst.name)
                    _rename_step = ApplyExpressionStep(
                        step_name=f"rename_{mpl_inst_name}",
                        df_input=mpl_input_df,
                        df_output=_rename_df,
                        computed_columns=[],
                    )
                    _rename_step.params["rename_columns"] = _expr_renames
                    _rename_step.params["expression_cfg"] = _build_expression_cfg(
                        _rename_step, [], [], None, None, {}, _re, self.transform_map)
                    steps.append(_rename_step)
                    mpl_input_df = _rename_df
                    self.logger.log_transformation(
                        mpl_inst_name, "Mapplet",
                        f"Renamed {len(_expr_renames)} columns for expression {mpl_inst_name}",
                        LogLevel.INFO)

                computed_cols = []
                mpl_output_ports = []
                for field in transform.fields:
                    if not field.expression:
                        continue
                    if "OUTPUT" in (field.port_type or "").upper() or "LOCAL VARIABLE" in (field.port_type or "").upper():
                        if "OUTPUT" in (field.port_type or "").upper():
                            mpl_output_ports.append(field.name)
                        expr_text = field.expression
                        translated = self.expr_translator.translate(
                            expr_text, "column", field.name
                        )
                        computed_cols.append(ComputedColumn(
                            name=field.name,
                            expression=translated,
                            datatype=field.datatype,
                        ))

                if computed_cols:
                    expr_df_name = self._df_name(_mpl_df_prefix, mpl_inst_name)
                    mpl_df_map[mpl_inst_name] = expr_df_name
                    _expr_step = ApplyExpressionStep(
                        step_name=f"apply_{_mplt_step_prefix}_{mpl_inst.name}",
                        df_input=mpl_input_df,
                        df_output=expr_df_name,
                        computed_columns=computed_cols,
                    )
                    if mpl_output_ports:
                        _expr_step.params["output_columns"] = mpl_output_ports
                    _expr_step.params["expression_cfg"] = _build_expression_cfg(
                        _expr_step, computed_cols, mpl_output_ports, transform, plan,
                        {}, _re, self.transform_map)
                    steps.append(_expr_step)
                else:
                    # Expression with no computed columns → pass-through
                    mpl_df_map[mpl_inst_name] = mpl_input_df

            elif "Filter" in mpl_inst_type:
                transform = mpl_transforms.get(
                    mpl_inst.transformation_name or mpl_inst.name
                )
                if transform:
                    filter_cond = transform.table_attributes.get("Filter Condition", "")
                    if filter_cond:
                        # Find input DataFrame and translate filter condition
                        _mpl_filt_input = None
                        for conn in mpl_connectors:
                            if conn.to_instance == mpl_inst_name:
                                _mpl_filt_input = mpl_df_map.get(conn.from_instance)
                                if _mpl_filt_input:
                                    break
                        if not _mpl_filt_input:
                            _mpl_filt_input = input_df
                        # Remap port names in filter condition using internal-only
                        # remaps (external input_field_map excluded — input rename
                        # step already aligned those column names).
                        _filter_expr = filter_cond
                        for _mpl_port, _actual_col in _internal_remap.items():
                            if _mpl_port != _actual_col:
                                _filter_expr = re.sub(r'\b' + re.escape(_mpl_port) + r'\b', _actual_col, _filter_expr)
                        # Translate the filter condition (handles Informatica SQL syntax)
                        _filter_expr = self.expr_translator.translate_for_filter(_filter_expr)
                        _mpl_filt_df = self._df_name(_mpl_df_prefix, mpl_inst_name)
                        mpl_df_map[mpl_inst_name] = _mpl_filt_df
                        _mpl_filt_step = ApplyFilterStep(
                            step_name=f"apply_{_mplt_step_prefix}_{mpl_inst.name}",
                            df_input=_mpl_filt_input,
                            df_output=_mpl_filt_df,
                            condition=_filter_expr,
                        )
                        # Component-method config: the template reads ONLY
                        # lib_filter_cfg — without it the condition silently
                        # falls back to 'TRUE' (the filter passes ALL rows).
                        _mpl_inner = re.match(r'expr\("(.*)"\)$', _filter_expr)
                        _mpl_filt_step.params["lib_filter_cfg"] = {
                            "rename_columns": [],
                            "condition": _mpl_inner.group(1) if _mpl_inner else _filter_expr,
                        }
                        steps.append(_mpl_filt_step)
                    else:
                        # Filter with no condition — pass-through
                        mpl_df_map[mpl_inst_name] = mpl_input_df or input_df

            elif "Aggregator" in mpl_inst_type:
                _agg_txf = mpl_transforms.get(mpl_inst.transformation_name or mpl_inst.name)
                if _agg_txf:
                    _agg_input_df = None
                    for conn in mpl_connectors:
                        if conn.to_instance == mpl_inst_name:
                            _agg_input_df = mpl_df_map.get(conn.from_instance)
                            if _agg_input_df:
                                break
                    if not _agg_input_df:
                        _agg_input_df = input_df
                    # Build field remap from mapplet internal connectors
                    _agg_remap = []
                    _agg_mapped_to = set()
                    for conn in mpl_connectors:
                        if conn.to_instance == mpl_inst_name:
                            _agg_remap.append({"from": conn.from_field, "to": conn.to_field})
                            _agg_mapped_to.add(conn.to_field)
                    # Detect Aggregator INPUT ports without connectors — add as null
                    if _agg_txf:
                        for _f in _agg_txf.fields:
                            _pt = (_f.port_type or "").upper()
                            if "INPUT" in _pt and _f.name not in _agg_mapped_to:
                                _agg_remap.append({"from": "__null__", "to": _f.name})
                    _orig_map = self.current_df_map.copy()
                    _mplt_inst = Instance(
                        name=_mplt_prefix + "_" + mpl_inst.name,
                        type=mpl_inst.type,
                        transformation_name=(_mplt_prefix + "_" + mpl_inst.transformation_name) if mpl_inst.transformation_name else None,
                        transformation_type=mpl_inst.transformation_type,
                    )
                    _mplt_txf_name = _mplt_prefix + "_" + _agg_txf.name if _agg_txf.name else None
                    if _mplt_txf_name and _mplt_txf_name not in self.transform_map:
                        self.transform_map[_mplt_txf_name] = _agg_txf
                    self.current_df_map[_mplt_inst.name] = _agg_input_df
                    _agg_result = self._handle_aggregator(_mplt_inst, plan)
                    if isinstance(_agg_result, list):
                        for _s in _agg_result:
                            if _s:
                                _s.df_input = _agg_input_df
                                if _agg_remap and not _s.params.get('agg_selects'):
                                    _s.params["agg_selects"] = _agg_remap
                                steps.append(_s)
                                mpl_df_map[mpl_inst_name] = _s.df_output
                    elif _agg_result:
                        _agg_result.df_input = _agg_input_df
                        if _agg_remap and not _agg_result.params.get('agg_selects'):
                            _agg_result.params["agg_selects"] = _agg_remap
                        steps.append(_agg_result)
                        mpl_df_map[mpl_inst_name] = _agg_result.df_output
                    self.current_df_map.clear()
                    self.current_df_map.update(_orig_map)
                    if _mplt_txf_name and _mplt_txf_name in self.transform_map:
                        del self.transform_map[_mplt_txf_name]

            elif "Sorter" in mpl_inst_type:
                _srt_txf = mpl_transforms.get(mpl_inst.transformation_name or mpl_inst.name)
                if _srt_txf:
                    _srt_input_df = None
                    for conn in mpl_connectors:
                        if conn.to_instance == mpl_inst_name:
                            _srt_input_df = mpl_df_map.get(conn.from_instance)
                            if _srt_input_df:
                                break
                    if not _srt_input_df:
                        _srt_input_df = input_df
                    _orig_map = self.current_df_map.copy()
                    _mplt_txf_name = _mplt_prefix + "_" + _srt_txf.name if _srt_txf.name else None
                    if _mplt_txf_name and _mplt_txf_name not in self.transform_map:
                        self.transform_map[_mplt_txf_name] = _srt_txf
                    _mplt_inst = Instance(
                        name=_mplt_prefix + "_" + mpl_inst.name,
                        type=mpl_inst.type,
                        transformation_name=_mplt_txf_name,
                        transformation_type=mpl_inst.transformation_type,
                    )
                    self.current_df_map[_mplt_inst.name] = _srt_input_df
                    _srt_result = self._handle_sorter(_mplt_inst, plan)
                    if _srt_result:
                        _srt_result.df_input = _srt_input_df
                        steps.append(_srt_result)
                        mpl_df_map[mpl_inst_name] = _srt_result.df_output
                    self.current_df_map.clear()
                    self.current_df_map.update(_orig_map)

        # --- 7. Map OUTPUT back to the calling mapping ----------------------------------
        if output_inst:
            # Find ALL internal instances that feed the OUTPUT
            output_input_df = None
            output_extra_dfs = []
            seen_output_dfs = set()
            for conn in mpl_connectors:
                if conn.to_instance == output_inst.name:
                    _odf = mpl_df_map.get(conn.from_instance)
                    if _odf and _odf not in seen_output_dfs:
                        seen_output_dfs.add(_odf)
                        if output_input_df is None:
                            output_input_df = _odf
                        else:
                            output_extra_dfs.append(_odf)

            if not output_input_df:
                # No internal feed; use the primary input
                output_input_df = input_df

            # When OUTPUT has multiple upstream internal DataFrames, merge them
            _all_out_steps = list(plan.steps) + steps
            _df_parent = self._build_df_parent_map(_all_out_steps)
            _step_map = {s.df_output: s for s in _all_out_steps if s.df_output}
            _needed = set()
            for _c in mpl_connectors:
                if _c.to_instance == output_inst.name:
                    _actual = _c.from_field
                    if (_c.from_instance in mpl_nlr_cols
                            and _c.from_field.upper() == "NEWLOOKUPROW"):
                        _actual = mpl_nlr_cols[_c.from_instance]
                    _needed.add(_actual)
            for _i, _extra_df in enumerate(output_extra_dfs):
                _redundant_to = self._redundant_merge_df(
                    _df_parent, _step_map, output_input_df, _extra_df, _needed)
                if _redundant_to is not None:
                    # The extra df is a descendant of the current df (or vice
                    # versa) — it already carries all needed columns, so a
                    # common-columns join is redundant and can make the Spark
                    # analyzer loop (e.g. EXP_UPD_STRATEGY vs EXP_CDC).
                    self.logger.log_transformation(
                        instance.name, "Mapplet",
                        f"Redundant output merge skipped: using {_redundant_to} directly",
                        LogLevel.INFO)
                    output_input_df = _redundant_to
                    continue
                _join_df = self._df_name(_mpl_df_prefix, "merge_output", _i)
                steps.append(ApplyLookupStep(
                    step_name=f"join_output_{instance.name}_{_i}",
                    df_input=output_input_df,
                    df_output=_join_df,
                    lookup_df=_extra_df,
                    join_predicates=[],
                    join_expr="__common_cols__",
                    output_columns=[],
                    lookup_type="left",
                ))
                _df_parent[_join_df] = output_input_df
                _step_map[_join_df] = steps[-1]
                output_input_df = _join_df

            # Collect output port names from the Mapplet-type wrapper transformation
            mapplet_wrapper = mpl_transforms.get(mapplet_name)
            output_ports: List[str] = []
            if mapplet_wrapper:
                for field in mapplet_wrapper.fields:
                    port_type = (field.port_type or "").upper()
                    if "OUTPUT" in port_type:
                        output_ports.append(field.name)

            df_output = self._get_df_name(_mplt_prefix, instance)
            self._register_df(instance, df_output)
            self._mapplet_output_df[instance.name] = df_output

            # Build output field remap: internal column → OUTPUT port name
            _output_renames = []
            for conn in mpl_connectors:
                if conn.to_instance == output_inst.name and conn.from_field.lower() != conn.to_field.lower():
                    _actual = conn.from_field
                    if (conn.from_instance in mpl_nlr_cols
                            and conn.from_field.upper() == "NEWLOOKUPROW"):
                        _actual = mpl_nlr_cols[conn.from_instance]
                    _output_renames.append([_actual, conn.to_field])
            if steps:
                # There are real transformation steps — the final step's df_output
                # is the mapplet result; register it under the mapplet instance name
                self.current_df_map[instance.name] = df_output
                if output_ports:
                    _out_step = ApplyExpressionStep(
                        step_name=f"apply_{instance.name}",
                        df_input=output_input_df,
                        df_output=df_output,
                        computed_columns=[],
                        output_columns=output_ports,
                    )
                    if _output_renames:
                        _out_step.params["rename_columns"] = _output_renames
                    _out_step.params["expression_cfg"] = _build_expression_cfg(
                        _out_step, [], output_ports, None, plan, {}, _re, self.transform_map)
                    steps.append(_out_step)
                else:
                    # Pass-through of the last internal DataFrame as output
                    steps.append(ApplyExpressionStep(
                        step_name=f"apply_{instance.name}",
                        df_input=output_input_df,
                        df_output=df_output,
                        computed_columns=[],
                    ))
            else:
                # No internal steps generated → pass-through with warning
                steps.append(ApplyExpressionStep(
                    step_name=f"apply_{instance.name}",
                    df_input=output_input_df,
                    df_output=df_output,
                    computed_columns=[],
                ))
                self.logger.log_mapplet(
                    instance.name,
                    "No mapplet internal steps generated; treated as pass-through",
                    LogLevel.WARNING,
                )
                plan.add_warning(
                    f"Mapplet {instance.name} treated as pass-through "
                    f"(no expressions or lookups could be generated)"
                )

            self.logger.log_mapplet(instance.name, f"Converted ({len(steps)} steps)", LogLevel.SUCCESS)
        else:
            # No OUTPUT instance — treat entire mapplet as pass-through
            self.current_df_map[instance.name] = input_df
            if instance.transformation_name and instance.transformation_name != instance.name:
                self.current_df_map[instance.transformation_name] = input_df
            self.logger.log_mapplet(
                instance.name,
                "No OUTPUT instance found; treated as pass-through",
                LogLevel.WARNING,
            )
            plan.add_warning(
                f"Mapplet {instance.name}: no OUTPUT instance found; "
                f"treated as pass-through"
            )

        return steps

    def _infer_type_from_transform(self, transform: Transformation) -> str:
        if not transform:
            return "UNKNOWN"

        if transform.type and transform.type not in ("", "TRANSFORMATION"):
            return transform.type

        attrs = transform.table_attributes

        if "Filter Condition" in attrs or any(f.name.upper() == "FILTER_CONDITION" for f in transform.fields):
            return "Filter"

        if "Lookup Sql Override" in attrs or "Lookup table name" in attrs or "Lookup condition" in attrs:
            return "Lookup Procedure"

        if "Join Condition" in attrs or "Join Type" in attrs:
            return "Joiner"

        if "Sql Query" in attrs or "Source Filter" in attrs:
            return "Source Qualifier"

        if "Update Strategy Expression" in attrs:
            return "Update Strategy"

        if any("GROUP BY" in f.port_type.upper() for f in transform.fields):
            return "Aggregator"

        if "Number of Ranks" in attrs or "Rank" in transform.type:
            return "Rank"

        if transform.type == "Normalizer":
            return "Normalizer"

        has_expr_outputs = any(
            f.expression and "OUTPUT" in f.port_type
            for f in transform.fields
        )
        if has_expr_outputs:
            return "Expression"

        return self._infer_type_from_name(transform.name)

    def _resolve_connection_alias(
        self,
        instance_name: str,
        target_name: str = "",
        target_config: Optional[TargetConfig] = None,
        plan: Optional[IRPlan] = None,
        is_target: bool = False
    ) -> str:
        if target_config and target_config.connection_alias:
            return target_config.connection_alias

        if instance_name in self.user_config.connection_mappings:
            mapping = self.user_config.connection_mappings[instance_name]
            if isinstance(mapping, dict) and "connection_alias" in mapping:
                return mapping["connection_alias"]
            elif isinstance(mapping, str):
                return mapping

        if target_name in self.user_config.connection_mappings:
            mapping = self.user_config.connection_mappings[target_name]
            if isinstance(mapping, dict) and "connection_alias" in mapping:
                return mapping["connection_alias"]
            elif isinstance(mapping, str):
                return mapping

        for k in self.user_config.db_connections.keys():
            if target_name and (target_name.lower() in k.lower() or k.lower() in target_name.lower()):
                return k
            if instance_name.lower() in k.lower() or k.lower() in instance_name.lower():
                return k

        db_conn_keys = list(self.user_config.db_connections.keys())
        if db_conn_keys:
            if is_target:
                landing_keys = [k for k in db_conn_keys if "LANDING" in k.upper()]
                if landing_keys:
                    return landing_keys[0]
            return db_conn_keys[0]

        # Fall back to source/target definition's db_name from XML
        lookup_name = target_name or instance_name
        for src in self.mapping.sources:
            if lookup_name and (lookup_name == src.name or lookup_name.startswith(src.name)):
                conn_name = src.db_name or src.name
                if plan:
                    plan.warnings = [w for w in plan.warnings if "No connection alias found" not in w]
                return conn_name
        for tgt in self.mapping.targets:
            if lookup_name and (lookup_name == tgt.name or lookup_name.startswith(tgt.name)):
                conn_name = tgt.db_name or "target"
                if plan:
                    plan.warnings = [w for w in plan.warnings if "No connection alias found" not in w]
                return conn_name

        default = "target_db" if is_target else "source_db"
        if plan:
            plan.add_warning(f"No connection alias found for {lookup_name}, using '{default}'")
        return default

    def _handle_with_type(self, instance: Instance, inferred_type: str, plan: IRPlan):
        handler_map = {
            "Source Qualifier": self._handle_source_qualifier,
            "Filter": self._handle_filter,
            "Expression": self._handle_expression,
            "Lookup Procedure": self._handle_lookup,
            "Joiner": self._handle_joiner,
            "Aggregator": self._handle_aggregator,
            "Sorter": self._handle_sorter,
            "Union": self._handle_union,
            "Router": self._handle_router,
            "Sequence Generator": self._handle_sequence,
            "Update Strategy": self._handle_update_strategy,
        }

        handler = handler_map.get(inferred_type)
        if handler:
            result = handler(instance, plan)
            if isinstance(result, list):
                for step in result:
                    plan.add_step(step)
            elif result:
                plan.add_step(result)
        else:
            plan.add_warning(f"No handler for inferred type: {inferred_type} ({instance.name})")

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
    ApplySequenceStep, ComputedColumn
)
from .expr_translator import ExpressionTranslator, sanitize_for_expr
from .graph_builder import GraphBuilder
from .logger import ConversionLogger, LogLevel, LogStage


class TransformHandlers:

    def __init__(self, mapping: MappingDefinition, user_config: UserConfig, logger: Optional[ConversionLogger] = None):
        self.mapping = mapping
        self.user_config = user_config
        self.logger = logger or ConversionLogger()
        self.logger.set_current_mapping(mapping.name)
        self.expr_translator = ExpressionTranslator(mapping_name=mapping.name, logger=self.logger)
        self._plan = None  # built in build_ir_plan
        self.transform_map: Dict[str, Transformation] = {}
        self.instance_map: Dict[str, Instance] = {}
        self.source_map: Dict[str, SourceDefinition] = {}
        self.target_map: Dict[str, TargetDefinition] = {}
        self.df_counter = 0
        self.current_df_map: Dict[str, str] = {}

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

        return "UNKNOWN"

    def _get_df_name(self, prefix: str = "df") -> str:
        self.df_counter += 1
        return f"{prefix}_{self.df_counter}"

    def _register_df(self, instance: Instance, df_name: str):
        self.current_df_map[instance.name] = df_name
        if instance.transformation_name and instance.transformation_name != instance.name:
            self.current_df_map[instance.transformation_name] = df_name

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
            if inst_type in ("TARGET", "Target Definition", "Filter", "Expression",
                             "Union", "Custom Transformation"):
                _input_ok = self._get_input_df(instance.name)
                if not _input_ok:
                    _deferred_insts.append(instance)
                    continue

            # Track this instance as processed so Phase 3 (post-cycle-resolution)
            # does not re-process it and create duplicate steps.
            _processed_inst_names.add(inst_name)

            if inst_type in ("SOURCE", "Source Definition"):
                self.logger.log_transformation(inst_name, "Source", "Processing source definition", LogLevel.INFO)
                step = self._handle_source(instance, plan)
                if step:
                    plan.add_step(step)
                    self.logger.log_transformation(inst_name, "Source", "Source converted", LogLevel.SUCCESS)

            elif inst_type == "Source Qualifier":
                self.logger.log_transformation(inst_name, "SourceQualifier", "Processing source qualifier", LogLevel.INFO)
                step = self._handle_source_qualifier(instance, plan)
                if step:
                    plan.add_step(step)
                    self.logger.log_transformation(inst_name, "SourceQualifier", "Source qualifier converted", LogLevel.SUCCESS)

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
                step = self._handle_aggregator(instance, plan)
                if step:
                    plan.add_step(step)
                    self.logger.log_transformation(inst_name, "Aggregator", "Aggregator converted", LogLevel.SUCCESS)

            elif inst_type == "Sorter":
                self.logger.log_transformation(inst_name, "Sorter", "Processing sorter transformation", LogLevel.INFO)
                step = self._handle_sorter(instance, plan)
                if step:
                    plan.add_step(step)
                    self.logger.log_transformation(inst_name, "Sorter", "Sorter converted", LogLevel.SUCCESS)

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
                step = self._handle_update_strategy(instance, plan)
                if step:
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

        # Process deferred instances. By now all upstream transforms have been
        # processed so _get_input_df should succeed.
        import logging as _logging
        if _deferred_insts:
            _logging.warning(f"DEFERRED instances: {[(d.name, type(d).__name__) for d in _deferred_insts]}")
            _logging.warning(f"current_df_map keys: {list(self.current_df_map.keys())}")
        for _d_inst in _deferred_insts:
            _d_type = self._resolve_transformation_type(_d_inst)
            _input_df = self._get_input_df(_d_inst.name)
            if not _input_df:
                # Still can't find input — let handler warn with fallback
                pass
            if _d_type in ("TARGET", "Target Definition"):
                self.logger.log_transformation(_d_inst.name, "Target",
                    "Processing target", LogLevel.INFO)
                _steps = self._handle_target(_d_inst, plan)
                for _s in _steps:
                    plan.add_step(_s)
                if _steps:
                    self.logger.log_transformation(_d_inst.name, "Target",
                        f"Target converted ({len(_steps)} steps)", LogLevel.SUCCESS)
            elif _d_type == "Expression":
                self.logger.log_transformation(_d_inst.name, "Expression",
                    "Processing expression", LogLevel.INFO)
                _result = self._handle_expression(_d_inst, plan)
                if isinstance(_result, list):
                    for _s in _result:
                        if _s:
                            plan.add_step(_s)
                elif _result:
                    plan.add_step(_result)
                if _result:
                    self.logger.log_transformation(_d_inst.name, "Expression",
                        "Expression converted", LogLevel.SUCCESS)
            elif _d_type == "Filter":
                self.logger.log_transformation(_d_inst.name, "Filter",
                    "Processing filter", LogLevel.INFO)
                _step = self._handle_filter(_d_inst, plan)
                if _step:
                    plan.add_step(_step)
            elif _d_type in ("Union", "Custom Transformation"):
                _logging.warning(f"Processing deferred Union: {_d_inst.name}, input_df={_input_df}")
                self.logger.log_transformation(_d_inst.name, "Union",
                    "Processing union", LogLevel.INFO)
                _result = self._handle_union(_d_inst, plan)
                if _result:
                    plan.add_step(_result)
                    self.logger.log_transformation(_d_inst.name, "Union",
                        "Union converted", LogLevel.SUCCESS)
                else:
                    _logging.warning(f"Union {_d_inst.name} returned None! inputs={self._get_all_input_dfs(_d_inst.name)}")

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

        df_name = self._get_df_name("df_src")
        self._register_df(instance, df_name)

        source_config = self._get_source_config(source.name)

        from .models import SourceType, normalize_db_type
        if source.source_type == SourceType.SQL:
            conn_alias = (source_config.connection_alias if source_config and source_config.connection_alias
                         else source.db_name or "default_conn")
            db_type = normalize_db_type(source.database_type)
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
            _has_upstream = any(
                c.to_instance == instance.name
                and c.from_instance != c.to_instance  # skip self-loops (Source Def → same-name SQ)
                for c in self.mapping.connectors
            )
            if _has_upstream:
                plan.add_warning(f"No input DataFrame found for {instance.name}")
            input_df = "df_source"

        df_output = self._get_df_name("df_sq")
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
        if filter_cond:
            translated_filter = self.expr_translator.translate_for_filter(filter_cond, "source_filter")

        source_inputs = self._get_source_inputs_for_sq(instance.name)
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
            step.params["sql_query"] = final_sql
            step.params["filter_condition"] = ""
            step.params["distinct"] = False
            step.params["db_type"] = source_db_type
            if source_inputs:
                step.params["source_schema"] = source_inputs[0].get("owner", "")
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
            step.params["distinct"] = distinct
            step.params["db_type"] = source_db_type

        step.params["connection_alias"] = conn_alias
        step.params["output_columns"] = output_columns

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

    def _handle_filter(self, instance: Instance, plan: IRPlan) -> Optional[IRStep]:
        input_df = self._get_input_df(instance.name)
        if not input_df:
            plan.add_warning(f"No input DataFrame found for {instance.name}")
            input_df = "df_input"

        df_output = self._get_df_name("df_fil")
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
        else:
            condition = "True"
            original_condition = ""
            plan.add_warning(f"No filter condition found for {instance.name}")

        return ApplyFilterStep(
            step_name=f"apply_{instance.name}",
            df_input=input_df,
            df_output=df_output,
            condition=condition,
            original_condition=original_condition
        )

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

        df_output = self._get_df_name("df_exp")
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

        if transform:
            for field in transform.fields:
                expr_text = field.expression or ""

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
                for _port, _col in _expr_field_remap.items():
                    if _port != _col:
                        expr_text = expr_text.replace(_port, _col)

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
                    # LOCAL VARIABLE with expression (e.g. :LKP.xxx() call) —
                    # promote to a computed column so downstream expressions can reference it
                    if expr_text:
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
            _all_names = {c.name for c in computed_columns}
            for _cc in computed_columns:
                _dep_graph.add_node(_cc.name)
                for _dep_name in _all_names:
                    if _dep_name != _cc.name and _dep_name in (_cc.expression or ""):
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
                                else:
                                    _sp_call = _sp_full
                                step.params["sp_call_text"] = _sp_call
                                break

        # When an expression has multiple upstream DataFrames (e.g. EXPTRANS1
        # receives columns from 10+ parallel mapplets), join the extra DFs
        # before the expression step. All upstream DFs share the source qualifier
        # columns (e.g. CASE_KEY). Use a runtime common-column join.
        if _multistep:
            result_steps: List[IRStep] = []
            _cur_df = input_df
            for _i, _extra_df in enumerate(extra_inputs):
                _join_df = self._get_df_name("df_exp_merge")
                result_steps.append(ApplyLookupStep(
                    step_name=f"join_{instance.name}_{_i}",
                    df_input=_cur_df,
                    df_output=_join_df,
                    lookup_df=_extra_df,
                    join_predicates=[],
                    join_expr="__common_cols__",
                    output_columns=[],
                    lookup_type="left",
                ))
                _cur_df = _join_df
            step.df_input = _cur_df
            result_steps.append(step)
            return result_steps

        return step

    def _handle_lookup(self, instance: Instance, plan: IRPlan) -> List[IRStep]:
        steps = []

        input_df = self._get_input_df(instance.name)

        transform = self.transform_map.get(instance.transformation_name or instance.name)
        if not transform:
            plan.add_warning(f"Lookup transformation not found: {instance.name}")
            return steps

        lookup_df = self._get_df_name("df_lkp")

        lookup_sql = transform.table_attributes.get("Lookup Sql Override", "")
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

        plan.lookup_dfs[instance.name] = lookup_df

        output_columns = []
        for field in transform.fields:
            if "OUTPUT" in field.port_type.upper() and "RETURN" not in field.port_type.upper():
                continue
            if "RETURN" in field.port_type.upper() or (field.expression and "OUTPUT" in field.port_type.upper()):
                output_columns.append(field.name)

        plan.lookup_return_ports[instance.name] = output_columns

        if input_df:
            df_output = self._get_df_name("df_lkp_result")
            self._register_df(instance, df_output)

            lookup_cond = transform.table_attributes.get("Lookup condition", "")
            parsed_condition = self._parse_lookup_condition(lookup_cond, lookup_df)

            join_predicates = parsed_condition.get("join_columns", [])
            join_expr = parsed_condition.get("condition_expr") or ""

            steps.append(ApplyLookupStep(
                step_name=f"apply_{instance.name}",
                df_input=input_df,
                df_output=df_output,
                lookup_df=lookup_df,
                join_predicates=join_predicates,
                join_expr=join_expr,
                output_columns=output_columns,
                lookup_type="left"
            ))

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
            return inputs[0], inputs[1] if len(inputs) > 1 else inputs[0]

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

        if master_from and master_from in self.current_df_map:
            df_master = self.current_df_map[master_from]
        else:
            df_master = inputs[0]

        if detail_from and detail_from in self.current_df_map:
            df_detail = self.current_df_map[detail_from]
        else:
            df_detail = inputs[1] if len(inputs) > 1 else inputs[0]

        return df_master, df_detail

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

        df_output = self._get_df_name("df_jnr")
        self._register_df(instance, df_output)

        join_condition = ""
        join_type = "inner"

        if transform:
            join_condition = transform.table_attributes.get("Join Condition", "")
            join_type_attr = transform.table_attributes.get("Join Type", "Normal")
            if "Master Outer" in join_type_attr:
                join_type = "left"
            elif "Detail Outer" in join_type_attr:
                join_type = "right"
            elif "Full Outer" in join_type_attr:
                join_type = "full"

        df_master, df_detail = self._joiner_pick_master_detail(instance, inputs)
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

        return step

    def _handle_aggregator(self, instance: Instance, plan: IRPlan) -> Optional[IRStep]:
        input_df = self._get_input_df(instance.name)
        if not input_df:
            input_df = "df_input"

        # Build field remap from main mapping connectors for INPUT port names
        _agg_field_remap: Dict[str, str] = {}
        for conn in self.mapping.connectors:
            if conn.to_instance == instance.name and conn.from_field != conn.to_field:
                _agg_field_remap[conn.to_field] = conn.from_field

        df_output = self._get_df_name("df_agg")
        self._register_df(instance, df_output)

        transform = self.transform_map.get(instance.transformation_name or instance.name)

        group_by = []
        aggregations = {}

        if transform:
            self.logger.log_transformation(instance.name, "Aggregator", f"Processing aggregator with {len(transform.fields)} fields", LogLevel.INFO)

            for field in transform.fields:
                if field.is_group_by or "GROUP BY" in field.port_type.upper():
                    group_by.append(field.name)
                    self.logger.log_transformation(instance.name, "Aggregator", f"GROUP BY: {field.name}", LogLevel.INFO)
                elif field.expression and "OUTPUT" in field.port_type.upper():
                    # Remap INPUT port names to upstream column names
                    expr_text = field.expression
                    # Replace $$ mapping variables with f-string placeholders
                    # (e.g. $$v_rpt_mth → {v_rpt_mth}) so they resolve at runtime
                    # from the Python variables loaded via job_params.
                    has_agg_dollar = False
                    for _mv_name in plan.mapping_variables:
                        if _mv_name in expr_text:
                            _py_name = _mv_name.replace('$', '')
                            expr_text = expr_text.replace(_mv_name, '{' + _py_name + '}')
                            has_agg_dollar = True
                    for _port, _col in _agg_field_remap.items():
                        if _port != _col:
                            expr_text = expr_text.replace(_port, _col)
                    # Signal to _translate_aggregation_expr to use f-string for expr()
                    pyspark_agg = self._translate_aggregation_expr(expr_text, field.name, use_fstr=has_agg_dollar)
                    if pyspark_agg:
                        aggregations[field.name] = pyspark_agg
                        self.logger.log_transformation(instance.name, "Aggregator", f"Aggregation: {field.name} = {pyspark_agg}", LogLevel.INFO)

            self.logger.log_transformation(instance.name, "Aggregator", f"Found {len(group_by)} GROUP BY keys, {len(aggregations)} aggregations", LogLevel.INFO)

        step = ApplyAggregatorStep(
            step_name=f"apply_{instance.name}",
            df_input=input_df,
            df_output=df_output,
            group_by=group_by,
            aggregations=aggregations
        )
        # Pass mapping variables so template can do runtime $$ substitution
        if plan.mapping_variables:
            step.params["mapping_variables"] = plan.mapping_variables
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
            match = re.match(_pat, expr, re.IGNORECASE)
            if match:
                inner_expr = match.group(1).strip()
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
        return translated

    def _handle_sorter(self, instance: Instance, plan: IRPlan) -> Optional[IRStep]:
        input_df = self._get_input_df(instance.name)
        if not input_df:
            input_df = "df_input"

        df_output = self._get_df_name("df_srt")
        self._register_df(instance, df_output)

        transform = self.transform_map.get(instance.transformation_name or instance.name)
        sort_columns = []

        if transform:
            for field in transform.fields:
                sort_dir = transform.table_attributes.get(f"{field.name} Sort Direction", "ASC")
                sort_columns.append({
                    "column": field.name,
                    "direction": sort_dir
                })

        return ApplySorterStep(
            step_name=f"apply_{instance.name}",
            df_input=input_df,
            df_output=df_output,
            sort_columns=sort_columns
        )

    def _handle_union(self, instance: Instance, plan: IRPlan) -> Optional[IRStep]:
        inputs = self._get_all_input_dfs(instance.name)

        if not inputs:
            plan.add_warning(f"No inputs found for Union {instance.name}")
            return None

        df_output = self._get_df_name("df_un")
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

        step = ApplyUnionStep(
            step_name=f"apply_{instance.name}",
            df_inputs=inputs,
            df_output=df_output,
            union_all=True
        )

        step.params["output_columns"] = output_columns
        step.params["flag_column"] = flag_column

        if flag_column:
            step.comments.append(f"Normalizing flag column to: {flag_column}")

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

        groups = []
        group_conditions = {}

        for field in transform.fields:
            if "OUTPUT" in field.port_type.upper():
                group_name = field.group_name or "DEFAULT"
                if group_name not in group_conditions:
                    condition = ""
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

        all_conditions = []
        trans_name = instance.transformation_name or instance.name

        for group_name, condition in group_conditions.items():
            df_output = self._get_df_name(f"df_rtr_{group_name.lower()}")

            self.current_df_map[f"{instance.name}_{group_name}"] = df_output
            self.current_df_map[f"{trans_name}_{group_name}"] = df_output
            self.current_df_map[f"{instance.name}{group_name}"] = df_output
            self.current_df_map[f"{trans_name}{group_name}"] = df_output
            self.current_df_map[f"{instance.name}.{group_name}"] = df_output
            self.current_df_map[f"{trans_name}.{group_name}"] = df_output

            if condition:
                translated = self.expr_translator.translate_for_filter(condition, f"router_{group_name}")
                all_conditions.append(translated)
            else:
                translated = ""

            groups.append({
                "name": group_name,
                "df_output": df_output,
                "condition": translated
            })

        if groups:
            default_group = next((g for g in groups if g["name"] == "DEFAULT"), None)
            if default_group and all_conditions:
                negated = " & ".join([f"~({c})" for c in all_conditions if c])
                default_group["condition"] = negated

        plan.router_outputs[instance.name] = [g["df_output"] for g in groups]

        steps.append(ApplyRouterStep(
            step_name=f"apply_{instance.name}",
            df_input=input_df,
            groups=groups
        ))

        return steps

    def _handle_sequence(self, instance: Instance, plan: IRPlan) -> Optional[IRStep]:
        input_df = self._get_input_df(instance.name)
        if not input_df:
            input_df = "df_input"

        df_output = self._get_df_name("df_seq")
        self._register_df(instance, df_output)

        transform = self.transform_map.get(instance.transformation_name or instance.name)
        seq_name = "NEXTVAL"
        start_value = 1

        if transform:
            for field in transform.fields:
                if field.name == "NEXTVAL":
                    seq_name = field.name
                    break
            start_value = int(transform.table_attributes.get("Start Value", 1))

        return ApplySequenceStep(
            step_name=f"apply_{instance.name}",
            df_input=input_df,
            df_output=df_output,
            sequence_name=seq_name,
            start_value=start_value
        )

    def _handle_update_strategy(self, instance: Instance, plan: IRPlan) -> Optional[IRStep]:
        input_df = self._get_input_df(instance.name)
        if not input_df:
            input_df = "df_input"

        df_output = self._get_df_name("df_upd")
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

        # Generate proper column update flag based on strategy
        if has_delete:
            step.params["update_condition"] = "lit(False)"
            step.params["delete_condition"] = "lit(True)"
            step.comments.append("DD_DELETE: All incoming rows marked for deletion")
            step.comments.append("Delete existing data before inserting new snapshot")
        elif has_update:
            step.params["update_condition"] = "lit(True)"
            step.params["delete_condition"] = "lit(False)"
            step.comments.append("DD_UPDATE: All incoming rows marked for update")
            step.comments.append("Consider using MergeDelta for upsert operations")
        else:
            step.params["update_condition"] = "lit(False)"
            step.params["delete_condition"] = "lit(False)"
            step.comments.append("DD_INSERT: All incoming rows marked for insert")

        return step

    def _handle_target(self, instance: Instance, plan: IRPlan) -> List[IRStep]:
        steps = []

        input_df = self._get_input_df(instance.name)
        if not input_df:
            plan.add_warning(f"No input DataFrame found for target {instance.name}")
            input_df = "df_final"
        else:
            # When a target has multiple upstream DataFrames, merge them
            # so all mapped columns are available for the write.
            _tgt_all_inputs = self._get_all_input_dfs(instance.name)
            _tgt_extra = [df for df in _tgt_all_inputs if df != input_df]
            for _i, _extra_df in enumerate(_tgt_extra):
                _join_df = self._get_df_name("df_tgt_merge")
                steps.append(ApplyLookupStep(
                    step_name=f"join_target_{instance.name}_{_i}",
                    df_input=input_df,
                    df_output=_join_df,
                    lookup_df=_extra_df,
                    join_predicates=[],
                    join_expr="__common_cols__",
                    output_columns=[],
                    lookup_type="left",
                ))
                input_df = _join_df

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

        # Detect if upstream is a DD_DELETE update strategy
        is_delete = False
        delete_keys = []
        for connector in self.mapping.connectors:
            if connector.to_instance == instance.name:
                from_trans = self.transform_map.get(connector.from_instance)
                if from_trans:
                    strategy_expr = from_trans.table_attributes.get("Update Strategy Expression", "")
                    if "DD_DELETE" in strategy_expr.upper():
                        is_delete = True
                        # Use the connector's effectively mapped field as delete key
                        if connector.from_field and connector.from_field not in delete_keys:
                            delete_keys.append(connector.from_field)
                        break

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
        if is_delete:
            write_step.params["is_delete"] = True
            write_step.params["delete_keys"] = delete_keys
            write_step.comments.append("DD_DELETE strategy detected — will DELETE matching rows instead of INSERT")

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
        for connector in self.mapping.connectors:
            if connector.to_instance == instance_name:
                upstream_instances.append(connector.from_instance)

        _matched_df = None
        _matched_values = []
        for from_inst in upstream_instances:
            if from_inst in self.current_df_map:
                _v = self.current_df_map[from_inst]
                _matched_values.append(_v)
                _matched_df = _v
        if _matched_df:
            if len(_matched_values) > 1:
                for _v in _matched_values:
                    if _v.startswith('df_lkp_result') or _v.startswith('df_jnr_result'):
                        return _v
            return _matched_df

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
                if from_inst in self.current_df_map:
                    df = self.current_df_map[from_inst]
                    if df not in inputs:
                        inputs.append(df)
        return inputs

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
        steps: List[IRStep] = []

        # --- 1. Resolve mapplet identity ------------------------------------------------
        input_df = self._get_input_df(instance.name)
        if not input_df:
            plan.add_warning(f"No input DataFrame found for mapplet {instance.name}")
            input_df = "df_input"

        # When a mapplet has multiple upstream DataFrames (e.g.
        # MPLT_GET_CMS_BLK_SCD_KEY receives columns from both
        # MPLT_LKP_EST_CODE and MPLT_LKP_BLK_CODE), merge them
        # before the entry-point remapping.
        all_inputs = self._get_all_input_dfs(instance.name)
        extra_inputs = [df for df in all_inputs if df != input_df]
        if extra_inputs and input_df != "df_input":
            _cur_df = input_df
            for _i, _extra_df in enumerate(extra_inputs):
                _join_df = self._get_df_name("df_mplt_merge")
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
                _cur_df = _join_df
            input_df = _cur_df

        mapplet_name = instance.transformation_name or instance.name
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

        # Identify INPUT / OUTPUT boundary instances
        input_inst = None
        output_inst = None
        for inst_obj in mpl_instances.values():
            itype = inst_obj.transformation_type or ""
            if "Input Transformation" in itype:
                input_inst = inst_obj
            elif "Output Transformation" in itype:
                output_inst = inst_obj

        # --- 4. Map INPUT ports to upstream columns ------------------------------------
        input_field_map: Dict[str, str] = {}
        for conn in self.mapping.connectors:
            if conn.to_instance == instance.name:
                input_field_map[conn.to_field] = conn.from_field

        # --- 5. Remap external input columns to mapplet INPUT port names ---------------
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
                remap_input_df = self._get_df_name("df_mplt_input")
                steps.append(ApplyExpressionStep(
                    step_name=f"input_{instance.name}",
                    df_input=input_df,
                    df_output=remap_input_df,
                    computed_columns=remap_cols,
                ))
                input_df = remap_input_df

        # --- 6. Track DataFrames within mapplet ----------------------------------------
        mpl_df_map: Dict[str, str] = {}
        if input_inst:
            mpl_df_map[input_inst.name] = input_df

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
            for conn in mpl_connectors:
                if conn.to_instance == mpl_inst_name and conn.from_field != conn.to_field:
                    inst_field_remap[conn.to_field] = conn.from_field

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
                lookup_table = lookup_transform.table_attributes.get("Lookup table name", "")
                lookup_cond = lookup_transform.table_attributes.get("Lookup condition", "")

                # Create lookup read step when we have SQL or table
                lookup_df_name = self._get_df_name("df_mplt_lkp")
                if lookup_sql:
                    lookup_conn = self._resolve_connection_alias(lookup_table or mpl_inst.name)
                    steps.append(ReadSQLStep(
                        step_name=f"read_mplt_{mpl_inst.name}",
                        df_output=lookup_df_name,
                        connection_alias=lookup_conn,
                        query=lookup_sql,
                        is_lookup=True,
                    ))
                elif lookup_table:
                    lookup_conn = self._resolve_connection_alias(lookup_table)
                    steps.append(ReadSQLStep(
                        step_name=f"read_mplt_{mpl_inst.name}",
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

                plan.lookup_dfs[f"mplt_{mpl_inst.name}"] = lookup_df_name

                # Find which DataFrame feeds this lookup within the mapplet
                mpl_input_df = None
                for conn in mpl_connectors:
                    if conn.to_instance == mpl_inst_name:
                        mpl_input_df = mpl_df_map.get(conn.from_instance)
                        if mpl_input_df:
                            break

                if mpl_input_df and lookup_df_name:
                    parsed = self._parse_lookup_condition(lookup_cond, lookup_df_name)
                    join_result_df = self._get_df_name("df_mplt_join")
                    mpl_df_map[mpl_inst_name] = join_result_df

                    # Remap source columns: mapplet port names → actual upstream
                    # column names from the connectors (internal + external).
                    join_predicates = parsed.get("join_columns", [])
                    for jc in join_predicates:
                        if jc.get("source_col") in inst_field_remap:
                            jc["source_col"] = inst_field_remap[jc["source_col"]]

                    # Also remap column names in complex join expressions
                    join_expr = parsed.get("condition_expr") or ""
                    if join_expr:
                        for mpl_port, actual_col in inst_field_remap.items():
                            if mpl_port != actual_col:
                                join_expr = join_expr.replace(mpl_port, actual_col)

                    # Collect output columns from the lookup
                    output_cols = []
                    for field in lookup_transform.fields:
                        port_type = (field.port_type or "").upper()
                        if "OUTPUT" in port_type and "RETURN" not in port_type:
                            output_cols.append(field.name)

                    plan.lookup_return_ports[f"mplt_{mpl_inst.name}"] = output_cols

                    steps.append(ApplyLookupStep(
                        step_name=f"apply_mplt_{mpl_inst.name}",
                        df_input=mpl_input_df,
                        df_output=join_result_df,
                        lookup_df=lookup_df_name,
                        join_predicates=join_predicates,
                        join_expr=join_expr,
                        output_columns=output_cols,
                        lookup_type="left",
                    ))

            elif "Expression" in mpl_inst_type:
                transform = mpl_transforms.get(
                    mpl_inst.transformation_name or mpl_inst.name
                )
                if not transform:
                    continue

                # Find input DataFrame for this expression
                mpl_input_df = None
                for conn in mpl_connectors:
                    if conn.to_instance == mpl_inst_name:
                        mpl_input_df = mpl_df_map.get(conn.from_instance)
                        if mpl_input_df:
                            break

                if not mpl_input_df:
                    # Fall back to the mapplet's primary input
                    mpl_input_df = input_df

                computed_cols = []
                for field in transform.fields:
                    if field.expression and "OUTPUT" in (field.port_type or "").upper():
                        expr_text = field.expression
                        # Remap port names to actual column names using both
                        # external and internal mapplet connector mappings.
                        for mpl_port, actual_col in inst_field_remap.items():
                            if mpl_port != actual_col:
                                expr_text = expr_text.replace(mpl_port, actual_col)
                        translated = self.expr_translator.translate(
                            expr_text, "column", field.name
                        )
                        computed_cols.append(ComputedColumn(
                            name=field.name,
                            expression=translated,
                            datatype=field.datatype,
                        ))

                if computed_cols:
                    expr_df_name = self._get_df_name("df_mplt_expr")
                    mpl_df_map[mpl_inst_name] = expr_df_name
                    steps.append(ApplyExpressionStep(
                        step_name=f"apply_mplt_{mpl_inst.name}",
                        df_input=mpl_input_df,
                        df_output=expr_df_name,
                        computed_columns=computed_cols,
                    ))
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
                        # Remap port names in filter condition using inst_field_remap
                        _filter_expr = filter_cond
                        for _mpl_port, _actual_col in inst_field_remap.items():
                            if _mpl_port != _actual_col:
                                _filter_expr = _filter_expr.replace(_mpl_port, _actual_col)
                        # Translate the filter condition (handles Informatica SQL syntax)
                        _filter_expr = self.expr_translator.translate_for_filter(_filter_expr)
                        _mpl_filt_df = self._get_df_name("df_mplt_fil")
                        mpl_df_map[mpl_inst_name] = _mpl_filt_df
                        steps.append(ApplyFilterStep(
                            step_name=f"apply_mplt_{mpl_inst.name}",
                            df_input=_mpl_filt_input,
                            df_output=_mpl_filt_df,
                            condition=_filter_expr,
                        ))
                    else:
                        # Filter with no condition — pass-through
                        mpl_df_map[mpl_inst_name] = mpl_input_df or input_df

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
            for _i, _extra_df in enumerate(output_extra_dfs):
                _join_df = self._get_df_name("df_mplt_merge")
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
                output_input_df = _join_df

            # Collect output port names from the Mapplet-type wrapper transformation
            mapplet_wrapper = mpl_transforms.get(mapplet_name)
            output_ports: List[str] = []
            if mapplet_wrapper:
                for field in mapplet_wrapper.fields:
                    port_type = (field.port_type or "").upper()
                    if "OUTPUT" in port_type:
                        output_ports.append(field.name)

            df_output = self._get_df_name("df_mplt")
            self._register_df(instance, df_output)

            if steps:
                # There are real transformation steps — the final step's df_output
                # is the mapplet result; register it under the mapplet instance name
                self.current_df_map[instance.name] = df_output
                if output_ports:
                    steps.append(ApplyExpressionStep(
                        step_name=f"apply_{instance.name}",
                        df_input=output_input_df,
                        df_output=df_output,
                        computed_columns=[],
                        output_columns=output_ports,
                    ))
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

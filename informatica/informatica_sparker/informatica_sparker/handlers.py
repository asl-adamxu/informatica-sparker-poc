import re
from typing import Dict, List, Optional, Any
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
        plan = IRPlan(mapping_name=self.mapping.name)

        self.logger.log(LogStage.GRAPH, "Mapping", self.mapping.name, "Building transformation graph", LogLevel.INFO)

        graph_builder = GraphBuilder(self.mapping)
        graph_builder.build()
        ordered_instances = graph_builder.get_topological_order()

        self.logger.log(LogStage.GRAPH, "Mapping", self.mapping.name, f"Graph built with {len(ordered_instances)} nodes", LogLevel.SUCCESS)

        for inst_name in ordered_instances:
            instance = self.instance_map.get(inst_name)
            if not instance:
                self.logger.log(LogStage.TRANSFORM, "Instance", inst_name, "Instance not found in map", LogLevel.WARNING)
                continue

            inst_type = self._resolve_transformation_type(instance)

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
                step = self._handle_expression(instance, plan)
                if step:
                    plan.add_step(step)
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

            elif inst_type == "Sequence Generator":
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
                        else f"/data/{source.name}")
            options = {}
            if source_config:
                options = {
                    "delimiter": source_config.delimiter,
                    "header": str(source_config.header).lower(),
                    "quote": source_config.quote_char
                }
            return ReadFileStep(
                step_name=f"read_{instance.name}",
                df_output=df_name,
                file_format=file_format,
                file_path=file_path,
                options=options,
                table_name=source.name
            )

    def _handle_source_qualifier(self, instance: Instance, plan: IRPlan) -> Optional[IRStep]:
        input_df = self._get_input_df(instance.name)
        if not input_df:
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

        conn_alias = "source_db"
        source_inputs = self._get_source_inputs_for_sq(instance.name)
        if source_inputs:
            source_conn = source_inputs[0].get("connection")
            if source_conn and source_conn in self.user_config.db_connections:
                conn_alias = source_conn
            elif source_conn:
                for k in self.user_config.db_connections.keys():
                    if source_conn.lower() in k.lower() or k.lower() in source_conn.lower():
                        conn_alias = k
                        break
                else:
                    conn_alias = source_conn

        # Determine source database type for potential SQL translation
        source_db_type = "oracle"
        if source_inputs:
            raw_type = source_inputs[0].get("database_type", "")
            if raw_type:
                from .models import normalize_db_type
                source_db_type = normalize_db_type(raw_type)
        
        if use_sql_pushdown:
            # Only translate SQL if source and target DB types differ
            target_db_type = getattr(self.user_config, 'target_db_type', 'spark')
            sql_translated = False
            if source_db_type != target_db_type and target_db_type != 'oracle':
                from .utils.sql_dialect import translate_sql
                translated_sql = translate_sql(final_sql, source_dialect=source_db_type, target_dialect=target_db_type)
                if translated_sql != final_sql:
                    sql_translated = True
                final_sql = translated_sql
            
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
            step.comments.append(f"SQL Pushdown: Executing SQ SQL on source database ({source_db_type})")
            if sql_translated:
                step.comments.append(f"SQL translated from {source_db_type} to {target_db_type}")
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
        input_df = self._get_input_df(instance.name)
        if not input_df:
            plan.add_warning(f"No input DataFrame found for {instance.name}")
            input_df = "df_input"

        df_output = self._get_df_name("df_exp")
        self._register_df(instance, df_output)

        transform = self.transform_map.get(instance.transformation_name or instance.name)
        computed_columns = []
        output_columns = []

        if transform:
            for field in transform.fields:
                if "OUTPUT" in field.port_type:
                    output_columns.append(field.name)
                    if field.expression:
                        translated = self.expr_translator.translate(field.expression, "column", field.name)
                        sanitized = sanitize_for_expr(translated)
                        computed_columns.append(ComputedColumn(
                            name=field.name,
                            expression=sanitized,
                            datatype=field.datatype
                        ))

        step = ApplyExpressionStep(
            step_name=f"apply_{instance.name}",
            df_input=input_df,
            df_output=df_output,
            computed_columns=computed_columns
        )
        step.params["output_columns"] = output_columns

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
            steps.append(ReadSQLStep(
                step_name=f"read_{instance.name}",
                df_output=lookup_df,
                connection_alias=lookup_conn,
                query=lookup_sql,
                is_lookup=True
            ))
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
                    pyspark_agg = self._translate_aggregation_expr(field.expression, field.name)
                    if pyspark_agg:
                        aggregations[field.name] = pyspark_agg
                        self.logger.log_transformation(instance.name, "Aggregator", f"Aggregation: {field.name} = {pyspark_agg}", LogLevel.INFO)

            self.logger.log_transformation(instance.name, "Aggregator", f"Found {len(group_by)} GROUP BY keys, {len(aggregations)} aggregations", LogLevel.INFO)

        return ApplyAggregatorStep(
            step_name=f"apply_{instance.name}",
            df_input=input_df,
            df_output=df_output,
            group_by=group_by,
            aggregations=aggregations
        )

    def _translate_aggregation_expr(self, expr: str, field_name: str) -> str:
        expr_upper = expr.upper().strip()

        agg_patterns = [
            (r'^MIN\s*\(\s*([^)]+)\s*\)$', 'F.min', False),
            (r'^MAX\s*\(\s*([^)]+)\s*\)$', 'F.max', False),
            (r'^SUM\s*\(\s*([^)]+)\s*\)$', 'F.sum', False),
            (r'^COUNT\s*\(\s*([^)]*)\s*\)$', 'F.count', False),
            (r'^AVG\s*\(\s*([^)]+)\s*\)$', 'F.avg', False),
            (r'^FIRST\s*\(\s*([^)]+)\s*\)$', 'F.first', False),
            (r'^LAST\s*\(\s*([^)]+)\s*\)$', 'F.last', False),
            (r'^MEDIAN\s*\(\s*([^)]+)\s*\)$', 'F.percentile_approx', True),
            (r'^STDDEV\s*\(\s*([^)]+)\s*\)$', 'F.stddev', False),
            (r'^VARIANCE\s*\(\s*([^)]+)\s*\)$', 'F.variance', False),
        ]

        for pattern, pyspark_func, is_median in agg_patterns:
            match = re.match(pattern, expr_upper, re.IGNORECASE)
            if match:
                col_name = match.group(1).strip() if match.group(1) else "*"
                if col_name == "*":
                    return f'{pyspark_func}("*")'
                if is_median:
                    return f'{pyspark_func}("{col_name}", 0.5)'
                return f'{pyspark_func}("{col_name}")'

        if expr_upper == field_name.upper():
            return ""

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

        for from_inst in upstream_instances:
            if from_inst in self.current_df_map:
                return self.current_df_map[from_inst]

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
            for sep in ['.', '_', ':', '']:
                for key in self.current_df_map:
                    if key.startswith(from_inst.split('.')[0].split('_')[0].split(':')[0]):
                        if from_inst.replace('.', sep).replace('_', sep).replace(':', sep) in key.replace('.', sep).replace('_', sep).replace(':', sep):
                            return self.current_df_map[key]

            for key in self.current_df_map:
                from_norm = from_inst.lower().replace('.', '_').replace(':', '_')
                key_norm = key.lower().replace('.', '_').replace(':', '_')
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
        steps = []

        input_df = self._get_input_df(instance.name)
        if not input_df:
            plan.add_warning(f"No input DataFrame found for mapplet {instance.name}")
            input_df = "df_input"

        df_output = self._get_df_name("df_mplt")
        self._register_df(instance, df_output)

        mapplet_name = instance.transformation_name or instance.name
        computed_columns = []

        mapplet_def = self.mapping.mapplets.get(mapplet_name) if hasattr(self.mapping, 'mapplets') else None

        if mapplet_def:
            for transform in mapplet_def.get("transformations", []):
                if transform.type and "Expression" in transform.type:
                    for field in transform.fields:
                        if field.expression and "OUTPUT" in field.port_type:
                            translated = self.expr_translator.translate(field.expression, "column", field.name)
                            computed_columns.append(ComputedColumn(
                                name=field.name,
                                expression=translated,
                                datatype=field.datatype
                            ))
                elif transform.type and "Filter" in transform.type:
                    filter_cond = transform.table_attributes.get("Filter Condition", "")
                    if filter_cond:
                        plan.add_warning(f"Mapplet {mapplet_name} contains Filter: {filter_cond}")

        if not computed_columns:
            transform = self.transform_map.get(mapplet_name)
            if transform:
                for field in transform.fields:
                    if field.expression and "OUTPUT" in field.port_type:
                        translated = self.expr_translator.translate(field.expression, "column", field.name)
                        computed_columns.append(ComputedColumn(
                            name=field.name,
                            expression=translated,
                            datatype=field.datatype
                        ))

        if computed_columns:
            self.logger.log_mapplet(instance.name, f"Converted {len(computed_columns)} expressions", LogLevel.SUCCESS)
            steps.append(ApplyExpressionStep(
                step_name=f"apply_{instance.name}",
                df_input=input_df,
                df_output=df_output,
                computed_columns=computed_columns
            ))
        else:
            self.current_df_map[instance.name] = input_df
            if instance.transformation_name and instance.transformation_name != instance.name:
                self.current_df_map[instance.transformation_name] = input_df
            self.logger.log_mapplet(instance.name, "Treated as pass-through (no expressions found)", LogLevel.WARNING)
            plan.add_warning(f"Mapplet {instance.name} treated as pass-through (no expressions found)")

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

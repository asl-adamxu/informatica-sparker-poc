import re
from typing import List, Dict, Any
from .models import (
    MappingDefinition, MappingAnalysis, TransformSummary,
    AnalysisResult, SourceType, ExpressionAnalysis
)
from .graph_builder import GraphBuilder
from .expr_scanner import ExpressionScanner


class Analyzer:

    TRANSFORM_TYPE_MAP = {
        "Source Qualifier": "source_qualifiers",
        "Expression": "expressions",
        "Filter": "filters",
        "Joiner": "joiners",
        "Lookup Procedure": "lookups",
        "Stored Procedure": "stored_procedures",
        "Update Strategy": "update_strategies",
        "Aggregator": "aggregators",
        "Sorter": "sorters",
        "Router": "routers",
        "Union": "unions"
    }

    def __init__(self, mappings: List[MappingDefinition], xml_type: str = "POWERMART",
                 folder_name: str = "", repository_name: str = ""):
        self.mappings = mappings
        self.xml_type = xml_type
        self.folder_name = folder_name
        self.repository_name = repository_name

    def analyze(self) -> AnalysisResult:
        result = AnalysisResult(
            xml_type=self.xml_type,
            folder_name=self.folder_name,
            repository_name=self.repository_name,
            mapping_count=len(self.mappings)
        )

        for mapping in self.mappings:
            analysis = self._analyze_mapping(mapping)
            result.mappings.append(analysis)

        return result

    def _analyze_mapping(self, mapping: MappingDefinition) -> MappingAnalysis:
        graph_builder = GraphBuilder(mapping)
        graph_builder.build()

        expr_scanner = ExpressionScanner(mapping)
        expr_analysis = expr_scanner.scan()

        raw_connectors = [
            {"from_instance": c.from_instance, "to_instance": c.to_instance}
            for c in mapping.connectors
            if c.from_instance and c.to_instance
        ]

        analysis = MappingAnalysis(
            mapping_name=mapping.name,
            sources=self._analyze_sources(mapping),
            targets=self._analyze_targets(mapping),
            transform_summary=self._count_transforms(mapping),
            transformations=self._analyze_transformations(mapping),
            sql_analysis=self._analyze_sql_elements(mapping),
            connectors_analysis=self._analyze_connectors(mapping),
            connectors=raw_connectors,
            parameters=self._extract_parameters(mapping),
            main_flow=graph_builder.get_ordered_flow(),
            detached_lookups=self._find_detached_lookups(mapping, graph_builder),
            detached_sps=self._find_detached_sps(mapping, graph_builder),
            expression_analysis=expr_analysis,
            required_user_inputs=self._determine_required_inputs(mapping),
            flow_text=graph_builder.get_flow_text()
        )

        return analysis

    def _analyze_sql_elements(self, mapping: MappingDefinition) -> Dict[str, Any]:
        sql_analysis = {
            "tables": [],
            "select_queries": [],
            "stored_procedures": [],
            "lookup_queries": [],
            "filter_conditions": [],
            "pre_post_sql": []
        }

        for source in mapping.sources:
            table_info = {
                "name": source.name,
                "type": "SOURCE",
                "owner": source.owner_name,
                "database": source.db_name,
                "database_type": source.database_type
            }
            sql_analysis["tables"].append(table_info)

        for target in mapping.targets:
            table_info = {
                "name": target.name,
                "type": "TARGET",
                "owner": "",
                "database": "",
                "database_type": target.database_type
            }
            sql_analysis["tables"].append(table_info)

        for transform in mapping.transformations:
            if transform.type == "Source Qualifier":
                sql_query = transform.table_attributes.get("Sql Query", "")
                if not sql_query:
                    sql_query = transform.table_attributes.get("User Defined Join", "")
                if sql_query:
                    sql_analysis["select_queries"].append({
                        "name": transform.name,
                        "type": "Source Qualifier",
                        "query": sql_query
                    })

            elif transform.type == "Lookup Procedure":
                lookup_sql = transform.table_attributes.get("Lookup Sql Override", "")
                if not lookup_sql:
                    lookup_sql = transform.table_attributes.get("Sql Query", "")
                lookup_table = transform.table_attributes.get("Lookup table name", "")

                if lookup_table:
                    sql_analysis["tables"].append({
                        "name": lookup_table,
                        "type": "LOOKUP",
                        "owner": "",
                        "database": "",
                        "database_type": ""
                    })

                if lookup_sql:
                    sql_analysis["lookup_queries"].append({
                        "name": transform.name,
                        "type": "Lookup Override",
                        "query": lookup_sql,
                        "table": lookup_table
                    })

            elif transform.type == "Stored Procedure":
                sp_call = transform.table_attributes.get("Call Text", "")
                sp_name = transform.table_attributes.get("Stored Procedure Name", transform.name)
                sql_analysis["stored_procedures"].append({
                    "name": sp_name,
                    "transformation": transform.name,
                    "type": transform.table_attributes.get("Stored Procedure Type", "Normal"),
                    "call_text": sp_call if sp_call else "No call text defined"
                })

            elif transform.type == "Filter":
                filter_condition = transform.table_attributes.get("Filter Condition", "")
                if not filter_condition:
                    for field in transform.fields:
                        if hasattr(field, 'expression') and field.expression:
                            filter_condition = field.expression
                            break
                if filter_condition:
                    sql_analysis["filter_conditions"].append({
                        "name": transform.name,
                        "type": "Filter",
                        "condition": filter_condition
                    })

            pre_sql = transform.table_attributes.get("Pre SQL", "")
            post_sql = transform.table_attributes.get("Post SQL", "")
            if pre_sql:
                sql_analysis["pre_post_sql"].append({
                    "name": transform.name,
                    "type": "Pre SQL",
                    "sql": pre_sql
                })
            if post_sql:
                sql_analysis["pre_post_sql"].append({
                    "name": transform.name,
                    "type": "Post SQL",
                    "sql": post_sql
                })

        return sql_analysis

    def _analyze_connectors(self, mapping: MappingDefinition) -> Dict[str, Any]:
        connectors_analysis = {
            "total_count": len(mapping.connectors),
            "connectors": [],
            "by_source_instance": {},
            "summary": {
                "source_to_sq": 0,
                "sq_to_transform": 0,
                "transform_to_transform": 0,
                "transform_to_target": 0
            }
        }

        instance_types = {}
        for instance in mapping.instances:
            instance_types[instance.name] = {
                "type": instance.transformation_type or instance.type,
                "transformation_name": instance.transformation_name
            }

        source_names = {s.name for s in mapping.sources}
        target_names = {t.name for t in mapping.targets}

        for connector in mapping.connectors:
            from_type = connector.from_instance_type or instance_types.get(connector.from_instance, {}).get("type", "Unknown")
            to_type = connector.to_instance_type or instance_types.get(connector.to_instance, {}).get("type", "Unknown")

            connector_info = {
                "from_instance": connector.from_instance,
                "from_field": connector.from_field,
                "from_type": from_type,
                "to_instance": connector.to_instance,
                "to_field": connector.to_field,
                "to_type": to_type
            }
            connectors_analysis["connectors"].append(connector_info)

            if connector.from_instance not in connectors_analysis["by_source_instance"]:
                connectors_analysis["by_source_instance"][connector.from_instance] = {
                    "type": from_type,
                    "connections": []
                }
            connectors_analysis["by_source_instance"][connector.from_instance]["connections"].append({
                "field": connector.from_field,
                "to_instance": connector.to_instance,
                "to_field": connector.to_field,
                "to_type": to_type
            })

            from_is_source = connector.from_instance in source_names or from_type in ["Source Definition", "Source Qualifier"]
            to_is_target = connector.to_instance in target_names or to_type in ["Target Definition"]
            from_is_sq = from_type == "Source Qualifier"
            to_is_sq = to_type == "Source Qualifier"

            if from_is_source and to_is_sq:
                connectors_analysis["summary"]["source_to_sq"] += 1
            elif from_is_sq and not to_is_target:
                connectors_analysis["summary"]["sq_to_transform"] += 1
            elif to_is_target:
                connectors_analysis["summary"]["transform_to_target"] += 1
            else:
                connectors_analysis["summary"]["transform_to_transform"] += 1

        return connectors_analysis

    def _analyze_transformations(self, mapping: MappingDefinition) -> List[Dict[str, Any]]:
        transformations = []

        pyspark_mapping = {
            "Source Qualifier": "spark.read.jdbc() / spark.read.format()",
            "Expression": "df.withColumn() / df.select()",
            "Filter": "df.filter() / df.where()",
            "Joiner": "df.join()",
            "Lookup Procedure": "df.join() with broadcast",
            "Stored Procedure": "spark.sql() / Custom UDF",
            "Update Strategy": "df.withColumn('update_flag')",
            "Aggregator": "df.groupBy().agg()",
            "Sorter": "df.orderBy() / df.sort()",
            "Router": "Multiple df.filter() branches",
            "Union": "df.union() / df.unionByName()",
            "Sequence Generator": "monotonically_increasing_id()",
            "Normalizer": "df.explode() / flatten()",
            "Rank": "Window functions with row_number()"
        }

        for transform in mapping.transformations:
            transform_info = {
                "name": transform.name,
                "type": transform.type,
                "description": transform.description,
                "field_count": len(transform.fields),
                "pyspark_equivalent": pyspark_mapping.get(transform.type, "Custom transformation"),
                "fields": [],
                "attributes": {}
            }

            for field in transform.fields:
                field_info = {
                    "name": field.name,
                    "datatype": field.datatype,
                    "precision": field.precision,
                    "scale": field.scale,
                    "expression": field.expression if hasattr(field, 'expression') else "",
                    "port_type": field.port_type if hasattr(field, 'port_type') else ""
                }
                transform_info["fields"].append(field_info)

            if hasattr(transform, 'attributes'):
                transform_info["attributes"] = transform.attributes

            transformations.append(transform_info)

        return transformations

    def _analyze_sources(self, mapping: MappingDefinition) -> List[Dict[str, Any]]:
        sources = []
        for source in mapping.sources:
            source_info = {
                "name": source.name,
                "type": source.source_type.value,
                "database_type": source.database_type,
                "db_name": source.db_name,
                "owner_name": source.owner_name,
                "field_count": len(source.fields),
                "confidence": "high" if source.source_type != SourceType.UNKNOWN else "low",
                "fields": []
            }

            for field in source.fields:
                field_info = {
                    "name": field.name,
                    "datatype": field.datatype,
                    "precision": field.precision,
                    "scale": field.scale,
                    "nullable": field.nullable,
                    "key_type": field.key_type
                }
                source_info["fields"].append(field_info)

            if source.source_type == SourceType.FILE:
                source_info["file_format"] = source.file_format.value if source.file_format else "unknown"
                source_info["needs_path"] = True
            else:
                source_info["needs_connection"] = True

            sql_query = source.table_attributes.get("Sql Query", "")
            if sql_query:
                source_info["has_sql_query"] = True
                source_info["sql_query"] = sql_query
                source_info["sql_query_preview"] = sql_query[:100] + "..." if len(sql_query) > 100 else sql_query

            sources.append(source_info)

        return sources

    def _analyze_targets(self, mapping: MappingDefinition) -> List[Dict[str, Any]]:
        targets = []
        for target in mapping.targets:
            db_type = target.database_type.lower() if target.database_type else ""
            if "file" in db_type or "flat" in db_type:
                target_type = "FILE"
            elif "salesforce" in db_type or "sfdc" in db_type:
                target_type = "SFDC"
            else:
                target_type = "SQL"

            target_info = {
                "name": target.name,
                "type": target_type,
                "database_type": target.database_type,
                "db_name": getattr(target, 'db_name', ''),
                "owner_name": getattr(target, 'owner_name', ''),
                "field_count": len(target.fields),
                "needs_destination": True,
                "pyspark_write": f"df.write.jdbc(url, '{target.name}', properties)",
                "fields": []
            }

            for field in target.fields:
                field_info = {
                    "name": field.name,
                    "datatype": field.datatype,
                    "precision": field.precision,
                    "scale": field.scale,
                    "nullable": field.nullable,
                    "key_type": getattr(field, 'key_type', '')
                }
                target_info["fields"].append(field_info)

            targets.append(target_info)

        return targets

    def _count_transforms(self, mapping: MappingDefinition) -> TransformSummary:
        summary = TransformSummary()

        for transform in mapping.transformations:
            transform_type = transform.type
            attr_name = self.TRANSFORM_TYPE_MAP.get(transform_type, "others")
            current_value = getattr(summary, attr_name, 0)
            setattr(summary, attr_name, current_value + 1)

        return summary

    def _find_detached_lookups(self, mapping: MappingDefinition,
                                graph_builder: GraphBuilder) -> List[str]:
        detached = graph_builder.get_detached_instances()
        lookups = []

        for inst_name in detached:
            instance = graph_builder.instance_map.get(inst_name)
            if instance and "Lookup" in (instance.type or instance.transformation_type or ""):
                lookups.append(inst_name)

        return lookups

    def _find_detached_sps(self, mapping: MappingDefinition,
                           graph_builder: GraphBuilder) -> List[str]:
        detached = graph_builder.get_detached_instances()
        sps = []

        for inst_name in detached:
            instance = graph_builder.instance_map.get(inst_name)
            if instance and "Stored Procedure" in (instance.type or instance.transformation_type or ""):
                sps.append(inst_name)

        return sps

    def _determine_required_inputs(self, mapping: MappingDefinition) -> List[Dict[str, Any]]:
        required = []

        for source in mapping.sources:
            if source.source_type == SourceType.SQL:
                required.append({
                    "type": "connection",
                    "source_name": source.name,
                    "label": f"Database connection for {source.name}",
                    "db_name": source.db_name,
                    "database_type": source.database_type
                })
            elif source.source_type == SourceType.FILE:
                required.append({
                    "type": "file_config",
                    "source_name": source.name,
                    "label": f"File configuration for {source.name}",
                    "needs": ["format", "location", "path"]
                })
            else:
                required.append({
                    "type": "source_type",
                    "source_name": source.name,
                    "label": f"Source type for {source.name}",
                    "needs": ["type"]
                })

        for target in mapping.targets:
            required.append({
                "type": "target_config",
                "target_name": target.name,
                "label": f"Target configuration for {target.name}",
                "needs": ["format", "destination", "write_mode"]
            })

        return required

    def _extract_parameters(self, mapping: MappingDefinition) -> Dict[str, Any]:
        parameters = {
            "session_variables": [],
            "mapping_variables": [],
            "workflow_variables": [],
            "parameter_files": [],
            "pm_variables": []
        }

        for map_var in mapping.mapping_variables:
            var_name = map_var.name
            parameters["mapping_variables"].append({
                "name": f"$${var_name}",
                "type": "Mapping Variable",
                "datatype": map_var.datatype,
                "precision": map_var.precision,
                "scale": map_var.scale,
                "default_value": map_var.default_value or "N/A",
                "is_expression_variable": map_var.is_expression_variable,
                "aggregation_type": map_var.aggregation_type or "N/A",
                "pyspark_equivalent": "Local variable in PySpark script"
            })

        all_text = []

        for transform in mapping.transformations:
            for field in transform.fields:
                if field.expression:
                    all_text.append(field.expression)
                if field.default_value:
                    all_text.append(field.default_value)

            for key, value in transform.table_attributes.items():
                if isinstance(value, str):
                    all_text.append(value)
                    if 'parameter' in key.lower() or 'prm' in key.lower():
                        if value and '.prm' in value.lower():
                            parameters["parameter_files"].append({
                                "name": key,
                                "value": value,
                                "source": transform.name
                            })

        for source in mapping.sources:
            for key, value in source.table_attributes.items():
                if isinstance(value, str):
                    all_text.append(value)

        for target in mapping.targets:
            if hasattr(target, 'table_attributes') and target.table_attributes:
                for key, value in target.table_attributes.items():
                    if isinstance(value, str):
                        all_text.append(value)

        combined_text = ' '.join(all_text)

        existing_var_names = {v["name"] for v in parameters["mapping_variables"]}
        session_vars = set(re.findall(r'\$\$([A-Za-z_][A-Za-z0-9_]*)', combined_text))
        for var in sorted(session_vars):
            var_full_name = f"$${var}"
            if var_full_name not in existing_var_names:
                if not var.startswith("MAPPING.") and not var.startswith("WORKFLOW."):
                    parameters["session_variables"].append({
                        "name": var_full_name,
                        "type": "Session Variable",
                        "pyspark_equivalent": "spark.conf.get() or environment variable"
                    })

        pm_vars = set(re.findall(r'\$PM([A-Za-z_][A-Za-z0-9_]*)', combined_text))
        for var in sorted(pm_vars):
            parameters["pm_variables"].append({
                "name": f"$PM{var}",
                "type": "PowerCenter Variable",
                "pyspark_equivalent": "Databricks widget or job parameter"
            })

        mapping_ref_vars = set(re.findall(r'\$\$MAPPING\.([A-Za-z_][A-Za-z0-9_]*)', combined_text))
        for var in sorted(mapping_ref_vars):
            var_full_name = f"$$MAPPING.{var}"
            if var_full_name not in existing_var_names:
                parameters["mapping_variables"].append({
                    "name": var_full_name,
                    "type": "Mapping Variable Reference",
                    "pyspark_equivalent": "Local variable in PySpark script"
                })

        workflow_vars = set(re.findall(r'\$\$WORKFLOW\.([A-Za-z_][A-Za-z0-9_]*)', combined_text))
        for var in sorted(workflow_vars):
            parameters["workflow_variables"].append({
                "name": f"$$WORKFLOW.{var}",
                "type": "Workflow Variable",
                "pyspark_equivalent": "Airflow/Databricks workflow variable"
            })

        prm_files = set(re.findall(r'([^\s"\'<>]+\.prm)', combined_text, re.IGNORECASE))
        for prm in sorted(prm_files):
            if not any(p["value"] == prm for p in parameters["parameter_files"]):
                parameters["parameter_files"].append({
                    "name": "Parameter File",
                    "value": prm,
                    "source": "Expression"
                })

        parameters["total_count"] = (
            len(parameters["session_variables"]) +
            len(parameters["pm_variables"]) +
            len(parameters["mapping_variables"]) +
            len(parameters["workflow_variables"]) +
            len(parameters["parameter_files"])
        )

        return parameters

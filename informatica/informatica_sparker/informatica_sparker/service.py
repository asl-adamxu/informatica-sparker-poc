import os
import re
import json
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Dict, Any
from .models import (
    UserConfig, GeneratedFile, GenerationResult, ConversionReport,
    SQLQueryInfo, SourceDetectionResult, SourceType, ReportItem,
    SourceConnectionInfo, ConnectorType
)
from .parser import InfaXMLParser
from .analyzer import Analyzer
from .handlers import TransformHandlers
from .codegen import CodeGenerator
from .ir import IRPlan, IRStepType
from .logger import ConversionLogger


class ConversionService:

    def __init__(self, user_config: Optional[UserConfig] = None):
        self.user_config = user_config or UserConfig()
        self.logger = ConversionLogger()
        self.codegen = CodeGenerator()

    def convert_file(self, xml_path: str, output_dir: str = "output") -> GenerationResult:
        xml_path = Path(xml_path)
        if not xml_path.exists():
            raise FileNotFoundError(f"XML file not found: {xml_path}")

        xml_content = xml_path.read_bytes()
        return self.convert_bytes(xml_content, output_dir=output_dir)

    def convert_bytes(self, xml_content: bytes, output_dir: str = "output") -> GenerationResult:
        parser = InfaXMLParser(xml_content)
        if not parser.parse():
            raise ValueError("Failed to parse XML content")

        mappings = parser.get_mappings()
        if not mappings:
            raise ValueError("No mappings found in XML")

        workflow_analysis = parser.get_workflow_analysis()

        all_files: List[GeneratedFile] = []
        all_warnings: List[str] = []
        all_errors: List[str] = []
        all_sql_queries: List[SQLQueryInfo] = []
        all_source_detections: List[SourceDetectionResult] = []
        all_reports: List[ConversionReport] = []
        ir_plans: List[IRPlan] = []
        mapping_names: List[str] = []

        for mapping in mappings:
            mapping_name = mapping.name
            mapping_names.append(mapping_name)

            source_detections = self._detect_sources(mapping)
            all_source_detections.extend(source_detections)

            try:
                handlers = TransformHandlers(mapping, self.user_config, self.logger)
                ir_plan = handlers.build_ir_plan()
                # Set source/target DB type on plan
                if self.user_config.source_db_type:
                    ir_plan.source_db_type = self.user_config.source_db_type
                if self.user_config.target_db_type:
                    ir_plan.target_db_type = self.user_config.target_db_type
                ir_plans.append(ir_plan)

                self.codegen.reset()
                generated = self.codegen.generate(ir_plan, self.user_config)
                all_files.extend(generated)
                all_warnings.extend(ir_plan.warnings)

                sql_queries = self._extract_sql_queries(ir_plan)
                all_sql_queries.extend(sql_queries)

                report = ConversionReport(
                    mapping_name=mapping_name,
                    status="success",
                    generated_files=[f.filename for f in generated],
                    warnings=[ReportItem(severity="warning", message=w) for w in ir_plan.warnings]
                )
                all_reports.append(report)

            except Exception as e:
                error_msg = f"Error converting mapping '{mapping_name}': {str(e)}"
                all_errors.append(error_msg)
                report = ConversionReport(
                    mapping_name=mapping_name,
                    status="failed",
                    errors=[ReportItem(severity="error", message=str(e))]
                )
                all_reports.append(report)

        # Generate runtime_lib.py (shared library)
        runtime_lib_file = self._generate_runtime_lib()
        if runtime_lib_file:
            all_files.append(runtime_lib_file)

        workflow_file = self._generate_workflow_file(
            mapping_names, workflow_analysis, parser.folder_name
        )
        if workflow_file:
            all_files.append(workflow_file)

        config_file = self._generate_config_file(
            mapping_names, mappings, all_source_detections, parser.folder_name
        )
        if config_file:
            all_files.append(config_file)

        if all_sql_queries:
            sql_file = self._generate_sql_file(all_sql_queries)
            all_files.append(sql_file)

        error_log = self._generate_error_log(
            all_warnings, all_errors, all_reports, all_source_detections
        )
        all_files.append(error_log)

        result = GenerationResult(
            files=all_files,
            warnings=all_warnings,
            errors=all_errors,
            mapping_count=len(mappings),
            mappings_processed=len([r for r in all_reports if r.status == "success"]),
            reports=all_reports,
            sql_queries=all_sql_queries,
            source_detections=all_source_detections,
        )

        if output_dir:
            self._write_output(all_files, output_dir)

        return result

    def analyze_file(self, xml_path: str) -> dict:
        xml_path = Path(xml_path)
        if not xml_path.exists():
            raise FileNotFoundError(f"XML file not found: {xml_path}")

        xml_content = xml_path.read_bytes()
        return self.analyze_bytes(xml_content)

    def analyze_bytes(self, xml_content: bytes) -> dict:
        parser = InfaXMLParser(xml_content)
        if not parser.parse():
            raise ValueError("Failed to parse XML content")

        mappings = parser.get_mappings()
        workflow_analysis = parser.get_workflow_analysis()
        configs = parser.get_configs()

        analyzer = Analyzer(
            mappings,
            xml_type=parser.xml_type,
            folder_name=parser.folder_name,
            repository_name=parser.repository_name
        )
        analysis_result = analyzer.analyze()

        source_detections = []
        for mapping in mappings:
            source_detections.extend(self._detect_sources(mapping))

        return {
            "xml_type": parser.xml_type,
            "folder_name": parser.folder_name,
            "repository_name": parser.repository_name,
            "mapping_count": len(mappings),
            "mappings": [m.__dict__ if hasattr(m, '__dict__') else m for m in analysis_result.mappings],
            "workflow_analysis": workflow_analysis,
            "configs": configs,
            "source_detections": [sd.model_dump() for sd in source_detections],
        }

    def _detect_sources(self, mapping) -> List[SourceDetectionResult]:
        detections = []
        for source in mapping.sources:
            detection = SourceDetectionResult(
                source_name=source.name,
                detected_type=source.source_type,
                file_format=source.file_format,
                connection_info=source.connection_info,
            )
            notes = []
            if source.source_type == SourceType.SQL:
                db_type = source.database_type or "unknown"
                notes.append(f"Database type: {db_type}")
                if source.db_name:
                    notes.append(f"Database name: {source.db_name}")
                if source.owner_name:
                    notes.append(f"Schema/Owner: {source.owner_name}")
                if source.connection_info:
                    if source.connection_info.driver_class:
                        notes.append(f"JDBC driver: {source.connection_info.driver_class}")
                    if source.connection_info.driver_jar:
                        notes.append(f"Driver JAR: {source.connection_info.driver_jar}")
                    if source.connection_info.connection_name:
                        notes.append(f"Connection name: {source.connection_info.connection_name}")
                detection.confidence = "high"
            elif source.source_type == SourceType.FILE:
                fmt = source.file_format.value if source.file_format else "unknown"
                notes.append(f"File format: {fmt}")
                if source.file_path:
                    notes.append(f"File path: {source.file_path}")
                elif source.file_name:
                    notes.append(f"File name: {source.file_name}")
                if source.file_directory:
                    notes.append(f"Directory: {source.file_directory}")
                if source.file_location:
                    notes.append(f"Location: {source.file_location.value}")
                if source.delimiter and source.delimiter != ",":
                    notes.append(f"Delimiter: {repr(source.delimiter)}")
                detection.confidence = "high"
            else:
                notes.append("Source type could not be determined automatically")
                notes.append(f"Database type attribute: '{source.database_type}'")
                detection.confidence = "low"
            detection.detection_notes = notes
            detections.append(detection)
        return detections

    def _extract_sql_queries(self, plan: IRPlan) -> List[SQLQueryInfo]:
        queries = []
        for step in plan.steps:
            if step.step_type == IRStepType.READ_SQL:
                query = step.params.get("query", "")
                table = step.params.get("table_name", "")
                conn = step.params.get("connection_alias", "")
                if query or table:
                    queries.append(SQLQueryInfo(
                        mapping_name=plan.mapping_name,
                        step_name=step.step_name,
                        query_type="SELECT" if query else "TABLE_READ",
                        query=query if query else f"SELECT * FROM {table}",
                        source_table=table,
                        connection=conn,
                    ))
            elif step.step_type == IRStepType.APPLY_SOURCE_QUALIFIER:
                sq_query = step.params.get("sql_query", "")
                if sq_query:
                    queries.append(SQLQueryInfo(
                        mapping_name=plan.mapping_name,
                        step_name=step.step_name,
                        query_type="SOURCE_QUALIFIER",
                        query=sq_query,
                    ))
            elif step.step_type == IRStepType.APPLY_LOOKUP:
                lkp_query = step.params.get("lookup_query", "")
                if not lkp_query:
                    lkp_query = step.params.get("condition", "")
                if lkp_query:
                    queries.append(SQLQueryInfo(
                        mapping_name=plan.mapping_name,
                        step_name=step.step_name,
                        query_type="LOOKUP",
                        query=lkp_query,
                    ))
            elif step.step_type == IRStepType.EXECUTE_SQL:
                exec_query = step.params.get("query", "")
                if exec_query:
                    queries.append(SQLQueryInfo(
                        mapping_name=plan.mapping_name,
                        step_name=step.step_name,
                        query_type="EXECUTE_SQL",
                        query=exec_query,
                    ))
            elif step.step_type == IRStepType.WRITE_TARGET:
                target_table = step.params.get("table_name", "")
                if target_table:
                    queries.append(SQLQueryInfo(
                        mapping_name=plan.mapping_name,
                        step_name=step.step_name,
                        query_type="INSERT_TARGET",
                        query=f"-- Write to target table: {target_table}",
                        source_table=target_table,
                        connection=step.params.get("connection_alias", ""),
                    ))
        return queries

    def _make_safe_name(self, name: str) -> str:
        safe = re.sub(r'[^a-zA-Z0-9_]', '_', name)
        if safe and safe[0].isdigit():
            safe = '_' + safe
        return safe.lower()

    def _generate_workflow_file(self, mapping_names: List[str],
                                 workflow_analysis: dict,
                                 folder_name: str) -> Optional[GeneratedFile]:
        if not mapping_names:
            return None

        workflows = workflow_analysis.get("workflows", [])
        workflow_name = workflows[0]["name"] if workflows else f"wf_{folder_name or 'workflow'}"

        mappings_info = []
        for name in mapping_names:
            safe = self._make_safe_name(name)
            mappings_info.append({
                "name": name,
                "safe_name": safe,
                "module_name": safe,
            })

        sessions = workflow_analysis.get("sessions", [])
        session_list = []
        for s in sessions:
            session_list.append({
                "name": s.get("name", ""),
                "mapping_name": s.get("mapping_name", ""),
            })
        if not session_list:
            for name in mapping_names:
                session_list.append({
                    "name": f"s_{self._make_safe_name(name)}",
                    "mapping_name": name,
                })

        dependencies = {}
        for dep in workflow_analysis.get("task_dependencies", []):
            to_task = dep.get("to_task", "")
            from_task = dep.get("from_task", "")
            if to_task:
                if to_task not in dependencies:
                    dependencies[to_task] = []
                if from_task:
                    dependencies[to_task].append(from_task)

        execution_order = [s["name"] for s in session_list]

        try:
            template = self.codegen.env.get_template("workflow_orchestration.py.j2")
            content = template.render(
                workflow_name=workflow_name,
                mappings=mappings_info,
                sessions=session_list,
                dependencies=dependencies,
                execution_order=execution_order,
            )
        except Exception:
            content = self._generate_workflow_fallback(workflow_name, mappings_info)

        return GeneratedFile(
            filename="workflow.py",
            content=content,
            file_type="python",
        )

    def _generate_workflow_fallback(self, workflow_name: str,
                                     mappings_info: List[dict]) -> str:
        lines = [
            '"""',
            f'Workflow Orchestration: {workflow_name}',
            'Auto-generated by informatica-sparker',
            '"""',
            'import sys',
            'import logging',
            'from datetime import datetime',
            '',
        ]
        for m in mappings_info:
            lines.append(f'from {m["module_name"]} import run_mapping as run_{m["safe_name"]}')
        lines.append('')
        lines.append('logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")')
        lines.append('logger = logging.getLogger(__name__)')
        lines.append('')
        lines.append('def run_workflow():')
        lines.append('    config = load_config("config.yml")')
        lines.append('    results = {}')
        for m in mappings_info:
            lines.append(f'    logger.info("Running mapping: {m["name"]}")')
            lines.append(f'    try:')
            lines.append(f'        run_{m["safe_name"]}(config)')
            lines.append(f'        results["{m["name"]}"] = "SUCCESS"')
            lines.append(f'    except Exception as e:')
            lines.append(f'        logger.error(f"Failed: {m["name"]} - {{e}}")')
            lines.append(f'        results["{m["name"]}"] = f"FAILED: {{e}}"')
        lines.append('    return results')
        lines.append('')
        lines.append('if __name__ == "__main__":')
        lines.append('    run_workflow()')
        return '\n'.join(lines) + '\n'

    def _generate_config_file(self, mapping_names: List[str],
                               mappings, source_detections: List[SourceDetectionResult],
                               folder_name: str) -> Optional[GeneratedFile]:
        try:
            # Collect database connections from source definitions
            db_connections = {}
            tables = {}
            mapping_variables = {}
            seen_db_names = set()

            for mapping in mappings:
                # Collect source tables and their connections
                for src in getattr(mapping, 'sources', []):
                    db_name = src.db_name or "default"
                    raw_type = src.database_type or "Oracle"
                    from .models import normalize_db_type, get_jdbc_driver, get_default_port
                    std_type = normalize_db_type(raw_type)
                    
                    if db_name not in seen_db_names:
                        seen_db_names.add(db_name)
                        db_connections[db_name] = {
                            "type": std_type,
                            "port": get_default_port(raw_type),
                            "database": db_name,
                            "schema": src.owner_name or "dbo",
                            "driver": get_jdbc_driver(raw_type),
                        }
                    
                    tables[src.name] = {
                        "connection": db_name,
                        "database": db_name,
                        "schema": src.owner_name or "dbo",
                        "type": "source",
                    }

                # Collect target tables
                for tgt in getattr(mapping, 'targets', []):
                    if tgt.name not in tables:
                        tables[tgt.name] = {
                            "connection": "target",
                            "database": "",
                            "schema": "dbo",
                            "type": "target",
                        }

                # Collect mapping variables
                for var in getattr(mapping, 'mapping_variables', []):
                    var_name = var.name.replace("$$", "")
                    if var_name not in mapping_variables:
                        mapping_variables[var_name] = {
                            "datatype": var.datatype or "string",
                            "default_value": var.default_value or "",
                        }

            # Also collect from source_detections to ensure full coverage
            for sd in source_detections:
                if sd.connection_info and sd.connection_info.database_name:
                    db_name = sd.connection_info.database_name
                    if db_name not in seen_db_names:
                        seen_db_names.add(db_name)
                        raw_type = sd.connection_info.database_type or "oracle"
                        from .models import normalize_db_type, get_jdbc_driver, get_default_port
                        std_type = normalize_db_type(raw_type)
                        db_connections[db_name] = {
                            "type": std_type,
                            "port": get_default_port(raw_type),
                            "database": db_name,
                            "schema": sd.connection_info.schema_name or "dbo",
                            "driver": get_jdbc_driver(raw_type),
                        }

            template = self.codegen.env.get_template("config.yml.j2")
            content = template.render(
                mapping_name=folder_name or mapping_names[0] if mapping_names else "default",
                db_connections=db_connections,
                tables=tables,
                mapping_variables=mapping_variables,
                user_config=self.user_config,
            )
        except Exception as e:
            import traceback
            traceback.print_exc()
            content = self._generate_config_fallback(
                mapping_names, mappings, source_detections, folder_name
            )

        return GeneratedFile(
            filename="config.yml",
            content=content,
            file_type="yaml",
        )

    def _generate_config_fallback(self, mapping_names, mappings,
                                    source_detections, folder_name) -> str:
        lines = [
            f"# Configuration for {folder_name or 'PySpark Job'}",
            f"# Auto-generated by informatica-sparker",
            f"# Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "",
            "spark:",
            f'  app_name: "{folder_name or "informatica_job"}"',
            '  master: "${SPARK_MASTER:local[*]}"',
            "",
            "connections:",
        ]

        # Collect unique database connections from source detections
        seen_conns = set()
        for sd in source_detections:
            if sd.connection_info and sd.connection_info.database_name:
                db_name = sd.connection_info.database_name
                if db_name not in seen_conns:
                    seen_conns.add(db_name)
                    db_type = (sd.connection_info.database_type or "oracle").lower()
                    db_type_short = "sqlserver" if "sql server" in db_type else db_type
                    port = 1521 if "oracle" in db_type else 1433
                    driver = "oracle.jdbc.driver.OracleDriver" if "oracle" in db_type else "com.microsoft.sqlserver.jdbc.SQLServerDriver"
                    schema = sd.connection_info.schema_name or "dbo"
                    
                    lines.append(f"  {db_name}:")
                    lines.append(f'    type: "{db_type_short}"')
                    lines.append(f'    host: "${{DB_HOST}}"')
                    lines.append(f'    port: {port}')
                    lines.append(f'    database: "{db_name}"')
                    lines.append(f'    username: "${{DB_USER}}"')
                    lines.append(f'    password: "${{DB_PASSWORD}}"')
                    lines.append(f'    schema: "{schema}"')
                    lines.append(f'    driver: "{driver}"')
                    lines.append("")

        # If no connections found from detections, use the mappings' sources
        if not seen_conns:
            for mapping in mappings:
                for src in mapping.sources:
                    db_name = src.db_name or "default"
                    if db_name not in seen_conns:
                        seen_conns.add(db_name)
                        db_type = (src.database_type or "oracle").lower()
                        db_type_short = "sqlserver" if "sql server" in db_type else ("oracle" if "oracle" in db_type else db_type)
                        port = 1521 if "oracle" in db_type else 1433
                        driver = "oracle.jdbc.driver.OracleDriver" if "oracle" in db_type else "com.microsoft.sqlserver.jdbc.SQLServerDriver"
                        schema = src.owner_name or "dbo"
                        
                        lines.append(f"  {db_name}:")
                        lines.append(f'    type: "{db_type_short}"')
                        lines.append(f'    host: "${{DB_HOST}}"')
                        lines.append(f'    port: {port}')
                        lines.append(f'    database: "{db_name}"')
                        lines.append(f'    username: "${{DB_USER}}"')
                        lines.append(f'    password: "${{DB_PASSWORD}}"')
                        lines.append(f'    schema: "{schema}"')
                        lines.append(f'    driver: "{driver}"')
                        lines.append("")
                
                for tgt in mapping.targets:
                    if tgt.database_type and tgt.database_type != "Flat File":
                        lines.append(f"  target:")
                        lines.append(f'    type: "oracle"')
                        lines.append(f'    host: "${{DB_HOST}}"')
                        lines.append(f'    port: 1521')
                        lines.append(f'    database: "${{TARGET_DB_NAME}}"')
                        lines.append(f'    username: "${{DB_USER}}"')
                        lines.append(f'    password: "${{DB_PASSWORD}}"')
                        lines.append(f'    schema: "{tgt.name}"')
                        lines.append(f'    driver: "oracle.jdbc.driver.OracleDriver"')
                        lines.append("")
                        break

        # Default connection if nothing found
        if not seen_conns:
            lines.append("  default:")
            lines.append('    type: "oracle"')
            lines.append('    host: "${DB_HOST}"')
            lines.append('    port: 1521')
            lines.append('    database: "${DB_NAME}"')
            lines.append('    username: "${DB_USER}"')
            lines.append('    password: "${DB_PASSWORD}"')
            lines.append("")

        lines.append("sources:")
        for sd in source_detections:
            lines.append(f"  {sd.source_name}:")
            lines.append(f'    type: "{sd.detected_type.value}"')
            if sd.file_format:
                lines.append(f'    format: "{sd.file_format.value}"')
            if sd.connection_info:
                if sd.connection_info.database_name:
                    lines.append(f'    database: "{sd.connection_info.database_name}"')
                if sd.connection_info.schema_name:
                    lines.append(f'    schema: "{sd.connection_info.schema_name}"')
        lines.append("")
        lines.append("mappings:")
        for name in mapping_names:
            lines.append(f"  - {name}")
        return '\n'.join(lines) + '\n'

    def _generate_runtime_lib(self) -> Optional[GeneratedFile]:
        """Generate the shared runtime library file."""
        try:
            template = self.codegen.env.get_template("runtime_lib.py.j2")
            content = template.render()
        except Exception:
            return None
        return GeneratedFile(
            filename="runtime_lib.py",
            content=content,
            file_type="python",
        )

    def _generate_sql_file(self, queries: List[SQLQueryInfo]) -> GeneratedFile:
        lines = [
            "-- =============================================================================",
            "-- ALL SQL QUERIES",
            "-- Auto-generated by informatica-sparker",
            f"-- Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "-- =============================================================================",
            "",
        ]

        by_mapping: Dict[str, List[SQLQueryInfo]] = {}
        for q in queries:
            by_mapping.setdefault(q.mapping_name, []).append(q)

        for mapping_name, mqueries in by_mapping.items():
            lines.append(f"-- =============================================================================")
            lines.append(f"-- Mapping: {mapping_name}")
            lines.append(f"-- =============================================================================")
            lines.append("")
            for i, q in enumerate(mqueries, 1):
                lines.append(f"-- [{q.query_type}] Step: {q.step_name}")
                if q.source_table:
                    lines.append(f"-- Table: {q.source_table}")
                if q.connection:
                    lines.append(f"-- Connection: {q.connection}")
                lines.append(q.query.rstrip(";") + ";")
                lines.append("")

        return GeneratedFile(
            filename="all_sql_queries.sql",
            content='\n'.join(lines),
            file_type="sql",
        )

    def _generate_error_log(self, warnings: List[str], errors: List[str],
                             reports: List[ConversionReport],
                             source_detections: List[SourceDetectionResult]) -> GeneratedFile:
        lines = [
            "=" * 80,
            "INFORMATICA-SPARKER CONVERSION LOG",
            f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "=" * 80,
            "",
        ]

        lines.append("SUMMARY")
        lines.append("-" * 40)
        total = len(reports)
        success = len([r for r in reports if r.status == "success"])
        failed = len([r for r in reports if r.status == "failed"])
        lines.append(f"Total Mappings: {total}")
        lines.append(f"Successful: {success}")
        lines.append(f"Failed: {failed}")
        lines.append(f"Total Warnings: {len(warnings)}")
        lines.append(f"Total Errors: {len(errors)}")
        lines.append("")

        if source_detections:
            lines.append("SOURCE DETECTION RESULTS")
            lines.append("-" * 40)
            for sd in source_detections:
                fmt_str = f" [{sd.file_format.value}]" if sd.file_format else ""
                conf = f" (confidence: {sd.confidence})"
                lines.append(f"  {sd.source_name}: {sd.detected_type.value}{fmt_str}{conf}")
                for note in sd.detection_notes:
                    lines.append(f"    - {note}")
            lines.append("")

        for report in reports:
            lines.append(f"MAPPING: {report.mapping_name}")
            lines.append(f"  Status: {report.status.upper()}")
            if report.generated_files:
                lines.append(f"  Generated Files: {', '.join(report.generated_files)}")
            if report.warnings:
                lines.append(f"  Warnings ({len(report.warnings)}):")
                for w in report.warnings:
                    lines.append(f"    [WARN] {w.message}")
            if report.errors:
                lines.append(f"  Errors ({len(report.errors)}):")
                for e in report.errors:
                    lines.append(f"    [ERROR] {e.message}")
            if report.unsupported_features:
                lines.append(f"  Unsupported Features:")
                for feat in report.unsupported_features:
                    lines.append(f"    [UNSUPPORTED] {feat}")
            lines.append("")

        if errors:
            lines.append("ERRORS")
            lines.append("-" * 40)
            for e in errors:
                lines.append(f"  [ERROR] {e}")
            lines.append("")

        if warnings:
            lines.append("ALL WARNINGS")
            lines.append("-" * 40)
            for w in warnings:
                lines.append(f"  [WARN] {w}")
            lines.append("")

        return GeneratedFile(
            filename="error_log.txt",
            content='\n'.join(lines),
            file_type="text",
        )

    def _write_output(self, files: List[GeneratedFile], output_dir: str):
        out_path = Path(output_dir)
        out_path.mkdir(parents=True, exist_ok=True)

        for gen_file in files:
            file_path = out_path / gen_file.filename
            file_path.write_text(gen_file.content, encoding="utf-8")

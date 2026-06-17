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

        # Store parser reference for workflow/worklet analysis
        self._parser = parser

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

        workflow_files = self._generate_workflow_file(
            mapping_names, workflow_analysis, parser.folder_name
        )
        if workflow_files:
            all_files.extend(workflow_files)

        config_file = self._generate_config_file(
            mapping_names, mappings, all_source_detections, parser.folder_name
        )
        if config_file:
            all_files.append(config_file)

        if all_sql_queries:
            sql_file = self._generate_sql_file(all_sql_queries)
            all_files.append(sql_file)

        conversion_log = self._generate_conversion_log(
            all_warnings, all_errors, all_reports, all_source_detections
        )
        all_files.append(conversion_log)

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
                                 folder_name: str) -> List[GeneratedFile]:
        if not mapping_names:
            return []

        workflows = workflow_analysis.get("workflows", [])
        workflow_name = workflows[0]["name"] if workflows else f"wf_{folder_name or 'workflow'}"

        # Build session-to-mapping mapping
        session_to_mapping = {}
        for s in workflow_analysis.get("sessions", []):
            sname = s.get("name", "")
            mname = s.get("mapping_name", "")
            if sname and mname:
                session_to_mapping[sname] = mname

        # Build worklet info: which sessions belong to which worklet
        worklet_info = {}  # worklet_name -> list of task instance dicts
        for wkl in workflow_analysis.get("worklets", []):
            wname = wkl.get("name", "")
            worklet_info[wname] = []

        # Parse task instances inside worklets
        def _parse_worklet_tasks(wkl_elem):
            tasks = []
            for ti in wkl_elem.findall("TASKINSTANCE"):
                task = {
                    "name": ti.get("NAME", ""),
                    "task_name": ti.get("TASKNAME", ""),
                    "task_type": ti.get("TASKTYPE", ""),
                }
                tasks.append(task)
            return tasks

        # Only parse worklets if we have the parser's XML data available
        wkl_task_map = {}
        # Use the stored parser reference if available
        if hasattr(self, '_parser') and self._parser and self._parser.root is not None:
            root = self._parser.root
            for wkl_elem in root.findall(".//WORKLET"):
                wname = wkl_elem.get("NAME", "")
                wkl_task_map[wname] = _parse_worklet_tasks(wkl_elem)
            # Also get worklet-internal links for proper ordering
            wkl_links = {}
            for wkl_elem in root.findall(".//WORKLET"):
                wname = wkl_elem.get("NAME", "")
                internal_links = []
                for link in wkl_elem.findall("WORKFLOWLINK"):
                    internal_links.append({
                        "from_task": link.get("FROMTASK", ""),
                        "to_task": link.get("TOTASK", ""),
                        "condition": link.get("CONDITION", ""),
                    })
                if internal_links:
                    wkl_links[wname] = internal_links

        # Build worklet content: for each worklet, list its session names and their mappings
        worklet_sessions = {}
        for wname in worklet_info:
            task_list = wkl_task_map.get(wname, [])
            sessions_in_wkl = []
            for t in task_list:
                if t["task_type"] == "Session":
                    sname = t.get("task_name") or t["name"]
                    mname = session_to_mapping.get(sname, "")
                    if mname:
                        sessions_in_wkl.append({
                            "session_name": sname,
                            "mapping_name": mname,
                        })
            if sessions_in_wkl:
                worklet_sessions[wname] = sessions_in_wkl

        # Build full execution plan from task dependencies
        # Group: [(stage_name, type, [items])]
        # type: "sequential" | "parallel"
        execution_plan = []
        dependencies = workflow_analysis.get("task_dependencies", [])
        
        # Build dependency graph
        dep_graph = {}
        all_tasks = set()
        for dep in dependencies:
            from_t = dep.get("from_task", "")
            to_t = dep.get("to_task", "")
            if to_t:
                all_tasks.add(to_t)
            if from_t:
                all_tasks.add(from_t)
            if to_t not in dep_graph:
                dep_graph[to_t] = []
            if from_t:
                dep_graph[to_t].append(from_t)

        # Topological sort with levels (for parallel grouping)
        levels = {}  # task_name -> level number
        for t in all_tasks:
            real_deps = [d for d in dep_graph.get(t, []) if d != "Start"]
            if not real_deps:
                levels[t] = 0

        changed = True
        while changed and len(levels) < len(all_tasks):
            changed = False
            for t in all_tasks:
                if t in levels:
                    continue
                real_deps = [d for d in dep_graph.get(t, []) if d != "Start"]
                if all(d in levels for d in real_deps):
                    levels[t] = max((levels[d] + 1 for d in real_deps), default=0)
                    changed = True

        # Group tasks by level
        level_groups = {}
        for t, lvl in levels.items():
            if lvl not in level_groups:
                level_groups[lvl] = []
            level_groups[lvl].append(t)

        # Build execution plan items - group same-level sessions into parallel groups
        for lvl in sorted(level_groups.keys()):
            tasks_at_level = level_groups[lvl]
            
            # Group sessions at the same level into parallel groups
            parallel_sessions = []
            worklet_found = None
            
            for task_name in tasks_at_level:
                if task_name in worklet_sessions:
                    worklet_found = task_name
                elif task_name in session_to_mapping:
                    parallel_sessions.append({
                        "session_name": task_name,
                        "mapping_name": session_to_mapping[task_name],
                    })
            
            # If multiple sessions at same level, emit as parallel group
            if len(parallel_sessions) > 1:
                execution_plan.append({
                    "type": "parallel_sessions",
                    "sessions": parallel_sessions,
                })
            elif len(parallel_sessions) == 1:
                s = parallel_sessions[0]
                execution_plan.append({
                    "type": "session",
                    "name": s["session_name"],
                    "mapping_name": s["mapping_name"],
                })
            
            # Emit worklet (runs after parallel sessions in its level complete)
            if worklet_found:
                execution_plan.append({
                    "type": "worklet",
                    "name": worklet_found,
                    "sessions": worklet_sessions[worklet_found],
                })
            
            # Emit non-worklet, non-session tasks (email etc.)
            for task_name in tasks_at_level:
                if (task_name not in worklet_sessions 
                    and task_name not in session_to_mapping 
                    and task_name != "Start"):
                    execution_plan.append({
                        "type": "task",
                        "name": task_name,
                    })

        # Build mappings info for imports
        all_mapping_names = set()
        for mname in mapping_names:
            all_mapping_names.add(mname)
        for wname, slist in worklet_sessions.items():
            for s in slist:
                all_mapping_names.add(s["mapping_name"])

        mappings_info = []
        for name in sorted(all_mapping_names):
            safe = self._make_safe_name(name)
            mappings_info.append({
                "name": name,
                "safe_name": safe,
                "module_name": safe,
            })
        mappings_info.sort(key=lambda x: x["name"])

        # Build task info (email attributes, etc.)
        task_info = {}
        for t in workflow_analysis.get("tasks", []):
            tname = t.get("name", "")
            ttype = t.get("type", "")
            attrs = t.get("attributes", {})
            if ttype == "Email" or "EMAIL" in ttype.upper():
                task_info[tname] = {
                    "type": "email",
                    "subject": attrs.get("Email Subject", ""),
                    "text": attrs.get("Email Text", ""),
                    "user": attrs.get("Email User Name", ""),
                }

        try:
            template = self.codegen.env.get_template("workflow_orchestration.py.j2")
            content = template.render(
                workflow_name=workflow_name,
                mappings=mappings_info,
                execution_plan=execution_plan,
                worklet_sessions=worklet_sessions,
            )
        except Exception:
            content = self._generate_workflow_fallback(
                workflow_name, mappings_info, execution_plan, worklet_sessions, task_info
            )

        # Generate separate markdown file with Mermaid flowchart
        md_content = self._generate_workflow_markdown(
            workflow_name, execution_plan, folder_name
        )

        return [
            GeneratedFile(filename="workflow.py", content=content, file_type="python"),
            GeneratedFile(filename=f"{self._make_safe_name(workflow_name)}.md",
                          content=md_content, file_type="markdown"),
        ]

    def _generate_workflow_markdown(self, workflow_name: str,
                                     execution_plan: List[dict],
                                     folder_name: str) -> str:
        """Generate a markdown file with Mermaid flowchart for the workflow."""
        mermaid = self._generate_mermaid_diagram(execution_plan, workflow_name)
        lines = [
            f"# {workflow_name}",
            "",
            "## Execution Flow",
            "",
            f"Auto-generated from Informatica PowerCenter workflow",
            "",
            mermaid,
            "",
            "## Session to Mapping",
            "",
        ]
        # Add session-to-mapping table
        lines.append("| Session | Mapping | Type |")
        lines.append("|---------|---------|------|")
        for step in execution_plan:
            step_type = step.get("type", "")
            if step_type == "parallel_sessions":
                for s in step.get("sessions", []):
                    lines.append(f'| {s["session_name"]} | {s["mapping_name"]} | Parallel |')
            elif step_type == "worklet":
                for s in step.get("sessions", []):
                    lines.append(f'| {s["session_name"]} | {s["mapping_name"]} | Worklet |')
            elif step_type == "session":
                lines.append(f'| {step["name"]} | {step["mapping_name"]} | Sequential |')
            elif step_type == "task":
                lines.append(f'| {step["name"]} | - | Task |')
        return "\n".join(lines)

    def _generate_mermaid_diagram(self, execution_plan: List[dict],
                                   workflow_name: str) -> str:
        """Generate a Mermaid flowchart from the execution plan."""
        lines = []
        lines.append("```mermaid")
        lines.append("flowchart TD")
        
        stage_count = 0
        subgraph_nodes = {}  # node_name -> subgraph_id
        node_labels = {}     # node_name -> display label
        
        for step in execution_plan:
            step_type = step.get("type", "")
            step_name = step.get("name", "")
            
            if step_type == "parallel_sessions":
                sessions = step.get("sessions", [])
                if len(sessions) > 1:
                    stage_count += 1
                    sg_id = f"Stage{stage_count}"
                    lines.append(f'    subgraph {sg_id}["Stage {stage_count} (Parallel)"]')
                    for s in sessions:
                        sname = s.get("session_name", "")
                        mname = s.get("mapping_name", "")
                        safe_name = sname.replace("-", "_").replace(" ", "_")
                        label = f"{sname}<br/>({mname})"
                        lines.append(f'        {safe_name}["{label}"]')
                        subgraph_nodes[sname] = sg_id
                        node_labels[sname] = safe_name
                    lines.append("    end")
                elif len(sessions) == 1:
                    s = sessions[0]
                    sname = s.get("session_name", "")
                    mname = s.get("mapping_name", "")
                    safe_name = sname.replace("-", "_").replace(" ", "_")
                    label = f"{sname}<br/>({mname})"
                    lines.append(f'    {safe_name}["{label}"]')
                    node_labels[sname] = safe_name
            
            elif step_type == "worklet":
                wname = step.get("name", "")
                sessions = step.get("sessions", [])
                stage_count += 1
                sg_id = f"Stage{stage_count}"
                safe_wname = wname.replace("-", "_").replace(" ", "_")
                lines.append(f'    subgraph {sg_id}["{wname} (Parallel)"]')
                for s in sessions:
                    sname = s.get("session_name", "")
                    mname = s.get("mapping_name", "")
                    safe_sname = sname.replace("-", "_").replace(" ", "_")
                    label = f"{mname}"
                    lines.append(f'        {safe_sname}["{label}"]')
                    subgraph_nodes[sname] = sg_id
                    node_labels[sname] = safe_sname
                lines.append("    end")
                node_labels[wname] = safe_wname
            
            elif step_type == "session":
                sname = step.get("name", "")
                mname = step.get("mapping_name", "")
                safe_name = sname.replace("-", "_").replace(" ", "_")
                label = f"{sname}<br/>({mname})"
                lines.append(f'    {safe_name}["{label}"]')
                node_labels[sname] = safe_name
            
            elif step_type == "task":
                tname = step.get("name", "")
                safe_name = tname.replace("-", "_").replace(" ", "_")
                lines.append(f'    {safe_name}["{tname}"]')
                node_labels[tname] = safe_name
        
        # Add connections between stages/steps in execution plan order
        # Use subgraph-to-subgraph flow for clean diagrams
        prev_sg_id = None
        for idx, step in enumerate(execution_plan):
            step_type = step.get("type", "")
            
            if step_type == "parallel_sessions":
                stage_num = sum(1 for s in execution_plan[:idx+1] 
                               if s.get("type") in ("parallel_sessions", "worklet"))
                current_sg_id = f"Stage{stage_num}"
                if prev_sg_id:
                    lines.append(f'    {prev_sg_id} --> {current_sg_id}')
                prev_sg_id = current_sg_id
            
            elif step_type == "worklet":
                stage_num = sum(1 for s in execution_plan[:idx+1] 
                               if s.get("type") in ("parallel_sessions", "worklet"))
                current_sg_id = f"Stage{stage_num}"
                if prev_sg_id:
                    lines.append(f'    {prev_sg_id} --> {current_sg_id}')
                prev_sg_id = current_sg_id
            
            elif step_type == "session":
                sname = step.get("name", "")
                safe = node_labels.get(sname, "")
                if safe and prev_sg_id:
                    lines.append(f'    {prev_sg_id} --> {safe}')
                elif safe:
                    prev_sg_id = safe
                if safe:
                    prev_sg_id = safe
            
            elif step_type == "task":
                tname = step.get("name", "")
                safe = node_labels.get(tname, "")
                if safe and prev_sg_id:
                    lines.append(f'    {prev_sg_id} --> {safe}')
                elif safe:
                    prev_sg_id = safe
                if safe:
                    prev_sg_id = safe
        
        lines.append("```")
        return "\n".join(lines)

    def _generate_workflow_fallback(self, workflow_name: str,
                                     mappings_info: List[dict],
                                     execution_plan: List[dict] = None,
                                     worklet_sessions: dict = None,
                                     task_info: dict = None) -> str:
        lines = [
            '"""',
            f'Workflow Orchestration: {workflow_name}',
            'Auto-generated by ASL informatica-sparker',
            '"""',
            'import os',
            'import sys',
            'import logging',
            'from datetime import datetime',
            'from concurrent.futures import ThreadPoolExecutor, as_completed',
            '',
        ]
        for m in mappings_info:
            lines.append(f'from {m["module_name"]} import run_mapping as run_{m["safe_name"]}')
        lines.append('')
        lines.append('from env.runtime_lib import load_config, get_spark_session')
        lines.append('')
        # Check if any task is an email task
        needs_email = False
        if task_info:
            for tname, ti in task_info.items():
                if ti.get("type") == "email":
                    needs_email = True
                    break
        if needs_email:
            lines.append('import smtplib')
            lines.append('from email.mime.text import MIMEText')
            lines.append('')
            lines.append('')
            lines.append('def _send_email(subject, body, to_addr=None):')
            lines.append('    """Send email notification. Configure SMTP settings as needed."""')
            lines.append('    try:')
            lines.append('        smtp_host = os.environ.get("SMTP_HOST", "localhost")')
            lines.append('        smtp_port = int(os.environ.get("SMTP_PORT", "25"))')
            lines.append('        from_addr = os.environ.get("SMTP_FROM", "noreply@example.com")')
            lines.append('        msg = MIMEText(body)')
            lines.append('        msg["Subject"] = subject')
            lines.append('        msg["From"] = from_addr')
            lines.append('        if to_addr:')
            lines.append('            msg["To"] = to_addr')
            lines.append('        with smtplib.SMTP(smtp_host, smtp_port) as server:')
            lines.append('            server.sendmail(from_addr, [to_addr or from_addr], msg.as_string())')
            lines.append('        logger.info(f"Email sent: {subject}")')
            lines.append('    except Exception as e:')
            lines.append('        logger.warning(f"Failed to send email: {e}")')
            lines.append('')
        lines.append('')
        lines.append('logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")')
        lines.append('logger = logging.getLogger(__name__)')
        lines.append('')

        def _run_step(session_name, mapping_name, config, results):
            logger.info(f"Running: {session_name} ({mapping_name})")
            try:
                run_func = f"run_{self._make_safe_name(mapping_name)}"
                globals()[run_func](config)
                results[mapping_name] = "SUCCESS"
                logger.info(f"Completed: {session_name}")
            except Exception as e:
                logger.error(f"Failed: {session_name} - {e}")
                results[mapping_name] = f"FAILED: {e}"

        lines.append('')
        lines.append('def run_workflow():')
        lines.append('    config = load_config("config.yml")')
        lines.append('    results = {}')
        lines.append('    success = True')
        lines.append('')

        if execution_plan:
            for step in execution_plan:
                step_type = step.get("type", "")
                step_name = step.get("name", "")
                deps = step.get("dependencies", [])
                
                if step_type == "worklet":
                    sessions = step.get("sessions", [])
                    if sessions:
                        lines.append(f'    # Worklet: {step_name} (parallel)')
                        lines.append(f'    logger.info("--- Starting Worklet: {step_name} ---")')
                        lines.append(f'    with ThreadPoolExecutor(max_workers={len(sessions)}) as executor:')
                        lines.append(f'        _futures = {{')
                        for s in sessions:
                            safe = self._make_safe_name(s["mapping_name"])
                            lines.append(f'            executor.submit(run_{safe}, config): "{s["session_name"]}",')
                        lines.append(f'        }}')
                        lines.append(f'        for _future in as_completed(_futures):')
                        lines.append(f'            _sname = _futures[_future]')
                        lines.append(f'            try:')
                        lines.append(f'                _future.result()')
                        lines.append(f'                results[_sname] = "SUCCESS"')
                        lines.append(f'            except Exception as e:')
                        lines.append(f'                logger.error(f"Failed: {{_sname}} - {{e}}")')
                        lines.append(f'                results[_sname] = f"FAILED: {{e}}"')
                        if task_info and "T_MAIL_FAIL" in task_info:
                            lines.append(f'                _send_email("CIS - Session Failed", f"{{_sname}}: {{e}}")')
                        lines.append(f'                success = False')
                        lines.append(f'    if not success:')
                        lines.append(f'        return results')
                        lines.append('')
                
                elif step_type == "session":
                    mname = step.get("mapping_name", "")
                    step_name = step.get("name", "")
                    safe = self._make_safe_name(mname)
                    fail_subject = "CIS - Session Failed"
                    lines.append(f'    # Session: {step_name}')
                    lines.append(f'    logger.info("Running session: {step_name}")')
                    lines.append(f'    try:')
                    lines.append(f'        run_{safe}(config)')
                    lines.append(f'        results["{step_name}"] = "SUCCESS"')
                    lines.append(f'    except Exception as e:')
                    lines.append(f'        logger.error(f"Session {step_name} failed: {{e}}")')
                    lines.append(f'        results["{step_name}"] = f"FAILED: {{e}}"')
                    if task_info and "T_MAIL_FAIL" in task_info:
                        fail_text = task_info["T_MAIL_FAIL"].get("text", "Session failed").replace('"', "'")
                        lines.append(f'        _send_email("{fail_subject}", "{step_name}: {{{{e}}}}")')
                    lines.append(f'        return results')
                    lines.append('')

                elif step_type == "parallel_sessions":
                    sessions = step.get("sessions", [])
                    if sessions:
                        lines.append(f'    # Parallel sessions')
                        lines.append(f'    logger.info("Running {len(sessions)} sessions in parallel")')
                        lines.append(f'    with ThreadPoolExecutor(max_workers={len(sessions)}) as executor:')
                        lines.append(f'        _futures = {{')
                        for s in sessions:
                            safe = self._make_safe_name(s["mapping_name"])
                            sname = s["session_name"]
                            lines.append(f'            executor.submit(run_{safe}, config): "{sname}",')
                        lines.append(f'        }}')
                        lines.append(f'        for _future in as_completed(_futures):')
                        lines.append(f'            _sname = _futures[_future]')
                        lines.append(f'            try:')
                        lines.append(f'                _future.result()')
                        lines.append(f'                results[_sname] = "SUCCESS"')
                        lines.append(f'            except Exception as e:')
                        lines.append(f'                logger.error(f"Failed: {{_sname}} - {{e}}")')
                        lines.append(f'                results[_sname] = f"FAILED: {{e}}"')
                        lines.append(f'                success = False')
                        lines.append(f'    if not success:')
                        lines.append(f'        return results')
                        lines.append('')
                
                elif step_type == "task":
                    lines.append(f'    # Task: {step_name}')
                    lines.append(f'    logger.info("Processing task: {step_name}")')
                    # Check if this is an email task
                    ti = (task_info or {}).get(step_name, {})
                    if ti.get("type") == "email":
                        subject = ti.get("subject", "").replace('"', "'")
                        text = ti.get("text", "").replace('"', "'").replace("\r\n", "\\n").replace("\n", "\\n")
                        lines.append(f'    try:')
                        lines.append(f'        _send_email("{subject}", """{text}""")')
                        lines.append(f'        logger.info("Email sent: {step_name}")')
                        lines.append(f'        results["{step_name}"] = "SUCCESS"')
                        lines.append(f'    except Exception as e:')
                        lines.append(f'        logger.error(f"Email {step_name} failed: {{e}}")')
                        lines.append('')
                    else:
                        lines.append('')
        else:
            # Fallback: sequential execution
            for m in mappings_info:
                lines.append(f'    logger.info("Running mapping: {m["name"]}")')
                lines.append(f'    try:')
                lines.append(f'        run_{m["safe_name"]}(config)')
                lines.append(f'        results["{m["name"]}"] = "SUCCESS"')
                lines.append(f'    except Exception as e:')
                lines.append(f'        logger.error(f"Failed: {m["name"]} - {{e}}")')
                lines.append(f'        results["{m["name"]}"] = f"FAILED: {{e}}"')
                lines.append(f'        return results')
                lines.append('')

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
            paths = {
                "input_base": "${INPUT_PATH:/tmp}",
                "output_base": "${OUTPUT_PATH:/tmp}",
            }

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
                    
                    is_flat = db_name.lower() in ('utl', 'flatfile')
                    tables[src.name] = {
                        "connection": db_name,
                        "database": db_name,
                        "schema": src.owner_name or "dbo",
                        "type": "source",
                        "format": src.file_format.value if src.file_format else "csv",
                        "path": f"/tmp/{src.name}.csv",
                    }

                # Collect target tables
                for tgt in getattr(mapping, 'targets', []):
                    if tgt.name not in tables:
                        from .models import normalize_db_type
                        is_flat = tgt.database_type and "flat" in tgt.database_type.lower()
                        tables[tgt.name] = {
                            "connection": "target" if not is_flat else db_name,
                            "database": "",
                            "schema": "dbo",
                            "type": "target",
                            "is_flat_file": is_flat,
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
                paths=paths,
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
            f"# Auto-generated by ASL informatica-sparker",
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
            "-- Auto-generated by ASL informatica-sparker",
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

    def _generate_conversion_log(self, warnings: List[str], errors: List[str],
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
            filename="conversion_log.txt",
            content='\n'.join(lines),
            file_type="text",
        )

    def _write_output(self, files: List[GeneratedFile], output_dir: str):
        out_path = Path(output_dir)
        out_path.mkdir(parents=True, exist_ok=True)
        env_path = out_path / "env"
        env_path.mkdir(parents=True, exist_ok=True)

        for gen_file in files:
            # .md report goes to current working directory (not in workflow folder)
            if gen_file.filename.endswith(".md"):
                file_path = Path.cwd() / gen_file.filename
            # Mapping scripts and workflow.py go directly in output_dir
            elif gen_file.filename == "workflow.py" or gen_file.filename.startswith("m_"):
                file_path = out_path / gen_file.filename
            # Everything else (config.yml, runtime_lib.py, etc.) goes in env/
            else:
                file_path = env_path / gen_file.filename
            file_path.parent.mkdir(parents=True, exist_ok=True)
            file_path.write_text(gen_file.content, encoding="utf-8")

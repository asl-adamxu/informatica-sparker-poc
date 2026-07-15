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
from .test_generator import TestGenerator


class ConversionService:

    def __init__(self, user_config: Optional[UserConfig] = None):
        self.user_config = user_config or UserConfig()
        self.logger = ConversionLogger()
        self.codegen = CodeGenerator()
        self.with_tests = False

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

                # A mapping with warnings is considered failed — every warning
                # represents an unresolved feature or data loss that must be fixed.
                _has_warnings = len(ir_plan.warnings) > 0
                report = ConversionReport(
                    mapping_name=mapping_name,
                    status="failed" if _has_warnings else "success",
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

        # Collect file source info from session configurations (overrides default paths)
        session_file_sources = {}
        for sess in workflow_analysis.get("sessions", []):
            for src_inst, src_info in sess.get("file_sources", {}).items():
                session_file_sources[src_inst] = src_info

        config_file = self._generate_config_file(
            mapping_names, mappings, all_source_detections, parser.folder_name,
            session_file_sources=session_file_sources,
        )
        if config_file:
            all_files.append(config_file)

        if all_sql_queries:
            sql_file = self._generate_sql_file(all_sql_queries)
            all_files.append(sql_file)

        conversion_log = self._generate_conversion_log(
            all_warnings, all_errors, all_reports, all_source_detections, all_files
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

            # Generate metadata.json for this workflow
            try:
                _workflows = workflow_analysis.get("workflows", [])
                _wf_name = _workflows[0]["name"] if _workflows else parser.folder_name
                _script = f"{self._make_safe_name(_wf_name)}.py"
                # Collect file-source info (same as config generation below)
                _file_sources = {}
                for _sess in workflow_analysis.get("sessions", []):
                    for _src_inst, _src_info in _sess.get("file_sources", {}).items():
                        _file_sources[_src_inst] = _src_info
                from informatica_sparker.metadata import generate_metadata as _gen_meta
                _gen_meta(mappings, _wf_name, _script, output_dir,
                          session_file_sources=_file_sources)
            except Exception as _meta_err:
                import sys as _sys
                print(f"Warning: metadata generation failed: {_meta_err}",
                      file=_sys.stderr)

        # Generate E2E test artifacts when --with-tests flag is set
        if output_dir and self.with_tests and not result.errors:
            wf_name = "workflow"
            workflows = workflow_analysis.get("workflows", [])
            if workflows:
                wf_name = workflows[0]["name"]
            test_gen = TestGenerator(
                mappings=mappings,
                workflow_analysis=workflow_analysis,
                snsh_date=os.environ.get("SNSH_DATE", "20260601"),
                workflow_name=wf_name,
            )
            test_gen.write_all(output_dir)

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
                is_odbc = "odbc" in db_type.lower()
                if is_odbc:
                    notes.append("ODBC source — converted to JDBC equivalent")
                    if source.connection_info:
                        resolved = source.connection_info.database_type or db_type
                        notes.append(f"Resolved database type: {resolved}")
                        if source.connection_info.connection_name:
                            notes.append(f"Connection name: {source.connection_info.connection_name}")
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
                    if source.connection_info.connection_name and not is_odbc:
                        notes.append(f"Connection name: {source.connection_info.connection_name}")
                    if is_odbc and not source.connection_info.driver_class:
                        notes.append("ODBC→JDBC: Configure the correct JDBC driver in config.yml")
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

        # ── Helper: build a DAG execution plan from links ──────────────────
        def _build_dag_plan(links: list, known_sessions: dict, known_worklets: dict,
                            known_tasks: set) -> list:
            """Return a list of plan-step dicts respecting topological order.

            Each step is one of:
              {"type": "parallel_group", "steps": [...]}  – concurrent items
              {"type": "worklet", "name": ..., "plan": [...]}  – nested plan
              {"type": "task", "name": ...}  – email / command
            """
            # Dependency graph: task_name → [prerequisites]  (exclude "Start")
            deps: dict = {}
            all_nodes: set = set()
            for link in links:
                f = link.get("from_task", "")
                t = link.get("to_task", "")
                if t:
                    all_nodes.add(t)
                if f and f != "Start":
                    all_nodes.add(f)
                if t:
                    deps.setdefault(t, [])
                if f and f != "Start" and t:
                    deps[t].append(f)

            # Ensure every known session/worklet/task is in all_nodes
            for s in known_sessions:
                all_nodes.add(s)
            for w in known_worklets:
                all_nodes.add(w)
            for t in known_tasks:
                all_nodes.add(t)

            # Compute level for each node (longest distance from a root)
            levels: dict = {}
            for n in all_nodes:
                prereqs = [d for d in deps.get(n, []) if d != "Start"]
                if not prereqs:
                    levels[n] = 0

            changed = True
            while changed and len(levels) < len(all_nodes):
                changed = False
                for n in all_nodes:
                    if n in levels:
                        continue
                    prereqs = [d for d in deps.get(n, []) if d != "Start"]
                    if all(d in levels for d in prereqs):
                        levels[n] = max((levels[d] + 1 for d in prereqs), default=0)
                        changed = True

            # Any node not reached → put at level 0
            for n in all_nodes:
                if n not in levels:
                    levels[n] = 0

            # Group by level
            level_groups: dict = {}
            for n, lvl in levels.items():
                level_groups.setdefault(lvl, []).append(n)

            plan = []
            for lvl in sorted(level_groups.keys()):
                items = level_groups[lvl]
                parallel_steps = []

                for name in items:
                    if name in known_worklets:
                        parallel_steps.append({
                            "type": "worklet",
                            "name": name,
                            "plan": known_worklets[name],
                        })
                    elif name in known_sessions:
                        parallel_steps.append({
                            "type": "session",
                            "name": name,
                            "mapping_name": known_sessions[name],
                        })
                    elif name in known_tasks:
                        parallel_steps.append({
                            "type": "task",
                            "name": name,
                        })

                if len(parallel_steps) == 1:
                    plan.append(parallel_steps[0])
                elif len(parallel_steps) > 1:
                    plan.append({
                        "type": "parallel_group",
                        "steps": parallel_steps,
                    })

            return plan

        # ── Build worklet sub-plans from worklet-internal links ────────────
        def _build_worklet_subplan(wkl_name: str,
                                   wkl_links: dict,
                                   session_to_mapping: dict) -> list:
            """Build a mini execution plan for one worklet."""
            links = wkl_links.get(wkl_name, [])
            if not links:
                # No internal links → all sessions can run in parallel
                wkl_sessions = worklet_sessions.get(wkl_name, [])
                session_steps = []
                for s in wkl_sessions:
                    session_steps.append({
                        "type": "session",
                        "name": s["session_name"],
                        "mapping_name": s["mapping_name"],
                    })
                if len(session_steps) == 1:
                    return session_steps
                elif session_steps:
                    return [{
                        "type": "parallel_group",
                        "steps": session_steps,
                    }]
                return []

            # Build local session map for this worklet
            local_sessions = {}
            local_tasks = set()
            for s in worklet_sessions.get(wkl_name, []):
                local_sessions[s["session_name"]] = s["mapping_name"]

            # Also check for task instances inside the worklet (e.g. Start)
            wkl_tasks = wkl_task_map.get(wkl_name, [])
            for t in wkl_tasks:
                tname = t.get("task_name") or t["name"]
                ttype = t.get("task_type", "")
                if ttype != "Session" and tname != "Start":
                    local_tasks.add(tname)

            return _build_dag_plan(links, local_sessions, {}, local_tasks)

        # Build worklet sub-plans
        worklet_plans = {}
        for wname in worklet_sessions:
            worklet_plans[wname] = _build_worklet_subplan(
                wname, wkl_links, session_to_mapping
            )

        # ── Build main workflow execution plan ────────────────────────────
        # Collect worklet-internal session names so they are excluded from the
        # main workflow plan (they already appear inside their worklet sub-plans).
        worklet_internal_session_names = set()
        for wname, slist in worklet_sessions.items():
            for s in slist:
                worklet_internal_session_names.add(s["session_name"])

        # Workflow-level sessions (exclude those belonging to worklets)
        main_sessions = {
            name: mname
            for name, mname in session_to_mapping.items()
            if name not in worklet_internal_session_names
        }

        # Collect task names that appear in workflow links (only these are
        # part of the main execution flow; tasks not linked are special
        # handlers like failure emails or reusable templates).
        linked_task_names = set()
        for link in workflow_analysis.get("task_dependencies", []):
            f = link.get("from_task", "")
            t = link.get("to_task", "")
            if f and f != "Start":
                linked_task_names.add(f)
            if t and t != "Start":
                linked_task_names.add(t)

        workflow_tasks = set()
        for t in workflow_analysis.get("tasks", []):
            tname = t.get("name", "")
            if tname and tname != "Start" and tname in linked_task_names:
                workflow_tasks.add(tname)

        main_links = workflow_analysis.get("task_dependencies", [])
        execution_plan = _build_dag_plan(
            main_links, main_sessions, worklet_plans, workflow_tasks
        )

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
                _subject = attrs.get("Email Subject", "")
                _text = attrs.get("Email Text", "")
                _subject = _subject.replace("[Workflow Name]", workflow_name)
                _text = _text.replace("[Workflow Name]", workflow_name)
                task_info[tname] = {
                    "type": "email",
                    "subject": _subject,
                    "text": _text,
                    "user": attrs.get("Email User Name", ""),
                }
            elif "COMMAND" in ttype.upper():
                task_info[tname] = {
                    "type": "command",
                    "commands": t.get("commands", []),
                }

        try:
            template = self.codegen.env.get_template("workflow_orchestration.py.j2")
            content = template.render(
                workflow_name=workflow_name,
                mappings=mappings_info,
                execution_plan=execution_plan,
                task_info=task_info,
            )
        except Exception:
            content = self._generate_workflow_fallback(
                workflow_name, mappings_info, execution_plan, task_info
            )

        # Generate separate markdown file with Mermaid flowchart
        md_content = self._generate_workflow_markdown(
            workflow_name, execution_plan, folder_name
        )

        return [
            GeneratedFile(filename=f"{self._make_safe_name(workflow_name)}.py", content=content, file_type="python"),
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
        # Add session-to-mapping table (recursively walks worklet sub-plans)
        lines.append("| Session | Mapping | Plan-Level |")
        lines.append("|---------|---------|------------|")

        def _walk_plan(plan: list, context: str = ""):
            for step in plan:
                stype = step.get("type", "")
                if stype == "parallel_group":
                    for s in step.get("steps", []):
                        _walk_plan([s], context)
                elif stype == "worklet":
                    wname = step.get("name", "")
                    for s in step.get("plan", []):
                        _walk_plan([s], f"Worklet: {wname}")
                elif stype == "session":
                    lines.append(f'| {step["name"]} | {step.get("mapping_name", "-")} | {context or "Top-Level"} |')
                elif stype == "task":
                    lines.append(f'| {step["name"]} | - | {context or "Top-Level"} |')

        _walk_plan(execution_plan)
        return "\n".join(lines)

    def _generate_mermaid_diagram(self, execution_plan, workflow_name):
        """Generate a detailed Mermaid flowchart from the nested DAG execution plan."""
        import re as _re
        lines = ["```mermaid", "flowchart TD"]
        nc = [0]

        def _rp(plan):
            lids = []
            for step in plan:
                st = step.get("type", "")
                if st == "parallel_group":
                    kids = []
                    for c in step.get("steps", []):
                        kids.extend(_rp([c]))
                    if len(kids) > 1:
                        nc[0] += 1
                        sid = f"pg{nc[0]}"
                        lines.append(f"    subgraph {sid}[Parallel]")
                        for k in kids:
                            lines.append(f"        {k}")
                        lines.append("    end")
                        lids.append(sid)
                    elif len(kids) == 1:
                        lids.append(kids[0])
                elif st == "session":
                    nc[0] += 1
                    nid = f"n{nc[0]}"
                    sn = step.get("name", "")
                    mn = step.get("mapping_name", "")
                    lb = f"{sn}<br/><i>({mn})</i>" if mn else sn
                    lines.append(f"    {nid}[\"{lb}\"]")
                    lids.append(nid)
                elif st == "worklet":
                    wn = step.get("name", "worklet")
                    wp = step.get("plan", [])
                    nc[0] += 1
                    sid = f"wkl{nc[0]}"
                    lines.append(f"    subgraph {sid}[\"{wn}\"]")
                    kids = _rp(wp)
                    if len(kids) > 1:
                        for i in range(len(kids) - 1):
                            lines.append(f"        {kids[i]} --> {kids[i+1]}")
                    lines.append("    end")
                    lids.append(sid)
                elif st == "task":
                    nc[0] += 1
                    nid = f"n{nc[0]}"
                    tn = step.get("name", "")
                    lines.append(f"    {nid}[\"{tn}\"]")
                    lines.append(f"    style {nid} fill:#ffd,stroke:#aaa,stroke-width:1px")
                    lids.append(nid)
            return lids

        tp = _rp(execution_plan)
        for i in range(len(tp) - 1):
            lines.append(f"    {tp[i]} --> {tp[i+1]}")
        lines.append("```")
        return "\n".join(lines)
    def _generate_workflow_fallback(self, workflow_name: str,
                                 mappings_info: List[dict],
                                 execution_plan: List[dict] = None,
                                 task_info: dict = None) -> str:
        import json as _json
        ex_plan_s = _json.dumps(execution_plan or [], indent=2, default=str)
        ti_s = _json.dumps(task_info or {}, indent=2, default=str)
        wf_lower = workflow_name.lower()
        lines = [
            '"""',
            f'Workflow Orchestration: {workflow_name}',
            'Auto-generated by ASL informatica-sparker',
            '"""',
            'import os, sys, glob, importlib, json',
            'import env.runtime_lib as lib',
            '',
            f"lib.init_logger('{wf_lower}')",
            f"logger = lib.get_logger('{wf_lower}')",
            '',
            '# Auto-discover mapping modules',
            'MAPPING_FUNCTIONS = {}',
            'for file_path in sorted(glob.glob(os.path.join(os.path.dirname(__file__) or ".", "m_*.py"))):',
            '    module_name = os.path.splitext(os.path.basename(file_path))[0]',
            '    try:',
            '        module = importlib.import_module(module_name)',
            "        if not hasattr(module, 'run_mapping'):",
            "            raise AttributeError(f\"Module '{module_name}' missing 'run_mapping' function\")",
            '        MAPPING_FUNCTIONS[module_name] = module.run_mapping',
            '        logger.debug("Registered mapping: %s", module_name)',
            '    except Exception as e:',
            "        logger.error(\"Failed to load mapping '%s': %s\", module_name, e)",
            '        raise',
            '',
            f'EXECUTION_PLAN = json.loads("""{ex_plan_s}""")',
            f'TASK_INFO = json.loads("""{ti_s}""")',
            '',
            'def send_email(subject, body, to_address=None):',
            '    import smtplib',
            '    from email.mime.text import MIMEText',
            '    try:',
            '        smtp_host = os.environ.get("SMTP_HOST", "localhost")',
            '        smtp_port = int(os.environ.get("SMTP_PORT", "25"))',
            '        from_address = os.environ.get("SMTP_FROM", "noreply@example.com")',
            '        message = MIMEText(body, _charset="utf-8")',
            '        message["Subject"] = subject',
            '        message["From"] = from_address',
            '        message["To"] = to_address or from_address',
            '        with smtplib.SMTP(smtp_host, smtp_port) as server:',
            '            server.sendmail(from_address, [message["To"]], message.as_string())',
            '        logger.info("Email sent: %s", subject)',
            '    except Exception as e:',
            '        logger.warning("Failed to send email: %s", e)',
            '',
            'def format_infa_template(template, context):',
            '    for ph, val in [("%s", context.get("session_name", "")),',
            '                   ("%n", context.get("folder_name", "")),',
            '                   ("%e", context.get("error_code", "")),',
            '                   ("%b", context.get("error_msg", "")),',
            '                   ("%c", context.get("session_status", "")),',
            '                   ("%i", context.get("run_id", "")),',
            '                   ("%g", context.get("log_file", ""))]:',
            '        template = template.replace(ph, val)',
            '    return template',
            '',
            'def run_workflow(config=None, fail_fast=True):',
            '    if config is None:',
            '        config = lib.load_config("env/config.yml")',
            '    return lib.run_workflow(',
            '        execution_plan=EXECUTION_PLAN,',
            '        mapping_functions=MAPPING_FUNCTIONS,',
            f'        workflow_name="{workflow_name}",',
            '        task_info=TASK_INFO,',
            '        send_email_fn=send_email,',
            '        format_infa_fn=format_infa_template,',
            '        config=config,',
            '        fail_fast=fail_fast,',
            '        metrics_cls=lib.MappingMetrics,',
            '    )',
            '',
            'def main():',
            '    import argparse',
            '    parser = argparse.ArgumentParser()',
            '    parser.add_argument("--config", "-c", default="env/config.yml")',
            '    parser.add_argument("--continue-on-error", action="store_true")',
            '    args = parser.parse_args()',
            '    config = lib.load_config(args.config)',
            '    result = run_workflow(config=config, fail_fast=not args.continue_on_error)',
            '    return 0 if result["status"] == "SUCCESS" else 1',
            '',
            'if __name__ == "__main__":',
            '    main()',
        ]
        return '\n'.join(lines) + '\n'

    def _generate_config_file(self, mapping_names: List[str],
                               mappings, source_detections: List[SourceDetectionResult],
                               folder_name: str,
                               session_file_sources: dict = None) -> Optional[GeneratedFile]:
        try:
            # Collect database connections from source definitions
            db_connections = {}
            tables = {}
            mapping_variables = {}
            seen_db_names = set()
            paths = {
                "input_base": "${INPUT_PATH:/tmp}",
                "output_base": "${OUTPUT_PATH:/tmp}",
                "source_file_dir": "${PMSOURCE_FILE_DIR:/tmp}",
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
                    # Use session file source info to override default path for flat files
                    _file_path = f"/tmp/{src.name}.csv"
                    if is_flat and session_file_sources:
                        _fs_info = session_file_sources.get(src.name)
                        if _fs_info:
                            _fname = _fs_info.get("Source filename", "")
                            _fdir = _fs_info.get("Source file directory", "")
                            if _fname:
                                # Map Informatica $PMSourceFileDir to config.yml's paths.source_file_dir
                                # Use __SOURCE_FILE_DIR__ marker — config.yml.j2 replaces it at render time
                                _fdir_resolved = _fdir.replace("$PMSourceFileDir", "__SOURCE_FILE_DIR__").replace("\\", "/").strip("/")
                                _file_path = f"{_fdir_resolved}/{_fname}" if _fdir_resolved else f"__SOURCE_FILE_DIR__/{_fname}"
                    tables[src.name] = {
                        "connection": db_name,
                        "database": db_name,
                        "schema": src.owner_name or "dbo",
                        "type": "source",
                        "format": src.file_format.value if src.file_format else "csv",
                        "path": _file_path,
                    }

                # Collect target tables
                for tgt in getattr(mapping, 'targets', []):
                    if tgt.name not in tables:
                        from .models import normalize_db_type
                        is_flat = tgt.database_type and "flat" in tgt.database_type.lower()
                        _tgt_path = None
                        if is_flat and session_file_sources:
                            _fs_info = session_file_sources.get(tgt.name)
                            if _fs_info:
                                _fname = _fs_info.get("Output filename", "")
                                _fdir = _fs_info.get("Output file directory", "")
                                if _fname and _fname != "/dev/null":
                                    _fdir_clean = _fdir.replace("$PMSourceFileDir", "__SOURCE_FILE_DIR__").replace("\\", "/").strip("/")
                                    _tgt_path = f"{_fdir_clean}/{_fname}" if _fdir_clean else f"__SOURCE_FILE_DIR__/{_fname}"
                                else:
                                    _tgt_path = _fname  # "/dev/null" or empty
                        tables[tgt.name] = {
                            "connection": "target" if not is_flat else db_name,
                            "database": "",
                            "schema": "dbo",
                            "type": "target",
                            "is_flat_file": is_flat,
                            "format": "csv" if is_flat else None,
                            "path": _tgt_path,
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
            f"-- Generated: {datetime.now().strftime('%Y-%m-%d')}",
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
                             source_detections: List[SourceDetectionResult],
                             all_files: List[GeneratedFile] = None) -> GeneratedFile:
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

        if all_files:
            lines.append("GENERATED FILES")
            lines.append("-" * 40)
            py_files = sorted([f.filename for f in all_files if f.file_type == "python"])
            other_files = sorted([(f.filename, f.file_type) for f in all_files if f.file_type != "python"])
            if py_files:
                lines.append("  Mapping Scripts:")
                for fname in py_files:
                    lines.append(f"    - {fname}")
            if other_files:
                lines.append("  Supporting Files:")
                for fname, ftype in other_files:
                    lines.append(f"    - {fname} ({ftype})")
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
            # .md report goes alongside the workflow file in output_dir
            if gen_file.filename.endswith(".md"):
                file_path = out_path / gen_file.filename
            # Mapping scripts and workflow file go directly in output_dir
            elif gen_file.filename.startswith("m_") or gen_file.filename.lower().startswith("wf_"):
                file_path = out_path / gen_file.filename
            # Everything else (config.yml, runtime_lib.py, etc.) goes in env/
            else:
                file_path = env_path / gen_file.filename
            file_path.parent.mkdir(parents=True, exist_ok=True)
            file_path.write_text(gen_file.content, encoding="utf-8")

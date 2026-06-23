import re
from typing import Dict, List, Any, Optional
from .ir import WorkflowDAG, WorkflowSession
from .models import MappingDefinition


class WorkflowBuilder:

    def __init__(self, workflow_data: Dict[str, Any], mappings: List[MappingDefinition]):
        self.workflow_data = workflow_data
        self.mappings = mappings
        self.mapping_map = {m.name: m for m in mappings}

    def build(self) -> Optional[WorkflowDAG]:
        if not self.workflow_data:
            return None

        workflow_name = self.workflow_data.get("workflow_name", "workflow")

        dag = WorkflowDAG(workflow_name=workflow_name)

        sessions = self.workflow_data.get("sessions", [])
        for session_data in sessions:
            session = self._build_session(session_data)
            dag.add_session(session)

        tasks = self.workflow_data.get("tasks", [])
        for task_data in tasks:
            if task_data.get("type") == "SESSION":
                continue
            session = self._build_task(task_data)
            dag.add_session(session)

        dag.execution_order = self._determine_execution_order(dag)

        dag.variables = self._extract_variables()

        return dag

    def _build_session(self, session_data: Dict[str, Any]) -> WorkflowSession:
        name = session_data.get("name", "")
        mapping_name = session_data.get("mapping_name", "")

        session = WorkflowSession(
            name=name,
            session_type="session",
            mapping_name=mapping_name
        )

        components = session_data.get("components", [])
        for comp in components:
            comp_type = comp.get("type", "")

            if comp_type == "Pre Session Command":
                sql = comp.get("value", "")
                if sql:
                    session.pre_sql.append(sql)
            elif comp_type == "Post Session Command":
                sql = comp.get("value", "")
                if sql:
                    session.post_sql.append(sql)
            elif comp_type == "Pre Session SQL":
                sql = comp.get("sql", "")
                if sql:
                    session.pre_sql.append(sql)
            elif comp_type == "Post Session SQL":
                sql = comp.get("sql", "")
                if sql:
                    session.post_sql.append(sql)

        attrs = session_data.get("attributes", {})
        for key, value in attrs.items():
            session.parameters[key] = value

        return session

    def _build_task(self, task_data: Dict[str, Any]) -> WorkflowSession:
        name = task_data.get("name", "")
        task_type = task_data.get("type", "COMMAND").lower()

        session = WorkflowSession(
            name=name,
            session_type=task_type
        )

        if task_type == "command":
            cmd = task_data.get("command", "")
            session.parameters["command"] = cmd

        return session

    def _determine_execution_order(self, dag: WorkflowDAG) -> List[str]:
        order = []
        links = self.workflow_data.get("links", [])

        deps: Dict[str, List[str]] = {}
        for session in dag.sessions:
            deps[session.name] = []

        for link in links:
            from_task = link.get("from_task", "")
            to_task = link.get("to_task", "")
            condition = link.get("condition", "")

            if to_task in deps:
                deps[to_task].append(from_task)

            for session in dag.sessions:
                if session.name == to_task:
                    session.dependencies.append(from_task)
                    cond_lower = condition.lower()
                    if "fail" in cond_lower and "never" not in cond_lower:
                        session.on_failure.append(from_task)
                    elif "abort" in cond_lower:
                        # ABORT links fire on abort; treat as failure dependency
                        session.on_failure.append(from_task)
                    else:
                        session.on_success.append(from_task)

        visited: set = set()
        visiting: set = set()  # back-edge detection for cycle breaking
        temp_order: List[str] = []

        def visit(name: str):
            if name in visited:
                return
            if name in visiting:
                # Cycle detected — skip this back-edge to avoid infinite recursion
                return
            visiting.add(name)
            for dep in deps.get(name, []):
                visit(dep)
            visiting.discard(name)
            visited.add(name)
            temp_order.append(name)

        for session in dag.sessions:
            visit(session.name)

        return temp_order

    def _extract_variables(self) -> Dict[str, str]:
        variables = {}

        attrs = self.workflow_data.get("attributes", {})
        for key, value in attrs.items():
            if key.startswith("$$") or key.startswith("$"):
                variables[key] = str(value)

        return variables

    def _safe_name(self, name: str) -> str:
        safe = re.sub(r'[^a-zA-Z0-9_]', '_', name)
        if safe and safe[0].isdigit():
            safe = '_' + safe
        return safe.lower()

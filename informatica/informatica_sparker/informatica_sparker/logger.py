from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
from datetime import datetime
from enum import Enum

class LogLevel(Enum):
    INFO = "info"
    SUCCESS = "success"
    WARNING = "warning"
    ERROR = "error"

class LogStage(Enum):
    PARSE = "parse"
    ANALYZE = "analyze"
    GRAPH = "graph"
    TRANSFORM = "transform"
    EXPRESSION = "expression"
    CODEGEN = "codegen"
    WORKFLOW = "workflow"
    CONFIG = "config"

@dataclass
class LogEntry:
    timestamp: str
    stage: str
    mapping_name: Optional[str]
    element_type: str
    element_name: str
    action: str
    status: str
    details: Optional[str] = None
    data: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "stage": self.stage,
            "mapping_name": self.mapping_name,
            "element_type": self.element_type,
            "element_name": self.element_name,
            "action": self.action,
            "status": self.status,
            "details": self.details,
            "data": self.data
        }

class ConversionLogger:
    def __init__(self):
        self.entries: List[LogEntry] = []
        self.entries_by_mapping: Dict[str, List[LogEntry]] = {}
        self.current_mapping: Optional[str] = None
        self.warnings: List[str] = []
        self.errors: List[str] = []

    def set_current_mapping(self, mapping_name: str):
        self.current_mapping = mapping_name
        if mapping_name not in self.entries_by_mapping:
            self.entries_by_mapping[mapping_name] = []

    def log(self,
            stage: LogStage,
            element_type: str,
            element_name: str,
            action: str,
            status: LogLevel = LogLevel.INFO,
            details: Optional[str] = None,
            data: Optional[Dict[str, Any]] = None,
            mapping_name: Optional[str] = None):

        timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        effective_mapping = mapping_name or self.current_mapping

        entry = LogEntry(
            timestamp=timestamp,
            stage=stage.value,
            mapping_name=effective_mapping,
            element_type=element_type,
            element_name=element_name,
            action=action,
            status=status.value,
            details=details,
            data=data
        )

        self.entries.append(entry)

        if effective_mapping:
            if effective_mapping not in self.entries_by_mapping:
                self.entries_by_mapping[effective_mapping] = []
            self.entries_by_mapping[effective_mapping].append(entry)

        if status == LogLevel.WARNING:
            self.warnings.append(f"[{element_type}] {element_name}: {details or action}")
        elif status == LogLevel.ERROR:
            self.errors.append(f"[{element_type}] {element_name}: {details or action}")

    def log_source(self, name: str, source_type: str, status: LogLevel = LogLevel.SUCCESS, details: Optional[str] = None):
        self.log(LogStage.PARSE, "Source", name, f"Parsed {source_type} source", status, details)

    def log_target(self, name: str, target_type: str, status: LogLevel = LogLevel.SUCCESS, details: Optional[str] = None):
        self.log(LogStage.PARSE, "Target", name, f"Parsed {target_type} target", status, details)

    def log_transformation(self, name: str, tx_type: str, action: str, status: LogLevel = LogLevel.SUCCESS, details: Optional[str] = None, data: Optional[Dict[str, Any]] = None):
        self.log(LogStage.TRANSFORM, tx_type, name, action, status, details, data)

    def log_mapplet(self, name: str, action: str, status: LogLevel = LogLevel.SUCCESS, details: Optional[str] = None):
        self.log(LogStage.TRANSFORM, "Mapplet", name, action, status, details)

    def log_expression(self, field_name: str, original: str, translated: str, status: LogLevel = LogLevel.SUCCESS, details: Optional[str] = None):
        data: Optional[Dict[str, Any]] = {"original": original, "translated": translated} if original or translated else None
        self.log(LogStage.EXPRESSION, "Expression", field_name, "Translated expression", status, details, data)

    def log_field_mapping(self, source_field: str, target_field: str, transformation: Optional[str] = None):
        details = f"{source_field} → {target_field}"
        if transformation:
            details += f" ({transformation})"
        self.log(LogStage.TRANSFORM, "FieldMapping", target_field, "Mapped field", LogLevel.INFO, details)

    def log_graph_node(self, node_name: str, node_type: str, status: LogLevel = LogLevel.SUCCESS):
        self.log(LogStage.GRAPH, "Node", node_name, f"Added {node_type} node", status)

    def log_graph_edge(self, from_node: str, to_node: str):
        self.log(LogStage.GRAPH, "Edge", f"{from_node}→{to_node}", "Connected nodes", LogLevel.INFO)

    def log_codegen(self, element_type: str, element_name: str, action: str, status: LogLevel = LogLevel.SUCCESS, details: Optional[str] = None):
        self.log(LogStage.CODEGEN, element_type, element_name, action, status, details)

    def get_all_entries(self) -> List[Dict[str, Any]]:
        return [e.to_dict() for e in self.entries]

    def get_entries_for_mapping(self, mapping_name: str) -> List[Dict[str, Any]]:
        return [e.to_dict() for e in self.entries_by_mapping.get(mapping_name, [])]

    def get_summary(self) -> Dict[str, Any]:
        return {
            "total_entries": len(self.entries),
            "warnings_count": len(self.warnings),
            "errors_count": len(self.errors),
            "mappings_processed": list(self.entries_by_mapping.keys()),
            "warnings": self.warnings,
            "errors": self.errors
        }

    def to_dict(self) -> Dict[str, Any]:
        return {
            "all": self.get_all_entries(),
            "by_mapping": {k: [e.to_dict() for e in v] for k, v in self.entries_by_mapping.items()},
            "summary": self.get_summary()
        }

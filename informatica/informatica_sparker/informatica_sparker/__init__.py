"""informatica-sparker: Convert Informatica PowerCenter XML to PySpark code.

A framework-like Python library that reads Informatica PowerCenter workflow/mapping
XML files and converts them to PySpark code deployable to Databricks.

Features:
- Handles any number of mappings per XML file
- Auto-detects source types: SQL databases, CSV, Parquet, DAT, XML, JSON, Text,
  files without extensions, and JAR connections
- Generates complete output package: mapping scripts, workflow orchestration,
  config file, SQL queries, and error logs
- Python 3.10+ compatible
"""

from .service import ConversionService
from .models import (
    UserConfig, GeneratedFile, GenerationResult,
    SourceType, FileFormat, FileLocation, ConnectorType,
    SourceConnectionInfo, SQLQueryInfo, SourceDetectionResult,
)
from .parser import InfaXMLParser
from .analyzer import Analyzer
from .codegen import CodeGenerator

__version__ = "v2026.08.04"

__all__ = [
    "ConversionService",
    "UserConfig",
    "GeneratedFile",
    "GenerationResult",
    "InfaXMLParser",
    "Analyzer",
    "CodeGenerator",
    "SourceType",
    "FileFormat",
    "FileLocation",
    "ConnectorType",
    "SourceConnectionInfo",
    "SQLQueryInfo",
    "SourceDetectionResult",
]

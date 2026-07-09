"""Validation Framework — Automated ETL validation for converted PySpark workflows.

This package is independent of the converter package.
It communicates only through manifest.json and metadata.json files on disk.
No Informatica XML parsing exists in this package.
"""

from .models import (
    ValidationTarget,
    ValidationResult,
    RowCountComparison,
    HashComparison,
)
from .loader import discover_targets, load_manifest, load_metadata
from .comparator import DatabaseClient, Comparator
from .report import generate_csv_report
from .runner import run_workflow
from .config import load_config, resolve_connection

__all__ = [
    "ValidationTarget",
    "ValidationResult",
    "RowCountComparison",
    "HashComparison",
    "discover_targets",
    "load_manifest",
    "load_metadata",
    "DatabaseClient",
    "Comparator",
    "generate_csv_report",
    "run_workflow",
    "load_config",
    "resolve_connection",
]

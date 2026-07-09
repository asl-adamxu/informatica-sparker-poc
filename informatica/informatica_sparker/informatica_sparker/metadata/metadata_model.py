"""Pydantic models for metadata.json and manifest.json.

These models define the contract between the converter (producer)
and the Validation Framework (consumer). The contract must remain
stable across converter refactoring.

metadata.json — per-workflow metadata (one per converted workflow)
manifest.json  — index of all converted workflows (one per batch)
"""

from pydantic import BaseModel
from typing import List

# This is the only version the current implementation produces.
# Bump minor (1.1) for backward-compatible additions,
# bump major (2.0) for breaking changes.
METADATA_VERSION = "1.0"


# ── metadata.json models ────────────────────────────────────────────────


class TargetEntry(BaseModel):
    """A single target table written by a mapping."""

    table: str
    connection: str


class MappingEntry(BaseModel):
    """A single mapping and its target tables."""

    mapping: str
    targets: List[TargetEntry]


class OutputInfo(BaseModel):
    """Information about the generated PySpark output file."""

    script: str


class MetadataModel(BaseModel):
    """Per-workflow metadata written alongside generated PySpark code.

    Serialises to JSON as:
        {
            "metadata_version": "1.0",
            "workflow": "WF_EMP",
            "mappings": [...],
            "output": {"script": "wf_emp.py"}
        }
    """

    metadata_version: str = METADATA_VERSION
    workflow: str
    mappings: List[MappingEntry]
    output: OutputInfo


# ── manifest.json models ────────────────────────────────────────────────


class ManifestEntry(BaseModel):
    """A single workflow entry in the batch manifest."""

    workflow: str
    metadata: str  # relative path, e.g. "WF_EMP/metadata.json"


class ManifestModel(BaseModel):
    """Batch-level manifest indexing all converted workflows.

    Serialises to JSON as:
        {
            "metadata_version": "1.0",
            "workflow_count": 6,
            "workflows": [{"workflow": "WF_EMP", "metadata": "..."}]
        }
    """

    metadata_version: str = METADATA_VERSION
    workflow_count: int
    workflows: List[ManifestEntry]

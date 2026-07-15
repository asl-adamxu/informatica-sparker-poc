"""Metadata export — Converter Metadata Export Framework.

Generates metadata.json and manifest.json alongside generated PySpark code.
"""

from .metadata_model import (
    METADATA_VERSION,
    MetadataModel,
    ManifestModel,
    ManifestEntry,
    MappingEntry,
    TargetEntry,
    OutputInfo,
)
from typing import Optional

from .metadata_builder import build_metadata
from .metadata_writer import write_metadata
from .manifest_builder import build_manifest

__all__ = [
    "METADATA_VERSION",
    "MetadataModel",
    "ManifestModel",
    "ManifestEntry",
    "MappingEntry",
    "TargetEntry",
    "OutputInfo",
    "build_metadata",
    "write_metadata",
    "build_manifest",
    "generate_metadata",
]


def generate_metadata(
    mappings,
    workflow_name: str,
    script_filename: str,
    output_dir: str,
    session_file_sources: Optional[dict] = None,
) -> str:
    """Build and write metadata.json for a single converted workflow.

    Args:
        mappings: List[MappingDefinition] from the parser.
        workflow_name: Workflow name (e.g. ``"WF_EMP"``).
        script_filename: Basename of the generated PySpark script.
        output_dir: Workflow output directory (where metadata.json is written).
        session_file_sources: Optional dict of instance name → file info,
                              used to resolve file paths for flat-file targets.

    Returns:
        Path to the written metadata.json file.
    """
    metadata = build_metadata(mappings, workflow_name, script_filename,
                               session_file_sources=session_file_sources)
    return write_metadata(metadata, output_dir)

"""Build MetadataModel from existing converter objects.

Reuses MappingDefinition, Instance, and TargetDefinition — no XML re-parsing.
"""

from typing import List, Optional

from .metadata_model import (
    MetadataModel,
    MappingEntry,
    TargetEntry,
    OutputInfo,
)


def _make_safe_name(name: str) -> str:
    """Lowercase-safe name matching the converter's filename convention."""
    safe = ""
    for ch in name:
        safe += ch if ch.isalnum() or ch == "_" else "_"
    if safe and safe[0].isdigit():
        safe = "_" + safe
    return safe.lower()


def _resolve_file_path(session_file_sources: dict, inst_name: str) -> Optional[str]:
    """Resolve file path for a file target from session configuration."""
    info = session_file_sources.get(inst_name) if session_file_sources else None
    if not info:
        return None
    # File Writer: Output filename + Output file directory
    filename = info.get("Output filename", "")
    if not filename:
        return None
    filedir = info.get("Output file directory", "")
    if filedir and filename:
        # Normalise path separators
        filedir = filedir.replace("\\", "/").rstrip("/")
        return f"{filedir}/{filename}"
    return filename


def _resolve_connection_from_sources(mapping, target_name: str) -> str:
    """Resolve a database connection name for a target.

    1. Uses the target's own ``db_name`` if set.
    2. Falls back to prefix matching *target_name* against known source
       database names (matching the codegen's pattern in
       ``_resolve_connection_alias``).
    3. Ultimate fallback: ``"target_db"``.
    """
    for tgt in mapping.targets:
        if tgt.name == target_name and tgt.db_name:
            return tgt.db_name

    source_dbs = set()
    for src in mapping.sources:
        if src.db_name:
            source_dbs.add(src.db_name.upper())

    target_upper = target_name.upper()
    for db in sorted(source_dbs, key=len, reverse=True):
        if target_upper.startswith(db + "_"):
            return db

    return "target_db"


def _is_flat_file(mapping, target_name: str) -> bool:
    """Check whether *target_name* is a flat file target."""
    for tgt in mapping.targets:
        if tgt.name == target_name:
            dtype = (tgt.database_type or "").lower()
            return "flat" in dtype
    return False


def _resolve_targets_from_instances(
    mapping,
    session_file_sources: Optional[dict] = None,
) -> List[TargetEntry]:
    """Extract target entries by matching instances to TargetDefinitions.

    Iterates over mapping.instances and resolves each instance whose
    transformation_name references a known TargetDefinition.  This
    preserves per-instance entries (multiple instances can reference the
    same TargetDefinition) and avoids reimplementing the codegen's
    complex fallback chain.

    For flat-file targets the ``connection`` field stores the file path.
    """
    target_defs = {t.name: t for t in mapping.targets}
    if not target_defs:
        return []

    targets: List[TargetEntry] = []
    for inst in mapping.instances:
        key = inst.transformation_name
        if key and key in target_defs:
            tgt = target_defs[key]

            if _is_flat_file(mapping, tgt.name):
                connection = _resolve_file_path(session_file_sources, inst.name) or ""
            else:
                connection = _resolve_connection_from_sources(mapping, tgt.name)

            targets.append(
                TargetEntry(
                    table=tgt.name,
                    connection=connection,
                )
            )

    return targets


def _build_mapping_entries(
    mappings,
    session_file_sources: Optional[dict] = None,
) -> List[MappingEntry]:
    """Build list of MappingEntry for all mappings."""
    entries: List[MappingEntry] = []
    for mapping in mappings:
        mapping_targets = _resolve_targets_from_instances(mapping, session_file_sources)
        entries.append(
            MappingEntry(
                mapping=mapping.name,
                targets=mapping_targets,
            )
        )
    return entries


def build_metadata(
    mappings,
    workflow_name: str,
    script_filename: str,
    session_file_sources: Optional[dict] = None,
) -> MetadataModel:
    """Build a MetadataModel from parsed converter objects.

    Args:
        mappings: List[MappingDefinition] from InfaXMLParser.get_mappings().
        workflow_name: Name of the workflow (from workflow_analysis).
        script_filename: Basename of the generated PySpark file (e.g. "wf_emp.py").
        session_file_sources: Optional dict of instance name → file info,
                              extracted from session-level file source config.

    Returns:
        Populated MetadataModel ready for serialisation.
    """
    mapping_entries = _build_mapping_entries(mappings, session_file_sources)

    return MetadataModel(
        workflow=workflow_name,
        mappings=mapping_entries,
        output=OutputInfo(script=script_filename),
    )

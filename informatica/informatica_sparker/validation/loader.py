"""Load manifest.json and metadata.json from disk.

This is the only module that reads the metadata contract.
No converter imports. No XML parsing.
"""

import json
from pathlib import Path
from typing import List, Tuple

from .models import ValidationTarget


def load_json(path: str) -> dict:
    """Load and return a JSON file from disk."""
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_manifest(manifest_path: str) -> dict:
    """Load manifest.json and return the parsed dict."""
    return load_json(manifest_path)


def load_metadata(metadata_path: str) -> dict:
    """Load a workflow's metadata.json and return the parsed dict."""
    data = load_json(metadata_path)
    # Basic structural validation
    if "workflow" not in data:
        raise ValueError(f"Invalid metadata.json: missing 'workflow' field in {metadata_path}")
    if "mappings" not in data:
        raise ValueError(f"Invalid metadata.json: missing 'mappings' field in {metadata_path}")
    return data


def _resolve_script_path(manifest_dir: Path, entry: dict, metadata: dict) -> str:
    """Resolve the absolute path to the generated PySpark script."""
    # metadata path: "WF_EMP/metadata.json" → parent = "WF_EMP"
    meta_rel = entry.get("metadata", "")
    workflow_dir = Path(meta_rel).parent
    script_name = metadata.get("output", {}).get("script", "")
    if not script_name:
        return ""
    return str((manifest_dir / workflow_dir / script_name).resolve())


def discover_targets(manifest_path: str) -> Tuple[List[ValidationTarget], List[str]]:
    """Build a list of ValidationTarget from manifest.json + metadata.json.

    Args:
        manifest_path: Path to manifest.json.

    Returns:
        (targets, errors) — targets to validate, and any loading errors.
    """
    manifest = load_manifest(manifest_path)
    manifest_dir = Path(manifest_path).parent
    targets: List[ValidationTarget] = []
    errors: List[str] = []

    for entry in manifest.get("workflows", []):
        wf_name = entry.get("workflow", "")
        meta_rel = entry.get("metadata", "")
        if not wf_name or not meta_rel:
            errors.append(f"Invalid manifest entry: {entry}")
            continue

        meta_path = str((manifest_dir / meta_rel).resolve())
        try:
            metadata = load_metadata(meta_path)
        except (FileNotFoundError, json.JSONDecodeError, ValueError) as e:
            errors.append(f"Failed to load {meta_rel}: {e}")
            continue

        for mapping in metadata.get("mappings", []):
            mapping_name = mapping.get("mapping", "")
            for tgt in mapping.get("targets", []):
                targets.append(
                    ValidationTarget(
                        workflow=wf_name,
                        mapping=mapping_name,
                        table=tgt.get("table", ""),
                        connection=tgt.get("connection", ""),
                    )
                )

    return targets, errors

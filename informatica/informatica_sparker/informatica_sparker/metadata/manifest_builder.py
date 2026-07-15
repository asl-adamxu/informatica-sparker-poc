"""Build manifest.json by scanning a directory tree for metadata.json files."""

import json
import os
import tempfile
from pathlib import Path
from typing import List

from .metadata_model import (
    METADATA_VERSION,
    ManifestModel,
    ManifestEntry,
)


def _discover_metadata_paths(root_dir: str) -> List[Path]:
    """Return all ``metadata.json`` paths found directly under *root_dir*.

    Only discovers files at depth 1 (``root_dir/<workflow>/metadata.json``)
    to avoid picking up stale artifacts from nested directory structures.
    """
    root = Path(root_dir)
    if not root.is_dir():
        return []

    found: List[Path] = []
    for child in sorted(root.iterdir()):
        if child.is_dir():
            meta = child / "metadata.json"
            if meta.is_file():
                found.append(meta)
    return found


def _read_workflow_name(metadata_path: Path) -> str:
    """Extract the workflow name from a metadata.json file."""
    try:
        with open(metadata_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get("workflow", "")
    except (json.JSONDecodeError, OSError):
        return ""


def build_manifest(root_dir: str) -> str:
    """Scan *root_dir* for workflow metadata and write ``manifest.json``.

    Returns the path to the written manifest.json file.
    """
    metadata_paths = _discover_metadata_paths(root_dir)

    entries: List[ManifestEntry] = []
    for meta_path in metadata_paths:
        wf_name = _read_workflow_name(meta_path)
        if not wf_name:
            continue
        # Relative path from root_dir, e.g. "WF_EMP/metadata.json"
        rel = str(meta_path.relative_to(root_dir))
        entries.append(ManifestEntry(workflow=wf_name, metadata=rel))

    # Sort for deterministic output
    entries.sort(key=lambda e: e.workflow)

    manifest = ManifestModel(
        workflow_count=len(entries),
        workflows=entries,
    )

    content = manifest.model_dump_json(indent=2)
    target = Path(root_dir) / "manifest.json"

    # Atomic write
    fd, tmp_path = tempfile.mkstemp(
        suffix=".json",
        prefix=".manifest_",
        dir=root_dir,
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(content)
            f.write("\n")
        os.replace(tmp_path, target)
    except BaseException:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise

    return str(target)

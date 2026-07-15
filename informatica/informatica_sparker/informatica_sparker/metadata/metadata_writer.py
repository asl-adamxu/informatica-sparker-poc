"""Write metadata.json to a workflow output directory."""

import json
import os
import tempfile
from pathlib import Path

from .metadata_model import MetadataModel


def write_metadata(metadata: MetadataModel, output_dir: str) -> str:
    """Write metadata.json to *output_dir* and return the file path.

    Uses an atomic write pattern: writes to a temporary file first,
    then renames to metadata.json.  This prevents partial writes from
    being read by the Validation Framework.
    """
    out_path = Path(output_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    target = out_path / "metadata.json"

    # Serialise with indentation for readability
    content = metadata.model_dump_json(indent=2)

    # Atomic write via temporary file + rename
    fd, tmp_path = tempfile.mkstemp(
        suffix=".json",
        prefix=".metadata_",
        dir=out_path,
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(content)
            f.write("\n")
        os.replace(tmp_path, target)
    except BaseException:
        # Clean up temp file on any failure
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise

    return str(target)

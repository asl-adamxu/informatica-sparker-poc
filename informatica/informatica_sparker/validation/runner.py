"""Execute generated PySpark workflows via subprocess.

This module has no validation logic — it only manages execution.
"""

import logging
import subprocess
import sys
from pathlib import Path
from typing import Dict, Optional

logger = logging.getLogger(__name__)


def run_workflow(
    script_path: str,
    config_path: Optional[str] = None,
    timeout: int = 7200,
) -> Dict:
    """Execute a generated PySpark workflow script.

    Args:
        script_path: Absolute path to the generated ``workflow.py``.
        config_path: Optional path to ``config.yml``. If not provided,
                     the default ``env/config.yml`` relative to the script
                     directory is used by the workflow itself.
        timeout: Maximum execution time in seconds (default 2 hours).

    Returns:
        Dict with keys:
            - status: "success" | "failed" | "timeout"
            - returncode: int or None
            - stdout: str
            - stderr: str
    """
    script = Path(script_path)
    if not script.exists():
        return {
            "status": "failed",
            "returncode": -1,
            "stdout": "",
            "stderr": f"Script not found: {script_path}",
        }

    cmd = [sys.executable, str(script)]

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=str(script.parent),
        )
        status = "success" if result.returncode == 0 else "failed"
        return {
            "status": status,
            "returncode": result.returncode,
            "stdout": result.stdout,
            "stderr": result.stderr,
        }
    except subprocess.TimeoutExpired:
        return {
            "status": "timeout",
            "returncode": None,
            "stdout": "",
            "stderr": f"Workflow timed out after {timeout}s",
        }
    except Exception as e:
        return {
            "status": "failed",
            "returncode": -1,
            "stdout": "",
            "stderr": str(e),
        }

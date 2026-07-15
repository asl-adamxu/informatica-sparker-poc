"""Validation domain models.

These models represent validation targets, results, and reports.
They are independent of converter models — the two systems communicate
only through manifest.json and metadata.json files on disk.
"""

from pydantic import BaseModel
from typing import List, Optional


class ValidationTarget(BaseModel):
    """A single target table to validate, resolved from metadata.json."""

    workflow: str
    mapping: str
    table: str
    connection: str


class RowCountComparison(BaseModel):
    source_count: Optional[int] = None
    target_count: Optional[int] = None
    match: bool = False


class HashComparison(BaseModel):
    source_hash: Optional[str] = None
    target_hash: Optional[str] = None
    match: bool = False


class ValidationResult(BaseModel):
    """Result of validating one target table."""

    target: ValidationTarget
    execution_status: str = "pending"  # pending | success | failed | skipped
    execution_time: str = ""           # duration string, e.g. "00:01:23"
    error_message: str = ""            # error details if execution or comparison failed
    validation_mode: str = "full"      # full | compare-only
    row_count: RowCountComparison = RowCountComparison()
    hash: HashComparison = HashComparison()
    final_result: str = "pending"  # PASS | FAIL | ERROR | SKIPPED

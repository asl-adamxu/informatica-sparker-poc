"""Generate CSV validation reports."""

import csv
from typing import List
from datetime import datetime

from .models import ValidationResult


REPORT_COLUMNS = [
    "Workflow",
    "Mapping",
    "Target Table",
    "Execution Status",
    "Execution Time",
    "Validation Mode",
    "Error Message",
    "Source Row Count",
    "Target Row Count",
    "Row Count Result",
    "Source Hash",
    "Target Hash",
    "Hash Result",
    "Final Result",
]


def generate_csv_report(
    results: List[ValidationResult],
    output_path: str,
) -> str:
    """Write a CSV validation report to *output_path*.

    Args:
        results: List of validation results to include.
        output_path: Path for the output CSV file.

    Returns:
        The *output_path* that was written to.
    """
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(REPORT_COLUMNS)

        for r in results:
            writer.writerow([
                r.target.workflow,
                r.target.mapping,
                r.target.table,
                r.execution_status,
                r.execution_time,
                r.validation_mode,
                r.error_message,
                r.row_count.source_count if r.row_count.source_count is not None else "",
                r.row_count.target_count if r.row_count.target_count is not None else "",
                _result_str(r.row_count.match, r.row_count.source_count is not None),
                r.hash.source_hash or "",
                r.hash.target_hash or "",
                _result_str(r.hash.match, r.hash.source_hash is not None),
                r.final_result,
            ])

    return output_path


def _result_str(match: bool, has_data: bool) -> str:
    """Format comparison result as PASS / FAIL / SKIP."""
    if not has_data:
        return "SKIP"
    return "PASS" if match else "FAIL"

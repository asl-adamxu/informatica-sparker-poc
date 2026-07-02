#!/bin/bash
#
# Batch convert Informatica PowerCenter XML to PySpark using informatica-sparker.
# Preserves relative folder structure in the output directory.
#
# Usage:
#   ./convert_infa-pyspark.sh <source_dir> <target_dir> [workflow_xml]
#
# Examples:
#   # Convert all XML files
#   ./convert_infa-pyspark.sh /var/lib/airflow/dags/adam/informatica/PowerCenter_workflows \
#                             /var/lib/airflow/dags/adam/informatica/PySpark_workflows
#
#   # Convert a single workflow
#   ./convert_infa-pyspark.sh /var/lib/airflow/dags/adam/informatica/PowerCenter_workflows \
#                             /var/lib/airflow/dags/adam/informatica/PySpark_workflows \
#                             WF_GMS_DDS_APLY_DLY.XML
#

set -euo pipefail

# Timestamp helper: [2026-07-02 14:30:01]
ts() {
    date '+[%Y-%m-%d %H:%M:%S]'
}

# ---------------------------------------------------------------------------
# Validate arguments
# ---------------------------------------------------------------------------
if [ $# -lt 2 ]; then
    echo "Usage: $0 <source_dir> <target_dir> [workflow_xml]" >&2
    exit 1
fi

SOURCE_DIR="$1"
TARGET_DIR="$2"
WORKFLOW_FILTER="${3:-}"

if [ ! -d "$SOURCE_DIR" ]; then
    echo "$(ts) Error: source directory does not exist: $SOURCE_DIR" >&2
    exit 1
fi

# Check that informatica-sparker is available
if ! command -v informatica-sparker &>/dev/null; then
    echo "$(ts) Error: 'informatica-sparker' not found in PATH." >&2
    echo "       Install with: pip3.11 install informatica-sparker" >&2
    exit 1
fi

# ---------------------------------------------------------------------------
# Gather XML files
# ---------------------------------------------------------------------------
if [ -n "$WORKFLOW_FILTER" ]; then
    # Only convert the specified workflow XML
    XML_FILES=$(find "$SOURCE_DIR" -type f -name "$WORKFLOW_FILTER" 2>/dev/null || true)
    if [ -z "$XML_FILES" ]; then
        echo "Error: no file matching '$WORKFLOW_FILTER' found under $SOURCE_DIR" >&2
        exit 1
    fi
else
    XML_FILES=$(find "$SOURCE_DIR" -type f -name '*.XML' 2>/dev/null || true)
    if [ -z "$XML_FILES" ]; then
        echo "No XML files found under $SOURCE_DIR" >&2
        exit 0
    fi
fi

# ---------------------------------------------------------------------------
# Convert each file
# ---------------------------------------------------------------------------
SUCCESS=0
FAILED=0

while IFS= read -r xml_path; do
    # Compute relative path and output directory
    rel_path="${xml_path#$SOURCE_DIR/}"
    rel_dir=$(dirname "$rel_path")
    base_name=$(basename "$xml_path" .XML)

    out_dir="$TARGET_DIR/$rel_dir/$base_name"

    # Check if already converted (skip if output exists and is non-empty)
    # if [ -d "$out_dir" ] && [ "$(find "$out_dir" -maxdepth 1 -name '*.py' -type f 2>/dev/null | wc -l)" -gt 0 ]; then
    #     echo "$(ts) [SKIP] $rel_path → $out_dir (already exists)"
    #     continue
    # fi

    echo "$(ts) [CONVERT] $rel_path"
    mkdir -p "$(dirname "$out_dir")"

    if informatica-sparker convert "$xml_path" -o "$out_dir" > /dev/null 2>&1; then
        # Print summary from conversion_log.txt
        log_file="$out_dir/env/conversion_log.txt"
        if [ -f "$log_file" ]; then
            sed -n '/^Total Mappings:/p; /^Successful:/p; /^Failed:/p; /^Total Warnings:/p; /^Total Errors:/p' "$log_file" | \
                while IFS= read -r line; do echo "$(ts) $line"; done
        fi
        echo "$(ts) [COMPLETED] ----------------------------------------"
        SUCCESS=$((SUCCESS + 1))
    else
        # On failure, re-run to show error
        echo "$(ts) [FAIL]    $rel_path"
        informatica-sparker convert "$xml_path" -o "$out_dir" 2>&1 | tail -20
        FAILED=$((FAILED + 1))
    fi
done <<< "$XML_FILES"

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo ""
echo "$(ts) =========================================="
echo "$(ts)  Conversion complete"
echo "$(ts)    Source: $SOURCE_DIR"
echo "$(ts)    Target: $TARGET_DIR"
echo "$(ts)    Succeeded: $SUCCESS"
echo "$(ts)    Failed:    $FAILED"
echo "$(ts) =========================================="

if [ "$FAILED" -gt 0 ]; then
    exit 1
fi

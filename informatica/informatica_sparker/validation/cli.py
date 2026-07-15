"""Validation CLI — orchestrate the full validation pipeline.

Usage (via main CLI):
    informatica-sparker validate --all --root output/
    informatica-sparker validate --workflow WF_EMP --root output/
    informatica-sparker validate --mapping m_employee --root output/
    informatica-sparker validate --compare-only --root output/
"""

import logging
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional

from .config import load_config, resolve_connection
from .loader import discover_targets
from .models import ValidationTarget, ValidationResult
from .runner import run_workflow
from .comparator import DatabaseClient, Comparator
from .report import generate_csv_report

logger = logging.getLogger(__name__)


def _load_json(path: str) -> dict:
    import json
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _resolve_script_path(manifest_path: str, workflow_name: str) -> str:
    """Find the workflow script path from manifest + metadata."""
    manifest_dir = Path(manifest_path).parent
    manifest = _load_json(manifest_path)

    for entry in manifest.get("workflows", []):
        if entry.get("workflow") == workflow_name:
            meta_rel = entry.get("metadata", "")
            meta_path = manifest_dir / meta_rel
            if meta_path.exists():
                meta = _load_json(str(meta_path))
                script_name = meta.get("output", {}).get("script", "")
                if script_name:
                    return str((meta_path.parent / script_name).resolve())
            break
    return ""


def _format_duration(seconds: float) -> str:
    """Format seconds as HH:MM:SS."""
    hours, remainder = divmod(int(seconds), 3600)
    minutes, secs = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def _print_summary(
    results: List[ValidationResult],
    duration: float,
    report_path: str,
) -> str:
    """Print validation summary to console and save to ``validation_summary.txt``.

    Returns the path to the written summary file.
    """
    workflows = set(r.target.workflow for r in results)
    passed = sum(1 for r in results if r.final_result == "PASS")
    failed = sum(1 for r in results if r.final_result == "FAIL")
    errors = sum(1 for r in results if r.final_result == "ERROR")
    skipped = sum(1 for r in results if r.final_result == "SKIPPED")

    lines = [
        "",
        "=" * 42,
        "  Validation Summary",
        "=" * 42,
        "",
        f"  Total Workflows : {len(workflows)}",
        f"  Total Targets   : {len(results)}",
        f"",
        f"  Passed          : {passed}",
        f"  Failed          : {failed}",
        f"  Execution Error : {errors}",
        f"  Skipped         : {skipped}",
        f"",
        f"  Duration        : {_format_duration(duration)}",
        f"",
        f"  Report          : {report_path}",
        "=" * 42,
        "",
    ]

    summary = "\n".join(lines)
    print(summary)

    # Save to file
    report_dir = Path(report_path).parent
    summary_path = str(report_dir / "validation_summary.txt")
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write(summary)

    return summary_path


def _validate_targets(
    targets: List[ValidationTarget],
    config: dict,
    exec_status: str,
    validation_mode: str = "full",
    source_env: Optional[str] = None,
    target_env: Optional[str] = None,
) -> List[ValidationResult]:
    """Run row-count and hash comparisons for each target.

    When *source_env* and *target_env* are both set, queries hit different
    environment databases (e.g. DEV vs TEST) enabling true source-to-target
    comparison.  When neither is set, a single connection is used for both
    sides (backward-compatible smoke-test mode).
    """
    results: List[ValidationResult] = []
    src_clients = {}
    tgt_clients = {}

    for target in targets:
        result = ValidationResult(
            target=target,
            execution_status=exec_status,
            validation_mode=validation_mode,
        )

        # Resolve source and target connection configs
        dual_mode = source_env != target_env
        src_config = resolve_connection(config, target.connection,
                                         environment=source_env)
        tgt_config = resolve_connection(config, target.connection,
                                         environment=target_env)

        if not src_config and not tgt_config:
            result.final_result = "SKIPPED"
            result.error_message = (
                f"No connection found for '{target.connection}' "
                f"in config connections"
            )
            results.append(result)
            continue

        if not src_config:
            src_config = tgt_config
        if not tgt_config:
            tgt_config = src_config

        # Cache DB clients by (environment, connection_name)
        def _get_client(cache, env, conn_cfg, conn_name):
            key = f"{env or ''}:{conn_name}"
            if key not in cache:
                try:
                    cache[key] = DatabaseClient(conn_cfg)
                except Exception as e:
                    return None, str(e)
            return cache[key], None

        src_client, src_err = _get_client(src_clients, source_env, src_config,
                                          target.connection)
        if src_err:
            result.final_result = "ERROR"
            result.error_message = f"Source connect failed: {src_err}"
            results.append(result)
            continue

        tgt_client, tgt_err = _get_client(tgt_clients, target_env, tgt_config,
                                          target.connection)
        if tgt_err:
            result.final_result = "ERROR"
            result.error_message = f"Target connect failed: {tgt_err}"
            results.append(result)
            continue

        src_schema = src_config.get("schema", "")
        tgt_schema = tgt_config.get("schema", "")

        # Dual-DB or single-DB comparator
        if dual_mode and src_config != tgt_config:
            comparator = Comparator(source_client=src_client,
                                    target_client=tgt_client)
        else:
            comparator = Comparator(source_client=src_client)

        try:
            row_count = comparator.compare_row_count(
                source_schema=src_schema,
                source_table=target.table,
                target_schema=tgt_schema,
                target_table=target.table,
            )
            hash_cmp = comparator.compare_table_hash(
                source_schema=src_schema,
                source_table=target.table,
                target_schema=tgt_schema,
                target_table=target.table,
            )

            all_valid = row_count.match and hash_cmp.match
            any_fail = (
                (row_count.source_count is not None and not row_count.match)
                or (hash_cmp.source_hash is not None and not hash_cmp.match)
            )

            result.final_result = "FAIL" if any_fail else "PASS" if all_valid else "ERROR"
            result.row_count = row_count
            result.hash = hash_cmp

        except Exception as e:
            result.final_result = "ERROR"
            result.error_message = str(e)

        results.append(result)

    for db in list(src_clients.values()) + list(tgt_clients.values()):
        try:
            db.close()
        except Exception:
            pass

    return results


def run_validate(args) -> None:
    """Main validation entry point called from converter CLI."""
    start_time = time.time()
    root_dir = Path(args.root)
    config_path = args.config or os.path.join(str(root_dir), "env", "config.yml")
    manifest_path = root_dir / "manifest.json"

    # Determine mode and profiles
    compare_only = getattr(args, "compare_only", False)
    validation_mode = "compare-only" if compare_only else "full"
    # Source = validation.informatica (Informatica target DB)
    # Target = base connections: (PySpark output — no separate profile needed)
    source_env = "informatica"
    target_env = None

    if not manifest_path.exists():
        print(f"Error: manifest.json not found at {manifest_path}", file=sys.stderr)
        sys.exit(1)

    if not os.path.exists(config_path):
        print(f"Error: config file not found at {config_path}", file=sys.stderr)
        print(f"  The validation framework uses the converter's env/config.yml by default.", file=sys.stderr)
        print(f"  Override with: --config /path/to/config.yml", file=sys.stderr)
        sys.exit(1)

    print(f"Loading config from {config_path}...")
    config = load_config(config_path)

    print(f"Loading manifest from {manifest_path}...")
    targets, load_errors = discover_targets(str(manifest_path))

    for err in load_errors:
        print(f"  Warning: {err}", file=sys.stderr)

    # Filter targets
    if args.workflow:
        targets = [t for t in targets if t.workflow == args.workflow]
        print(f"Filtered to workflow: {args.workflow}")
    elif args.mapping:
        targets = [t for t in targets if t.mapping == args.mapping]
        print(f"Filtered to mapping: {args.mapping}")
    else:
        # --all (default)
        pass

    if not targets:
        print("No targets to validate.", file=sys.stderr)
        sys.exit(0)

    # Group targets by workflow
    workflow_names = sorted(set(t.workflow for t in targets))
    print(f"Found {len(targets)} target(s) across {len(workflow_names)} workflow(s)")
    if compare_only:
        print("Mode: COMPARE ONLY (skipping workflow execution)")
    print()

    all_results: List[ValidationResult] = []

    for i, wf_name in enumerate(workflow_names, 1):
        wf_targets = [t for t in targets if t.workflow == wf_name]
        print(f"[{i}/{len(workflow_names)}] {wf_name} ({len(wf_targets)} target(s))")

        exec_status = "success"

        if not compare_only:
            # Execute workflow
            script_path = _resolve_script_path(str(manifest_path), wf_name)
            if not script_path or not os.path.exists(script_path):
                print(f"  Script not found, skipping execution")
                exec_status = "skipped"
            else:
                print(f"  Executing: {script_path}")
                try:
                    exec_result = run_workflow(script_path)
                    exec_status = exec_result["status"]
                    print(f"  Execution: {exec_status.upper()}")
                except Exception as e:
                    exec_status = "failed"
                    print(f"  Execution: FAILED ({e})")
                    # Continue processing next workflow (continue-on-error is default)

        # Validate targets
        results = _validate_targets(
            wf_targets, config, exec_status, validation_mode,
            source_env=source_env, target_env=target_env,
        )

        # Capture execution time and error messages
        for r in results:
            r.validation_mode = validation_mode

        all_results.extend(results)

        # Print per-target summary
        for r in results:
            status_icon = "✓" if r.final_result == "PASS" else "✗" if r.final_result == "FAIL" else "·"
            parts = [
                f"    {status_icon} {r.target.mapping} → {r.target.table}: {r.final_result}",
                f"(rows={r.row_count.target_count}, hash={r.hash.target_hash or 'N/A'})",
            ]
            if r.error_message:
                parts.append(f" [{r.error_message}]")
            print(" ".join(parts))
        print()

    # Generate report
    report_path = str(root_dir / "validation_report.csv")
    generate_csv_report(all_results, report_path)
    print(f"Report written to {report_path}")

    # Summary
    duration = time.time() - start_time
    summary_path = _print_summary(all_results, duration, report_path)
    print(f"Summary saved to {summary_path}")

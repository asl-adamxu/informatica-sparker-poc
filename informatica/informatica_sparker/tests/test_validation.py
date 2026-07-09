"""Unit and integration tests for the Validation Framework.

Tests are independent of the converter package — they only use
validation framework modules and standard library.
"""

import json
import os
import csv
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock

from validation.models import (
    ValidationTarget,
    ValidationResult,
    RowCountComparison,
    HashComparison,
)
from validation.loader import discover_targets, load_manifest, load_metadata
from validation.runner import run_workflow
from validation.report import generate_csv_report, REPORT_COLUMNS
from validation.config import load_config, resolve_connection
from validation.comparator import DatabaseClient, Comparator


# ── Model tests ──────────────────────────────────────────────────────────


class TestValidationModels:
    def test_validation_target(self):
        t = ValidationTarget(
            workflow="WF_EMP", mapping="m_emp", table="TGT_EMP",
            connection="DB_DEV"
        )
        assert t.workflow == "WF_EMP"
        assert t.mapping == "m_emp"
        assert t.table == "TGT_EMP"
        assert t.connection == "DB_DEV"

    def test_validation_result_defaults(self):
        r = ValidationResult(
            target=ValidationTarget(
                workflow="WF_A", mapping="m_a", table="T_A", connection="DB"
            )
        )
        assert r.execution_status == "pending"
        assert r.final_result == "pending"
        assert r.row_count.match is False
        assert r.hash.match is False

    def test_validation_result_pass(self):
        t = ValidationTarget(workflow="W", mapping="m", table="T", connection="C")
        r = ValidationResult(
            target=t,
            execution_status="success",
            row_count=RowCountComparison(source_count=100, target_count=100, match=True),
            hash=HashComparison(source_hash="abc", target_hash="abc", match=True),
            final_result="PASS",
        )
        assert r.final_result == "PASS"
        d = r.model_dump()
        assert d["target"]["table"] == "T"
        assert d["row_count"]["match"] is True

    def test_validation_target_json_roundtrip(self):
        t = ValidationTarget(
            workflow="WF_X", mapping="m_x", table="T_X", connection="DB_X"
        )
        d = t.model_dump()
        assert d["workflow"] == "WF_X"
        assert d["mapping"] == "m_x"
        assert d["table"] == "T_X"
        assert d["connection"] == "DB_X"


# ── Loader tests ─────────────────────────────────────────────────────────


class TestLoader:
    def setup_method(self):
        self.tmpdir = tempfile.mkdtemp()

    def teardown_method(self):
        import shutil
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _create_workflow(self, name, mappings, script="wf.py"):
        wf_dir = os.path.join(self.tmpdir, name)
        os.makedirs(wf_dir, exist_ok=True)
        meta = {
            "metadata_version": "1.0",
            "workflow": name,
            "mappings": mappings,
            "output": {"script": script},
        }
        with open(os.path.join(wf_dir, "metadata.json"), "w") as f:
            json.dump(meta, f)

    def _create_manifest(self, workflows):
        manifest = {
            "metadata_version": "1.0",
            "workflow_count": len(workflows),
            "workflows": [
                {"workflow": w, "metadata": f"{w}/metadata.json"} for w in workflows
            ],
        }
        with open(os.path.join(self.tmpdir, "manifest.json"), "w") as f:
            json.dump(manifest, f)

    def test_discover_single_workflow(self):
        self._create_workflow("WF_A", [
            {"mapping": "m_a1", "targets": [{"table": "T_A1", "connection": "C_A"}]}
        ])
        self._create_manifest(["WF_A"])

        targets, errors = discover_targets(os.path.join(self.tmpdir, "manifest.json"))
        assert len(errors) == 0
        assert len(targets) == 1
        assert targets[0].workflow == "WF_A"
        assert targets[0].table == "T_A1"

    def test_discover_multiple_workflows(self):
        self._create_workflow("WF_A", [
            {"mapping": "m_a1", "targets": [{"table": "T_A1", "connection": "C_A"}]},
            {"mapping": "m_a2", "targets": [{"table": "T_A2", "connection": "C_A"}]},
        ])
        self._create_workflow("WF_B", [
            {"mapping": "m_b1", "targets": [{"table": "T_B1", "connection": "C_B"}]},
        ])
        self._create_manifest(["WF_A", "WF_B"])

        targets, errors = discover_targets(os.path.join(self.tmpdir, "manifest.json"))
        assert len(errors) == 0
        assert len(targets) == 3
        workflows = set(t.workflow for t in targets)
        assert workflows == {"WF_A", "WF_B"}

    def test_missing_metadata_reported_as_error(self):
        self._create_manifest(["WF_MISSING"])
        targets, errors = discover_targets(os.path.join(self.tmpdir, "manifest.json"))
        assert len(errors) == 1
        assert len(targets) == 0

    def test_empty_manifest(self):
        self._create_manifest([])
        targets, errors = discover_targets(os.path.join(self.tmpdir, "manifest.json"))
        assert len(errors) == 0
        assert len(targets) == 0

    def test_load_manifest(self):
        self._create_manifest(["WF_A"])
        m = load_manifest(os.path.join(self.tmpdir, "manifest.json"))
        assert m["workflow_count"] == 1
        assert m["workflows"][0]["workflow"] == "WF_A"

    def test_load_metadata(self):
        self._create_workflow("WF_A", [
            {"mapping": "m_a1", "targets": [{"table": "T_A1", "connection": "C_A"}]}
        ])
        m = load_metadata(os.path.join(self.tmpdir, "WF_A", "metadata.json"))
        assert m["workflow"] == "WF_A"
        assert len(m["mappings"]) == 1
        assert m["mappings"][0]["targets"][0]["table"] == "T_A1"

    def test_invalid_metadata_raises(self):
        path = os.path.join(self.tmpdir, "bad.json")
        with open(path, "w") as f:
            json.dump({"foo": "bar"}, f)
        import json as _json
        try:
            load_metadata(path)
            assert False, "Should have raised ValueError"
        except ValueError as e:
            assert "missing" in str(e).lower()


# ── Runner tests ────────────────────────────────────────────────────────


class TestRunner:
    def test_successful_execution(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            script = os.path.join(tmpdir, "test.py")
            with open(script, "w") as f:
                f.write("print('OK')\n")
            result = run_workflow(script)
            assert result["status"] == "success"
            assert result["returncode"] == 0
            assert "OK" in result["stdout"]

    def test_failed_execution(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            script = os.path.join(tmpdir, "fail.py")
            with open(script, "w") as f:
                f.write("import sys; sys.exit(1)\n")
            result = run_workflow(script)
            assert result["status"] == "failed"
            assert result["returncode"] == 1

    def test_missing_script(self):
        result = run_workflow("/nonexistent/script.py")
        assert result["status"] == "failed"
        assert "not found" in result["stderr"].lower()


# ── Report tests ─────────────────────────────────────────────────────────


class TestReport:
    def test_csv_columns(self):
        assert REPORT_COLUMNS == [
            "Workflow", "Mapping", "Target Table", "Execution Status",
            "Execution Time", "Validation Mode", "Error Message",
            "Source Row Count", "Target Row Count", "Row Count Result",
            "Source Hash", "Target Hash", "Hash Result", "Final Result",
        ]

    def test_csv_output(self):
        results = [
            ValidationResult(
                target=ValidationTarget(
                    workflow="W", mapping="m", table="T", connection="C"
                ),
                execution_status="success",
                row_count=RowCountComparison(source_count=10, target_count=10, match=True),
                hash=HashComparison(source_hash="h1", target_hash="h1", match=True),
                validation_mode="full",
                final_result="PASS",
            ),
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "report.csv")
            generate_csv_report(results, path)
            with open(path) as f:
                rows = list(csv.reader(f))
            assert len(rows) == 2  # header + 1 result
            assert rows[1][0] == "W"
            assert rows[1][1] == "m"
            assert rows[1][2] == "T"
            assert rows[1][4] == ""   # execution_time (empty default)
            assert rows[1][5] == "full"  # validation_mode
            assert rows[1][9] == "PASS"  # Row Count Result (was [6])
            assert rows[1][12] == "PASS" # Hash Result
            assert rows[1][13] == "PASS" # Final Result

    def test_csv_with_errors(self):
        results = [
            ValidationResult(
                target=ValidationTarget(
                    workflow="W", mapping="m", table="T", connection="C"
                ),
                execution_status="failed",
                row_count=RowCountComparison(),
                hash=HashComparison(),
                final_result="ERROR",
            ),
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "report.csv")
            generate_csv_report(results, path)
            with open(path) as f:
                rows = list(csv.reader(f))
            assert rows[1][3] == "failed"
            assert rows[1][9] == "SKIP"  # Row Count Result (was [6])
            assert rows[1][13] == "ERROR"  # Final Result (was [10])

    def test_csv_empty_results(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "report.csv")
            generate_csv_report([], path)
            with open(path) as f:
                rows = list(csv.reader(f))
            assert len(rows) == 1  # header only


# ── Config tests ─────────────────────────────────────────────────────────


class TestConfig:
    def test_load_config(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cfg_path = os.path.join(tmpdir, "validation.yaml")
            with open(cfg_path, "w") as f:
                f.write("connections:\n  DB_A:\n    type: oracle\n    schema: SCH_A\n")
            cfg = load_config(cfg_path)
            assert cfg["connections"]["DB_A"]["type"] == "oracle"

    def test_resolve_connection_exact_match(self):
        cfg = {
            "connections": {
                "DB_A": {"type": "oracle", "schema": "SCH_A"},
            }
        }
        conn = resolve_connection(cfg, "DB_A")
        assert conn is not None
        assert conn["schema"] == "SCH_A"

    def test_resolve_connection_nonexistent(self):
        cfg = {"connections": {"DB_A": {"type": "oracle"}}}
        conn = resolve_connection(cfg, "DB_NONEXIST")
        assert conn is None


# ── Comparator tests (with mocked DB) ────────────────────────────────────


class TestComparator:
    def test_compare_row_count_match(self):
        mock_db = MagicMock()
        mock_db.get_row_count.return_value = 100

        comp = Comparator(mock_db)
        result = comp.compare_row_count("SCH", "TBL")
        assert result.source_count == 100
        assert result.target_count == 100
        assert result.match is True

    def test_compare_row_count_fail(self):
        mock_db = MagicMock()
        mock_db.get_row_count.return_value = None

        comp = Comparator(mock_db)
        result = comp.compare_row_count("SCH", "TBL")
        assert result.source_count is None
        assert result.match is False

    def test_compare_table_hash_match(self):
        mock_db = MagicMock()
        mock_db.get_table_hash.return_value = "abc123"

        comp = Comparator(mock_db)
        result = comp.compare_table_hash("SCH", "TBL")
        assert result.source_hash == "abc123"
        assert result.match is True

    def test_compare_table_hash_fail(self):
        mock_db = MagicMock()
        mock_db.get_table_hash.return_value = None

        comp = Comparator(mock_db)
        result = comp.compare_table_hash("SCH", "TBL")
        assert result.source_hash is None
        assert result.match is False


# ── End-to-end validation pipeline test ──────────────────────────────────


class TestValidationPipeline:
    def test_full_pipeline(self):
        """Simulate the full validation flow: load → filter → run → report.

        Uses mocked DatabaseClient to avoid requiring a real database.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create workflow directories with metadata
            for wf in ["WF_A", "WF_B"]:
                wf_dir = os.path.join(tmpdir, wf)
                os.makedirs(wf_dir)
                meta = {
                    "metadata_version": "1.0",
                    "workflow": wf,
                    "mappings": [
                        {
                            "mapping": f"m_{wf.lower()}",
                            "targets": [
                                {"table": f"TGT_{wf}", "connection": "DB_DEV"},
                            ],
                        }
                    ],
                    "output": {"script": f"{wf.lower()}.py"},
                }
                with open(os.path.join(wf_dir, "metadata.json"), "w") as f:
                    json.dump(meta, f)

                # Create workflow script (simple Python script for testing)
                with open(os.path.join(wf_dir, f"{wf.lower()}.py"), "w") as f:
                    f.write("print('Workflow executed successfully')\n")

            # Create manifest
            manifest = {
                "metadata_version": "1.0",
                "workflow_count": 2,
                "workflows": [
                    {"workflow": "WF_A", "metadata": "WF_A/metadata.json"},
                    {"workflow": "WF_B", "metadata": "WF_B/metadata.json"},
                ],
            }
            with open(os.path.join(tmpdir, "manifest.json"), "w") as f:
                json.dump(manifest, f)

            # Create validation config
            cfg = {
                "connections": {
                    "DB_DEV": {"type": "oracle", "schema": "TST", "dsn": "mock"},
                }
            }

            # ── Step 1: Load targets ──
            targets, errors = discover_targets(
                os.path.join(tmpdir, "manifest.json")
            )
            assert len(errors) == 0
            assert len(targets) == 2

            # ── Step 2: Filter by workflow ──
            wf_a_targets = [t for t in targets if t.workflow == "WF_A"]
            assert len(wf_a_targets) == 1

            # ── Step 3: Filter by mapping ──
            m_a_targets = [t for t in targets if t.mapping == "m_wf_a"]
            assert len(m_a_targets) == 1

            # ── Step 4: Run workflow scripts ──
            for t in targets:
                script = os.path.join(tmpdir, t.workflow,
                                      f"{t.workflow.lower()}.py")
                result = run_workflow(script)
                assert result["status"] == "success"

            # ── Step 5: Compare (mocked DB) ──
            from validation.config import resolve_connection

            conn_config = resolve_connection(cfg, "DB_DEV")
            assert conn_config is not None

            mock_db = MagicMock()
            mock_db.get_row_count.return_value = 50
            mock_db.get_table_hash.return_value = "hash123"
            comp = Comparator(mock_db)

            row_cmp = comp.compare_row_count("TST", "TGT_WF_A")
            hash_cmp = comp.compare_table_hash("TST", "TGT_WF_A")

            assert row_cmp.match is True
            assert hash_cmp.match is True

            # ── Step 6: Build results and report ──
            results = [
                ValidationResult(
                    target=t,
                    execution_status="success",
                    row_count=RowCountComparison(
                        source_count=50, target_count=50, match=True
                    ),
                    hash=HashComparison(
                        source_hash="hash123", target_hash="hash123", match=True
                    ),
                    final_result="PASS",
                )
                for t in targets
            ]

            report_path = os.path.join(tmpdir, "validation_report.csv")
            generate_csv_report(results, report_path)
            assert os.path.exists(report_path)

            with open(report_path) as f:
                rows = list(csv.reader(f))

            # Header + 2 results
            assert len(rows) == 3
            assert rows[1][13] == "PASS"
            assert rows[2][13] == "PASS"

            # Verify the CSV content for row 1 (new column layout)
            # 0=Workflow, 1=Mapping, 2=Target Table, 3=Execution Status,
            # 4=Execution Time, 5=Validation Mode, 6=Error Message,
            # 7=Source Row Count, 8=Target Row Count, 9=Row Count Result,
            # 10=Source Hash, 11=Target Hash, 12=Hash Result, 13=Final Result
            assert rows[1][0] == "WF_A"
            assert rows[1][1] == "m_wf_a"
            assert rows[1][2] == "TGT_WF_A"
            assert rows[1][3] == "success"
            assert rows[1][7] == "50"
            assert rows[1][8] == "50"
            assert rows[1][9] == "PASS"
            assert rows[1][10] == "hash123"
            assert rows[1][11] == "hash123"
            assert rows[1][12] == "PASS"
            assert rows[1][13] == "PASS"


# ── Config: source/target connection resolution tests ────────────────────


# ── Config: resolve_connection from env/config.yml format ───────────────


class TestConfigResolver:
    def test_exact_match(self):
        """resolve_connection finds a connection by exact name."""
        cfg = {"connections": {"DPA": {"schema": "airflow", "database": "DPA"}}}
        from validation.config import resolve_connection
        conn = resolve_connection(cfg, "DPA")
        assert conn is not None
        assert conn["schema"] == "airflow"

    def test_prefix_fallback(self):
        """Prefix fallback matches when exact name doesn't exist."""
        cfg = {"connections": {"DPA": {"schema": "airflow"}}}
        from validation.config import resolve_connection
        conn = resolve_connection(cfg, "DPA_FACT_CMS")
        assert conn is not None
        assert conn["schema"] == "airflow"

    def test_missing_connection(self):
        from validation.config import resolve_connection
        assert resolve_connection({}, "NONEXIST") is None
        assert resolve_connection({"connections": {}}, "NONEXIST") is None


# ── Config: validation.connections override resolution ──────────────────


class TestConfigValidationOverride:
    def test_validation_overrides_merge_onto_base(self):
        """validation.connections fields override base connection when environment set."""
        cfg = {
            "connections": {"DPA": {"host": "base-host", "schema": "base", "port": 1521}},
            "validation": {
                "connections": {"host": "val-host", "schema": "val"},
            },
        }
        from validation.config import resolve_connection
        base = resolve_connection(cfg, "DPA")
        src = resolve_connection(cfg, "DPA", environment="informatica")
        assert base["host"] == "base-host"
        assert base["schema"] == "base"
        assert src["host"] == "val-host"   # overridden
        assert src["schema"] == "val"      # overridden
        assert src["port"] == 1521          # unchanged (not in overrides)

    def test_validation_override_fields_are_additive(self):
        """Only specified fields are overridden; others come from base."""
        cfg = {
            "connections": {"DPA": {"host": "base", "schema": "base", "username": "pyspark"}},
            "validation": {
                "connections": {"username": "informatica"},
            },
        }
        from validation.config import resolve_connection
        src = resolve_connection(cfg, "DPA", environment="informatica")
        assert src["username"] == "informatica"   # overridden
        assert src["host"] == "base"               # unchanged
        assert src["schema"] == "base"             # unchanged

    def test_no_environment_returns_base_unchanged(self):
        """Without environment, validation.connections is not applied."""
        cfg = {
            "connections": {"DPA": {"schema": "base"}},
            "validation": {"connections": {"schema": "val"}},
        }
        from validation.config import resolve_connection
        conn = resolve_connection(cfg, "DPA")  # no environment
        assert conn["schema"] == "base"

    def test_no_validation_section_returns_base(self):
        """When validation section is absent, base connection is returned."""
        cfg = {"connections": {"DPA": {"schema": "base"}}}
        from validation.config import resolve_connection
        conn = resolve_connection(cfg, "DPA", environment="informatica")
        assert conn["schema"] == "base"

    def test_missing_base_connection_returns_none(self):
        """If base connection is missing, None is returned regardless."""
        cfg = {
            "connections": {"DPA": {"schema": "base"}},
            "validation": {"connections": {"host": "val"}},
        }
        from validation.config import resolve_connection
        assert resolve_connection(cfg, "NONEXIST", environment="inf") is None
        assert resolve_connection(cfg, "NONEXIST") is None


# ── Comparator: dual-DB tests ────────────────────────────────────────────


class TestComparatorDualDB:
    def test_dual_db_row_count_match(self):
        src_db = MagicMock()
        src_db.get_row_count.return_value = 100
        tgt_db = MagicMock()
        tgt_db.get_row_count.return_value = 100

        from validation.comparator import Comparator
        comp = Comparator(source_client=src_db, target_client=tgt_db)
        result = comp.compare_row_count("SRC", "TBL", "TGT", "TBL")
        assert result.source_count == 100
        assert result.target_count == 100
        assert result.match is True

    def test_dual_db_row_count_mismatch(self):
        src_db = MagicMock()
        src_db.get_row_count.return_value = 100
        tgt_db = MagicMock()
        tgt_db.get_row_count.return_value = 99

        from validation.comparator import Comparator
        comp = Comparator(source_client=src_db, target_client=tgt_db)
        result = comp.compare_row_count("SRC", "TBL", "TGT", "TBL")
        assert result.match is False

    def test_dual_db_hash_mismatch(self):
        src_db = MagicMock()
        src_db.get_table_hash.return_value = "hash_a"
        tgt_db = MagicMock()
        tgt_db.get_table_hash.return_value = "hash_b"

        from validation.comparator import Comparator
        comp = Comparator(source_client=src_db, target_client=tgt_db)
        result = comp.compare_table_hash("SRC", "TBL", "TGT", "TBL")
        assert result.match is False
        assert result.source_hash == "hash_a"
        assert result.target_hash == "hash_b"

    def test_single_db_fallback(self):
        """When only source_client is passed, target_db == source_db."""
        db = MagicMock()
        db.get_row_count.return_value = 100
        db.get_table_hash.return_value = "hash_x"

        from validation.comparator import Comparator
        comp = Comparator(source_client=db)  # no target_client
        rc = comp.compare_row_count("SCH", "TBL")
        assert rc.match is True
        assert rc.source_count == rc.target_count

        hc = comp.compare_table_hash("SCH", "TBL")
        assert hc.match is True
        assert hc.source_hash == hc.target_hash


# ── Report: new fields appearance tests ──────────────────────────────────


class TestReportEnhancements:
    def test_csv_includes_new_columns(self):
        """Verify Execution Time, Validation Mode, Error Message columns exist."""
        from validation.report import REPORT_COLUMNS
        assert "Execution Time" in REPORT_COLUMNS
        assert "Validation Mode" in REPORT_COLUMNS
        assert "Error Message" in REPORT_COLUMNS

    def test_csv_contains_execution_time_and_error(self):
        results = [
            ValidationResult(
                target=ValidationTarget(
                    workflow="W", mapping="m", table="T", connection="C"
                ),
                execution_status="failed",
                execution_time="00:01:30",
                error_message="Connection refused",
                validation_mode="full",
                final_result="ERROR",
            ),
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "report.csv")
            generate_csv_report(results, path)
            with open(path) as f:
                rows = list(csv.reader(f))
            # Find column indices from header
            header = rows[0]
            assert rows[1][header.index("Execution Time")] == "00:01:30"
            assert rows[1][header.index("Error Message")] == "Connection refused"
            assert rows[1][header.index("Validation Mode")] == "full"


# ── ValidationResult: new model field defaults ──────────────────────────


class TestValidationResultDefaults:
    def test_new_fields_have_sensible_defaults(self):
        """New fields should not break existing code that omits them."""
        t = ValidationTarget(workflow="W", mapping="m", table="T", connection="C")
        r = ValidationResult(target=t)
        assert r.execution_time == ""
        assert r.error_message == ""
        assert r.validation_mode == "full"

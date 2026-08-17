"""Tests for lib.load_mapping_variables (the shared UTL_JOB_PARAM reader)."""

import logging


def _cfg(path):
    return {"objects": {"UTL_JOB_PARAM": {"path": str(path)}}}


def test_reads_variables_and_strips_dollar_prefix(runtime_lib, tmp_path):
    f = tmp_path / "job_params.txt"
    f.write_text("$$v_load_start_ds=20260601\n$$v_load_end_ds=20260701\n")
    out = runtime_lib.load_mapping_variables(
        _cfg(f), ["$$v_load_start_ds", "$$v_load_end_ds"])
    assert out == {"v_load_start_ds": "20260601", "v_load_end_ds": "20260701"}


def test_values_may_contain_equals(runtime_lib, tmp_path):
    f = tmp_path / "job_params.txt"
    f.write_text("$$v_filter=FL_ALLO_ST='0'\n")
    out = runtime_lib.load_mapping_variables(_cfg(f), ["$$v_filter"])
    assert out == {"v_filter": "FL_ALLO_ST='0'"}


def test_absent_variable_absent_from_result(runtime_lib, tmp_path):
    f = tmp_path / "job_params.txt"
    f.write_text("$$v_a=1\n")
    out = runtime_lib.load_mapping_variables(_cfg(f), ["$$v_a", "$$v_b"])
    assert out == {"v_a": "1"}  # v_b missing → caller .get()s its default


def test_missing_config_entry_warns_and_returns_empty(runtime_lib, caplog):
    with caplog.at_level(logging.WARNING):
        out = runtime_lib.load_mapping_variables({"objects": {}}, ["$$v_a"])
    assert out == {}
    assert any("UTL_JOB_PARAM" in r.message for r in caplog.records)


def test_missing_file_warns_and_returns_empty(runtime_lib, tmp_path, caplog):
    with caplog.at_level(logging.WARNING):
        out = runtime_lib.load_mapping_variables(
            _cfg(tmp_path / "nope.txt"), ["$$v_a"])
    assert out == {}
    assert any("UTL_JOB_PARAM" in r.message for r in caplog.records)


def test_extra_lines_ignored(runtime_lib, tmp_path):
    f = tmp_path / "job_params.txt"
    f.write_text("garbage line\n$$v_a=1\n\n")
    out = runtime_lib.load_mapping_variables(_cfg(f), ["$$v_a"])
    assert out == {"v_a": "1"}

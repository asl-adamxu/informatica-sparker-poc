"""Tests for the exact dynamic-lookup conversion (applyInPandas + RDD fallback).

Source of truth is WF_NHS_TL.XML only; WF_EMS_TL is intentionally not parsed.
The runtime helper is exercised by rendering runtime_lib.py.j2 directly, so
these tests cover the exact code that gets deployed into every workflow env.
"""

import os
import re
import sys
import types
from pathlib import Path

import pytest
from jinja2 import Environment, FileSystemLoader

from informatica_sparker.handlers import TransformHandlers
from informatica_sparker.ir import ApplyLookupStep
from informatica_sparker.models import UserConfig
from informatica_sparker.parser import InfaXMLParser


ROOT = Path(__file__).resolve().parents[2]
NHS_TL_XML = ROOT / "PowerCenter_workflows" / "transform_and_load" / "WF_NHS_TL.XML"
TEMPLATES_DIR = ROOT / "informatica_sparker" / "informatica_sparker" / "templates"
NHS_OUTPUT_DIR = ROOT / "PySpark_workflows" / "transform_and_load" / "WF_NHS_TL"


def _load_nhs_mappings():
    parser = InfaXMLParser(NHS_TL_XML.read_bytes())
    assert parser.parse()
    return parser.get_mappings()


def _render_runtime_lib():
    """Render the exact runtime_lib.py.j2 and import it as a module."""
    try:
        import pyspark  # noqa: F401
    except ImportError:
        # python3.11 has no system pyspark; the CDP parcel provides it.
        spark_home = (
            "/opt/cloudera/parcels/"
            "SPARK3-3.5.4.3.5.7191000.0-30-1.p0.68499982/lib/spark3"
        )
        sys.path.insert(0, str(Path(spark_home) / "python"))
        sys.path.insert(
            0,
            str(Path(spark_home) / "python" / "lib" / "py4j-0.10.9.7-src.zip"),
        )
    env = Environment(loader=FileSystemLoader(str(TEMPLATES_DIR)))
    source = env.get_template("runtime_lib.py.j2").render()
    module = types.ModuleType("runtime_lib_render")
    exec(compile(source, "runtime_lib.py.j2", "exec"), module.__dict__)
    return module


@pytest.fixture(scope="module")
def runtime_lib():
    return _render_runtime_lib()


@pytest.fixture(scope="module")
def spark():
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    from pyspark.sql import SparkSession

    session = SparkSession.builder.master("local[2]").appName(
        "dynamic_lookup_test"
    ).config("spark.ui.enabled", "false").getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def _sample_cfg(**overrides):
    cfg = {
        "name": "TEST_LKP",
        "join_predicates": [{"source_col": "IN_KEY", "lookup_col": "KEY"}],
        "output_columns": ["SEQ", "VAL"],
        "lookup_output_fields": [
            {
                "name": "SEQ",
                "ref_field": "Sequence-Id",
                "ignore_in_compare": False,
                "ignore_null_inputs": False,
                "datatype": "integer",
            },
            {
                "name": "VAL",
                "ref_field": "IN_VAL",
                "ignore_in_compare": False,
                "ignore_null_inputs": False,
                "datatype": "string",
            },
        ],
        "new_lookup_row_col": "NewLookupRow",
        "sequence_config": {"output_col": "SEQ"},
        "insert_else_update": True,
        "update_else_insert": False,
        "update_condition": "TRUE",
        "output_old_value_on_update": False,
        "case_sensitive_string_comparison": False,
        "lookup_policy": "Report Error",
        "order_by_columns": [],
        "_input_columns": ["IN_KEY", "IN_VAL"],
    }
    cfg.update(overrides)
    return cfg


def _row(key, val, seq, base_exists=0, seq_key=None, seq_no=0):
    return {
        "IN_KEY": key,
        "IN_VAL": val,
        "__lkp_KEY": key if base_exists else None,
        "__lkp_VAL": val if base_exists else None,
        "__lkp_SEQ": seq if base_exists else None,
        "__base_exists": base_exists,
        "__dyn_seq": seq_no,
        "__dyn_seq_key": seq_key,
    }


def test_parser_preserves_ignore_compare_and_ignore_null_inputs():
    parser = InfaXMLParser(NHS_TL_XML.read_bytes())
    assert parser.parse()
    mapplets = parser.get_mapplets()
    transforms = {
        t.name: t
        for t in mapplets["MPLT_AGMT_NHS_REF_CODE_TYPE"]["transformations"]
    }
    lkp = transforms["LKP_DYN_SOR_NHS_REF_CODE_TYPE"]
    by_name = {f.name: f for f in lkp.fields}
    assert by_name["REF_CODE_TYPE_KEY"].ignore_in_compare == "NO"
    assert by_name["REF_CODE_TYPE_KEY"].ignore_null_inputs == "NO"
    assert by_name["NHS_REF_CODE_TYPE_CODE"].ignore_in_compare == "YES"
    assert by_name["NHS_REF_CODE_TYPE_CODE"].ignore_null_inputs == "NO"


def test_nhs_dynamic_lookup_ir_carries_full_config():
    mappings = _load_nhs_mappings()
    wanted = {
        "M_NHS_SSAL2_TRAN_NHS_DUP_CHK_RSLT",
        "M_NHS_SSAL2_TRAN_NHS_PHASE_ASP",
        "M_NHS_SSAL2_TRAN_NHS_EST_BANK_ITEM",
    }
    checked = 0
    for mapping in mappings:
        if mapping.name not in wanted:
            continue
        handlers = TransformHandlers(mapping, UserConfig())
        plan = handlers.build_ir_plan()
        dyn_steps = [
            step
            for step in plan.steps
            if isinstance(step, ApplyLookupStep)
            and step.params.get("dynamic_lookup")
        ]
        assert dyn_steps, mapping.name
        used_cols = {}
        for step in dyn_steps:
            cfg = step.params["dynamic_lookup"]
            assert cfg["lookup_policy"] == "Report Error"
            assert cfg["join_predicates"]
            assert cfg["new_lookup_row_col"]
            if step.step_name.startswith("apply_MPLT_"):
                assert cfg["new_lookup_row_col"] not in used_cols, (
                    f"{mapping.name}: duplicate mapplet NewLookupRow column "
                    f"{cfg['new_lookup_row_col']}"
                )
                used_cols[cfg["new_lookup_row_col"]] = step.step_name
            assert set(cfg["output_columns"]) == {
                f["name"] for f in cfg["lookup_output_fields"]
            }
            for field in cfg["lookup_output_fields"]:
                assert isinstance(field["ignore_in_compare"], bool)
                assert isinstance(field["ignore_null_inputs"], bool)
            if any(
                f["ref_field"] == "Sequence-Id"
                for f in cfg["lookup_output_fields"]
            ):
                assert cfg["sequence_config"] is not None
            checked += 1
    assert checked >= 15, f"expected many dynamic steps, got {checked}"


def test_state_machine_insert_then_update_same_key(runtime_lib):
    rows = [
        _row("1", "a", seq=0, seq_key=10, seq_no=0),
        _row("1", "b", seq=0, seq_key=None, seq_no=1),
    ]
    result = runtime_lib._process_dynamic_lookup_rows(rows, _sample_cfg())
    assert [(r["IN_KEY"], r["IN_VAL"], r["SEQ"], r["NewLookupRow"]) for r in result] == [
        ("1", "a", 10, 1),
        ("1", "b", 10, 2),
    ]

    second = runtime_lib._process_dynamic_lookup_rows(
        [_row("2", "a", seq=0, seq_key=11, seq_no=0)], _sample_cfg())
    assert (second[0]["SEQ"], second[0]["NewLookupRow"]) == (11, 1)


def test_state_machine_insert_else_update_no(runtime_lib):
    cfg = _sample_cfg(insert_else_update=False)
    rows = [
        _row("1", "a", seq=0, seq_key=10, seq_no=0),
        _row("1", "b", seq=0, seq_key=None, seq_no=1),
    ]
    result = runtime_lib._process_dynamic_lookup_rows(rows, cfg)
    assert result[1]["NewLookupRow"] == 0
    assert result[1]["VAL"] == "a"


def test_state_machine_output_old_value_on_update(runtime_lib):
    cfg = _sample_cfg(output_old_value_on_update=True)
    rows = [
        _row("1", "a", seq=0, seq_key=10, seq_no=0),
        _row("1", "b", seq=0, seq_key=None, seq_no=1),
    ]
    result = runtime_lib._process_dynamic_lookup_rows(rows, cfg)
    assert result[1]["NewLookupRow"] == 2
    assert result[1]["VAL"] == "a"  # old value on output, cache still updated


def test_state_machine_base_hit_no_change(runtime_lib):
    rows = [
        _row("1", "x", seq=5, base_exists=1, seq_key=None, seq_no=0),
    ]
    result = runtime_lib._process_dynamic_lookup_rows(rows, _sample_cfg())
    assert result[0]["NewLookupRow"] == 0
    assert result[0]["SEQ"] == 5
    assert result[0]["VAL"] == "x"


def test_state_machine_ignore_null_inputs_keeps_old_value(runtime_lib):
    fields = list(_sample_cfg()["lookup_output_fields"])
    fields[1]["ignore_null_inputs"] = True
    cfg = _sample_cfg(lookup_output_fields=fields)
    rows = [
        _row("1", "x", seq=5, base_exists=1, seq_key=None, seq_no=0),
        _row("1", None, seq=5, base_exists=1, seq_key=None, seq_no=1),
    ]
    result = runtime_lib._process_dynamic_lookup_rows(rows, cfg)
    assert result[1]["NewLookupRow"] == 0
    assert result[1]["VAL"] == "x"


def test_state_machine_ignore_in_compare_does_not_trigger_update(runtime_lib):
    fields = list(_sample_cfg()["lookup_output_fields"])
    fields[1]["ignore_in_compare"] = True
    cfg = _sample_cfg(lookup_output_fields=fields)
    rows = [
        _row("1", "x", seq=5, base_exists=1, seq_key=None, seq_no=0),
        _row("1", "y", seq=5, base_exists=1, seq_key=None, seq_no=1),
    ]
    result = runtime_lib._process_dynamic_lookup_rows(rows, cfg)
    assert result[1]["NewLookupRow"] == 0
    assert result[1]["VAL"] == "x"


def test_state_machine_case_sensitive_string_comparison(runtime_lib):
    rows = [
        _row("1", "A", seq=5, base_exists=1, seq_key=None, seq_no=0),
        _row("1", "a", seq=5, base_exists=1, seq_key=None, seq_no=1),
    ]
    insensitive = runtime_lib._process_dynamic_lookup_rows(
        rows, _sample_cfg(case_sensitive_string_comparison=False))
    assert insensitive[1]["NewLookupRow"] == 0

    sensitive = runtime_lib._process_dynamic_lookup_rows(
        rows, _sample_cfg(case_sensitive_string_comparison=True))
    assert sensitive[1]["NewLookupRow"] == 2


def test_spark_apply_in_pandas_insert_update_and_null_key(runtime_lib, spark):
    from pyspark.sql.types import LongType, StringType, StructField, StructType

    input_df = spark.createDataFrame(
        [("1", "a"), ("1", "b"), ("2", "a"), (None, "z")],
        ["IN_KEY", "IN_VAL"],
    )
    lookup_df = spark.createDataFrame(
        [],
        StructType([
            StructField("KEY", StringType(), True),
            StructField("VAL", StringType(), True),
            StructField("SEQ", LongType(), True),
        ]),
    )
    cfg = _sample_cfg()
    cfg.pop("_input_columns", None)
    out = runtime_lib.dynamic_lookup(
        spark, input_df, lookup_df, cfg,
        config={"dynamic_lookup": {"executor": "apply_in_pandas"}},
    )
    rows = out.orderBy("IN_KEY", "IN_VAL").collect()
    assert [(r.IN_KEY, r.IN_VAL, r.SEQ, r.NewLookupRow) for r in rows] == [
        (None, "z", 3, 1),
        ("1", "a", 1, 1),
        ("1", "b", 1, 2),
        ("2", "a", 2, 1),
    ]


def test_spark_rdd_fallback_matches_apply_in_pandas(runtime_lib, spark):
    from pyspark.sql.types import LongType, StringType, StructField, StructType

    input_df = spark.createDataFrame(
        [("1", "a"), ("1", "b")], ["IN_KEY", "IN_VAL"]
    )
    lookup_df = spark.createDataFrame(
        [],
        StructType([
            StructField("KEY", StringType(), True),
            StructField("VAL", StringType(), True),
            StructField("SEQ", LongType(), True),
        ]),
    )
    cfg = _sample_cfg()
    cfg.pop("_input_columns", None)
    out = runtime_lib.dynamic_lookup(
        spark, input_df, lookup_df, cfg,
        config={"dynamic_lookup": {"executor": "rdd"}},
    )
    rows = out.orderBy("IN_VAL").collect()
    assert [(r.IN_VAL, r.SEQ, r.NewLookupRow) for r in rows] == [
        ("a", 1, 1),
        ("b", 1, 2),
    ]


def test_spark_base_duplicate_keys_raise_report_error(runtime_lib, spark):
    from pyspark.sql.types import LongType, StringType, StructField, StructType

    input_df = spark.createDataFrame([("1", "a")], ["IN_KEY", "IN_VAL"])
    lookup_df = spark.createDataFrame(
        [("1", "x", 5), ("1", "y", 6)],
        StructType([
            StructField("KEY", StringType(), True),
            StructField("VAL", StringType(), True),
            StructField("SEQ", LongType(), True),
        ]),
    )
    cfg = _sample_cfg()
    cfg.pop("_input_columns", None)
    with pytest.raises(RuntimeError, match="duplicate keys"):
        runtime_lib.dynamic_lookup(spark, input_df, lookup_df, cfg)


def test_generated_nhs_uses_dynamic_lookup_helper():
    files = sorted(NHS_OUTPUT_DIR.glob("m_nhs_ssal2_tran_nhs_*.py"))
    assert files, "NHS TL generated files not found"
    checked = 0
    old_approx = re.compile(
        r'withColumn\("[Nn]ew[Ll]ookup[Rr]ow[^"]*", '
        r'expr\("CASE WHEN'
    )
    for path in files:
        text = path.read_text(encoding="utf-8")
        if "lib.dynamic_lookup(" not in text:
            continue
        assert old_approx.search(text) is None, (
            f"{path.name}: legacy CASE WHEN NewLookupRow approximation still "
            "generated"
        )
        compile(text, str(path), "exec")
        checked += 1
    assert checked >= 40, f"expected many generated dynamic lookups, got {checked}"

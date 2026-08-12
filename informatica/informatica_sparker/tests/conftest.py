"""Shared fixtures for runtime-lib component-method tests.

Renders runtime_lib.py.j2 via Jinja2 and imports it as a module, so the
tests cover the exact code deployed into every workflow env.
"""

import os
import sys
import types
from pathlib import Path

import pytest
from jinja2 import Environment, FileSystemLoader

ROOT = Path(__file__).resolve().parents[1]
TEMPLATES_DIR = ROOT / "informatica_sparker" / "templates"


def _render_runtime_lib():
    try:
        import pyspark  # noqa: F401
    except ImportError:
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
        "component_methods_test"
    ).config("spark.ui.enabled", "false").getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()

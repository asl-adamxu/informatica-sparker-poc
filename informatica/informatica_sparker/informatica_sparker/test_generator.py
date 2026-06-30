"""E2E test artifact generator for informatica-sparker.

Discovers all tables from a workflow XML (5 sources), generates schema DDL,
reference data INSERTs, and pytest-based test scripts that run the converted
wf_*.py / m_*.py files against a real Oracle database.
"""

from __future__ import annotations
import os
import re
import textwrap
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Set
from .models import (
    MappingDefinition, SourceDefinition, TargetDefinition,
    SourceField, TargetField, Transformation,
)


@dataclass
class FieldDef:
    """Unified field definition (merged from SOURCEFIELD and TARGETFIELD)."""
    name: str
    datatype: str          # normalized Oracle type: "NUMBER", "VARCHAR2", "DATE"
    precision: int = 0
    scale: int = 0
    nullable: bool = True
    is_pk: bool = False


@dataclass
class TableDef:
    """Unified table definition with origin tracking."""
    schema_name: str       # e.g. "PDDS", "PSOR", "PDPA"
    table_name: str        # e.g. "DDS_FACT_GMS_DLY_MSD_SMRY"
    fields: List[FieldDef] = field(default_factory=list)
    origin: str = "unknown"   # "source_def", "target_def", "sql_inferred", "lookup_name"
    is_reference: bool = False  # True if dimension/hierarchy/code table -- gets pre-filled INSERTs

    @property
    def full_name(self) -> str:
        return f"{self.schema_name}.{self.table_name}" if self.schema_name else self.table_name

    @property
    def key(self) -> str:
        if self.schema_name:
            return f"{self.schema_name}.{self.table_name}".upper()
        return self.table_name.upper()


@dataclass
class TableRef:
    """Lightweight table reference from SQL parsing (before field resolution)."""
    schema_name: str
    table_name: str

    @property
    def key(self) -> str:
        if self.schema_name:
            return f"{self.schema_name}.{self.table_name}".upper()
        return self.table_name.upper()


# ── Type Mapping ──────────────────────────────────────────────────────────────

INFA_TO_ORACLE_TYPE = {
    "number":      "NUMBER",
    "decimal":     "NUMBER",
    "numeric":     "NUMBER",
    "float":       "FLOAT",
    "double":      "FLOAT",
    "real":        "FLOAT",
    "integer":     "INTEGER",
    "smallint":    "SMALLINT",
    "bigint":      "NUMBER(19,0)",
    "varchar":     "VARCHAR2",
    "varchar2":    "VARCHAR2",
    "char":        "CHAR",
    "nchar":       "CHAR",
    "nvarchar2":   "VARCHAR2",
    "date":        "DATE",
    "timestamp":   "TIMESTAMP",
    "datetime":    "DATE",
    "string":      "VARCHAR2",    # flat file source type
    "text":        "CLOB",
    "blob":        "BLOB",
    "clob":        "CLOB",
}


def infa_to_oracle_type(datatype: str, precision: int = 0, scale: int = 0) -> str:
    """Convert an Informatica field type to an Oracle SQL column type."""
    raw_key = datatype.lower().strip()
    base = INFA_TO_ORACLE_TYPE.get(raw_key, "VARCHAR2")

    # Some dict values are fully-qualified (e.g. "NUMBER(19,0)") -- return as-is
    if "(" in base:
        return base

    if base == "NUMBER":
        if precision > 0 and scale > 0:
            return f"NUMBER({precision},{scale})"
        elif precision > 0:
            return f"NUMBER({precision},0)"
        return "NUMBER"
    elif base in ("VARCHAR2", "CHAR"):
        p = precision if precision > 0 else 255
        return f"{base}({p})"
    elif base == "FLOAT":
        return "FLOAT"
    elif base in ("INTEGER", "SMALLINT"):
        return base
    elif base in ("DATE", "TIMESTAMP", "CLOB", "BLOB"):
        return base
    return "VARCHAR2(255)"

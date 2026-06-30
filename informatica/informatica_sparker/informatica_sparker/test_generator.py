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

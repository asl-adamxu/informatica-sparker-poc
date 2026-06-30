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


# ── SQL Table Extractor ─────────────────────────────────────────────────────────

# Simple regex-based Oracle SQL FROM-clause extractor.
# Handles: FROM table, FROM table alias, JOIN table, JOIN table alias,
#          FROM schema.table, schema.table alias, comma-separated tables,
#          subqueries (skipped), CTE with clause (skipped).
# Limitations: does not fully parse recursive CTEs or PIVOT/UNPIVOT.

_SQL_FROM_PATTERN = re.compile(
    r'(?:FROM|JOIN)\s+'
    r'(?:\(?\s*)([A-Za-z_][A-Za-z0-9_$#]*(?:\.[A-Za-z_][A-Za-z0-9_$#]*)?)'
    r'(?:\s+(?:AS\s+)?[A-Za-z_][A-Za-z0-9_$#]*)?'
    r'(?:\s*[,)]?\s*)',
    re.IGNORECASE
)

# Pattern for comma-separated tables after FROM (no JOIN keyword)
_SQL_COMMA_TABLE = re.compile(
    r'(?:^|,\s*)([A-Za-z_][A-Za-z0-9_$#]*(?:\.[A-Za-z_][A-Za-z0-9_$#]*)?)'
    r'(?:\s+(?:AS\s+)?[A-Za-z_][A-Za-z0-9_$#]*)?',
    re.MULTILINE
)


def _strip_sql_comments(sql: str) -> str:
    """Remove SQL comments (single-line and multi-line)."""
    sql = re.sub(r'--[^\n]*', '', sql)
    sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)
    return sql


def _normalize_sql(sql: str) -> str:
    """Normalize SQL for parsing: lowercase, collapse whitespace."""
    sql = _strip_sql_comments(sql)
    sql = re.sub(r'\s+', ' ', sql).strip()
    return sql


_FROM_CLAUSE_TERMINATORS = r'\b(?:WHERE|GROUP\s+BY|ORDER\s+BY|HAVING|START\s+WITH|CONNECT\s+BY|UNION|MINUS|INTERSECT|INTO|FOR\s+UPDATE)\b'


def _find_from_portion(sql: str, from_match: re.Match) -> str:
    """Extract the text between FROM keyword and the next clause-terminating keyword."""
    rest = sql[from_match.end():]
    end = re.search(_FROM_CLAUSE_TERMINATORS, rest, re.IGNORECASE)
    if end:
        return rest[:end.start()]
    return rest


def _find_main_select_pos(sql: str, cte_names: set) -> int:
    """Find the position of the main SELECT outside CTE definitions."""
    depth = 0
    for i, ch in enumerate(sql):
        if ch == '(':
            depth += 1
        elif ch == ')':
            depth -= 1
        elif depth == 0 and sql[i:i+6].upper() == 'SELECT':
            # Word boundary check: ensure 'SELECT' is not part of a larger identifier
            if i > 0 and sql[i-1].isalnum():
                continue
            if i+6 < len(sql) and sql[i+6].isalnum():
                continue
            return i
    return 0


def _extract_cte_names(sql: str) -> Set[str]:
    """Extract CTE names from a WITH clause using paren-depth tracking."""
    cte_names: Set[str] = set()
    if not re.match(r'^\s*WITH\s+', sql, re.I):
        return cte_names
    main_pos = _find_main_select_pos(sql[4:], set())
    if main_pos < 0:
        return cte_names
    with_portion = sql[4:4 + main_pos]
    for m in re.finditer(r'(\w+)\s+AS\s*\(', with_portion):
        cte_names.add(m.group(1).upper())
    return cte_names


def _extract_tables_from_cte_body(sql: str, cte_names: Set[str]) -> List[TableRef]:
    """Extract tables referenced inside WITH clause CTE definitions.

    Applies the FROM/JOIN pattern directly on the CTE body text
    (parens are not stripped -- the pattern can match inside them).
    CTE names themselves are filtered out.
    """
    main_pos = _find_main_select_pos(sql[4:], cte_names)
    if main_pos < 0:
        return []
    body = sql[4:4 + main_pos]  # everything between WITH and the main SELECT

    tables: List[TableRef] = []
    seen: Set[str] = set()
    for m in _SQL_FROM_PATTERN.finditer(body):
        raw = m.group(1).strip()
        if '.' in raw:
            parts = raw.split('.', 1)
            schema, table = parts[0].upper(), parts[1].upper()
        else:
            schema, table = '', raw.upper()
        if table in ('DUAL', 'SYSTEM', 'SELECT', 'WHERE', 'AND', 'OR', 'ON', 'AS', 'NULL'):
            continue
        if table in cte_names:
            continue
        key = f"{schema}.{table}"
        if key not in seen:
            seen.add(key)
            tables.append(TableRef(schema_name=schema, table_name=table))
    return tables


def _remove_cte_block(sql: str) -> str:
    """Remove the entire WITH clause, returning the main SELECT query."""
    if not re.match(r'^\s*WITH\s+', sql, re.I):
        return sql
    main_pos = _find_main_select_pos(sql[4:], set())
    if main_pos < 0:
        return sql
    return sql[4 + main_pos:]


def extract_tables_from_sql(sql_text: str) -> List[TableRef]:
    """Extract table references from an Oracle SQL query.

    Returns a list of TableRef (schema.table or just table).
    Skips subquery aliases and CTE names.
    """
    if not sql_text or not sql_text.strip():
        return []

    sql = _normalize_sql(sql_text)

    # Extract CTE names before removal so we can filter them out later
    cte_names = _extract_cte_names(sql)

    # Extract tables from WITH clause CTE bodies before removal
    tables: List[TableRef] = []
    seen: Set[str] = set()
    if cte_names:
        cte_tables = _extract_tables_from_cte_body(sql, cte_names)
        for t in cte_tables:
            key = t.key
            if key not in seen:
                seen.add(key)
                tables.append(t)

    # Remove the CTE block to avoid double-counting or mis-parsing
    sql = _remove_cte_block(sql)

    # Remove parenthesized subqueries first (replace with empty)
    # This prevents FROM clauses inside subqueries from being matched at outer level
    while True:
        simplified = re.sub(r'\([^()]*\)', '()', sql)
        if simplified == sql:
            break
        sql = simplified
    # Now remove actual parens left
    sql = sql.replace('()', ' ')

    # Extract FROM table / JOIN table patterns
    for m in _SQL_FROM_PATTERN.finditer(sql):
        raw = m.group(1).strip()
        if '.' in raw:
            parts = raw.split('.', 1)
            schema, table = parts[0].upper(), parts[1].upper()
        else:
            schema, table = '', raw.upper()

        # Skip common non-table references
        if table in ('DUAL', 'SYSTEM', 'SELECT', 'WHERE', 'AND', 'OR', 'ON', 'AS', 'NULL'):
            continue
        # Skip subquery aliases (single words after FROM that aren't tables)
        if not schema and table in ('SELECT', 'WITH', 'FROM', 'WHERE'):
            continue

        ref = TableRef(schema_name=schema, table_name=table)
        if ref.key not in seen:
            seen.add(ref.key)
            tables.append(ref)

    # Extract comma-separated tables from FROM clauses
    for m in re.finditer(r'\bFROM\s+', sql, re.I):
        from_portion = _find_from_portion(sql, m)
        for cm in _SQL_COMMA_TABLE.finditer(from_portion):
            raw = cm.group(1).strip()
            if '.' in raw:
                parts = raw.split('.', 1)
                schema, table = parts[0].upper(), parts[1].upper()
            else:
                schema, table = '', raw.upper()
            # Skip common non-table references
            if table in ('DUAL', 'SYSTEM', 'SELECT', 'WHERE', 'AND', 'OR', 'ON', 'AS', 'NULL'):
                continue
            ref = TableRef(schema_name=schema, table_name=table)
            if ref.key not in seen:
                seen.add(ref.key)
                tables.append(ref)

    # Filter out CTE names — they are not real database tables
    if cte_names:
        tables = [t for t in tables if t.table_name not in cte_names]

    return tables

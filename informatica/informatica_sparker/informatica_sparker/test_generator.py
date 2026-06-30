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


def _find_main_select_pos(sql: str) -> int:
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
    return -1


def _extract_cte_names(sql: str) -> Set[str]:
    """Extract CTE names from a WITH clause using paren-depth tracking."""
    cte_names: Set[str] = set()
    if not re.match(r'^\s*WITH\s+', sql, re.I):
        return cte_names
    main_pos = _find_main_select_pos(sql[4:])
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
    main_pos = _find_main_select_pos(sql[4:])
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
    main_pos = _find_main_select_pos(sql[4:])
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


# ── Table Discoverer ──────────────────────────────────────────────────────────

REFERENCE_TABLE_PATTERNS = [
    re.compile(r'DDS_DMNS_', re.I),
    re.compile(r'_DMNS_', re.I),
    re.compile(r'_HRCHY_', re.I),
    re.compile(r'_REF_', re.I),
    re.compile(r'SOR_SYS_', re.I),
]


def _is_reference_table(table_name: str) -> bool:
    """Heuristic: dimension/hierarchy/reference/code tables get pre-filled data."""
    return any(p.search(table_name) for p in REFERENCE_TABLE_PATTERNS)


def _build_field_def(field, is_target: bool = False) -> FieldDef:
    """Build a FieldDef from either SourceField or TargetField."""
    # Handle both SourceField and TargetField (they share the same attributes)
    return FieldDef(
        name=field.name,
        datatype=field.datatype,
        precision=getattr(field, 'precision', 0),
        scale=getattr(field, 'scale', 0),
        nullable=getattr(field, 'nullable', True),
        is_pk=getattr(field, 'key_type', 'NOT A KEY') == 'PRIMARY KEY',
    )


class TableDiscoverer:
    """Discovers all unique tables from 5 XML sources with priority resolution."""

    # Priority: 1=SOURCE/TARGET definition (highest), 2=SQL inferred, 3=lookup name only
    TABLE_SOURCE_PRIORITY = {
        "source_def": 1,
        "target_def": 1,
        "sql_inferred": 2,
        "lookup_name": 3,
    }

    def __init__(self, mappings: List[MappingDefinition]):
        self.mappings = mappings

    def discover_all(self) -> Dict[str, TableDef]:
        """Collect all unique tables from all 5 sources.

        Returns dict keyed by TableDef.key (uppercase SCHEMA.TABLE).
        """
        tables: Dict[str, TableDef] = {}

        for mapping in self.mappings:
            self._collect_from_sources(mapping, tables)
            self._collect_from_targets(mapping, tables)
            self._collect_from_transformations(mapping, tables)

        # Mark reference tables
        for key, tdef in tables.items():
            if _is_reference_table(tdef.table_name):
                tdef.is_reference = True

        return tables

    def _collect_from_sources(self, mapping: MappingDefinition,
                               tables: Dict[str, TableDef]):
        """Source 1: SOURCE definitions with full field list."""
        for src in mapping.sources:
            key = self._make_table_key(src.owner_name, src.name)
            if self._should_override(tables.get(key), "source_def"):
                fields = [_build_field_def(f) for f in src.fields]
                tables[key] = TableDef(
                    schema_name=src.owner_name.upper() if src.owner_name else '',
                    table_name=src.name.upper(),
                    fields=fields,
                    origin="source_def",
                )

    def _collect_from_targets(self, mapping: MappingDefinition,
                               tables: Dict[str, TableDef]):
        """Source 2: TARGET definitions with full field list."""
        for tgt in mapping.targets:
            key = self._make_table_key('', tgt.name)
            if self._should_override(tables.get(key), "target_def"):
                fields = [_build_field_def(f, is_target=True) for f in tgt.fields]
                tables[key] = TableDef(
                    schema_name='',
                    table_name=tgt.name.upper(),
                    fields=fields,
                    origin="target_def",
                )

    def _collect_from_transformations(self, mapping: MappingDefinition,
                                       tables: Dict[str, TableDef]):
        """Sources 3-5: Source Qualifier SQL, Lookup SQL Override, Lookup table name."""
        for transform in mapping.transformations:
            attrs = transform.table_attributes

            if transform.type == "Source Qualifier":
                sql = attrs.get("Sql Query", "") or attrs.get("sql_query", "")
                if sql.strip():
                    refs = extract_tables_from_sql(sql)
                    for ref in refs:
                        key = ref.key
                        if self._should_override(tables.get(key), "sql_inferred"):
                            tables[key] = TableDef(
                                schema_name=ref.schema_name,
                                table_name=ref.table_name,
                                origin="sql_inferred",
                            )

            elif transform.type == "Lookup Procedure":
                # Source 4: Lookup Sql Override
                sql_override = attrs.get("Lookup Sql Override", "")
                if sql_override.strip():
                    refs = extract_tables_from_sql(sql_override)
                    for ref in refs:
                        key = ref.key
                        if self._should_override(tables.get(key), "sql_inferred"):
                            tables[key] = TableDef(
                                schema_name=ref.schema_name,
                                table_name=ref.table_name,
                                origin="sql_inferred",
                            )

                # Source 5: Lookup table name (when no SQL Override)
                if not sql_override.strip():
                    table_name = attrs.get("Lookup table name", "")
                    if table_name.strip():
                        # Determine schema from connection info
                        conn_info = attrs.get("Connection Information", "")
                        schema = self._resolve_lookup_schema(conn_info, tables)
                        key = self._make_table_key(schema, table_name)
                        if self._should_override(tables.get(key), "lookup_name"):
                            tables[key] = TableDef(
                                schema_name=schema,
                                table_name=table_name.upper(),
                                origin="lookup_name",
                            )

    def _should_override(self, existing: Optional[TableDef],
                         new_origin: str) -> bool:
        """Priority-based override: SOURCE/TARGET always wins over SQL/lookup."""
        if existing is None:
            return True
        existing_prio = self.TABLE_SOURCE_PRIORITY.get(existing.origin, 99)
        new_prio = self.TABLE_SOURCE_PRIORITY.get(new_origin, 99)
        # Equal priority: merge fields if existing has none
        if existing_prio == new_prio:
            return not existing.fields
        return new_prio < existing_prio

    def _resolve_lookup_schema(self, conn_info: str,
                                existing_tables: Dict[str, TableDef]) -> str:
        """Resolve schema for Lookup table name from connection info."""
        ci_upper = conn_info.upper()
        if '$TARGET' in ci_upper or '$SOURCE' in ci_upper:
            # Try to find a schema from existing tables
            for tdef in existing_tables.values():
                if tdef.schema_name and tdef.origin in ("source_def", "target_def"):
                    return tdef.schema_name
        return ''

    @staticmethod
    def _make_table_key(schema: str, table_name: str) -> str:
        s = schema.upper().strip() if schema else ''
        t = table_name.upper().strip() if table_name else ''
        return f"{s}.{t}" if s else t


# ── Schema Renderer ───────────────────────────────────────────────────────────

class SchemaRenderer:
    """Generates CREATE TABLE and DROP TABLE SQL from TableDef collection."""

    def __init__(self, tables: Dict[str, TableDef]):
        self.tables = tables

    def render_create_all(self) -> str:
        """Generate CREATE TABLE statements in dependency-safe order."""
        ordered = self._topological_sort()
        lines = [
            "-- =============================================================================",
            "-- TEST SCHEMA: CREATE ALL TABLES",
            f"-- Auto-generated by informatica-sparker on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "-- =============================================================================",
            "",
        ]

        current_layer = ""
        for tdef in ordered:
            # Determine layer comment
            layer = self._classify_layer(tdef)
            if layer != current_layer:
                current_layer = layer
                lines.append("")
                lines.append(f"-- ========================================================")
                lines.append(f"-- {layer}")
                lines.append(f"-- ========================================================")
                lines.append("")

            lines.append(self._render_create_one(tdef))
            lines.append("")

        return "\n".join(lines)

    def render_drop_all(self) -> str:
        """Generate DROP TABLE statements (reverse dependency order)."""
        ordered = self._topological_sort()
        lines = [
            "-- =============================================================================",
            "-- TEST SCHEMA: DROP ALL TABLES",
            f"-- Auto-generated by informatica-sparker on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "-- =============================================================================",
            "",
        ]
        for tdef in reversed(ordered):
            if tdef.schema_name:
                lines.append(f"DROP TABLE {tdef.full_name} PURGE;")
            else:
                lines.append(f"DROP TABLE {tdef.table_name} PURGE;")
            lines.append("")
        return "\n".join(lines)

    def render_truncate_all(self) -> str:
        """Generate TRUNCATE TABLE statements (reverse dependency order)."""
        ordered = self._topological_sort()
        lines = [
            "-- =============================================================================",
            "-- TEST SCHEMA: TRUNCATE ALL TABLES",
            "-- =============================================================================",
            "",
        ]
        for tdef in reversed(ordered):
            lines.append(f"TRUNCATE TABLE {tdef.full_name};")
        lines.append("")
        return "\n".join(lines)

    def _render_create_one(self, tdef: TableDef) -> str:
        """Generate a single CREATE TABLE statement."""
        if not tdef.fields:
            return (
                f"-- ⚠️ Table {tdef.full_name} has no field definitions.\n"
                f"-- Origin: {tdef.origin}. Add DDL manually.\n"
                f"-- CREATE TABLE {tdef.full_name} ( ... );"
            )

        col_lines = []
        pk_cols = []

        for f in tdef.fields:
            oracle_type = infa_to_oracle_type(f.datatype, f.precision, f.scale)
            nullable_str = "NOT NULL" if not f.nullable else ""
            col_lines.append(f"    {f.name} {oracle_type}{' ' + nullable_str if nullable_str else ''}".rstrip())
            if f.is_pk:
                pk_cols.append(f.name)

        # Add primary key constraint
        if pk_cols:
            pk_str = ", ".join(pk_cols)
            col_lines.append(f"    CONSTRAINT PK_{tdef.table_name} PRIMARY KEY ({pk_str})")

        cols_sql = ",\n".join(col_lines)
        return f"CREATE TABLE {tdef.full_name} (\n{cols_sql}\n);"

    def _classify_layer(self, tdef: TableDef) -> str:
        """Classify table into a layer for section headers."""
        upper_name = tdef.table_name.upper()
        if upper_name.startswith("DDS_DMNS") or upper_name.startswith("DDS_HRCHY"):
            return "DDS Layer — Dimension / Hierarchy"
        elif upper_name.startswith("DDS_FACT"):
            return "DDS Layer — Fact Tables"
        elif upper_name.startswith("DPA_"):
            return "DPA Layer — Data Processing Area"
        elif upper_name.startswith("SOR_") or tdef.schema_name.upper() in ("PSOR", "SOR"):
            return "SOR Layer — Source of Record"
        elif upper_name.startswith("UTL_"):
            return "UTL Layer — Utility / File-Based"
        else:
            return "Other Tables"

    def _topological_sort(self) -> List[TableDef]:
        """Sort tables so that referenced tables come before referencing tables.

        TODO (Phase 2): Use XML REFERENCEDFIELD/REFERENCEDTABLE for authoritative FK inference.
        Current implementation uses _KEY naming pattern as fallback.
        """
        all_tables = list(self.tables.values())
        # Build dependency graph: table_key -> set of dependency keys
        deps: Dict[str, Set[str]] = {}
        pk_map: Dict[str, Set[str]] = {}  # table_key -> set of PK column names

        for tdef in all_tables:
            key = tdef.key
            deps.setdefault(key, set())
            pk_map[key] = {f.name for f in tdef.fields if f.is_pk}

        # Infer FK: if a table has a *_KEY column that is a PK in another table
        pk_col_names: Dict[str, str] = {}  # pk_column_name -> table_key that owns it
        for tdef in all_tables:
            for f in tdef.fields:
                if f.is_pk:
                    pk_col_names[f.name] = tdef.key

        for tdef in all_tables:
            key = tdef.key
            for f in tdef.fields:
                if f.name.endswith("_KEY") and f.name in pk_col_names and pk_col_names[f.name] != key:
                    deps[key].add(pk_col_names[f.name])

        # Topological sort (simple Kahn's algorithm)
        in_degree: Dict[str, int] = {k: 0 for k in deps}
        for key, dep_set in deps.items():
            for dep in dep_set:
                if dep in in_degree:
                    in_degree[key] = in_degree.get(key, 0) + 1

        queue = [k for k, v in in_degree.items() if v == 0]
        sorted_keys = []
        while queue:
            node = queue.pop(0)
            sorted_keys.append(node)
            for other_key, other_deps in deps.items():
                if node in other_deps:
                    in_degree[other_key] -= 1
                    if in_degree[other_key] == 0:
                        queue.append(other_key)

        # Append any remaining (cyclic) nodes
        remaining = [t.key for t in all_tables if t.key not in sorted_keys]
        sorted_keys.extend(remaining)

        # Map back to TableDef objects preserving order
        key_to_tdef = {t.key: t for t in all_tables}
        result = []
        for k in sorted_keys:
            if k in key_to_tdef:
                result.append(key_to_tdef[k])
        return result

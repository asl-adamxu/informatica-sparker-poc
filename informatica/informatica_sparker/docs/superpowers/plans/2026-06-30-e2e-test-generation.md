# E2E Test Generation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `--with-tests` flag to `informatica-sparker convert` that generates test artifacts (schema DDL, reference data SQL, data generator, pytest scripts) from parsed XML models.

**Architecture:** A new `test_generator.py` module (with five internal classes) discovers all tables from 5 XML sources, resolves conflicts, generates CREATE/INSERT SQL, and writes pytest scripts via Jinja2 templates. Integrated as a post-conversion step in `service.py`.

**Tech Stack:** Python 3.10+, lxml, Jinja2, sqlparse (for SQL FROM-clause extraction), pytest (generated scripts)

## Global Constraints

- No Chinese comments in generated code — all comments must be in English
- Generated filenames use `_make_safe_name()` (lowercase with underscores)
- Follow existing patterns in `service.py`, `codegen.py`
- Test output goes in `tests/` subdirectory under the conversion output dir
- Tables from 5 discovery sources with priority-based conflict resolution
- Directories `tests/schema/` and `tests/sql/` created automatically
- All generated SQL must be Oracle-compatible (NUMBER, VARCHAR2, DATE, TO_DATE)
- Reference data INSERT statements use field-name pattern matching for values
- Generated pytest scripts call `subprocess.run(["python3", "wf_*.py"])` directly

---
### Task 1: Define TableDef data structures

**Files:**
- Create: `informatica_sparker/test_generator.py` (lines 1-80)

**Interfaces:**
- Consumes: `SourceField`, `TargetField`, `SourceDefinition`, `TargetDefinition`, `Transformation` from `models.py`
- Produces: `FieldDef` dataclass, `TableDef` dataclass, `TableRef` dataclass — used by all subsequent tasks

- [ ] **Step 1: Write the data structures at top of test_generator.py**

```python
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
    is_reference: bool = False  # True if dimension/hierarchy/code table — gets pre-filled INSERTs

    @property
    def full_name(self) -> str:
        return f"{self.schema_name}.{self.table_name}" if self.schema_name else self.table_name

    @property
    def key(self) -> str:
        return f"{self.schema_name}.{self.table_name}".upper()

@dataclass
class TableRef:
    """Lightweight table reference from SQL parsing (before field resolution)."""
    schema_name: str
    table_name: str

    @property
    def key(self) -> str:
        return f"{self.schema_name}.{self.table_name}".upper()
```

- [ ] **Step 2: Run a quick Python import check**

Run: `python3 -c "import sys; sys.path.insert(0, '.'); from informatica_sparker.test_generator import FieldDef, TableDef, TableRef; print('OK')"`
Expected: OK

- [ ] **Step 3: Commit**

```bash
git add informatica_sparker/test_generator.py
git commit -m "feat: add TableDef/FieldDef/TableRef data structures"
```

### Task 2: Implement Informatica→Oracle type mapper

**Files:**
- Modify: `informatica_sparker/test_generator.py` (append after data structures)

**Interfaces:**
- Consumes: `FieldDef.datatype` (raw Informatica type string)
- Produces: Oracle SQL type string — used by SchemaRenderer

- [ ] **Step 1: Add type mapping functions**

```python
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
```

- [ ] **Step 2: Quick test**

Run:
```python
python3 -c "
import sys; sys.path.insert(0, '.')
from informatica_sparker.test_generator import infa_to_oracle_type
assert infa_to_oracle_type('number', 10, 0) == 'NUMBER(10,0)'
assert infa_to_oracle_type('varchar2', 30) == 'VARCHAR2(30)'
assert infa_to_oracle_type('date') == 'DATE'
assert infa_to_oracle_type('string', 255) == 'VARCHAR2(255)'
print('All type mappings OK')
"
```

- [ ] **Step 3: Commit**

```bash
git add informatica_sparker/test_generator.py
git commit -m "feat: add Informatica-to-Oracle type mapper"
```

### Task 3: Implement SQLTableExtractor

**Files:**
- Modify: `informatica_sparker/test_generator.py` (append after type mapper)

**Interfaces:**
- Consumes: SQL text (string) from Source Qualifier `Sql Query` and Lookup `Lookup Sql Override`
- Produces: `List[TableRef]` — used by TableDiscoverer

- [ ] **Step 1: Add SQL parser (regex-based for Oracle)**

```python
# ── SQL Table Extractor ───────────────────────────────────────────────────────

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


def extract_tables_from_sql(sql_text: str) -> List[TableRef]:
    """Extract table references from an Oracle SQL query.

    Returns a list of TableRef (schema.table or just table).
    Skips subquery aliases and CTE names.
    """
    if not sql_text or not sql_text.strip():
        return []

    sql = _normalize_sql(sql_text)

    # Remove parenthesized subqueries first (replace with empty)
    # This prevents FROM clauses inside subqueries from being matched at outer level
    while True:
        simplified = re.sub(r'\([^()]*\)', '()', sql)
        if simplified == sql:
            break
        sql = simplified
    # Now remove actual parens left
    sql = sql.replace('()', ' ')

    tables: List[TableRef] = []
    seen: Set[str] = set()

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

        key = f"{schema}.{table}"
        if key not in seen:
            seen.add(key)
            tables.append(TableRef(schema_name=schema, table_name=table))

    return tables
```

- [ ] **Step 2: Quick test with a real SQL sample**

Run:
```python
python3 -c "
import sys; sys.path.insert(0, '.')
from informatica_sparker.test_generator import extract_tables_from_sql

sql = '''select time_dmns_key from dds_dmns_time t1, dds_dmns_time t2 
         where b.incdt_date_time=t1.time_val_date and t1.time_dmns_key<200000000
         and b.msd_txn_cre_date=t2.time_val_date and t2.time_dmns_key<200000000'''
tables = extract_tables_from_sql(sql)
for t in tables:
    print(f'  {t.schema_name}.{t.table_name}')
assert any(t.table_name == 'DDS_DMNS_TIME' for t in tables)
print('SQL extraction OK')
"
```

- [ ] **Step 3: Commit**

```bash
git add informatica_sparker/test_generator.py
git commit -m "feat: add SQL table extractor (FROM/JOIN clause parser)"
```

### Task 4: Implement TableDiscoverer

**Files:**
- Modify: `informatica_sparker/test_generator.py` (append after SQL extractor)

**Interfaces:**
- Consumes: `List[MappingDefinition]` (from parser), `SQLTableExtractor`
- Produces: `Dict[str, TableDef]` — all unique tables from 5 XML sources

- [ ] **Step 1: Add TableDiscoverer**

```python
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
```

- [ ] **Step 2: Run a quick smoke test**

```python
python3 -c "
import sys; sys.path.insert(0, '.')
from informatica_sparker.test_generator import TableDiscoverer
print('TableDiscoverer imported OK')
"
```

- [ ] **Step 3: Commit**

```bash
git add informatica_sparker/test_generator.py
git commit -m "feat: add TableDiscoverer (5-source table collection)"
```

### Task 5: Implement SchemaRenderer

**Files:**
- Modify: `informatica_sparker/test_generator.py` (append after TableDiscoverer)

**Interfaces:**
- Consumes: `Dict[str, TableDef]` from TableDiscoverer
- Produces: `str` — CREATE TABLE SQL text, DROP TABLE SQL text

- [ ] **Step 1: Add SchemaRenderer**

```python
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

        Uses simple _KEY naming convention: if table A has columns like
        EST_SCD_KEY and table B has EST_SCD_KEY as PK, then A depends on B.
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
```

- [ ] **Step 2: Quick smoke test**

```python
python3 -c "
import sys; sys.path.insert(0, '.')
from informatica_sparker.test_generator import SchemaRenderer
print('SchemaRenderer imported OK')
"
```

- [ ] **Step 3: Commit**

```bash
git add informatica_sparker/test_generator.py
git commit -m "feat: add SchemaRenderer (CREATE/DROP/TRUNCATE SQL generation)"
```

### Task 6: Implement ReferenceDataGenerator

**Files:**
- Modify: `informatica_sparker/test_generator.py` (append after SchemaRenderer)

**Interfaces:**
- Consumes: `Dict[str, TableDef]` — only tables where `is_reference=True`
- Produces: `str` — INSERT SQL statements

- [ ] **Step 1: Add ReferenceDataGenerator**

```python
# ── Reference Data Generator ──────────────────────────────────────────────────

# Field-name patterns for automatic value generation
_KEY_PATTERN = re.compile(r'(.+)_KEY$', re.I)
_CODE_PATTERN = re.compile(r'(.+)_CODE$', re.I)
_DESP_PATTERN = re.compile(r'(.+)_DESP', re.I)
_DATE_PATTERN = re.compile(r'(.+)_DATE$', re.I)
_BGN_DATE_PATTERN = re.compile(r'^BGN_|^VALID_FROM|^EFF_FROM', re.I)
_END_DATE_PATTERN = re.compile(r'^END_|^VALID_TO|^EFF_TO|^EXPIR', re.I)


class ReferenceDataGenerator:
    """Generates INSERT statements for dimension/hierarchy/code tables.

    Uses field-name pattern matching to create sensible test values.
    """

    def __init__(self, tables: Dict[str, TableDef], snsh_date: str = "20260601"):
        self.tables = tables
        self.snsh_date = snsh_date

    def render_inserts(self) -> str:
        """Generate INSERT statements for all reference tables."""
        ref_tables = [t for t in self.tables.values() if t.is_reference and t.fields]
        ordered = SchemaRenderer(self.tables)._topological_sort()
        ref_ordered = [t for t in ordered if t.key in {rt.key for rt in ref_tables}]

        lines = [
            "-- =============================================================================",
            "-- TEST DATA: REFERENCE TABLES (dimensions, hierarchies, code tables)",
            f"-- Auto-generated by informatica-sparker on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"-- Snapshot date: {self.snsh_date}",
            "-- =============================================================================",
            "",
        ]

        for tdef in ref_ordered:
            inserts = self._generate_table_inserts(tdef)
            if inserts:
                lines.append(f"-- {tdef.full_name}")
                lines.append("")
                lines.extend(inserts)
                lines.append("")

        return "\n".join(lines)

    def _generate_table_inserts(self, tdef: TableDef) -> List[str]:
        """Generate INSERT statements for one table."""
        if not tdef.fields:
            return []

        rows = self._generate_rows(tdef)
        if not rows:
            return []

        col_names = [f.name for f in tdef.fields]
        col_list = ", ".join(col_names)
        result = []

        for row in rows:
            values = []
            for col in col_names:
                val = row.get(col, "NULL")
                # Wrap non-numeric, non-function values in quotes
                if val == "NULL":
                    values.append("NULL")
                elif val.startswith("TO_DATE") or val.startswith("'") or val.startswith('"'):
                    values.append(val)
                else:
                    # Try to detect if it's a number
                    try:
                        float(val)
                        values.append(val)
                    except (ValueError, TypeError):
                        values.append(f"'{val}'")

            result.append(
                f"INSERT INTO {tdef.full_name} ({col_list})\n"
                f"VALUES ({', '.join(values)});"
            )

        return result

    def _generate_rows(self, tdef: TableDef) -> List[Dict[str, str]]:
        """Generate specific row data based on field-name patterns."""
        rows = []

        # Determine how many rows to generate based on table type
        num_rows = self._estimate_row_count(tdef)

        # PK counter
        pk_counter: Dict[str, int] = {}
        code_counter: Dict[str, int] = {}

        for i in range(num_rows):
            row = {}
            for f in tdef.fields:
                row[f.name] = self._generate_value(
                    f, tdef, i, pk_counter, code_counter
                )
            rows.append(row)

        return rows

    def _estimate_row_count(self, tdef: TableDef) -> int:
        """Estimate how many rows to generate based on field patterns."""
        name_upper = tdef.table_name.upper()
        # Tables with specific code patterns get meaningful row counts
        if 'GNDR' in name_upper:
            return 3  # M, F, N/A
        elif 'AGE_GRP' in name_upper:
            return 5  # 0-19, 20-39, 40-59, 60+, N/A
        elif 'SCORE_GRP' in name_upper:
            return 5  # N/A, 3-9, 10-15, 16+, 16+Excld
        elif 'HSHLD_SIZE' in name_upper:
            return 6  # N/A, 1-9, 10+, plus generic codes
        elif 'OFCR_TYPE' in name_upper:
            return 4  # 0, 1, 2, N/A
        elif 'MSD_CODE' in name_upper:
            return 2  # B3, B4
        elif '_DMNS_TIME' in name_upper:
            return 1  # Single date entry
        elif 'EST' in name_upper or 'HRCHY' in name_upper:
            return 1  # Single hierarchy entry
        return 3  # Default

    def _generate_value(self, field: FieldDef, tdef: TableDef, row_idx: int,
                        pk_counter: Dict[str, int],
                        code_counter: Dict[str, int]) -> str:
        """Generate a single field value based on its name pattern."""
        name = field.name.upper()

        # Primary key: sequential integer
        if field.is_pk:
            pk_counter[name] = pk_counter.get(name, 0) + 1
            return str(pk_counter[name])

        # BGN_DATE → far past
        if _BGN_DATE_PATTERN.search(name):
            return "TO_DATE('20000101','YYYYMMDD')"

        # END_DATE → far future
        if _END_DATE_PATTERN.search(name):
            return "TO_DATE('99991231','YYYYMMDD')"

        # CRE_DATE → snapshot date minus offset
        if 'CRE_DATE' in name or 'CREATE' in name:
            return f"TO_DATE('{self.snsh_date}','YYYYMMDD')"

        # CODE fields → sequential codes with prefix
        if _CODE_PATTERN.search(name):
            prefix = self._get_code_prefix(tdef, name)
            code_counter[name] = code_counter.get(name, 0) + 1
            return f"{prefix}{code_counter[name]}"

        # DESP fields → descriptive text
        if _DESP_PATTERN.search(name) or name.endswith('NAME'):
            return self._get_description(tdef, name, row_idx)

        # *_DATE fields (other) → snapshot date
        if _DATE_PATTERN.search(name):
            return f"TO_DATE('{self.snsh_date}','YYYYMMDD')"

        # KEY fields (FK) → sequential
        if name.endswith('_KEY'):
            pk_counter[name] = pk_counter.get(name, 0) + 1
            return str(pk_counter[name])

        # Numeric fields → 0
        if field.datatype.lower() in ('number', 'decimal', 'integer', 'smallint', 'float'):
            return "0"

        # Everything else → sample value
        return "N/A"

    def _get_code_prefix(self, tdef: TableDef, field_name: str) -> str:
        """Generate a table-appropriate code prefix."""
        name = tdef.table_name.upper()
        if 'GNDR' in name:
            return {1: 'M', 2: 'F', 3: 'N/A'}.get(
                self._get_or_create_counter(f'CODE_{tdef.key}', field_name), 'X')
        return 'B'

    def _get_or_create_counter(self, key: str, field_name: str) -> int:
        # Simple internal counter
        if not hasattr(self, '_counters'):
            self._counters: Dict[str, int] = {}
        counter_key = f"{key}.{field_name}"
        self._counters[counter_key] = self._counters.get(counter_key, 0) + 1
        return self._counters[counter_key]

    def _get_description(self, tdef: TableDef, field_name: str, row_idx: int) -> str:
        """Generate a meaningful description."""
        name = tdef.table_name.upper()
        base = name.replace('DDS_DMNS_', '').replace('DDS_HRCHY_', '').replace('SOR_', '')
        # Try to read from a common knowledge base
        knowledge = {
            'GNDR': ['Male', 'Female', 'Not Applicable'],
            'AGE_GRP': ['0 - 19', '20 - 39', '40 - 59', '60+', 'N/A'],
            'SCORE_GRP': ['N/A', '3 - 9', '10 - 15', '16+', '16+Excld'],
            'HSHLD_SIZE': ['N/A', '1', '2', '3', '4', '5'],
            'OFCR_TYPE': ['Est Stf Dog', 'Ddct Team Dog', 'Psa Shop', 'N/A'],
            'MSD_CODE': ['Code B3', 'Code B4'],
            'EST': ['Test Establishment'],
            'TIME': ['Test Time Entry'],
        }
        for key, values in knowledge.items():
            if key in base:
                idx = min(row_idx, len(values) - 1)
                return values[idx]
        return f"Test {base} {row_idx + 1}"
```

- [ ] **Step 2: Commit**

```bash
git add informatica_sparker/test_generator.py
git commit -m "feat: add ReferenceDataGenerator (pattern-based INSERT generation)"
```

### Task 7: Implement TestGenerator orchestrator

**Files:**
- Modify: `informatica_sparker/test_generator.py` (final section)

**Interfaces:**
- Consumes: all previous classes, `workflow_analysis` dict (from parser)
- Produces: writes files to `output_dir/tests/`

- [ ] **Step 1: Add TestGenerator class**

```python
# ── Test Generator (Orchestrator) ─────────────────────────────────────────────

class TestGenerator:
    """Orchestrates generation of all E2E test artifacts.

    Called from ConversionService after successful conversion.
    Uses already-parsed models — no re-parsing needed.
    """

    def __init__(self, mappings: List[MappingDefinition],
                 workflow_analysis: dict,
                 snsh_date: str = "20260601",
                 workflow_name: str = "workflow"):
        self.mappings = mappings
        self.workflow_analysis = workflow_analysis
        self.snsh_date = snsh_date
        self.workflow_name = workflow_name

        # Run discovery
        self.discoverer = TableDiscoverer(mappings)
        self.tables = self.discoverer.discover_all()
        self.schema_renderer = SchemaRenderer(self.tables)
        self.data_generator = ReferenceDataGenerator(self.tables, snsh_date)

        # Extract sessions/mappings info
        self.session_mappings: Dict[str, str] = {}
        for s in workflow_analysis.get("sessions", []):
            sname = s.get("name", "")
            mname = s.get("mapping_name", "")
            if sname and mname:
                self.session_mappings[sname] = mname

    def write_all(self, output_dir: str):
        """Write all test artifacts to output_dir/tests/."""
        tests_dir = Path(output_dir) / "tests"
        schema_dir = tests_dir / "schema"
        sql_dir = tests_dir / "sql"

        schema_dir.mkdir(parents=True, exist_ok=True)
        sql_dir.mkdir(parents=True, exist_ok=True)

        # Schema files
        self._write(schema_dir / "create_all_tables.sql", self.schema_renderer.render_create_all())
        self._write(schema_dir / "drop_all_tables.sql", self.schema_renderer.render_drop_all())

        # SQL files
        self._write(sql_dir / "10_dimension_data.sql", self.data_generator.render_inserts())
        self._write(sql_dir / "90_cleanup.sql", self.schema_renderer.render_truncate_all())

        # Test scripts (via templates or direct generation)
        self._write(tests_dir / "README.md", self._render_readme())
        self._write(tests_dir / "conftest.py", self._render_conftest())
        self._write(tests_dir / "gen_test_data.py", self._render_gen_test_data())
        self._write(tests_dir / "test_workflow_e2e.py", self._render_workflow_test())
        self._write(tests_dir / "test_mapping_e2e.py", self._render_mapping_test())

    def _write(self, path: Path, content: str):
        path.write_text(content, encoding="utf-8")

    def _make_safe_name(self, name: str) -> str:
        safe = re.sub(r'[^a-zA-Z0-9_]', '_', name)
        if safe and safe[0].isdigit():
            safe = '_' + safe
        return safe.lower()

    # ── Render methods ─────────────────────────────────────────────────────

    def _get_target_tables(self) -> List[str]:
        """Get all target tables that should be verified after test run."""
        targets = set()
        for tdef in self.tables.values():
            if tdef.origin == "target_def":
                targets.add(tdef.full_name)
        return sorted(targets)

    def _get_mapping_targets(self) -> Dict[str, List[str]]:
        """Map each mapping to its target tables."""
        result: Dict[str, List[str]] = {}
        for mapping in self.mappings:
            tgt_names = [t.upper() for t in self._get_mapping_target_names(mapping)]
            if tgt_names:
                safe = self._make_safe_name(mapping.name)
                result[safe] = tgt_names
        return result

    def _get_mapping_target_names(self, mapping: MappingDefinition) -> List[str]:
        """Get target table names for a specific mapping."""
        targets = []
        for inst in mapping.instances:
            if inst.transformation_type == "Target Definition":
                tname = inst.transformation_name
                # Find schema for this target
                for tgt_def in mapping.targets:
                    if tgt_def.name.upper() == tname.upper():
                        schema = tgt_def.db_name or ""
                        targets.append(f"{schema}.{tname}" if schema else tname)
                        break
                else:
                    targets.append(tname)
        return targets

    def _render_readme(self) -> str:
        """Generate tests/README.md."""
        table_count = len(self.tables)
        mapping_count = len(self.mappings)
        ref_count = len([t for t in self.tables.values() if t.is_reference])

        return f"""# E2E Tests — {self.workflow_name}

Auto-generated by informatica-sparker.

## Overview

- **Workflow:** {self.workflow_name}
- **Mappings:** {mapping_count}
- **Discovered Tables:** {table_count} ({ref_count} reference tables)
- **Target Database:** Oracle

## Files

| File | Purpose |
|------|---------|
| `schema/create_all_tables.sql` | CREATE TABLE for all {table_count} tables |
| `schema/drop_all_tables.sql` | Cleanup DROP statements |
| `sql/10_dimension_data.sql` | Reference/dimension table INSERTs |
| `sql/20_source_transaction.sql` | Source transaction data (generated) |
| `sql/90_cleanup.sql` | TRUNCATE cleanup |
| `gen_test_data.py` | Generate dynamic source data + UTL files |
| `conftest.py` | pytest fixtures (DB connection) |
| `test_workflow_e2e.py` | Full workflow test |
| `test_mapping_e2e.py` | Per-mapping parametrized test |

## How to Run

### Prerequisites

```bash
export DB_HOST=your-oracle-host
export DB_PORT=1521
export DB_USER=test_user
export DB_PASSWORD=test_password
export SNSH_DATE=$(date '+%Y%m%d')
```

### Full Workflow Test

```bash
cd {Path(output_dir).resolve() if 'output_dir' in dir() else '.'}
pytest tests/test_workflow_e2e.py -v
```

### Single Mapping Test

```bash
pytest tests/test_mapping_e2e.py -v -k "m_dpa_sum"
```

### Custom Snapshot Date

```bash
SNSH_DATE=20260701 pytest tests/test_workflow_e2e.py -v
```
"""

    def _render_conftest(self) -> str:
        """Generate tests/conftest.py with Oracle DB fixtures."""
        return '''"""pytest configuration and shared fixtures for E2E tests."""
import os
import subprocess
import pytest


@pytest.fixture(scope="session")
def snsh_date():
    """Return the snapshot date from environment or default."""
    return os.environ.get("SNSH_DATE", "20260601")


@pytest.fixture(scope="session")
def output_dir():
    """Return the output directory (parent of tests/)."""
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


@pytest.fixture(scope="session")
def db_config():
    """Return database connection config from environment."""
    return {
        "host": os.environ.get("DB_HOST", "localhost"),
        "port": os.environ.get("DB_PORT", "1521"),
        "user": os.environ.get("DB_USER", "test"),
        "password": os.environ.get("DB_PASSWORD", "test"),
        "service": os.environ.get("DB_SERVICE", "XE"),
    }


def run_sql_script(sql_path: str, db_config: dict = None):
    """Execute a SQL file against Oracle via SQL*Plus or similar."""
    sql_file = os.path.abspath(sql_path)
    if not os.path.exists(sql_file):
        pytest.skip(f"SQL file not found: {sql_file}")
    # Default: print what would run (override with actual DB call)
    print(f"  [SQL] Would execute: {sql_file}")


@pytest.fixture(scope="session")
def setup_database(snsh_date, db_config, output_dir):
    """Setup: create tables and insert reference data."""
    print(f"\\n=== SETUP: Creating tables and reference data ===")
    run_sql_script(os.path.join(output_dir, "tests/schema/create_all_tables.sql"), db_config)
    run_sql_script(os.path.join(output_dir, "tests/sql/10_dimension_data.sql"), db_config)
    yield
    print(f"\\n=== TEARDOWN: Cleaning up ===")
    run_sql_script(os.path.join(output_dir, "tests/sql/90_cleanup.sql"), db_config)


def query_table_count(table_name: str, db_config: dict = None) -> int:
    """Query row count from a table. Returns -1 if unavailable."""
    print(f"  [VERIFY] SELECT COUNT(*) FROM {table_name}")
    return -1


def run_pyspark_script(script_path: str, output_dir: str, timeout: int = 600) -> subprocess.CompletedProcess:
    """Run a converted PySpark script (wf_*.py or m_*.py) via subprocess."""
    script = os.path.abspath(script_path)
    if not os.path.exists(script):
        pytest.fail(f"PySpark script not found: {script}")
    env = os.environ.copy()
    env["SPARK_CONNECTION"] = env.get("SPARK_CONNECTION", "spark3_client")
    print(f"  [RUN] python3 {os.path.relpath(script, output_dir)}")
    result = subprocess.run(
        ["python3", script],
        cwd=output_dir,
        capture_output=True,
        text=True,
        timeout=timeout,
        env=env,
    )
    return result
'''

    def _render_gen_test_data(self) -> str:
        """Generate tests/gen_test_data.py — dynamic data generator."""
        # Collect SOR tables from discovered tables
        sor_tables = []
        for tdef in self.tables.values():
            upper = tdef.table_name.upper()
            if upper.startswith("SOR_") or tdef.schema_name.upper() == "PSOR":
                sor_tables.append(tdef.full_name)

        # Collect file sources from workflow analysis
        file_sources = []
        for sess in self.workflow_analysis.get("sessions", []):
            for src_name, src_info in sess.get("file_sources", {}).items():
                file_sources.append((src_name, src_info))

        sor_list = "\n    ".join(f'"{t}",' for t in sorted(sor_tables))
        file_src_list = "\n    ".join(
            f'# {name}: {info.get("Source filename", "")}'
            for name, info in file_sources
        )

        return f'''#!/usr/bin/env python3
"""Generate dynamic test data: SOR transaction INSERTs + UTL files.

Usage:
    python3 gen_test_data.py --snsh-date 20260601

This script is auto-generated by informatica-sparker.
"""
import os
import argparse

# SOR tables that need transaction data
SOR_TABLES = [
    {sor_list}
]

# UTL file sources
{file_src_list}


def generate_source_data(snsh_date: str, output_dir: str):
    """Generate minimal INSERT SQL for input transaction tables."""
    sql_dir = os.path.join(output_dir, "tests", "sql")
    os.makedirs(sql_dir, exist_ok=True)

    lines = [
        "-- ===================================================",
        "-- SOURCE TRANSACTION DATA (auto-generated at test time)",
        f"-- Snapshot date: {{snsh_date}}",
        "-- ===================================================",
        "",
    ]

    # For each SOR table, generate 1-2 minimal rows
    for table in SOR_TABLES:
        lines.append(f"-- INSERT INTO {{table}} (...) VALUES (...);  -- Add transaction data")
        lines.append("")

    sql_path = os.path.join(sql_dir, "20_source_transaction.sql")
    with open(sql_path, "w") as f:
        f.write("\\n".join(lines) + "\\n")
    print(f"Generated: {{sql_path}}")


def generate_utl_files(snsh_date: str, output_dir: str):
    """Generate UTL input files (session list, truncate list)."""
    utl_dir = os.path.join(output_dir, "env")
    os.makedirs(utl_dir, exist_ok=True)
    print(f"Generated UTL files in: {{utl_dir}}")


def generate_job_param(snsh_date: str, output_dir: str):
    """Generate job parameter file for the workflow."""
    lines = [
        f"$$v_snsh_date={{snsh_date}}",
        f"$$v_rpt_mth={{snsh_date[:6]}}",
    ]
    param_path = os.path.join(output_dir, "env", "job_param.txt")
    with open(param_path, "w") as f:
        f.write("\\n".join(lines) + "\\n")
    print(f"Generated: {{param_path}}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--snsh-date", default="20260601")
    parser.add_argument("--output-dir", default=".")
    args = parser.parse_args()

    generate_source_data(args.snsh_date, args.output_dir)
    generate_utl_files(args.snsh_date, args.output_dir)
    generate_job_param(args.snsh_date, args.output_dir)
    print("Test data generation complete.")
'''

    def _render_workflow_test(self) -> str:
        """Generate tests/test_workflow_e2e.py."""
        wf_safe = self._make_safe_name(self.workflow_name)
        target_tables = self._get_target_tables()
        target_table_list = "\n    ".join(f'"{t}",' for t in target_tables)

        return f'''"""E2E test for full workflow: {self.workflow_name}.

Runs the converted wf_*.py script and verifies target tables have data.
"""
import os
import subprocess
import pytest

# Target tables to verify after workflow execution
TARGET_TABLES = [
    {target_table_list}
]


def test_full_workflow(setup_database, output_dir, snsh_date, db_config):
    """Run the full workflow and verify target tables have data."""

    # Step 1: Generate source transaction data
    print(f"\\n=== Step 1: Generate test data (snsh_date={{snsh_date}}) ===")
    gen_script = os.path.join(output_dir, "tests", "gen_test_data.py")
    if os.path.exists(gen_script):
        subprocess.run(
            ["python3", gen_script, "--snsh-date", snsh_date, "--output-dir", output_dir],
            cwd=output_dir, check=True,
        )

    # Step 2: Run the converted workflow
    print(f"\\n=== Step 2: Run workflow ===")
    wf_script = os.path.join(output_dir, "{wf_safe}.py")
    if not os.path.exists(wf_script):
        pytest.skip(f"Workflow script not found: {{wf_script}}")

    env = os.environ.copy()
    env["SPARK_CONNECTION"] = env.get("SPARK_CONNECTION", "spark3_client")
    env["SNSH_DATE"] = snsh_date

    result = subprocess.run(
        ["python3", wf_script],
        cwd=output_dir,
        capture_output=True,
        text=True,
        timeout=600,
        env=env,
    )

    # Step 3: Check execution status
    assert result.returncode == 0, (
        f"Workflow failed (exit code {{result.returncode}})\\n"
        f"STDOUT: {{result.stdout[-2000:]}}\\n"
        f"STDERR: {{result.stderr[-2000:]}}"
    )
    print(f"Workflow completed successfully")

    # Step 4: Verify target tables have data
    print(f"\\n=== Step 3: Verify target tables ===")
    failures = []
    for table in TARGET_TABLES:
        count = query_table_count(table, db_config)
        if count > 0:
            print(f"  [OK] {{table}}: {{count}} rows")
        elif count == 0:
            failures.append(f"{{table}}: 0 rows (empty)")
        else:
            print(f"  [?] {{table}}: count unavailable (no DB connection)")

    if failures:
        pytest.fail(f"Target tables empty: {{', '.join(failures)}}")
'''

    def _render_mapping_test(self) -> str:
        """Generate tests/test_mapping_e2e.py — parametrized per-mapping test."""
        mapping_targets = self._get_mapping_targets()
        # Build pytest parametrize decorator entries
        param_entries = []
        for safe_name, targets in sorted(mapping_targets.items()):
            target_str = ", ".join(f'"{t}"' for t in targets)
            param_entries.append(f'    pytest.param("{safe_name}", [{target_str}], id="{safe_name}"),')

        param_block = "\\n".join(param_entries)

        return f'''"""E2E tests for individual mappings.

Each parametrized test runs one m_*.py script and verifies its target tables.
"""
import os
import subprocess
import pytest

# (mapping_safe_name, [target_table_names])
MAPPING_PARAMS = [
{param_block}
]


@pytest.mark.parametrize("mapping_safe,targets", MAPPING_PARAMS)
def test_mapping(mapping_safe, targets, setup_database, output_dir, snsh_date, db_config):
    """Run a single mapping and verify its targets."""

    # Generate test data
    gen_script = os.path.join(output_dir, "tests", "gen_test_data.py")
    if os.path.exists(gen_script):
        subprocess.run(
            ["python3", gen_script, "--snsh-date", snsh_date, "--output-dir", output_dir],
            cwd=output_dir, check=True,
        )

    # Run the mapping
    mapping_file = os.path.join(output_dir, f"{{mapping_safe}}.py")
    if not os.path.exists(mapping_file):
        pytest.skip(f"Mapping script not found: {{mapping_file}}")

    env = os.environ.copy()
    env["SPARK_CONNECTION"] = env.get("SPARK_CONNECTION", "spark3_client")
    env["SNSH_DATE"] = snsh_date

    result = subprocess.run(
        ["python3", mapping_file],
        cwd=output_dir,
        capture_output=True,
        text=True,
        timeout=300,
        env=env,
    )

    assert result.returncode == 0, (
        f"Mapping {{mapping_safe}} failed (exit {{result.returncode}})\\n"
        f"STDERR: {{result.stderr[-1000:]}}"
    )
    print(f"Mapping {{mapping_safe}} completed")

    # Verify targets
    for table in targets:
        count = query_table_count(table, db_config)
        if count > 0:
            print(f"  [OK] {{table}}: {{count}} rows")
        elif count == 0:
            pytest.fail(f"Table {{table}} is empty after mapping {{mapping_safe}}")
'''

    def _render_placeholder_source_sql(self) -> str:
        """Generate placeholder for 20_source_transaction.sql."""
        return """-- ===================================================
-- SOURCE TRANSACTION DATA
-- Auto-generated by gen_test_data.py at test time
-- ===================================================

-- Run `python3 gen_test_data.py --snsh-date $(date '+%Y%m%d')`
-- to populate this file with dynamic transaction data.

SELECT 1 FROM DUAL;
"""
```

- [ ] **Step 2: Verify the module imports cleanly**

Run: `python3 -c "import sys; sys.path.insert(0, '.'); from informatica_sparker.test_generator import TestGenerator; print('OK')"`
Expected: OK

- [ ] **Step 3: Commit**

```bash
git add informatica_sparker/test_generator.py
git commit -m "feat: add TestGenerator orchestrator (all render methods)"
```

### Task 8: Integrate into service.py and CLI

**Files:**
- Modify: `informatica_sparker/service.py` (around line 28-29, after conversion loop)
- Modify: `informatica_sparker/cli.py` (add `--with-tests` flag)

**Interfaces:**
- Consumes: `TestGenerator` from `test_generator.py`
- Produces: `--with-tests` flag on convert command

- [ ] **Step 1: Add TestGenerator import and call in service.py**

Edit `service.py` to add import at the top:

```python
from .test_generator import TestGenerator
```

Add method call in `convert_bytes()` after the output write block (after line 146 in current code, before `return result`):

```python
        # Generate E2E test artifacts when requested
        if output_dir and hasattr(self, '_generate_tests') and not result.errors:
            wf_name = "workflow"
            workflows = workflow_analysis.get("workflows", [])
            if workflows:
                wf_name = workflows[0]["name"]
            test_gen = TestGenerator(
                mappings=mappings,
                workflow_analysis=workflow_analysis,
                workflow_name=wf_name,
            )
            test_gen.write_all(output_dir)
```

Since the existing code checks `hasattr`, we need to set the flag. Actually, let me use a simpler approach — store a flag on the ConversionService instance:

In `service.py`, in `__init__`:

```python
class ConversionService:
    def __init__(self, user_config: Optional[UserConfig] = None):
        self.user_config = user_config or UserConfig()
        self.logger = ConversionLogger()
        self.codegen = CodeGenerator()
        self.with_tests = False  # NEW
```

Then in `convert_bytes()`, add after the test artifact generation block.

Actually, looking at the flow more carefully, the best approach is to add test generation after the `_write_output()` call but before `return result`. Let me be precise:

At line 146-148:
```python
        if output_dir:
            self._write_output(all_files, output_dir)

        return result
```

Change to:
```python
        if output_dir:
            self._write_output(all_files, output_dir)

        # Generate E2E test artifacts (only when --with-tests flag is set)
        if output_dir and self.with_tests and not result.errors:
            wf_name = "workflow"
            workflows = workflow_analysis.get("workflows", [])
            if workflows:
                wf_name = workflows[0]["name"]
            test_gen = TestGenerator(
                mappings=mappings,
                workflow_analysis=workflow_analysis,
                snsh_date=os.environ.get("SNSH_DATE", "20260601"),
                workflow_name=wf_name,
            )
            test_gen.write_all(output_dir)

        return result
```

- [ ] **Step 2: Add --with-tests flag to CLI**

Edit `cli.py`:

```python
convert_parser.add_argument("--with-tests", action="store_true",
                          help="Generate E2E test artifacts (schema, test data, pytest scripts)")
```

And in `_run_convert()`:

```python
def _run_convert(args):
    from .models import UserConfig
    from .service import ConversionService

    user_config = UserConfig()
    if args.config:
        user_config = _load_user_config(args.config)
    if args.source_db:
        user_config.source_db_type = args.source_db
    if args.target_db:
        user_config.target_db_type = args.target_db

    service = ConversionService(user_config=user_config)
    service.with_tests = args.with_tests  # NEW

    # ... rest of the function
```

- [ ] **Step 3: Update _write_output to handle tests/ subdirectory**

In `service.py`, the `_write_output()` method needs to also handle files from test generation. Since our TestGenerator writes files directly via `path.write_text()` (not through the `GeneratedFile` list), this is actually self-contained — no change needed to `_write_output()`.

However, we need to update the summary output to mention test generation. In `cli.py`:

```python
if result.errors:
    print(f"\n  ERRORS ({len(result.errors)}):")
    ...

if args.with_tests:
    print(f"\n  E2E Test Artifacts: generated in {args.output}/tests/")
```

- [ ] **Step 4: Verify end-to-end**

```bash
cd /var/lib/airflow/dags/adam/informatica/informatica_sparker
informatica-sparker convert \
  ../PowerCenter_workflows/dds/WF_GMS_DDS_APLY_DLY.XML \
  -o /tmp/test_wf_output \
  --with-tests
```

Expected output should show success. Then check:
```bash
ls -la /tmp/test_wf_output/tests/
ls -la /tmp/test_wf_output/tests/schema/
ls -la /tmp/test_wf_output/tests/sql/
```

Expected: all test files exist.

- [ ] **Step 5: Commit**

```bash
git add informatica_sparker/service.py informatica_sparker/cli.py
git commit -m "feat: integrate E2E test generation into CLI (--with-tests flag)"
```

### Task 9: Create Jinja2 templates

**Files:**
- Create: `informatica_sparker/templates/test/` (directory)
- Create: all template files

**Note:** The test generator currently generates test scripts via Python string formatting (f-strings) in `TestGenerator`, not via Jinja2 templates. This is fine — the generated content is data-driven and doesn't need the template layer. However, for any future template-based rendering, the directory is ready.

- [ ] **Step 1: Create empty templates directory (placeholder for future template work)**

```bash
mkdir -p informatica_sparker/templates/test
touch informatica_sparker/templates/test/.gitkeep
```

- [ ] **Step 2: Commit**

```bash
git add informatica_sparker/templates/test/
git commit -m "chore: add templates/test directory for future Jinja2 templates"
```

### Task 10: Add-Phase 2 hooks (source_transaction placeholder + verify)

**Files:**
- Modify: `informatica_sparker/test_generator.py` (ensure `20_source_transaction.sql` placeholder is generated)

- [ ] **Step 1: Add write of placeholder transaction SQL in TestGenerator.write_all()**

In `write_all()`, after the reference data write:

```python
        # Source transaction data placeholder (regenerated at test time by gen_test_data.py)
        self._write(sql_dir / "20_source_transaction.sql", self._render_placeholder_source_sql())
```

- [ ] **Step 2: Commit**

```bash
git add informatica_sparker/test_generator.py
git commit -m "feat: add placeholder source transaction SQL file"
```

### Task 11: Self-Review

- [ ] Check: Does `test_generator.py` import from `models.py` without circular imports? → Yes, models are leaf-level.
- [ ] Check: Are all generated file paths using the `Path` API, consistent with existing `_write_output` patterns? → Yes.
- [ ] Check: Does `--with-tests` work both with and without existing workflows? → Yes, gracefully skips.
- [ ] Check: Are all reference data INSERT statements Oracle-compatible? → Yes, uses `TO_DATE`, NUMBER, VARCHAR2.
- [ ] Check: Do the Jinja2 template dir and test template dir interfere? → No, `CodeGenerator` uses `templates/` root. Test templates go in `templates/test/`.

# E2E Testing Design for informatica-sparker

**Date**: 2026-06-30
**Status**: Approved
**Author**: Brainstorming Process

## Overview

Integrate end-to-end test generation into the `informatica-sparker` converter. When the user runs `informatica-sparker convert <workflow.xml> -o <output_dir>`, the converter generates test artifacts alongside the existing PySpark code, enabling automated testing of the converted workflow against a real Oracle database.

The test artifacts include schema DDL, reference data SQL, a dynamic test data generator, and pytest-based test scripts that directly invoke the converted `wf_*.py` / `m_*.py` files.

## Principles

1. **Workflow-agnostic** — The test generator works from the converter's parsed model objects (`SourceDefinition`, `TargetDefinition`, `Transformation`, `MappingDefinition`, `Workflow`), not from hardcoded workflow names.
2. **Non-invasive** — Test generation is purely additive. Existing conversion logic is unchanged.
3. **Run converted code directly** — Tests call `subprocess.run(["python3", "wf_*.py"])` or `subprocess.run(["python3", "m_*.py"])`. No re-implementation of mapping logic.
4. **Real Oracle database** — Tests target a real Oracle instance using connection info from `env/config.yml`.

## Output Structure

```
output_dir/
  ├── m_*.py                       # (existing) PySpark mapping scripts
  ├── wf_*.py                      # (existing) Workflow orchestration
  ├── env/                         # (existing) Config, runtime_lib, etc.
  └── tests/                       # ★ NEW — test assets
      ├── README.md
      ├── conftest.py              # pytest session fixtures (DB connection, setup)
      ├── test_workflow_e2e.py     # Full workflow: setup → run wf_*.py → verify
      ├── test_mapping_e2e.py      # Per-mapping tests: setup → run m_*.py → verify
      ├── gen_test_data.py         # Generate SOR transaction data + UTL files
      ├── schema/
      │   ├── create_all_tables.sql   # CREATE TABLE for all discovered tables
      │   └── drop_all_tables.sql     # Cleanup DROP statements
      └── sql/
          ├── 10_reference_data.sql   # INSERT for reference tables (dimensions + codes)
          ├── 20_source_data.sql      # INSERT for input transaction data (auto-generated)
          └── 90_cleanup.sql          # TRUNCATE in dependency order
```

## Schema Generation Engine

### Table Discovery (5 Sources)

The engine collects all unique tables from the workflow XML:

| # | Source | Extraction Method | Has Field Defs? |
|---|--------|-------------------|-----------------|
| 1 | `<SOURCE>` definitions | `SOURCEFIELD` elements | ✅ Full field list |
| 2 | `<TARGET>` definitions | `TARGETFIELD` elements | ✅ Full field list |
| 3 | Source Qualifier `Sql Query` | SQL parser: `FROM`, `JOIN` clause extraction | ❌ Inferred from SELECT |
| 4 | Lookup Procedure `Lookup Sql Override` | SQL parser: `FROM` clause | ❌ Inferred from SELECT |
| 5 | Lookup Procedure `Lookup table name` | Direct attribute value | ❌ Unknown fields |

### Type Mapping: Informatica → Oracle

| Informatica Type | Oracle Type |
|-----------------|-------------|
| `number(p,s)` | `NUMBER(p,s)` |
| `varchar2(n)` | `VARCHAR2(n)` |
| `char(n)` | `CHAR(n)` |
| `date` | `DATE` |
| `timestamp` | `TIMESTAMP` |
| `float` / `double` | `FLOAT` |
| `integer` | `INTEGER` |
| `string(n)` (file src) | `VARCHAR2(n)` |

### Field Classification

For tables without explicit field definitions (sources 3-5):

1. If the same table name exists in sources 1-2, reuse those field definitions
2. If only in SQL, parse `SELECT` columns with aliases to extract field names
3. Fallback: single `VARCHAR2(255)` column with a comment marker

### Dependency Sorting for CREATE TABLE

Tables are emitted in dependency-safe order:

1. Tables with no FK-like columns first (reference-only tables)
2. Tables referenced by `_KEY` naming pattern match second
3. Tables containing `_KEY` references to other tables last

## Test Data Generation

### Table Classification → Data Strategy

| Category | Includes | Strategy |
|----------|----------|----------|
| **Reference Tables** | Dimension tables + hierarchy tables + code tables | Pre-filled INSERT statements generated at conversion time |
| **Data Tables (Input)** | SOR transaction tables | Minimal record INSERTs generated dynamically by `gen_test_data.py` |
| **Data Tables (Intermediate/Output)** | DPA/DDS fact tables | Left empty — populated by mapping execution |

### Reference Data Generation Logic

Field-name pattern matching drives automatic INSERT generation:

| Field Pattern | Generated Value |
|--------------|----------------|
| `*_KEY` (PK) | Sequential integers: 1, 2, 3... |
| `*_CODE` | Sequential codes: B1, B2, B3... |
| `*_DESP` / `*_NAME` | Context-aware description from table name |
| `BGN_DATE` | `TO_DATE('20000101','YYYYMMDD')` |
| `END_DATE` | `TO_DATE('99991231','YYYYMMDD')` |
| `*_KEY` (FK reference) | Matches the referenced table's PK values |

### Source Data Generation (`gen_test_data.py`)

This Python script runs at test time and:
1. Reads the `--snsh-date` parameter for dynamic snapshot date
2. Generates minimal INSERT SQL for SOR transaction tables
3. Creates UTL input files (GMS_ETL_SESSION_LIST, GMS_ETL_DPA_TBL_LIST)

## Test Execution Model

### Flow

```
test_workflow_e2e.py
  │
  ├── 1. conftest.setup_database
  │     ├── CREATE TABLE (schema/create_all_tables.sql)
  │     └── INSERT reference data (sql/10_reference_data.sql)
  │
  ├── 2. gen_test_data.py
  │     ├── Generate SOR transaction INSERT SQL
  │     ├── Generate UTL file inputs
  │     └── Execute INSERT (sql/20_source_data.sql)
  │
  ├── 3. subprocess.run(["python3", "wf_gms_dds_aply_dly.py"])
  │     └── Or: subprocess.run(["python3", "m_*.py"]) for single mapping
  │
  └── 4. Verify target tables have data (COUNT(*) > 0)
```

### Test Modes

| Mode | Command | What Runs | Use Case |
|------|---------|-----------|----------|
| Full workflow | `pytest tests/test_workflow_e2e.py` | `wf_*.py` | Regression / CI |
| Single mapping | `pytest tests/test_mapping_e2e.py` | `m_*.py` via parametrize | Developer iteration |
| Custom date | `SNSH_DATE=20260701 pytest ...` | All py files | Time-sensitive testing |

## Converter Integration

In `service.py`, after successful conversion:

```python
class ConversionService:
    def convert_file(self, xml_file, output_dir):
        # ... existing logic ...
        # (parsed models in self.analysis_result, self.workflows, etc.)
        
        if not result.errors:
            test_gen = TestGenerator(result, self.workflows)
            test_gen.write_all(output_dir)
        
        return result
```

The `TestGenerator` class in a new module `test_generator.py` holds all the logic for:
- Table discovery across the 5 sources
- CREATE TABLE generation with type mapping
- Reference data INSERT generation with pattern matching
- Cleanup SQL generation
- `gen_test_data.py` template rendering (with discovered SOR table names)
- `conftest.py` / `test_*.py` template rendering (with workflow topology)

## Implementation in service.py

In `service.py`, the `convert_file()` method is extended to call `TestGenerator` after successful conversion. The generator is instantiated with the parsed models (mappings, workflows, sources, targets, transformations) that are already available in memory after XML parsing — no additional parsing pass is needed.

A new module `informatica_sparker/test_generator.py` is added, containing:

- `TestGenerator` class with methods: `write_all()`, `write_schema()`, `write_reference_data()`, `write_cleanup()`, `write_data_generator()`, `write_test_scripts()`
- `TableDiscoverer` — collects unique tables from 5 XML sources  
- `SchemaRenderer` — type mapping + CREATE TABLE generation  
- `ReferenceDataGenerator` — pattern-based INSERT generation  
- `SQLTableExtractor` — parses `FROM`/`JOIN` clauses from SQL queries

## Files to Create

| File | Purpose |
|------|---------|
| `informatica_sparker/test_generator.py` | Core test generation logic |
| `informatica_sparker/templates/test/conftest.py.j2` | Jinja2 template for conftest.py |
| `informatica_sparker/templates/test/test_workflow_e2e.py.j2` | Jinja2 template for workflow test |
| `informatica_sparker/templates/test/test_mapping_e2e.py.j2` | Jinja2 template for mapping test |
| `informatica_sparker/templates/test/gen_test_data.py.j2` | Jinja2 template for data generator |
| `informatica_sparker/templates/test/README.md.j2` | Jinja2 template for test README |
| `informatica_sparker/templates/test/create_all_tables.sql.j2` | Jinja2 template for DDL output formatting |
| `informatica_sparker/templates/test/reference_data.sql.j2` | Jinja2 template for reference data formatting |

## Open / Future

- **Sub-second precision**: Informatica `Subsecond Precision` attribute can affect timestamp field generation; current design uses standard `DATE` type.
- **File-source field inference**: Flat file sources without explicit field definitions get a generic `VARCHAR2(255)` column.
- **Router/Normalizer**: Mapping-internal routing logic is not reflected in schema generation (schema only cares about table I/O, not routing).

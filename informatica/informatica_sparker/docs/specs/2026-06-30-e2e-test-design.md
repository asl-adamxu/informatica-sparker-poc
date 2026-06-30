# E2E Testing Design for informatica-sparker

**Date**: 2026-06-30
**Status**: Approved (updated per design review)
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
      ├── gen_test_data.py         # Generate transaction data + UTL/param files
      ├── schema/
      │   ├── create_all_tables.sql   # CREATE TABLE for all discovered tables
      │   └── drop_all_tables.sql     # Cleanup DROP statements
      └── sql/
          ├── 10_dimension_data.sql   # INSERT for dimension/reference/code tables
          ├── 20_source_transaction.sql # INSERT for input transaction data (auto-generated)
          └── 90_cleanup.sql          # TRUNCATE in dependency order
```

## Schema Generation Engine

### Table Discovery (5 Sources)

The engine collects all unique tables from the workflow XML:

| # | Source | Extraction Method | Has Field Defs? | Priority |
|---|--------|-------------------|-----------------|----------|
| 1 | `<SOURCE>` definitions | `SOURCEFIELD` elements | ✅ Full field list | Highest |
| 2 | `<TARGET>` definitions | `TARGETFIELD` elements | ✅ Full field list | Highest |
| 3 | Source Qualifier `Sql Query` | SQL parser: `FROM`, `JOIN` clause extraction | ❌ Inferred | Medium |
| 4 | Lookup Procedure `Lookup Sql Override` | SQL parser: `FROM` clause | ❌ Inferred | Medium |
| 5 | Lookup Procedure `Lookup table name` | Direct attribute value | ❌ Unknown | Lowest |

### Conflict Resolution

When the same table appears in multiple sources with potentially different field lists:

1. If a table has full field definitions (priority 1-2), those definitions **always win** and override partial definitions from SQL (priority 3-5).
2. If a table exists in SQL (priority 3-4) but not in SOURCE/TARGET definitions, the engine attempts to extract field names from `SELECT` columns with aliases.
3. If a table only has a name (priority 5 — e.g. `Lookup table name`), the table is created with a generic `VARCHAR2` column structure and annotated with: `-- ⚠️ Table from Lookup table name — add field DDL manually if needed`.
4. Deduplication is by uppercase `{OWNER}.{TABLE_NAME}` composite key.

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

### Field Inference from SQL

When a table has no explicit field definition but appears in a SQL query:

1. Parse `SELECT` expressions and aliases to extract field names:
   - `select a.cust_rqs_key, b.case_no` → fields `CUST_RQS_KEY`, `CASE_NO`
   - Compex expressions like `CASE WHEN ... END` get generic names if unaliased.
2. Types default to `VARCHAR2(255)` for inferred fields, with a comment noting the source SQL.
3. Fields from the XML definition (if the same table is found in SOURCE/TARGET) take precedence — SQL-inferred fields are discarded in favor of the explicit definition.

### Foreign Key Inference

Two strategies, used together:

1. **XML-native FK** (preferred): Some `<SOURCEFIELD>` entries have `REFERENCEDFIELD` and `REFERENCEDTABLE` attributes from the Informatica source definition. These are the most authoritative FK relationship source.
2. **Naming pattern fallback**: When XML FK attributes are absent, use `_KEY` naming pattern: if table A has `EST_SCD_KEY` and table B is named `DDS_HRCHY_GMS_EST` and has `EST_SCD_KEY` as a PK, then table A references table B.

### Dependency Sorting for CREATE TABLE

Tables are emitted in dependency-safe topological order using the discovered FK graph:

1. Tables with no outgoing FK references first (reference-only tables like `DDS_DMNS_GNDR`).
2. Tables that are only referenced by other tables second (dimension/hierarchy tables).
3. Tables that reference other tables last (fact/transaction tables).

Code generation uses cycle detection (a `visiting` set, same pattern as the existing workflow builder) to handle cross-references gracefully.

## Test Data Generation

### Table Classification → Data Strategy

| Category | Includes | Strategy |
|----------|----------|----------|
| **Reference Tables** | Dimension tables + hierarchy tables + code tables | Pre-filled INSERT statements generated at conversion time |
| **Data Tables (Input)** | SOR + source transaction tables | Minimal record INSERTs generated dynamically by `gen_test_data.py` |
| **Data Tables (Intermediate/Output)** | DPA/DDS fact tables | Left empty — populated by mapping execution |

### Reference Data Generation Logic

Field-name pattern matching drives automatic INSERT generation:

| Field Pattern | Generated Value |
|--------------|----------------|
| `*_KEY` (PK) | Sequential integers: 1, 2, 3... |
| `*_CODE` | Sequential codes: B1, B2, B3... |
| `*_DESP` / `*_NAME` | Context-aware description from table name |
| `BGN_DATE` | `TO_DATE('20000101','YYYYMMDD')` (sufficiently far past) |
| `END_DATE` | `TO_DATE('99991231','YYYYMMDD')` (sufficiently far future) |
| `CRE_DATE` / `CREATE_DATE` | Dynamically computed: `TO_DATE('{snsh_date}','YYYYMMDD') - N` |
| `*_KEY` (FK reference) | Matches the referenced table's PK values |

Date fields that participate in range-filtering conditions (e.g. `between bgn_date and end_date`, `last_day(...) between bgn_date and end_date`) are covered by the broad `BGN_DATE`/`END_DATE` defaults, ensuring Lookup queries with date-range filters return matches.

### Source Data Generation (`gen_test_data.py`)

This Python script runs at test time and:

1. Reads the `--snsh-date` parameter for dynamic snapshot date.
2. Reads the `--output-dir` parameter to know where `env/` and UTL files live.
3. Generates minimal INSERT SQL for SOR/input transaction tables, writing to `tests/sql/20_source_transaction.sql`.
4. Creates UTL input files (GMS_ETL_SESSION_LIST, GMS_ETL_DPA_TBL_LIST) in the location the workflow expects them (typically `$PMSourceFileDir/PCIS01/scripts/` or a configurable path).

### Workflow Parameter File

The generated `wf_*.py` may require a job parameter file referenced by `$PMSourceFileDir/.../job_param.txt`. `gen_test_data.py` also generates this file at runtime:

```
$$v_snsh_date={snsh_date}
$$v_rpt_mth={snsh_date[:6]}
```

The path is read from the `env/config.yml` objects section or defaults to `env/job_param.txt`.

## Test Execution Model

### Flow

```
test_workflow_e2e.py
  │
  ├── 1. conftest.setup_database
  │     ├── Use isolated schema prefix (TEST_{pid}_)
  │     ├── CREATE TABLE (schema/create_all_tables.sql)
  │     └── INSERT dimension data (sql/10_dimension_data.sql)
  │
  ├── 2. gen_test_data.py
  │     ├── Generate source transaction INSERT SQL
  │     ├── Generate UTL file inputs
  │     ├── Generate job param file
  │     └── Execute INSERT (sql/20_source_transaction.sql)
  │
  ├── 3. subprocess.run(["python3", "wf_gms_dds_aply_dly.py"])
  │     └── Or: subprocess.run(["python3", "m_*.py"]) for single mapping
  │
  └── 4. Verify target tables have data (COUNT(*) > 0)
```

### Test Isolation

Each test session uses an isolated schema prefix to allow concurrent runs:

```python
# conftest.py
import os, getpass

@pytest.fixture(scope="session")
def schema_prefix():
    """Unique schema prefix per test run to avoid cross-test interference."""
    pid = os.getpid()
    return f"T{pid % 10000}_"

# All table references in generated SQL get prefixed:
# CREATE TABLE T1234_PDDS.DDS_FACT_...
# This requires the Oracle user to have CREATE ANY TABLE privilege.
```

When schema prefixing is not feasible, the test falls back to truncate-before-insert with a session-level advisory lock.

### Test Modes

| Mode | Command | What Runs | Use Case |
|------|---------|-----------|----------|
| Full workflow | `pytest tests/test_workflow_e2e.py` | `wf_*.py` | Regression / CI |
| Single mapping | `pytest tests/test_mapping_e2e.py` | `m_*.py` via parametrize | Developer iteration |
| Custom date | `SNSH_DATE=20260701 pytest ...` | All py files | Time-sensitive testing |
| Dry-run | `informatica-sparker convert ... --test-only` | Generate artifacts only, no execution | Preview |

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

A CLI flag `--test-only` generates test artifacts without performing the PySpark conversion:

```bash
informatica-sparker convert WF_GMS_DDS_APLY_DLY.XML -o output_dir --test-only
```

## Implementation in service.py

In `service.py`, the `convert_file()` method is extended to call `TestGenerator` after successful conversion. The generator is instantiated with the parsed models (mappings, workflows, sources, targets, transformations) that are already available in memory after XML parsing — no additional parsing pass is needed.

A new module `informatica_sparker/test_generator.py` is added, containing:

- `TestGenerator` — orchestrator class with methods: `write_all()`, `write_schema()`, `write_reference_data()`, `write_cleanup()`, `write_data_generator()`, `write_test_scripts()`
- `TableDiscoverer` — collects unique tables from 5 XML sources with conflict resolution
- `SchemaRenderer` — Informatica→Oracle type mapping + CREATE TABLE generation with dependency ordering
- `ReferenceDataGenerator` — pattern-based INSERT generation with FK-aware ordering
- `SQLTableExtractor` — parses `FROM`/`JOIN` clauses from SQL queries using sqlparse or regex
- `TableDependencySorter` — topological sort with cycle detection (reuses pattern from the existing `workflow_builder.py`)

## Phased Implementation

### Phase 1: Core Functionality

- `TableDiscoverer` with 5-source collection + conflict resolution
- `SchemaRenderer` with full type mapping
- Basic `ReferenceDataGenerator` with date-aware defaults
- `TestGenerator.write_schema()` + `write_reference_data()` + `write_cleanup()`
- `conftest.py`, `test_workflow_e2e.py`, `test_mapping_e2e.py` templates
- Integration hook in `service.py`

### Phase 2: Enhanced Features

- FK inference via XML `REFERENCEDFIELD` in addition to naming patterns
- SQL field extraction from `SELECT` columns for priority-3/4 tables
- `gen_test_data.py` with dynamic SOR transaction generation
- Workflow parameter file generation
- Schema prefix isolation for concurrent test runs
- `--test-only` CLI flag

## Files to Create

| File | Purpose |
|------|---------|
| `informatica_sparker/test_generator.py` | Core test generation logic (all sub-classes) |
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
- **Scale testing**: Future `--scale N` flag for generating large volumes of transaction data for performance testing.
- **Custom data hooks**: Allow user-provided Python hooks to override default data generation for specific tables.

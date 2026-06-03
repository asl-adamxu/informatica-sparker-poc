# informatica-sparker

A Python framework that converts Informatica PowerCenter workflow/mapping XML exports into PySpark code deployable to Databricks.

## Features

- **Multi-Mapping Support**: Handles any number of mappings per XML file, generating separate `.py` files for each mapping
- **Auto Source Detection**: Automatically identifies source types and connection details:
  - SQL databases (SQL Server, Oracle, MySQL, PostgreSQL, DB2, Teradata, Netezza, Sybase, Informix)
  - File formats: CSV, Parquet, DAT, XML, JSON, Text, Fixed-Width, Avro, ORC, Excel
  - Files without extensions
  - JDBC/ODBC connections with driver JAR detection
- **Complete Output Package**: Generates a full deployment-ready package:
  - `mapping_name.py` - PySpark script for each mapping
  - `workflow.py` - Workflow orchestration with dependency management
  - `config.yml` - Unified YAML configuration with environment variable support
  - `all_sql_queries.sql` - All extracted SQL queries organized by mapping
  - `error_log.txt` - Detailed conversion log with warnings, errors, and source detection results
- **Transformation Coverage**: Supports Source Qualifier, Expression, Filter, Lookup, Joiner, Aggregator, Sorter, Union, Router, Sequence Generator, Update Strategy, Stored Procedure, Mapplet
- **Python 3.10+ Compatible**

## Installation

```bash
pip install informatica-sparker
```

## Quick Start

### Command Line

```bash
# Convert XML to PySpark
informatica-sparker convert mapping_export.xml -o output_dir

# Analyze XML without converting
informatica-sparker analyze mapping_export.xml

# Analyze with JSON output
informatica-sparker analyze mapping_export.xml --json

# Use custom config
informatica-sparker convert mapping_export.xml -o output_dir -c my_config.yml
```

### Python API

```python
from informatica_sparker import ConversionService, UserConfig

# Basic conversion
service = ConversionService()
result = service.convert_file("mapping_export.xml", output_dir="output")

print(f"Mappings converted: {result.mappings_processed}/{result.mapping_count}")
print(f"Files generated: {len(result.files)}")
print(f"SQL queries found: {len(result.sql_queries)}")

# Check source detections
for detection in result.source_detections:
    print(f"  {detection.source_name}: {detection.detected_type.value}")
    if detection.file_format:
        print(f"    Format: {detection.file_format.value}")
    for note in detection.detection_notes:
        print(f"    {note}")

# Inspect extracted SQL queries
for query in result.sql_queries:
    print(f"  [{query.query_type}] {query.step_name}: {query.query[:80]}...")
```

### With Custom Configuration

```python
from informatica_sparker import ConversionService, UserConfig

config = UserConfig(
    db_connections={
        "source_db": {
            "host": "myserver.database.windows.net",
            "database": "mydb",
            "user": "admin",
            "password": "secret",
        }
    }
)

service = ConversionService(user_config=config)
result = service.convert_file("export.xml", output_dir="spark_output")
```

## Output Structure

```
output/
  mapping_1.py          # PySpark code for mapping 1
  mapping_2.py          # PySpark code for mapping 2
  mapping_N.py          # PySpark code for mapping N
  workflow.py           # Workflow orchestration (runs all mappings in order)
  config.yml            # Unified YAML config (connections, sources, targets)
  all_sql_queries.sql   # All SQL queries extracted from all mappings
  error_log.txt         # Conversion log with warnings, errors, detections
```

## Source Type Detection

The framework automatically identifies what each source in the XML is:

| Source Type | Detection Method |
|------------|-----------------|
| SQL Server | `DATABASETYPE` attribute, connection properties |
| Oracle | `DATABASETYPE` attribute, JDBC driver class |
| Flat File (CSV) | File extension, `DATABASETYPE=Flat File`, delimiter attributes |
| Parquet | `.parquet` file extension in source attributes |
| DAT | `.dat` file extension |
| XML | `.xml` extension or `DATABASETYPE=XML` |
| JSON | `.json` extension or `DATABASETYPE=JSON` |
| Text | `.txt`/`.text`/`.log` extension |
| No Extension | File source with no recognizable extension |
| Fixed Width | `DATABASETYPE=Fixed-Width` or file type attribute |

Connection details (JDBC URLs, driver JARs, host/port) are automatically extracted and included in the generated `config.yml`.

## Supported Transformations

| Informatica Transform | PySpark Equivalent |
|----------------------|-------------------|
| Source Qualifier | `spark.read.format("jdbc")` / `spark.read.csv()` etc. |
| Expression | `.withColumn()` / `.select()` with expressions |
| Filter | `.filter()` / `.where()` |
| Lookup | `.join()` with broadcast hint |
| Joiner | `.join()` (inner, left, right, full) |
| Aggregator | `.groupBy().agg()` |
| Sorter | `.orderBy()` |
| Union | `df1.unionByName(df2)` |
| Router | Multiple `.filter()` branches |
| Sequence Generator | `monotonically_increasing_id()` |
| Update Strategy | Insert/Update/Delete flags |
| Target | `.write.format("jdbc")` / `.write.format("delta")` |

## Configuration File (config.yml)

The generated `config.yml` supports environment variable substitution:

```yaml
spark:
  app_name: "my_workflow"
  master: "${SPARK_MASTER:local[*]}"

connections:
  CDM_PRE_LANDING:
    db_type: "sqlserver"
    host: "${MSSQL_HOST}"
    database: "msscdm_dev"
    user: "${MSSQL_USER}"
    password: "${MSSQL_PASSWORD}"
    driver: "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    driver_jar: "${MSSQL_DRIVER_JAR:/opt/drivers/mssql-jdbc.jar}"
```

## Requirements

- Python >= 3.10
- lxml >= 4.9.0
- pydantic >= 2.0.0
- jinja2 >= 3.1.0
- networkx >= 3.0
- pyyaml >= 6.0

## License

MIT

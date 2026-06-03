from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from enum import Enum


CONNECTION_TO_DATABASE_MAP = {
    "CDM_PRE_LANDING": "msscdm_dev",
    "CDM_PRE_LANDING_INV": "msscdm_inv",
    "CDM_LANDING": "cmx_ors_10_3",
    "CDM_LANDING_INV": "cmx_ors_inv",
}

KNOWN_DATABASES = ["msscdm_dev", "msscdm_inv", "cmx_ors_10_3", "cmx_ors_inv"]


def resolve_database_name(connection_name: str, xml_db_name: str = "") -> str:
    if not connection_name:
        return xml_db_name or ""

    conn_upper = connection_name.upper().strip()

    for known_conn, db_name in CONNECTION_TO_DATABASE_MAP.items():
        if known_conn.upper() in conn_upper or conn_upper in known_conn.upper():
            return db_name

    return xml_db_name or connection_name


class SourceType(str, Enum):
    SQL = "SQL"
    FILE = "FILE"
    UNKNOWN = "UNKNOWN"


class FileFormat(str, Enum):
    PARQUET = "parquet"
    CSV = "csv"
    DAT = "dat"
    XML = "xml"
    JSON = "json"
    TEXT = "text"
    FIXED_WIDTH = "fixed_width"
    AVRO = "avro"
    ORC = "orc"
    EXCEL = "excel"
    NO_EXT = "no_ext"
    UNKNOWN = "unknown"


class FileLocation(str, Enum):
    LOCAL = "local"
    S3 = "s3"
    ADLS = "adls"
    GCS = "gcs"
    DBFS = "dbfs"


class ConnectorType(str, Enum):
    JDBC = "jdbc"
    ODBC = "odbc"
    FILE = "file"
    JAR = "jar"
    UNKNOWN = "unknown"


class SourceConnectionInfo(BaseModel):
    connection_name: str = ""
    connection_type: ConnectorType = ConnectorType.UNKNOWN
    database_type: str = ""
    database_name: str = ""
    schema_name: str = ""
    host: str = ""
    port: int = 0
    jdbc_url: str = ""
    driver_class: str = ""
    driver_jar: str = ""
    connection_string: str = ""
    properties: Dict[str, str] = Field(default_factory=dict)


class SourceField(BaseModel):
    name: str
    datatype: str
    precision: int = 0
    scale: int = 0
    nullable: bool = True
    key_type: str = "NOT A KEY"


class SourceDefinition(BaseModel):
    name: str
    database_type: str = ""
    db_name: str = ""
    owner_name: str = ""
    source_type: SourceType = SourceType.UNKNOWN
    file_format: Optional[FileFormat] = None
    file_path: Optional[str] = None
    file_directory: Optional[str] = None
    file_name: Optional[str] = None
    file_location: Optional[FileLocation] = None
    delimiter: Optional[str] = None
    encoding: str = "UTF-8"
    has_header: bool = True
    connection_info: Optional[SourceConnectionInfo] = None
    fields: List[SourceField] = Field(default_factory=list)
    table_attributes: Dict[str, str] = Field(default_factory=dict)


class TargetField(BaseModel):
    name: str
    datatype: str
    precision: int = 0
    scale: int = 0
    nullable: bool = True
    key_type: str = "NOT A KEY"


class TargetDefinition(BaseModel):
    name: str
    database_type: str = ""
    table_options: str = ""
    fields: List[TargetField] = Field(default_factory=list)


class TransformField(BaseModel):
    name: str
    datatype: str
    port_type: str = "INPUT/OUTPUT"
    precision: int = 0
    scale: int = 0
    expression: Optional[str] = None
    default_value: Optional[str] = None
    group_name: Optional[str] = None
    is_group_by: bool = False


class Transformation(BaseModel):
    name: str
    type: str
    description: str = ""
    reusable: bool = False
    fields: List[TransformField] = Field(default_factory=list)
    table_attributes: Dict[str, str] = Field(default_factory=dict)


class Instance(BaseModel):
    name: str
    type: str
    transformation_name: str = ""
    transformation_type: str = ""
    description: str = ""


class Connector(BaseModel):
    from_instance: str
    from_field: str
    to_instance: str
    to_field: str
    from_instance_type: str = ""
    to_instance_type: str = ""


class MappingVariable(BaseModel):
    name: str
    datatype: str = "string"
    precision: int = 0
    scale: int = 0
    aggregation_type: str = ""
    is_expression_variable: bool = False
    description: str = ""
    default_value: str = ""


class MappingDefinition(BaseModel):
    name: str
    description: str = ""
    is_valid: bool = True
    sources: List[SourceDefinition] = Field(default_factory=list)
    targets: List[TargetDefinition] = Field(default_factory=list)
    transformations: List[Transformation] = Field(default_factory=list)
    instances: List[Instance] = Field(default_factory=list)
    connectors: List[Connector] = Field(default_factory=list)
    mapplets: Dict[str, Any] = Field(default_factory=dict)
    mapping_variables: List[MappingVariable] = Field(default_factory=list)


class LookupReference(BaseModel):
    lookup_name: str
    input_ports: List[str] = Field(default_factory=list)
    output_ports: List[str] = Field(default_factory=list)
    condition: str = ""


class StoredProcReference(BaseModel):
    sp_name: str
    input_ports: List[str] = Field(default_factory=list)
    output_ports: List[str] = Field(default_factory=list)


class ExpressionAnalysis(BaseModel):
    lookups: List[LookupReference] = Field(default_factory=list)
    stored_procs: List[StoredProcReference] = Field(default_factory=list)
    pm_variables: List[str] = Field(default_factory=list)
    unknown_references: List[str] = Field(default_factory=list)


class TransformSummary(BaseModel):
    source_qualifiers: int = 0
    expressions: int = 0
    filters: int = 0
    joiners: int = 0
    lookups: int = 0
    stored_procedures: int = 0
    update_strategies: int = 0
    aggregators: int = 0
    sorters: int = 0
    routers: int = 0
    unions: int = 0
    others: int = 0


class FlowStep(BaseModel):
    instance_name: str
    instance_type: str
    order: int = 0


class ConfigAttribute(BaseModel):
    name: str
    value: str
    description: str = ""


class ConfigDefinition(BaseModel):
    name: str
    description: str = ""
    is_default: bool = False
    version: str = ""
    attributes: List[ConfigAttribute] = Field(default_factory=list)


class MappingAnalysis(BaseModel):
    mapping_name: str
    sources: List[Dict[str, Any]] = Field(default_factory=list)
    targets: List[Dict[str, Any]] = Field(default_factory=list)
    transform_summary: TransformSummary = Field(default_factory=TransformSummary)
    transformations: List[Dict[str, Any]] = Field(default_factory=list)
    sql_analysis: Dict[str, Any] = Field(default_factory=dict)
    connectors_analysis: Dict[str, Any] = Field(default_factory=dict)
    connectors: List[Dict[str, str]] = Field(default_factory=list)
    parameters: Dict[str, Any] = Field(default_factory=dict)
    main_flow: List[FlowStep] = Field(default_factory=list)
    detached_lookups: List[str] = Field(default_factory=list)
    detached_sps: List[str] = Field(default_factory=list)
    expression_analysis: ExpressionAnalysis = Field(default_factory=ExpressionAnalysis)
    required_user_inputs: List[Dict[str, Any]] = Field(default_factory=list)
    flow_text: str = ""


class AnalysisResult(BaseModel):
    xml_type: str = "POWERMART"
    folder_name: str = ""
    repository_name: str = ""
    mapping_count: int = 0
    mappings: List[MappingAnalysis] = Field(default_factory=list)
    configs: List[ConfigDefinition] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)
    errors: List[str] = Field(default_factory=list)


class SourceConfig(BaseModel):
    source_name: str
    source_type: SourceType
    connection_alias: Optional[str] = None
    file_format: Optional[FileFormat] = None
    file_location: Optional[FileLocation] = None
    file_path: Optional[str] = None
    delimiter: str = ","
    header: bool = True
    quote_char: str = '"'
    escape_char: str = "\\"
    encoding: str = "UTF-8"


class TargetConfig(BaseModel):
    target_name: str
    output_format: str = "delta"
    destination_path: str = ""
    table_name: str = ""
    write_mode: str = "append"
    connection_alias: Optional[str] = None


class UserConfig(BaseModel):
    artifact_id: str = ""
    sources: List[SourceConfig] = Field(default_factory=list)
    targets: List[TargetConfig] = Field(default_factory=list)
    db_connections: Dict[str, Dict[str, str]] = Field(default_factory=dict)
    connection_mappings: Dict[str, Dict[str, Any]] = Field(default_factory=dict)
    options: Dict[str, Any] = Field(default_factory=dict)


class GeneratedFile(BaseModel):
    filename: str
    content: str
    file_type: str = "python"


class ReportItem(BaseModel):
    severity: str = "info"
    category: str = ""
    message: str = ""
    details: Optional[str] = None


class ConversionReport(BaseModel):
    mapping_name: str
    status: str = "success"
    coverage_percent: float = 100.0
    generated_files: List[str] = Field(default_factory=list)
    warnings: List[ReportItem] = Field(default_factory=list)
    errors: List[ReportItem] = Field(default_factory=list)
    manual_items: List[ReportItem] = Field(default_factory=list)
    unsupported_features: List[str] = Field(default_factory=list)


class SQLQueryInfo(BaseModel):
    mapping_name: str = ""
    step_name: str = ""
    query_type: str = ""
    query: str = ""
    source_table: str = ""
    connection: str = ""


class SourceDetectionResult(BaseModel):
    source_name: str
    detected_type: SourceType = SourceType.UNKNOWN
    file_format: Optional[FileFormat] = None
    connection_info: Optional[SourceConnectionInfo] = None
    confidence: str = "high"
    detection_notes: List[str] = Field(default_factory=list)


class GenerationResult(BaseModel):
    artifact_id: str = ""
    mapping_count: int = 0
    mappings_processed: int = 0
    files: List[GeneratedFile] = Field(default_factory=list)
    reports: List[ConversionReport] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)
    errors: List[str] = Field(default_factory=list)
    sql_queries: List[SQLQueryInfo] = Field(default_factory=list)
    source_detections: List[SourceDetectionResult] = Field(default_factory=list)
    zip_path: Optional[str] = None

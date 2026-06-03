import os
from lxml import etree
from typing import List, Dict, Any, Optional, Tuple
from .models import (
    SourceDefinition, SourceField, SourceType, FileFormat, FileLocation,
    TargetDefinition, TargetField,
    Transformation, TransformField,
    Instance, Connector, MappingDefinition, MappingVariable,
    SourceConnectionInfo, ConnectorType
)


class InfaXMLParser:

    SQL_DB_TYPES = {
        "microsoft sql server", "oracle", "db2", "sybase",
        "teradata", "netezza", "postgresql", "mysql", "informix",
        "sql server"
    }

    FILE_INDICATORS = {
        "source file directory", "source file name", "delimited",
        "flat file", "line sequential", "code page", "file type"
    }

    def __init__(self, xml_content: bytes):
        self.xml_content = xml_content
        self.tree = None
        self.root = None
        self.xml_type = "UNKNOWN"
        self.folder_name = ""
        self.repository_name = ""

    def parse(self) -> bool:
        try:
            self.tree = etree.fromstring(self.xml_content)
            self.root = self.tree

            if self.root.tag == "POWERMART":
                self.xml_type = "POWERMART"
                repo = self.root.find(".//REPOSITORY")
                if repo is not None:
                    self.repository_name = repo.get("NAME", "")
                folder = self.root.find(".//FOLDER")
                if folder is not None:
                    self.folder_name = folder.get("NAME", "")
            elif self.root.find(".//WORKFLOW") is not None:
                self.xml_type = "WORKFLOW"

            return True
        except Exception as e:
            print(f"Error parsing XML: {e}")
            return False

    def get_mapplets(self) -> Dict[str, Any]:
        mapplets = {}

        for mapplet_elem in self.root.findall(".//MAPPLET"):
            mapplet_name = mapplet_elem.get("NAME", "")
            mapplet = {
                "name": mapplet_name,
                "transformations": [],
                "instances": [],
                "connectors": []
            }

            for transform_elem in mapplet_elem.findall("TRANSFORMATION"):
                transform = self._parse_transformation(transform_elem)
                mapplet["transformations"].append(transform)

            for instance_elem in mapplet_elem.findall("INSTANCE"):
                instance = self._parse_instance(instance_elem)
                mapplet["instances"].append(instance)

            for connector_elem in mapplet_elem.findall("CONNECTOR"):
                connector = self._parse_connector(connector_elem)
                mapplet["connectors"].append(connector)

            mapplets[mapplet_name] = mapplet

        return mapplets

    def get_mappings(self) -> List[MappingDefinition]:
        mappings = []

        mapplets = self.get_mapplets()

        for mapping_elem in self.root.findall(".//MAPPING"):
            mapping = self._parse_mapping(mapping_elem)
            mapping.mapplets = mapplets
            mappings.append(mapping)

        return mappings

    def _parse_mapping(self, mapping_elem) -> MappingDefinition:
        mapping = MappingDefinition(
            name=mapping_elem.get("NAME", ""),
            description=mapping_elem.get("DESCRIPTION", ""),
            is_valid=mapping_elem.get("ISVALID", "YES") == "YES"
        )

        folder = self.root.find(".//FOLDER")
        if folder is not None:
            for source_elem in folder.findall("SOURCE"):
                source = self._parse_source(source_elem)
                mapping.sources.append(source)

            for target_elem in folder.findall("TARGET"):
                target = self._parse_target(target_elem)
                mapping.targets.append(target)

        for transform_elem in mapping_elem.findall("TRANSFORMATION"):
            transform = self._parse_transformation(transform_elem)
            mapping.transformations.append(transform)

        for instance_elem in mapping_elem.findall("INSTANCE"):
            instance = self._parse_instance(instance_elem)
            mapping.instances.append(instance)

        for connector_elem in mapping_elem.findall("CONNECTOR"):
            connector = self._parse_connector(connector_elem)
            mapping.connectors.append(connector)

        for var_elem in mapping_elem.findall("MAPPINGVARIABLE"):
            mapping_var = self._parse_mapping_variable(var_elem)
            mapping.mapping_variables.append(mapping_var)

        return mapping

    def _parse_mapping_variable(self, var_elem) -> MappingVariable:
        return MappingVariable(
            name=var_elem.get("NAME", ""),
            datatype=var_elem.get("DATATYPE", "string"),
            precision=int(var_elem.get("PRECISION", "0")),
            scale=int(var_elem.get("SCALE", "0")),
            aggregation_type=var_elem.get("AGGREGATIONTYPE", ""),
            is_expression_variable=var_elem.get("ISEXPRESSIONVARIABLE", "NO") == "YES",
            description=var_elem.get("DESCRIPTION", ""),
            default_value=var_elem.get("DEFAULTVALUE", "")
        )

    def _parse_source(self, source_elem) -> SourceDefinition:
        source = SourceDefinition(
            name=source_elem.get("NAME", ""),
            database_type=source_elem.get("DATABASETYPE", ""),
            db_name=source_elem.get("DBDNAME", ""),
            owner_name=source_elem.get("OWNERNAME", "")
        )

        source.source_type = self._detect_source_type(source_elem)

        if source.source_type == SourceType.FILE:
            source.file_format = self._detect_file_format(source_elem)
            source.file_location = self._detect_file_location(source_elem)
            file_details = self._extract_file_details(source_elem)
            source.file_directory = file_details["file_directory"]
            source.file_name = file_details["file_name"]
            source.delimiter = file_details["delimiter"]
            source.encoding = file_details["encoding"]
            source.has_header = file_details["has_header"]
            if source.file_directory and source.file_name:
                source.file_path = f"{source.file_directory}/{source.file_name}"
            elif source.file_name:
                source.file_path = source.file_name

        if source.source_type == SourceType.SQL:
            source.connection_info = self._build_connection_info(source_elem)

        for attr_elem in source_elem.findall("TABLEATTRIBUTE"):
            attr_name = attr_elem.get("NAME", "")
            attr_value = attr_elem.get("VALUE", "")
            source.table_attributes[attr_name] = attr_value

        for field_elem in source_elem.findall("SOURCEFIELD"):
            field = SourceField(
                name=field_elem.get("NAME", ""),
                datatype=field_elem.get("DATATYPE", ""),
                precision=int(field_elem.get("PRECISION", "0")),
                scale=int(field_elem.get("SCALE", "0")),
                nullable=field_elem.get("NULLABLE", "NULL") == "NULL",
                key_type=field_elem.get("KEYTYPE", "NOT A KEY")
            )
            source.fields.append(field)

        return source

    def _parse_target(self, target_elem) -> TargetDefinition:
        target = TargetDefinition(
            name=target_elem.get("NAME", ""),
            database_type=target_elem.get("DATABASETYPE", ""),
            table_options=target_elem.get("TABLEOPTIONS", "")
        )

        for field_elem in target_elem.findall("TARGETFIELD"):
            field = TargetField(
                name=field_elem.get("NAME", ""),
                datatype=field_elem.get("DATATYPE", ""),
                precision=int(field_elem.get("PRECISION", "0")),
                scale=int(field_elem.get("SCALE", "0")),
                nullable=field_elem.get("NULLABLE", "NULL") == "NULL",
                key_type=field_elem.get("KEYTYPE", "NOT A KEY")
            )
            target.fields.append(field)

        return target

    def _parse_transformation(self, transform_elem) -> Transformation:
        transform = Transformation(
            name=transform_elem.get("NAME", ""),
            type=transform_elem.get("TYPE", ""),
            description=transform_elem.get("DESCRIPTION", ""),
            reusable=transform_elem.get("REUSABLE", "NO") == "YES"
        )

        for attr_elem in transform_elem.findall("TABLEATTRIBUTE"):
            attr_name = attr_elem.get("NAME", "")
            attr_value = attr_elem.get("VALUE", "")
            transform.table_attributes[attr_name] = attr_value

        for field_elem in transform_elem.findall("TRANSFORMFIELD"):
            is_group_by = field_elem.get("GROUPBY", "NO") == "YES" or "GROUP BY" in field_elem.get("PORTTYPE", "").upper()
            field = TransformField(
                name=field_elem.get("NAME", ""),
                datatype=field_elem.get("DATATYPE", ""),
                port_type=field_elem.get("PORTTYPE", "INPUT/OUTPUT"),
                precision=int(field_elem.get("PRECISION", "0")),
                scale=int(field_elem.get("SCALE", "0")),
                expression=field_elem.get("EXPRESSION", None),
                default_value=field_elem.get("DEFAULTVALUE", None),
                group_name=field_elem.get("GROUP", None),
                is_group_by=is_group_by
            )
            transform.fields.append(field)

        return transform

    def _parse_instance(self, instance_elem) -> Instance:
        return Instance(
            name=instance_elem.get("NAME", ""),
            type=instance_elem.get("TYPE", ""),
            transformation_name=instance_elem.get("TRANSFORMATION_NAME", ""),
            transformation_type=instance_elem.get("TRANSFORMATION_TYPE", ""),
            description=instance_elem.get("DESCRIPTION", "")
        )

    def _parse_connector(self, connector_elem) -> Connector:
        return Connector(
            from_instance=connector_elem.get("FROMINSTANCE", ""),
            from_field=connector_elem.get("FROMFIELD", ""),
            to_instance=connector_elem.get("TOINSTANCE", ""),
            to_field=connector_elem.get("TOFIELD", ""),
            from_instance_type=connector_elem.get("FROMINSTANCETYPE", ""),
            to_instance_type=connector_elem.get("TOINSTANCETYPE", "")
        )

    FILE_EXT_FORMAT_MAP = {
        ".parquet": FileFormat.PARQUET,
        ".csv": FileFormat.CSV,
        ".dat": FileFormat.DAT,
        ".xml": FileFormat.XML,
        ".json": FileFormat.JSON,
        ".txt": FileFormat.TEXT,
        ".text": FileFormat.TEXT,
        ".log": FileFormat.TEXT,
        ".tsv": FileFormat.CSV,
        ".psv": FileFormat.CSV,
        ".avro": FileFormat.AVRO,
        ".orc": FileFormat.ORC,
        ".xls": FileFormat.EXCEL,
        ".xlsx": FileFormat.EXCEL,
    }

    JDBC_DRIVER_MAP = {
        "sqlserver": ("com.microsoft.sqlserver.jdbc.SQLServerDriver", "mssql-jdbc"),
        "oracle": ("oracle.jdbc.driver.OracleDriver", "ojdbc"),
        "mysql": ("com.mysql.cj.jdbc.Driver", "mysql-connector"),
        "postgresql": ("org.postgresql.Driver", "postgresql"),
        "db2": ("com.ibm.db2.jcc.DB2Driver", "db2jcc"),
        "teradata": ("com.teradata.jdbc.TeraDriver", "terajdbc"),
        "netezza": ("org.netezza.Driver", "nzjdbc"),
        "informix": ("com.informix.jdbc.IfxDriver", "ifxjdbc"),
        "sybase": ("com.sybase.jdbc4.jdbc.SybDriver", "jconn4"),
    }

    def _detect_source_type(self, source_elem) -> SourceType:
        db_type = source_elem.get("DATABASETYPE", "").lower()

        if any(sql_type in db_type for sql_type in self.SQL_DB_TYPES):
            return SourceType.SQL

        for attr_elem in source_elem.findall("TABLEATTRIBUTE"):
            attr_name = attr_elem.get("NAME", "").lower()
            attr_value = attr_elem.get("VALUE", "").lower()
            if any(indicator in attr_name for indicator in self.FILE_INDICATORS):
                return SourceType.FILE
            if "source file" in attr_name and attr_value:
                return SourceType.FILE

        if db_type in ("flat file", "flatfile", "delimited", "fixed-width",
                        "line sequential", "xml", "json"):
            return SourceType.FILE

        if source_elem.get("DBDNAME", ""):
            return SourceType.SQL

        return SourceType.UNKNOWN

    def _detect_file_format(self, source_elem) -> FileFormat:
        file_name = ""
        file_type_attr = ""
        delimiter = ""

        for attr_elem in source_elem.findall("TABLEATTRIBUTE"):
            attr_name = attr_elem.get("NAME", "").lower()
            attr_value = attr_elem.get("VALUE", "").lower()

            if "file name" in attr_name or "filename" in attr_name or "source file name" in attr_name:
                file_name = attr_value
            if "file type" in attr_name or "filetype" in attr_name:
                file_type_attr = attr_value
            if "delimiter" in attr_name or "delimiters" in attr_name:
                delimiter = attr_value

        if file_name:
            _, ext = os.path.splitext(file_name)
            ext = ext.lower()
            if ext in self.FILE_EXT_FORMAT_MAP:
                return self.FILE_EXT_FORMAT_MAP[ext]
            if ext == "" and file_name:
                return FileFormat.NO_EXT

        db_type = source_elem.get("DATABASETYPE", "").lower()
        if "xml" in db_type:
            return FileFormat.XML
        if "json" in db_type:
            return FileFormat.JSON
        if "delimited" in db_type or "flat file" in db_type or "flatfile" in db_type:
            if delimiter == "\t" or "tab" in delimiter:
                return FileFormat.CSV
            return FileFormat.CSV
        if "fixed" in db_type:
            return FileFormat.FIXED_WIDTH

        if file_type_attr:
            if "delimited" in file_type_attr:
                return FileFormat.CSV
            if "fixed" in file_type_attr:
                return FileFormat.FIXED_WIDTH
            if "xml" in file_type_attr:
                return FileFormat.XML

        return FileFormat.UNKNOWN

    def _detect_file_location(self, source_elem) -> FileLocation:
        for attr_elem in source_elem.findall("TABLEATTRIBUTE"):
            attr_name = attr_elem.get("NAME", "").lower()
            attr_value = attr_elem.get("VALUE", "").lower()
            combined = f"{attr_name} {attr_value}"
            if "s3://" in combined or "s3a://" in combined:
                return FileLocation.S3
            if "abfss://" in combined or "wasbs://" in combined or "adl://" in combined:
                return FileLocation.ADLS
            if "gs://" in combined:
                return FileLocation.GCS
            if "dbfs:/" in combined:
                return FileLocation.DBFS
        return FileLocation.LOCAL

    def _extract_file_details(self, source_elem) -> dict:
        details = {
            "file_directory": "",
            "file_name": "",
            "delimiter": ",",
            "encoding": "UTF-8",
            "has_header": True,
        }
        for attr_elem in source_elem.findall("TABLEATTRIBUTE"):
            attr_name = attr_elem.get("NAME", "").lower()
            attr_value = attr_elem.get("VALUE", "")
            if "source file directory" in attr_name or "source filedir" in attr_name:
                details["file_directory"] = attr_value
            elif "source file name" in attr_name or "source filename" in attr_name:
                details["file_name"] = attr_value
            elif "delimiter" in attr_name:
                details["delimiter"] = attr_value
            elif "code page" in attr_name or "codepage" in attr_name:
                details["encoding"] = attr_value
            elif "header" in attr_name:
                details["has_header"] = attr_value.upper() not in ("NO", "FALSE", "0")
        return details

    def _build_connection_info(self, source_elem) -> SourceConnectionInfo:
        db_type = source_elem.get("DATABASETYPE", "").lower()
        db_name = source_elem.get("DBDNAME", "")
        owner = source_elem.get("OWNERNAME", "")

        conn = SourceConnectionInfo(
            database_type=db_type,
            database_name=db_name,
            schema_name=owner,
        )

        for sql_key, (driver_class, jar_hint) in self.JDBC_DRIVER_MAP.items():
            if sql_key in db_type:
                conn.connection_type = ConnectorType.JDBC
                conn.driver_class = driver_class
                conn.driver_jar = f"{jar_hint}.jar"
                break

        for attr_elem in source_elem.findall("TABLEATTRIBUTE"):
            attr_name = attr_elem.get("NAME", "").lower()
            attr_value = attr_elem.get("VALUE", "")
            if not attr_value:
                continue
            if "connection" in attr_name and "name" in attr_name:
                conn.connection_name = attr_value
            elif "jdbc" in attr_name and "url" in attr_name:
                conn.jdbc_url = attr_value
            elif "driver" in attr_name and "class" in attr_name:
                conn.driver_class = attr_value
            elif attr_name == "host" or "server" in attr_name:
                conn.host = attr_value
            elif attr_name == "port":
                try:
                    conn.port = int(attr_value)
                except ValueError:
                    pass

        return conn

    def get_workflow_analysis(self) -> Dict[str, Any]:
        workflow_analysis = {
            "workflows": [],
            "sessions": [],
            "tasks": [],
            "task_dependencies": [],
            "worklets": [],
            "schedulers": [],
            "workflow_variables": [],
            "orchestration_mapping": []
        }

        for wf_elem in self.root.findall(".//WORKFLOW"):
            workflow = {
                "name": wf_elem.get("NAME", ""),
                "description": wf_elem.get("DESCRIPTION", ""),
                "is_valid": wf_elem.get("ISVALID", "YES") == "YES",
                "is_restartable": wf_elem.get("REUSABLE_SCHEDULER", "") == "YES",
                "pyspark_equivalent": "Databricks Workflow / Airflow DAG"
            }
            workflow_analysis["workflows"].append(workflow)

            for task_elem in wf_elem.findall("TASK"):
                task = {
                    "name": task_elem.get("NAME", ""),
                    "type": task_elem.get("TYPE", ""),
                    "description": task_elem.get("DESCRIPTION", ""),
                    "pyspark_equivalent": self._get_task_pyspark_equivalent(task_elem.get("TYPE", ""))
                }
                workflow_analysis["tasks"].append(task)

            for link_elem in wf_elem.findall("WORKFLOWLINK"):
                dependency = {
                    "from_task": link_elem.get("FROMTASK", ""),
                    "to_task": link_elem.get("TOTASK", ""),
                    "condition": link_elem.get("CONDITION", "")
                }
                workflow_analysis["task_dependencies"].append(dependency)

            for var_elem in wf_elem.findall("WORKFLOWVARIABLE"):
                variable = {
                    "name": var_elem.get("NAME", ""),
                    "datatype": var_elem.get("DATATYPE", ""),
                    "default_value": var_elem.get("DEFAULTVALUE", "N/A"),
                    "pyspark_equivalent": "Airflow Variable / Databricks parameter / Environment variable"
                }
                workflow_analysis["workflow_variables"].append(variable)

            for sched_elem in wf_elem.findall("SCHEDULER"):
                schedule_info = sched_elem.find("SCHEDULEINFO")
                schedule_type = schedule_info.get("SCHEDULETYPE", "ONDEMAND") if schedule_info is not None else "ONDEMAND"
                start_date = schedule_info.get("STARTDATE", "") if schedule_info is not None else ""
                end_date = schedule_info.get("ENDDATE", "") if schedule_info is not None else ""
                repeat_interval = schedule_info.get("REPEATINTERVAL", "") if schedule_info is not None else ""

                scheduler = {
                    "name": sched_elem.get("NAME", "Scheduler"),
                    "description": sched_elem.get("DESCRIPTION", ""),
                    "reusable": sched_elem.get("REUSABLE", "NO") == "YES",
                    "schedule_type": schedule_type,
                    "start_date": start_date,
                    "end_date": end_date,
                    "repeat_interval": repeat_interval,
                    "pyspark_equivalent": self._get_scheduler_pyspark_equivalent(schedule_type)
                }
                workflow_analysis["schedulers"].append(scheduler)

        for session_elem in self.root.findall(".//SESSION"):
            pre_session = []
            post_success = []
            post_failure = []

            for comp in session_elem.findall("SESSIONCOMPONENT"):
                comp_type = comp.get("TYPE", "")
                comp_name = comp.get("REFOBJECTNAME", "")
                if "Pre-session" in comp_type:
                    pre_session.append(comp_name)
                elif "success" in comp_type.lower():
                    post_success.append(comp_name)
                elif "failure" in comp_type.lower():
                    post_failure.append(comp_name)

            transformations = []
            for trans_inst in session_elem.findall("SESSTRANSFORMATIONINST"):
                transformations.append({
                    "name": trans_inst.get("SINSTANCENAME", ""),
                    "type": trans_inst.get("TRANSFORMATIONTYPE", ""),
                    "pipeline": trans_inst.get("PIPELINE", "")
                })

            session = {
                "name": session_elem.get("NAME", ""),
                "mapping_name": session_elem.get("MAPPINGNAME", ""),
                "description": session_elem.get("DESCRIPTION", ""),
                "is_valid": session_elem.get("ISVALID", "YES") == "YES",
                "reusable": session_elem.get("REUSABLE", "NO") == "YES",
                "sort_order": session_elem.get("SORTORDER", "Binary"),
                "pre_session_components": pre_session,
                "post_success_components": post_success,
                "post_failure_components": post_failure,
                "transformation_count": len(transformations),
                "pyspark_equivalent": "PySpark Script/Notebook task"
            }
            workflow_analysis["sessions"].append(session)

        for worklet_elem in self.root.findall(".//WORKLET"):
            worklet = {
                "name": worklet_elem.get("NAME", ""),
                "description": worklet_elem.get("DESCRIPTION", ""),
                "pyspark_equivalent": "Nested DAG / TaskGroup"
            }
            workflow_analysis["worklets"].append(worklet)

        workflow_analysis["orchestration_mapping"] = self._build_orchestration_mapping(workflow_analysis)

        return workflow_analysis

    def _get_task_pyspark_equivalent(self, task_type: str) -> str:
        mapping = {
            "SESSION": "PySpark Script/Notebook task",
            "COMMAND": "BashOperator / Shell task",
            "START": "DAG start (implicit)",
            "DECISION": "BranchPythonOperator",
            "ASSIGNMENT": "PythonOperator (variable assignment)",
            "EMAIL": "EmailOperator",
            "TIMER": "TimeSensor / Wait task",
            "EVENT_WAIT": "ExternalTaskSensor",
            "EVENT_RAISE": "TriggerDagRunOperator"
        }
        return mapping.get(task_type.upper(), f"PythonOperator (custom for {task_type})")

    def _get_scheduler_pyspark_equivalent(self, schedule_type: str) -> str:
        mapping = {
            "ONDEMAND": "Manual trigger / API trigger",
            "DAILY": "schedule_interval='@daily' / cron('0 0 * * *')",
            "WEEKLY": "schedule_interval='@weekly' / cron('0 0 * * 0')",
            "MONTHLY": "schedule_interval='@monthly' / cron('0 0 1 * *')",
            "HOURLY": "schedule_interval='@hourly' / cron('0 * * * *')",
            "CUSTOMIZED": "Custom cron expression",
            "RUN_EVERY_N": "timedelta-based schedule"
        }
        return mapping.get(schedule_type.upper(), f"Custom schedule ({schedule_type})")

    def _build_orchestration_mapping(self, analysis: Dict) -> List[Dict]:
        mapping = []

        mapping_count = len(self.root.findall(".//MAPPING"))
        workflow_count = len(analysis["workflows"])
        session_count = len(analysis["sessions"])
        task_count = len(analysis["tasks"])
        link_count = len(analysis["task_dependencies"])
        scheduler_count = len(analysis["schedulers"])
        variable_count = len(analysis["workflow_variables"])

        if mapping_count > 0:
            mapping.append({
                "element": "MAPPING",
                "count": mapping_count,
                "pyspark_equivalent": "PySpark DataFrame Pipeline / Notebook",
                "notes": "Convert transformations to DataFrame operations with spark.read/write"
            })

        if workflow_count > 0:
            mapping.append({
                "element": "WORKFLOW",
                "count": workflow_count,
                "pyspark_equivalent": "Databricks Workflow / Airflow DAG",
                "notes": "Create job definition with name, retry policies, and scheduling"
            })

        if session_count > 0:
            mapping.append({
                "element": "SESSION",
                "count": session_count,
                "pyspark_equivalent": "PySpark Script/Notebook task",
                "notes": "Convert mapping to PySpark script. Use spark.read for sources, df.write for targets"
            })

        if link_count > 0:
            mapping.append({
                "element": "WORKFLOWLINK",
                "count": link_count,
                "pyspark_equivalent": "Task dependencies (>>)",
                "notes": "Define task1 >> task2 chains in Airflow or depends_on in Databricks"
            })

        if scheduler_count > 0:
            mapping.append({
                "element": "SCHEDULER",
                "count": scheduler_count,
                "pyspark_equivalent": "schedule_interval / Job trigger",
                "notes": "Convert schedule to cron expression or Databricks trigger"
            })

        if variable_count > 0:
            mapping.append({
                "element": "WORKFLOWVARIABLE",
                "count": variable_count,
                "pyspark_equivalent": "DAG variables / Job parameters",
                "notes": "Pass via config dict, env vars, or Airflow Variables"
            })

        for task in analysis["tasks"]:
            task_type = task["type"]
            existing = next((m for m in mapping if m["element"] == task_type), None)
            if existing:
                existing["count"] += 1
            else:
                mapping.append({
                    "element": task_type,
                    "count": 1,
                    "pyspark_equivalent": task["pyspark_equivalent"],
                    "notes": f"Custom implementation required for {task_type}"
                })

        return mapping

    def get_configs(self) -> List[Dict[str, Any]]:
        configs = []

        attr_descriptions = {
            "Constraint based load ordering": "Determines if target data loading follows referential integrity constraints",
            "Cache LOOKUP() function": "Enables caching for LOOKUP() function to improve performance",
            "Default buffer block size": "Size of memory buffer blocks for data processing",
            "Line Sequential buffer length": "Buffer size for line sequential file processing",
            "Maximum Memory Allowed For Auto Memory Attributes": "Maximum memory allocation for automatic memory settings",
            "Maximum Percentage of Total Memory Allowed for Auto Memory Attributes": "Max percentage of total memory for auto settings",
            "Additional Concurrent Pipelines for Lookup Cache Creation": "Extra pipelines for creating lookup caches",
            "Custom Properties": "User-defined custom configuration properties",
            "Override Tracing": "Override default tracing level for detailed debugging",
            "Pre-session shell command": "Shell command executed before session starts",
            "Post-session success shell command": "Shell command executed after successful session",
            "Post-session failure shell command": "Shell command executed after session failure",
            "Log Options": "Configuration for session logging",
            "Session Log File Name": "Name pattern for session log files",
            "Session Log File directory": "Directory path for session log files",
            "Parameter Filename": "Path to parameter file",
            "Enable High Precision": "Enable high precision for decimal/numeric calculations",
            "Treat source rows as": "How to treat incoming rows (Insert, Update, Delete, Data driven)",
            "Commit Type": "Source or Target based commit",
            "Commit Interval": "Number of rows between commits",
            "Commit On End Of File": "Commit remaining data when source EOF reached",
            "Rollback Transactions on Errors": "Whether to rollback on errors",
            "Recovery Strategy": "Resume from last checkpoint or restart",
            "Java Classpath": "Java classpath for JDBC drivers",
        }

        for config_elem in self.root.findall(".//CONFIG"):
            config_name = config_elem.get("NAME", "")
            config_desc = config_elem.get("DESCRIPTION", "")
            config_version = config_elem.get("VERSIONNUMBER", "")
            is_default = config_elem.get("ISDEFAULT", "NO") == "YES"

            attributes = []
            for attr_elem in config_elem.findall("ATTRIBUTE"):
                attr_name = attr_elem.get("NAME", "")
                attr_value = attr_elem.get("VALUE", "")

                description = attr_descriptions.get(attr_name, "")

                attributes.append({
                    "name": attr_name,
                    "value": attr_value,
                    "description": description
                })

            configs.append({
                "name": config_name,
                "description": config_desc,
                "is_default": is_default,
                "version": config_version,
                "attributes": attributes
            })

        return configs

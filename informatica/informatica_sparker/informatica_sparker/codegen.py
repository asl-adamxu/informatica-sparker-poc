import os
import re
from pathlib import Path
from typing import Dict, List, Any
import json
from jinja2 import Environment, FileSystemLoader, select_autoescape
from .ir import IRPlan, IRStep, IRStepType
import datetime
from .models import UserConfig, GeneratedFile, GenerationResult


class CodeGenerator:

    def __init__(self, template_dir: str = None):
        if template_dir is None:
            template_dir = str(Path(__file__).parent / "templates")
        self.template_dir = template_dir
        self.env = Environment(
            loader=FileSystemLoader(template_dir),
            autoescape=select_autoescape(['html', 'xml']),
            trim_blocks=True,
            lstrip_blocks=True
        )
        self.env.filters['topython'] = lambda v: json.dumps(v, indent=2).replace(
            'true', 'True').replace('false', 'False').replace('null', 'None')

    def generate(self, plan: IRPlan, user_config: UserConfig) -> List[GeneratedFile]:
        files = []

        mapping_content = self._generate_mapping(plan, user_config)
        safe_name = self._make_safe_name(plan.mapping_name)
        files.append(GeneratedFile(
            filename=f"{safe_name}.py",
            content=mapping_content,
            file_type="python"
        ))

        return files

    def _get_db_type_from_plan(self, plan: IRPlan) -> str:
        """Extract database type from plan steps."""
        for step in plan.steps:
            db_type = step.params.get("db_type", "")
            if db_type:
                return db_type
        return "oracle"

    def reset(self):
        pass

    def _make_safe_name(self, name: str) -> str:
        safe = re.sub(r'[^a-zA-Z0-9_]', '_', name)
        if safe[0].isdigit():
            safe = '_' + safe
        return safe.lower()

    def _generate_mapping(self, plan: IRPlan, user_config: UserConfig) -> str:
        try:
            template = self.env.get_template("mapping.py.j2")
            
            # Get database types from plan
            source_db_type = self._get_db_type_from_plan(plan)
            target_db_type = plan.target_db_type or source_db_type
            
            # Extract mapping variables from plan
            mapping_vars = dict(plan.mapping_variables)
            
            # Determine connection names from plan
            source_conn_name = "source"
            lookup_conn_name = ""
            target_conn_name = "target"
            
            # Collect source and target connection names
            hardcoded_defaults = {"source_db", "target_db", "default_conn", "lookup_conn", "lookup"}
            for step in plan.steps:
                conn_alias = step.params.get("connection_alias", "")
                if conn_alias:
                    if step.step_type in (IRStepType.READ_SQL, IRStepType.APPLY_SOURCE_QUALIFIER):
                        if not step.params.get("is_lookup", False):
                            if conn_alias.lower() not in hardcoded_defaults:
                                source_conn_name = conn_alias
                    elif step.step_type == IRStepType.WRITE_TARGET:
                        if conn_alias and conn_alias.lower() not in hardcoded_defaults:
                            target_conn_name = conn_alias
            
            # Resolve target connection: if still default "target", fall back to source or oracle-defaults
            if target_conn_name == "target" or not target_conn_name:
                target_conn_name = source_conn_name if source_conn_name != "source" else "oracle-defaults"
            
            # Resolve lookup connection: prefer its actual alias, only fall back if it's a hardcoded default
            hardcoded_defaults = {"lookup_conn", "lookup", "default_conn"}
            for step in plan.steps:
                conn_alias = step.params.get("connection_alias", "")
                if conn_alias and step.params.get("is_lookup", False):
                    if conn_alias.lower() in hardcoded_defaults:
                        lookup_conn_name = source_conn_name
                    else:
                        lookup_conn_name = conn_alias
                    break
            if not lookup_conn_name:
                lookup_conn_name = source_conn_name
            
            return template.render(
                mapping_name=plan.mapping_name,
                steps=plan.steps,
                lookup_dfs=plan.lookup_dfs,
                user_config=user_config,
                IRStepType=IRStepType,
                source_db_type=source_db_type,
                target_db_type=target_db_type,
                source_conn_name=source_conn_name,
                lookup_conn_name=lookup_conn_name,
                target_conn_name=target_conn_name,
                mapping_variables=mapping_vars
                , generation_date=datetime.date.today().isoformat()
            )
        except Exception as e:
            import traceback
            print(f"Template render failed: {e}")
            traceback.print_exc()
            return self._generate_mapping_fallback(plan, user_config)

    def _generate_mapping_fallback(self, plan: IRPlan, user_config: UserConfig) -> str:
        mapping_name = plan.mapping_name
        safe_name = self._make_safe_name(mapping_name)

        fallback_code = f'''
\'\'\'
* Script - {mapping_name}.py
* @date    Generated by Informatica XML to PySpark Converter
* @version 1.0
* @par Remarks
*   This code is auto-generated. Please review the business rules before productionize.
\'\'\'

import pyspark
from pyspark import SparkConf, SQLContext, SparkContext, StorageLevel
from pyspark.sql import SparkSession, DataFrame, Window, Row
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import pyspark.sql.functions as f
import pyspark.sql.types as t
import datetime
import time
import os
import sys
import re
import yaml
from functools import reduce
from typing import Dict, Any, List, Optional
from os.path import join, abspath

# ============================================================================
# CONFIGURATION LOADER
# ============================================================================

def resolve_env_vars(value):
    """Resolve environment variable placeholders in config values.

    Supports ${{VAR_NAME}} and ${{VAR_NAME:default_value}} syntax.
    """
    if isinstance(value, str):
        pattern = r'\\$\\{{([^}}:]+)(?::([^}}]*))?\\}}'

        def replace_var(match):
            var_name = match.group(1)
            default_value = match.group(2) if match.group(2) is not None else ''
            return os.environ.get(var_name, default_value)

        return re.sub(pattern, replace_var, value)
    elif isinstance(value, dict):
        return {{k: resolve_env_vars(v) for k, v in value.items()}}
    elif isinstance(value, list):
        return [resolve_env_vars(item) for item in value]
    return value


def load_config(config_path=None):
    """Load configuration from YAML file with environment variable resolution."""
    if config_path is None:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        config_path = os.environ.get('CONFIG_PATH', os.path.join(script_dir, 'config_{safe_name}.yml'))

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found: {{config_path}}")

    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)

    return resolve_env_vars(config)


# Load configuration
config = load_config()

# ============================================================================
# CONNECTION DETAILS FROM CONFIG
# ============================================================================

CONNECTION_DB_MAPPING = {{
    "CDM_PRE_LANDING": "msscdm_dev",
    "CDM_PRE_LANDING_INV": "msscdm_inv",
    "CDM_LANDING": "cmx_ors_10_3",
    "CDM_LANDING_INV": "cmx_ors_inv"
}}

server_name = config.get('connections', {{}}).get('CDM_PRE_LANDING', {{}}).get('host', '${{MSSQL_HOST}}')
user = config.get('connections', {{}}).get('CDM_PRE_LANDING', {{}}).get('user', '${{MSSQL_USER}}')
password = config.get('connections', {{}}).get('CDM_PRE_LANDING', {{}}).get('password', '${{MSSQL_PASSWORD}}')

database_name_source = config.get('connections', {{}}).get('CDM_PRE_LANDING', {{}}).get('database', 'msscdm_dev')
database_name_lkp = config.get('connections', {{}}).get('CDM_PRE_LANDING', {{}}).get('database', 'msscdm_dev')
database_name_target = "msscdm_dev3"

jdbc_url_source = f"jdbc:sqlserver://{{server_name}}; databaseName={{database_name_source}}; user={{user}}; password={{password}}; trustServerCertificate=true; encrypt=false"
jdbc_url_lkp = f"jdbc:sqlserver://{{server_name}}; databaseName={{database_name_lkp}}; user={{user}}; password={{password}}; trustServerCertificate=true; encrypt=false"
jdbc_url_target = f"jdbc:sqlserver://{{server_name}}; databaseName={{database_name_target}}; user={{user}}; password={{password}}; trustServerCertificate=true; encrypt=false"

jdbc_driver_path = config.get('connections', {{}}).get('CDM_PRE_LANDING', {{}}).get('driver_jar', '${{MSSQL_DRIVER_JAR:/opt/drivers/mssql-jdbc-12.4.2.jre11.jar}}')

mssql_driver = 'com.microsoft.sqlserver.jdbc.SQLServerDriver'
mssql_fetchsize = '10000'
target_mode = 'append'

params = config.get('params', {{}})
SRC_SYSTEM_NM = params.get('SRC_SYSTEM_NM', 'SSC')

spark_config = config.get('spark', {{}}).get('configs', {{}})

# ============================================================================
# SPARK SESSION INITIALIZATION
# ============================================================================

spark_builder = SparkSession.builder.appName(config.get('spark', {{}}).get('app_name', '{mapping_name}'))

for key, value in spark_config.items():
    spark_builder = spark_builder.config(key, str(value))

spark = spark_builder.getOrCreate()

PMMappingName = "{mapping_name}_pyspark"
PMWorkflowName = "wf_{mapping_name}"


# ============================================================================
# JDBC Helper Functions
# ============================================================================
MSSQL_DRIVER = mssql_driver

def read_mssql_query(query: str) -> DataFrame:
    """Read data from MSSQL using a SQL query."""
    return (spark.read.format("jdbc")
        .option("url", jdbc_url_source)
        .option("driver", MSSQL_DRIVER)
        .option("user", user)
        .option("password", password)
        .option("query", query)
        .option("fetchsize", mssql_fetchsize)
        .load()
    )

def read_mssql_table(table: str) -> DataFrame:
    """Read entire table from MSSQL."""
    return (spark.read.format("jdbc")
        .option("url", jdbc_url_source)
        .option("driver", MSSQL_DRIVER)
        .option("user", user)
        .option("password", password)
        .option("dbtable", table)
        .option("fetchsize", mssql_fetchsize)
        .load()
    )

def read_mssql_lkp_query(query: str) -> DataFrame:
    """Read data from MSSQL lookup database using a SQL query."""
    return (spark.read.format("jdbc")
        .option("url", jdbc_url_lkp)
        .option("driver", MSSQL_DRIVER)
        .option("user", user)
        .option("password", password)
        .option("query", query)
        .option("fetchsize", mssql_fetchsize)
        .load()
    )

def write_mssql_target(df: DataFrame, table: str, mode: str = None):
    """Write dataframe to MSSQL target (msscdm_dev3)."""
    write_mode = mode if mode else target_mode
    (df.write.format("jdbc")
        .option("url", jdbc_url_target)
        .option("dbtable", table)
        .option("user", user)
        .option("password", password)
        .option("driver", MSSQL_DRIVER)
        .mode(write_mode)
        .save()
    )

# ============================================================================
# Lookup Helper Function
# ============================================================================
def lkp_id(df: DataFrame, lkp_df: DataFrame, src_col: str, attrib_nm: str, out_col: str, default_val=9999):
    l = (lkp_df
        .filter(col("attrib_nm") == lit(attrib_nm))
        .select(trim(col("src_val")).alias("lk_src"), col("std_lkup_id").cast("int").alias("lk_id"))
    )
    joined = df.join(broadcast(l), trim(col(src_col)) == col("lk_src"), "left")
    return (joined
        .withColumn(out_col, coalesce(col("lk_id"), lit(default_val)))
        .drop("lk_src", "lk_id")
    )


try:
'''

        for step in plan.steps:
            step_lines = self._generate_step_code_v2(step)
            for line in step_lines:
                fallback_code += '\t' + line + '\n'
            fallback_code += '\n'

        fallback_code += '''
    print(f"Mapping {PMMappingName} completed successfully")

except Exception as e:
    print(f"Mapping {PMMappingName} failed with error: {e}")
    raise

finally:
    spark.stop()
'''
        return fallback_code

    def _generate_step_code_v2(self, step: IRStep) -> List[str]:
        lines = []

        if step.comments:
            for comment in step.comments:
                lines.append(f'# {comment}')

        if step.step_type == IRStepType.READ_SQL:
            table = step.params.get("table_name", "")
            query = step.params.get("query", "")
            conn_name = step.params.get("connection_alias", "source")
            db_type = step.params.get("db_type", "oracle")
            is_lookup = step.params.get("is_lookup", False)
            
            lines.append(f'# Reading Data From Source - {step.step_name or table}')
            lines.append(f'# Description : {"Lookup" if is_lookup else "Relational Reader"}')
            lines.append(f'# Connection : {conn_name} ({db_type})')
            lines.append('')

            if query:
                lines.append(f'{step.df_output} = read_sql(spark, {conn_name}_conn, query="""')
                lines.append(f'{query.strip()}')
                lines.append(f'""")')
            else:
                lines.append(f'{step.df_output} = read_sql(spark, {conn_name}_conn, table="{table}")')

            lines.append(f'print("Data Count {step.df_output}:", {step.df_output}.count())')

        elif step.step_type == IRStepType.APPLY_SOURCE_QUALIFIER:
            sql_query = step.params.get("sql_query", "")
            use_sql_override = step.params.get("use_sql_override", False)

            lines.append(f'# Reading Data From Source - {step.step_name}')
            lines.append(f'# Description : Relational Reader (Source Qualifier)')
            lines.append('')

            if use_sql_override and sql_query:
                lines.append(f'{step.df_output} = read_mssql_query("""')
                lines.append(f'{sql_query.strip()}')
                lines.append(f'""")')
            else:
                lines.append(f'{step.df_output} = {step.df_input}')

            lines.append(f'print("Source Data Count {step.df_output}:", {step.df_output}.count())')

        elif step.step_type == IRStepType.APPLY_FILTER:
            cond = step.params.get("condition", "lit(True)")
            orig_cond = step.params.get("original_condition", "")

            lines.append(f'# Filter Transformation - {step.step_name}')
            if orig_cond:
                lines.append(f'# Original: {orig_cond}')
            lines.append('')

            lines.append(f'{step.df_output} = {step.df_input}.filter({cond})')

        elif step.step_type == IRStepType.APPLY_EXPRESSION:
            lines.append(f'# Expression Transformation - {step.step_name}')
            lines.append('')

            lines.append(f'{step.df_output} = {step.df_input}')

            computed = step.params.get("computed_columns", [])
            if computed:
                for col_def in computed:
                    name = col_def.get("name", "col")
                    expr_val = col_def.get("expression", "")
                    col_type = col_def.get("col_type", "")

                    if not expr_val:
                        lines.append(f'{step.df_output} = {step.df_output}.withColumn("{name}", col("{name}"))')
                    elif expr_val.lower().startswith('col(') or expr_val.lower().startswith('lit(') or expr_val.lower().startswith('expr('):
                        lines.append(f'{step.df_output} = {step.df_output}.withColumn("{name}", {expr_val})')
                    elif self._is_global_variable(expr_val):
                        lines.append(f'{step.df_output} = {step.df_output}.withColumn("{name}", lit({expr_val}))')
                    elif col_type == "passthrough" or self._is_passthrough_column(expr_val):
                        col_name = self._get_column_name(expr_val)
                        lines.append(f'{step.df_output} = {step.df_output}.withColumn("{name}", col("{col_name}"))')
                    elif col_type == "literal" or self._is_literal(expr_val):
                        lit_val = self._format_literal(expr_val)
                        lines.append(f'{step.df_output} = {step.df_output}.withColumn("{name}", lit({lit_val}))')
                    elif col_type == "sql" or self._is_sql_expression(expr_val):
                        escaped_expr = expr_val.replace('"', '\\"')
                        lines.append(f'{step.df_output} = {step.df_output}.withColumn("{name}", expr("{escaped_expr}"))')
                    else:
                        col_name = self._get_column_name(expr_val)
                        lines.append(f'{step.df_output} = {step.df_output}.withColumn("{name}", col("{col_name}"))')

        elif step.step_type == IRStepType.APPLY_ROUTER:
            lines.append(f'# Router Transformation - {step.step_name}')
            lines.append(f'# Splits data into multiple output groups based on conditions')
            lines.append('')

            groups = step.params.get("groups", [])
            if groups:
                for group in groups:
                    group_name = group.get("name", "default")
                    condition = group.get("condition", "")
                    df_out = group.get("df_output", f"df_{group_name}")

                    if condition and condition.upper() != "DEFAULT":
                        lines.append(f'# Group: {group_name}')
                        lines.append(f'{df_out} = {step.df_input}.filter({condition})')
                    else:
                        lines.append(f'# Default Group: {group_name}')
                        prev_conditions = [g.get("condition", "") for g in groups if g.get("condition", "") and g.get("condition", "").upper() != "DEFAULT"]
                        if prev_conditions:
                            neg_conds = ' & '.join([f'~({c})' for c in prev_conditions])
                            lines.append(f'{df_out} = {step.df_input}.filter({neg_conds})')
                        else:
                            lines.append(f'{df_out} = {step.df_input}')
            else:
                lines.append(f'# TODO: Define router group conditions')
                lines.append(f'{step.df_output} = {step.df_input}')

        elif step.step_type == IRStepType.APPLY_LOOKUP:
            lookup_df = step.params.get("lookup_df", "df_lkp")
            join_predicates = step.params.get("join_predicates", [])
            result_cols = step.params.get("result_columns", [])
            attrib_nm = step.params.get("attrib_nm", "")
            default_val = step.params.get("default_value", 9999)

            lines.append(f'# Lookup Transformation - {step.step_name}')
            lines.append('')

            if attrib_nm and join_predicates:
                src_col = join_predicates[0].get("source_col", "") if join_predicates else ""
                out_col = result_cols[0] if result_cols else "lookup_result"
                default_str = str(default_val) if default_val is not None else "None"
                lines.append(f'{step.df_output} = lkp_id({step.df_input}, {lookup_df}, "{src_col}", "{attrib_nm}", "{out_col}", {default_str})')
            elif join_predicates:
                join_parts = []
                for jp in join_predicates:
                    src_col = jp.get("source_col", "")
                    lkp_col = jp.get("lookup_col", "")
                    join_parts.append(f'(trim({step.df_input}["{src_col}"]) == trim({lookup_df}["{lkp_col}"]))')
                join_cond = ' & '.join(join_parts)
                lines.append(f'{step.df_output} = {step.df_input}.join(broadcast({lookup_df}), {join_cond}, "left")')
            else:
                lines.append(f'# TODO: Define lookup join condition')
                lines.append(f'{step.df_output} = {step.df_input}.join(broadcast({lookup_df}), lit(True), "left")')

        elif step.step_type == IRStepType.APPLY_JOINER:
            master = step.params.get("df_master", step.df_input)
            detail = step.params.get("df_detail", "df_detail")
            join_cond = step.params.get("raw_condition", "")
            join_type = step.params.get("join_type", "inner")

            lines.append(f'# Joiner Transformation - {step.step_name}')
            lines.append('')

            if join_cond:
                lines.append(f'{step.df_output} = {master}.join({detail}, {join_cond}, "{join_type}")')
            else:
                lines.append(f'{step.df_output} = {master}.join({detail}, lit(True), "{join_type}")')

        elif step.step_type == IRStepType.APPLY_AGGREGATOR:
            group_cols = step.params.get("group_by", [])
            agg_cols = step.params.get("aggregations", [])

            lines.append(f'# Aggregator Transformation - {step.step_name}')
            lines.append('')

            if group_cols:
                group_str = ', '.join([f'"{c}"' for c in group_cols])
                if agg_cols:
                    agg_parts = []
                    for agg in agg_cols:
                        agg_func = agg.get("function", "first")
                        agg_col = agg.get("column", "")
                        alias = agg.get("alias", agg_col)
                        agg_parts.append(f'{agg_func}("{agg_col}").alias("{alias}")')
                    agg_str = ', '.join(agg_parts)
                    lines.append(f'{step.df_output} = {step.df_input}.groupBy({group_str}).agg({agg_str})')
                else:
                    lines.append(f'{step.df_output} = {step.df_input}.groupBy({group_str}).count()')
            else:
                lines.append(f'{step.df_output} = {step.df_input}')

        elif step.step_type == IRStepType.APPLY_UNION:
            union_dfs = step.params.get("dataframes", [])

            lines.append(f'# Union Transformation - {step.step_name}')
            lines.append('')

            if union_dfs:
                union_str = '.unionByName('.join(union_dfs) + ')' * (len(union_dfs) - 1)
                lines.append(f'{step.df_output} = {union_str}')
            else:
                lines.append(f'{step.df_output} = {step.df_input}')

        elif step.step_type == IRStepType.WRITE_TARGET:
            table = step.params.get("table_name", "target_table")
            mode = step.params.get("write_mode", "append")
            target_cols = step.params.get("target_columns", [])

            lines.append(f'# Target - {step.step_name or table}')
            lines.append(f'# Writing to target database')
            lines.append('')

            if target_cols:
                cols_str = ', '.join([f'"{c}"' for c in target_cols])
                lines.append(f'# Select exact target columns in order')
                lines.append(f'df_target_out = {step.df_input}.select({cols_str})')
                lines.append(f'print("Target Data Count for {table}:", df_target_out.count())')
                lines.append(f'write_mssql_target(df_target_out, "{table}", "{mode}")')
            else:
                lines.append(f'print("Target Data Count for {table}:", {step.df_input}.count())')
                lines.append(f'write_mssql_target({step.df_input}, "{table}", "{mode}")')

        else:
            lines.append(f'# {step.step_type.name}: {step.step_name}')
            lines.append(f'{step.df_output} = {step.df_input}')

        return lines

    def _is_sql_expression(self, expr_val: str) -> bool:
        sql_keywords = ['CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'DECODE', 'IIF', 'ISNULL',
                       'COALESCE', 'UPPER', 'LOWER', 'TRIM', 'LTRIM', 'RTRIM', 'CONCAT',
                       'SUBSTRING', 'IN(', 'NOT IN', 'BETWEEN', 'LIKE', 'AND', 'OR']
        expr_upper = expr_val.upper()
        return any(kw in expr_upper for kw in sql_keywords)

    def _is_global_variable(self, expr_val: str) -> bool:
        if not expr_val:
            return False
        cleaned = expr_val.strip()
        global_vars = ['PMMappingName', 'PMWorkflowName', 'PMSessionName', 'PMFolderName',
                       'PMIntegrationService', 'PMRepositoryService', 'PMRepositoryUserName',
                       'PMTargetDBName', 'PMSourceDBName', 'SYSDATE', 'SYSTIMESTAMP', 'SESSSTARTTIME']
        if cleaned in global_vars:
            return True
        if cleaned.startswith('$$') or cleaned.startswith('$PM'):
            return True
        return False

    def _is_passthrough_column(self, expr_val: str) -> bool:
        if not expr_val:
            return False
        if self._is_global_variable(expr_val):
            return False
        cleaned = expr_val.strip()
        if '.' in cleaned:
            parts = cleaned.rsplit('.', 1)
            if len(parts) == 2 and re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', parts[1]):
                return True
        if re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', cleaned):
            return True
        return False

    def _get_column_name(self, expr_val: str) -> str:
        if not expr_val:
            return expr_val
        cleaned = expr_val.strip()
        if '.' in cleaned:
            return cleaned.rsplit('.', 1)[-1]
        return cleaned

    def _is_literal(self, expr_val: str) -> bool:
        if not expr_val:
            return False
        cleaned = expr_val.strip()
        if cleaned.startswith("'") and cleaned.endswith("'"):
            return True
        if cleaned.startswith('"') and cleaned.endswith('"'):
            return True
        if cleaned.lower() in ('true', 'false', 'null', 'none'):
            return True
        try:
            float(cleaned)
            return True
        except ValueError:
            pass
        return False

    def _format_literal(self, expr_val: str) -> str:
        cleaned = expr_val.strip()
        if cleaned.startswith("'") and cleaned.endswith("'"):
            return cleaned
        if cleaned.startswith('"') and cleaned.endswith('"'):
            inner = cleaned[1:-1]
            return f'"{inner}"'
        if cleaned.lower() in ('true', 'false'):
            return cleaned.capitalize()
        if cleaned.lower() in ('null', 'none'):
            return 'None'
        try:
            if '.' in cleaned:
                return str(float(cleaned))
            return str(int(cleaned))
        except ValueError:
            return f'"{cleaned}"'

    def _generate_step_code(self, step: IRStep) -> List[str]:
        lines = []

        if step.comments:
            for comment in step.comments:
                lines.append(f'    # {comment}')

        if step.step_type == IRStepType.READ_SQL:
            conn = step.params.get("connection_alias", "source_db")
            table = step.params.get("table_name", "")
            query = step.params.get("query", "")

            lines.append(f'    # Read from SQL: {table or "custom query"}')
            lines.append(f'    conn_config = ctx.get_connection("{conn}")')

            if query:
                lines.append(f'    {step.df_output} = read_sql(spark, conn_config, query="""')
                lines.append(f'{query}')
                lines.append(f'    """)')
            else:
                lines.append(f'    {step.df_output} = read_sql(spark, conn_config, table="{table}")')
            lines.append(f'    ctx.register_df("{step.df_output}", {step.df_output})')

        elif step.step_type == IRStepType.READ_FILE:
            fmt = step.params.get("file_format", "csv")
            path = step.params.get("file_path", "")
            options = step.params.get("options", {})

            lines.append(f'    # Read from file: {path}')
            lines.append(f'    {step.df_output} = read_file(spark, "{path}", format="{fmt}", options={options})')
            lines.append(f'    ctx.register_df("{step.df_output}", {step.df_output})')

        elif step.step_type == IRStepType.APPLY_SOURCE_QUALIFIER:
            lines.append(f'    # Source Qualifier: {step.step_name}')
            sql_query = step.params.get("sql_query", "")
            use_sql_override = step.params.get("use_sql_override", False)
            conn_alias = step.params.get("connection_alias", "source_db")
            output_columns = step.params.get("output_columns", [])
            filter_cond = step.params.get("filter_condition", "")
            distinct = step.params.get("distinct", False)

            if use_sql_override and sql_query:
                lines.append(f'    conn_config = ctx.get_connection("{conn_alias}")')
                lines.append(f'    {step.df_output} = read_sql(spark, conn_config, query="""')
                for sql_line in sql_query.strip().split('\n'):
                    lines.append(f'        {sql_line}')
                lines.append(f'    """)')
            else:
                lines.append(f'    {step.df_output} = {step.df_input}')
                if filter_cond:
                    lines.append(f'    {step.df_output} = {step.df_output}.filter({filter_cond})')
                if distinct:
                    lines.append(f'    {step.df_output} = {step.df_output}.distinct()')

            if output_columns:
                cols_str = ', '.join(f'"{c}"' for c in output_columns)
                lines.append(f'    {step.df_output} = {step.df_output}.select({cols_str})')

            lines.append(f'    ctx.register_df("{step.df_output}", {step.df_output})')

        elif step.step_type == IRStepType.APPLY_FILTER:
            cond = step.params.get("condition", "True")
            orig_cond = step.params.get("original_condition", "")
            lines.append(f'    # Filter: {step.step_name}')
            if orig_cond:
                lines.append(f'    # Original: {orig_cond}')
            lines.append(f'    {step.df_output} = {step.df_input}.filter({cond})')
            lines.append(f'    ctx.register_df("{step.df_output}", {step.df_output})')

        elif step.step_type == IRStepType.APPLY_EXPRESSION:
            lines.append(f'    # Expression: {step.step_name}')
            lines.append(f'    {step.df_output} = {step.df_input}')
            computed = step.params.get("computed_columns", [])
            for col_item in computed:
                name = col_item.get("name", "col")
                expr_val = col_item.get("expression", "None")
                lines.append(f'    {step.df_output} = {step.df_output}.withColumn("{name}", expr("{expr_val}"))')
            lines.append(f'    ctx.register_df("{step.df_output}", {step.df_output})')

        elif step.step_type == IRStepType.APPLY_LOOKUP:
            lines.append(f'    # Lookup: {step.step_name}')
            lookup_df = step.params.get("lookup_df", "df_lookup")
            join_cond = step.params.get("join_condition", "")
            lookup_type = step.params.get("lookup_type", "left")

            if join_cond:
                lines.append(f'    {step.df_output} = {step.df_input}.join(broadcast({lookup_df}), {join_cond}, "{lookup_type}")')
            else:
                lines.append(f'    {step.df_output} = {step.df_input}.join(broadcast({lookup_df}), how="{lookup_type}")')
            lines.append(f'    ctx.register_df("{step.df_output}", {step.df_output})')

        elif step.step_type == IRStepType.APPLY_JOINER:
            lines.append(f'    # Joiner: {step.step_name}')
            master = step.params.get("df_master", step.df_input)
            detail = step.params.get("df_detail", "df_detail")
            join_cond = step.params.get("join_condition", "")
            join_type = step.params.get("join_type", "inner")
            if join_cond:
                lines.append(f'    {step.df_output} = {master}.join({detail}, {join_cond}, "{join_type}")')
            else:
                lines.append(f'    {step.df_output} = {master}.join({detail}, how="{join_type}")')
            lines.append(f'    ctx.register_df("{step.df_output}", {step.df_output})')

        elif step.step_type == IRStepType.APPLY_AGGREGATOR:
            lines.append(f'    # Aggregator: {step.step_name}')
            group_by = step.params.get("group_by", [])
            aggs = step.params.get("aggregations", {})
            if group_by or aggs:
                if group_by:
                    gb_str = ', '.join([f'"{c}"' for c in group_by])
                    lines.append(f'    {step.df_output} = {step.df_input}.groupBy({gb_str})')
                else:
                    lines.append(f'    {step.df_output} = {step.df_input}.groupBy()')
                if aggs:
                    agg_exprs = ', '.join([f'{v}.alias("{k}")' for k, v in aggs.items()])
                    lines.append(f'    {step.df_output} = {step.df_output}.agg({agg_exprs})')
            else:
                lines.append(f'    {step.df_output} = {step.df_input}')
            lines.append(f'    ctx.register_df("{step.df_output}", {step.df_output})')

        elif step.step_type == IRStepType.APPLY_SORTER:
            lines.append(f'    # Sorter: {step.step_name}')
            sort_cols = step.params.get("sort_columns", [])
            if sort_cols:
                sort_exprs = []
                for sc in sort_cols:
                    col_name = sc.get("column", "")
                    direction = sc.get("direction", "ASC")
                    if direction.upper() == "DESC":
                        sort_exprs.append(f'desc("{col_name}")')
                    else:
                        sort_exprs.append(f'asc("{col_name}")')
                lines.append(f'    {step.df_output} = {step.df_input}.orderBy({", ".join(sort_exprs)})')
            else:
                lines.append(f'    {step.df_output} = {step.df_input}')
            lines.append(f'    ctx.register_df("{step.df_output}", {step.df_output})')

        elif step.step_type == IRStepType.APPLY_UNION:
            lines.append(f'    # Union: {step.step_name}')
            df_inputs = step.params.get("df_inputs", [])
            flag_column = step.params.get("flag_column", "")
            output_columns = step.params.get("output_columns", [])

            if len(df_inputs) >= 2:
                if flag_column:
                    normalized_dfs = [f'{df}_norm' for df in df_inputs]
                    for i, df in enumerate(df_inputs):
                        lines.append(f'    {normalized_dfs[i]} = normalize_flag_column({df}, "{flag_column}")')
                    lines.append(f'    {step.df_output} = {normalized_dfs[0]}')
                    for ndf in normalized_dfs[1:]:
                        lines.append(f'    {step.df_output} = {step.df_output}.unionByName({ndf}, allowMissingColumns=True)')
                else:
                    lines.append(f'    {step.df_output} = {df_inputs[0]}')
                    for df in df_inputs[1:]:
                        lines.append(f'    {step.df_output} = {step.df_output}.unionByName({df}, allowMissingColumns=True)')
            else:
                lines.append(f'    {step.df_output} = {step.df_input}')

            if output_columns:
                cols_str = ', '.join(f'"{c}"' for c in output_columns)
                lines.append(f'    {step.df_output} = {step.df_output}.select({cols_str})')
            lines.append(f'    ctx.register_df("{step.df_output}", {step.df_output})')

        elif step.step_type == IRStepType.APPLY_ROUTER:
            lines.append(f'    # Router: {step.step_name}')
            groups = step.params.get("groups", [])
            for group in groups:
                g_name = group.get("name", "GROUP")
                g_output = group.get("df_output", "df_out")
                g_cond = group.get("condition", "")
                if g_cond:
                    lines.append(f'    {g_output} = {step.df_input}.filter({g_cond})')
                else:
                    lines.append(f'    {g_output} = {step.df_input}')
                lines.append(f'    ctx.register_df("{g_output}", {g_output})')

        elif step.step_type == IRStepType.APPLY_SEQUENCE:
            lines.append(f'    # Sequence Generator: {step.step_name}')
            seq_name = step.params.get("sequence_name", "NEXTVAL")
            start_val = step.params.get("start_value", 1)
            lines.append(f'    {step.df_output} = {step.df_input}.withColumn("{seq_name}", monotonically_increasing_id() + {start_val})')
            lines.append(f'    ctx.register_df("{step.df_output}", {step.df_output})')

        elif step.step_type == IRStepType.APPLY_UPDATE_STRATEGY:
            lines.append(f'    # Update Strategy: {step.step_name}')
            strategy = step.params.get("strategy_expression", "DD_INSERT")
            needs_merge = step.params.get("needs_merge", False)
            lines.append(f'    # Strategy: {strategy}')
            if needs_merge:
                lines.append(f'    {step.df_output} = {step.df_input}.withColumn("_update_flag",')
                lines.append(f'        when(col("_needs_update"), lit("U"))')
                lines.append(f'        .when(col("_needs_delete"), lit("D"))')
                lines.append(f'        .otherwise(lit("I"))')
                lines.append(f'    )')
            else:
                lines.append(f'    {step.df_output} = {step.df_input}')
            lines.append(f'    ctx.register_df("{step.df_output}", {step.df_output})')

        elif step.step_type == IRStepType.EXECUTE_SQL:
            lines.append(f'    # Execute SQL: {step.step_name}')
            sql_type = step.params.get("sql_type", "pre")
            sql_stmt = step.params.get("sql_statement", "")
            conn_alias = step.params.get("connection_alias", "target_db")
            lines.append(f'    conn_config = ctx.get_connection("{conn_alias}")')
            lines.append(f'    execute_sql_statement(spark, conn_config, """{sql_stmt}""")')

        elif step.step_type == IRStepType.WRITE_TARGET:
            lines.append(f'    # Write to Target: {step.step_name}')
            sink_type = step.params.get("sink_type", "delta")
            path = step.params.get("path", "")
            table_name = step.params.get("table_name", "")
            mode = step.params.get("mode", "append")
            conn_alias = step.params.get("connection_alias", "target_db")
            unmapped_columns = step.params.get("unmapped_columns", [])
            drop_columns = step.params.get("drop_columns", [])
            target_columns = step.params.get("target_columns", [])
            post_sql = step.params.get("post_sql", "")

            lines.append(f'    df_write = {step.df_input}')

            if drop_columns:
                for col_item in drop_columns:
                    lines.append(f'    if "{col_item}" in df_write.columns:')
                    lines.append(f'        df_write = df_write.drop("{col_item}")')

            if unmapped_columns:
                for col_item in unmapped_columns:
                    lines.append(f'    df_write = df_write.withColumn("{col_item}", lit(None))')

            if target_columns:
                lines.append(f'    target_col_map = {{c.lower(): c for c in {target_columns}}}')
                lines.append(f'    for c in df_write.columns:')
                lines.append(f'        if c.lower() in target_col_map and c != target_col_map[c.lower()]:')
                lines.append(f'            df_write = df_write.withColumnRenamed(c, target_col_map[c.lower()])')
                lines.append(f'    df_write = df_write.select(*[col for col in {target_columns} if col in df_write.columns])')

            if sink_type == "jdbc":
                lines.append(f'    conn_config = ctx.get_connection("{conn_alias}")')
                lines.append(f'    write_sql(df_write, conn_config, "{table_name}", mode="{mode}")')
            elif path:
                lines.append(f'    write_file(df_write, "{path}", format="{sink_type}", mode="{mode}")')
            elif table_name:
                lines.append(f'    df_write.write.format("{sink_type}").mode("{mode}").saveAsTable("{table_name}")')
            else:
                lines.append(f'    write_file(df_write, "output/{table_name}", format="{sink_type}", mode="{mode}")')

            if post_sql:
                lines.append(f'    conn_config = ctx.get_connection("{conn_alias}")')
                lines.append(f'    execute_sql_statement(spark, conn_config, """{post_sql}""")')

        if step.warnings:
            for warning in step.warnings:
                lines.append(f'    # WARNING: {warning}')

        return lines

    def _generate_config(self, plan: IRPlan, user_config: UserConfig) -> str:
        try:
            template = self.env.get_template("config.yml.j2")
            return template.render(
                mapping_name=plan.mapping_name,
                user_config=user_config
            )
        except Exception:
            return self._generate_config_fallback(plan, user_config)

    def _generate_config_fallback(self, plan: IRPlan, user_config: UserConfig) -> str:
        lines = []
        lines.append(f'# Configuration for {plan.mapping_name}')
        lines.append('')
        lines.append('environment:')
        lines.append('  name: ${ENV_NAME:development}')
        lines.append('  log_level: ${LOG_LEVEL:INFO}')
        lines.append('')
        lines.append('spark:')
        lines.append(f'  app_name: "{plan.mapping_name}"')
        lines.append('  master: ${SPARK_MASTER:local[*]}')
        lines.append('')
        lines.append('connections:')

        for conn_name, conn_config in user_config.db_connections.items():
            lines.append(f'  {conn_name}:')
            for key, value in conn_config.items():
                lines.append(f'    {key}: "{value}"')

        if not user_config.db_connections:
            lines.append('  CDM_PRE_LANDING:')
            lines.append('    db_type: "sqlserver"')
            lines.append('    host: "${SOURCE_DB_HOST}"')
            lines.append('    port: 1433')
            lines.append('    database: "msscdm_dev"')
            lines.append('    user: "${SOURCE_DB_USER}"')
            lines.append('    password: "${SOURCE_DB_PASSWORD}"')

        lines.append('')
        lines.append('paths:')
        lines.append('  input_base: "${INPUT_PATH:/tmp}"')
        lines.append('  output_base: "${OUTPUT_PATH:/tmp}"')
        lines.append('')
        lines.append('options:')
        lines.append('  use_delta_format: true')
        lines.append('  broadcast_lookups: true')

        return '\n'.join(lines)

    def generate_workflow_orchestration(self, workflow_name: str,
                                         mappings: List[Dict[str, Any]],
                                         sessions: List[Dict[str, Any]],
                                         dependencies: Dict[str, List[str]],
                                         execution_order: List[str]) -> str:
        try:
            template = self.env.get_template("workflow_orchestration.py.j2")

            mapping_info = []
            for mapping in mappings:
                name = mapping.get("name", "")
                safe_name = self._make_safe_name(name)
                mapping_info.append({
                    "name": name,
                    "safe_name": safe_name,
                    "module_name": f"mapping_{safe_name}"
                })

            return template.render(
                workflow_name=workflow_name,
                mappings=mapping_info,
                sessions=sessions,
                dependencies=dependencies,
                execution_order=execution_order
            )
        except Exception:
            return self._generate_orchestration_fallback(
                workflow_name, mappings, sessions, dependencies, execution_order
            )

    def _generate_orchestration_fallback(self, workflow_name: str,
                                          mappings: List[Dict[str, Any]],
                                          sessions: List[Dict[str, Any]],
                                          dependencies: Dict[str, List[str]],
                                          execution_order: List[str]) -> str:
        lines = []
        lines.append('"""')
        lines.append(f'Workflow Orchestration: {workflow_name}')
        lines.append('"""')
        lines.append('import sys')
        lines.append('')

        for mapping in mappings:
            name = mapping.get("name", "")
            safe_name = self._make_safe_name(name)
            lines.append(f'from mapping_{safe_name} import run_mapping as run_{safe_name}')

        lines.append('')
        lines.append(f'EXECUTION_ORDER = {execution_order}')
        lines.append('')
        lines.append('def run_workflow():')
        lines.append('    try:')
        lines.append('        for session in EXECUTION_ORDER:')
        lines.append('            print(f"Executing: {session}")')
        lines.append('        return True')
        lines.append('    except Exception as e:')
        lines.append('        print(f"Workflow failed: {e}")')
        lines.append('        return False')
        lines.append('')
        lines.append('if __name__ == "__main__":')
        lines.append('    success = run_workflow()')
        lines.append('    sys.exit(0 if success else 1)')

        return '\n'.join(lines)

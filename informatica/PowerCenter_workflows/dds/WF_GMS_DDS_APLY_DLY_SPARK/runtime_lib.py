"""
Runtime library for PySpark jobs.
Provides helper functions for database connections, file reading, transformations, and metrics.
"""
import logging
import os
from datetime import datetime
from typing import Dict, Any, Optional, List

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
import yaml


def get_db_config(config: Dict[str, Any], conn_name: str = "source") -> Dict[str, Any]:
    """Get database connection configuration by name."""
    return config.get("connections", {}).get(conn_name, config.get("connections", {}).get("source", {}))


def get_jdbc_url(conn_config: Dict[str, Any]) -> str:
    """Build JDBC URL from connection configuration."""
    db_type = conn_config.get("type", "oracle").lower()
    host = conn_config.get("host", "localhost")
    port = conn_config.get("port", 1521)
    database = conn_config.get("database", "")
    
    if db_type in ("sqlserver", "mssql", "microsoft sql server"):
        return f"jdbc:sqlserver://{host}:{port};databaseName={database};encrypt=false"
    elif db_type == "oracle":
        return f"jdbc:oracle:thin:@{host}:{port}:{database}"
    elif db_type == "postgresql":
        return f"jdbc:postgresql://{host}:{port}/{database}"
    elif db_type == "mysql":
        return f"jdbc:mysql://{host}:{port}/{database}"
    else:
        return f"jdbc:{db_type}://{host}:{port}/{database}"


def read_sql(spark: SparkSession, conn_config: Dict[str, Any],
             table: str = None, query: str = None) -> DataFrame:
    """Read data from SQL database."""
    jdbc_url = get_jdbc_url(conn_config)
    user = conn_config.get("username", "")
    password = conn_config.get("password", "")
    driver = conn_config.get("driver", "oracle.jdbc.driver.OracleDriver")
    
    reader = spark.read.format("jdbc") \
        .option("url", jdbc_url) \
        .option("user", user) \
        .option("password", password) \
        .option("driver", driver)
    
    if query:
        reader = reader.option("query", query)
    elif table:
        reader = reader.option("dbtable", table)
    else:
        raise ValueError("Either table or query must be provided")
    
    return reader.load()


def read_file(spark: SparkSession, path: str, format: str = "csv", options: Dict[str, Any] = None) -> DataFrame:
    """Read a local file into a Spark DataFrame. Used by generated mappings during tests.

    This is a lightweight helper that prefers local file paths. It supports CSV with header by default.
    """
    opts = options or {}
    fmt = (format or "csv").lower()
    if fmt == "csv":
        reader = spark.read.options(header=opts.get("header", "true"))
        # support file:// prefix or plain path
        if path.startswith("file://"):
            target = path
        else:
            target = f"file://{path}" if path.startswith("/") else path
        return reader.csv(target)
    # fallback: try generic spark reader
    reader = spark.read.format(fmt)
    for k, v in opts.items():
        reader = reader.option(k, v)
    return reader.load(path)


def write_sql(df: DataFrame, conn_config: Dict[str, Any], table: str,
              mode: str = "append") -> None:
    """Write DataFrame to SQL database."""
    jdbc_url = get_jdbc_url(conn_config)
    user = conn_config.get("username", "")
    password = conn_config.get("password", "")
    driver = conn_config.get("driver", "oracle.jdbc.driver.OracleDriver")
    
    df.write.format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table) \
        .option("user", user) \
        .option("password", password) \
        .option("driver", driver) \
        .mode(mode) \
        .save()


def execute_sql(spark: SparkSession, conn_config: Dict[str, Any], sql: str) -> None:
    """Execute a SQL statement (DDL/DML)."""
    jdbc_url = get_jdbc_url(conn_config)
    user = conn_config.get("username", "")
    password = conn_config.get("password", "")
    driver = conn_config.get("driver", "oracle.jdbc.driver.OracleDriver")
    
    spark._jvm.java.lang.Class.forName(driver)
    conn = spark._jvm.java.sql.DriverManager.getConnection(jdbc_url, user, password)
    try:
        stmt = conn.createStatement()
        stmt.execute(sql)
        conn.commit()
        stmt.close()
    finally:
        conn.close()


def load_config(config_path: str = "config.yml") -> Dict[str, Any]:
    """Load configuration from YAML file."""
    if not os.path.exists(config_path):
        return {}
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f) or {}
    return config


def get_spark_session(app_name: str, config: Dict[str, Any] = None) -> SparkSession:
    """Get or create Spark session."""
    builder = SparkSession.builder.appName(app_name)
    if config and "spark" in config:
        for key, value in config["spark"].get("config", {}).items():
            builder = builder.config(key, str(value))
    return builder.getOrCreate()


def normalize_column_names(df: DataFrame) -> DataFrame:
    """Normalize column names to lowercase."""
    for col_name in df.columns:
        new_name = col_name.lower()
        if col_name != new_name:
            df = df.withColumnRenamed(col_name, new_name)
    return df


def safe_col(df: DataFrame, col_name: str):
    """Get a column with case-insensitive matching."""
    col_lower = col_name.lower()
    for c in df.columns:
        if c.lower() == col_lower:
            return col(c)
    return col(col_name)


def infa_iif(condition, true_val, false_val):
    """Informatica IIF equivalent."""
    return when(condition, true_val).otherwise(false_val)


def infa_decode(col_expr, *args):
    """Informatica DECODE equivalent."""
    if len(args) < 2:
        return lit(None)
    result = None
    pairs = list(args)
    default_val = pairs.pop() if len(pairs) % 2 == 1 else lit(None)
    for i in range(0, len(pairs), 2):
        if result is None:
            result = when(col_expr == pairs[i], pairs[i + 1])
        else:
            result = result.when(col_expr == pairs[i], pairs[i + 1])
    return result.otherwise(default_val) if result else default_val


def infa_nvl(col_expr, default_val):
    """Informatica NVL equivalent."""
    return coalesce(col_expr, default_val)


def smart_repartition(df: DataFrame, target_partitions: int = 20, 
                      min_rows_per_partition: int = 1000) -> DataFrame:
    """Dynamically repartition DataFrame based on data size."""
    try:
        row_count = df.count()
        if row_count == 0:
            return df
        optimal = max(1, min(target_partitions, row_count // min_rows_per_partition))
        if optimal < df.rdd.getNumPartitions():
            return df.coalesce(optimal)
        elif optimal > df.rdd.getNumPartitions():
            return df.repartition(optimal)
        return df
    except Exception:
        return df


def safe_write_jdbc(df: DataFrame, jdbc_url: str, table_name: str, user: str,
                    password: str, driver: str, mode: str = "append",
                    batch_size: int = 10000, partitions: int = 20) -> bool:
    """Safely write DataFrame to JDBC target."""
    try:
        row_count = df.count()
        if row_count == 0:
            return False
        partitioned_df = smart_repartition(df, partitions)
        partitioned_df.write.format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", table_name) \
            .option("user", user) \
            .option("password", password) \
            .option("driver", driver) \
            .option("batchsize", batch_size) \
            .mode(mode) \
            .save()
        return True
    except Exception as e:
        logging.error(f"Error writing to {table_name}: {e}")
        raise


def write_target(df: DataFrame, table_name: str, mode: str = "append"):
    """Write to target database."""
    return safe_write_jdbc(df=df, jdbc_url="", table_name=table_name,
                           user="", password="", driver="",
                           mode=mode, batch_size=10000, partitions=20)


class SparkContext:
    """Context holder for Spark session and registered DataFrames."""
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.dataframes: Dict[str, DataFrame] = {}
        self.logger = logging.getLogger(self.__class__.__name__)

    def register_df(self, name: str, df: DataFrame):
        self.dataframes[name] = df

    def get_df(self, name: str) -> Optional[DataFrame]:
        return self.dataframes.get(name)


class MappingMetrics:
    """Track metrics for a mapping execution."""
    def __init__(self, mapping_name: str):
        self.mapping_name = mapping_name
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        self.row_counts: Dict[str, int] = {}
        self.warnings: List[str] = []
        self.status: str = "PENDING"
        self.error: Optional[str] = None

    def start(self):
        self.start_time = datetime.now()
        self.status = "RUNNING"

    def complete(self):
        self.end_time = datetime.now()
        self.status = "SUCCESS"

    def fail(self, error: Exception):
        self.end_time = datetime.now()
        self.status = "FAILED"
        self.error = str(error)

    def log_row_count(self, step_name: str, count: int):
        self.row_counts[step_name] = count
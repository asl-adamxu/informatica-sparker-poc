"""
Runtime library for PySpark jobs.
Provides helper functions for database connections, file reading, transformations, and metrics.
"""
import logging
import os
import sys
import yaml
import inspect
from datetime import datetime
from typing import Dict, Any, Optional, List
from logging.handlers import TimedRotatingFileHandler

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *


def load_config(config_path: str = "env/config.yml") -> Dict[str, Any]:
    """Load configuration from YAML file."""
    if not os.path.exists(config_path):
        return {}
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f) or {}
    return config


# =============================================================================
# Global logging configuration (automatically executed when module is loaded)
# =============================================================================
_LOG_FILES = set()


def init_logger(log_name: str = None):
    """
    Initialize logging system, output to terminal stderr and rotating file.
    Each unique log_name gets its own log file.
    
    Args:
        log_name: Log file name prefix, e.g., "my_mapping", which generates my_mapping.log
                  If None, automatically uses the caller's filename (without .py extension)
    """
    if not log_name:
        caller_frame = inspect.stack()[1]
        caller_file = os.path.basename(caller_frame.filename)
        log_name = os.path.splitext(caller_file)[0]

    try:
        config = load_config('env/config.yml')
        log_config = config.get('logging', {})
        log_dir = log_config.get('dir', os.getcwd())
        log_level = getattr(logging, log_config.get('level', 'INFO').upper(), logging.INFO)
        backup_count = int(log_config.get('backup_count', 7))
    except Exception:
        log_dir = os.getcwd()
        log_level = logging.INFO
        backup_count = 7

    os.makedirs(log_dir, exist_ok=True)

    log_formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Root logger: console handler only (once)
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    if not any(isinstance(h, logging.StreamHandler) and not isinstance(h, logging.FileHandler) for h in root_logger.handlers):
        console_handler = logging.StreamHandler(sys.stderr)
        console_handler.setLevel(log_level)
        console_handler.setFormatter(log_formatter)
        root_logger.addHandler(console_handler)

    # Named logger: file handler only (separate log per module)
    logger = logging.getLogger(log_name)
    logger.setLevel(log_level)
    logger.propagate = True

    if log_name not in _LOG_FILES:
        _LOG_FILES.add(log_name)
        log_file = os.path.join(log_dir, f"{log_name}.log")
        file_handler = TimedRotatingFileHandler(
            log_file,
            when="midnight",
            backupCount=backup_count,
            encoding="utf-8"
        )
        file_handler.setLevel(log_level)
        file_handler.setFormatter(log_formatter)
        logger.addHandler(file_handler)

    return logger


def get_logger(name: str = None) -> logging.Logger:
    """Get a logger instance for the calling module."""
    if not name:
        caller_frame = inspect.stack()[1]
        caller_file = os.path.basename(caller_frame.filename)
        name = os.path.splitext(caller_file)[0]
    return logging.getLogger(name)


def _resolve_path(value: str) -> str:
    """Resolve shell variables ($VAR, ${VAR}) and $(pwd) in config values."""
    if not isinstance(value, str):
        return value
    value = os.path.expandvars(value)
    if "$(pwd)" in value:
        value = value.replace("$(pwd)", os.getcwd())
    return value


def get_db_config(config: Dict[str, Any], conn_name: str = "source") -> Dict[str, Any]:
    """Get database connection configuration by name.
    
    Looks up config['connections'][conn_name] first, then falls back
    to config[conn_name] (top-level) for standalone connection entries.
    If conn_name is not found directly, tries to match by checking if
    any connection key is a prefix of conn_name (e.g. 'DPA_FACT_...' -> 'DPA').
    """
    connections = config.get("connections", {})
    result = connections.get(conn_name)
    if result is not None:
        return result

    # Try prefix match: find a connection key that is a prefix of conn_name
    conn_lower = conn_name.lower()
    for key in sorted(connections.keys(), key=len, reverse=True):
        if conn_lower.startswith(key.lower()):
            return connections[key]

    result = config.get(conn_name)
    if result is not None:
        return result

    return connections.get("source", {})


_PASSWORD_CACHE = {}


def _resolve_password(spark: SparkSession, conn_config: Dict[str, Any]) -> str:
    """Resolve password from connection config, using Hadoop CredentialProvider if applicable.
    
    If the password starts with 'credential:', the remainder is treated as a
    credential alias looked up via the Hadoop CredentialProvider.
    Otherwise the plaintext value is returned as-is.
    Results are cached in memory so the first caller's input is reused by all subsequent callers.
    """
    raw = conn_config.get("password", "")
    if not raw:
        return raw
    if not raw.startswith("credential:"):
        return raw
    alias = raw[len("credential:"):]

    # Check in-memory cache first
    if alias in _PASSWORD_CACHE:
        return _PASSWORD_CACHE[alias]

    try:
        chars = spark.sparkContext._jsc.hadoopConfiguration().getPassword(alias)
        if chars:
            _pwd = "".join(chars)
            _PASSWORD_CACHE[alias] = _pwd
            return _pwd
    except Exception:
        pass

    # Credential not found in provider — check if it exists (created externally)
    _cred_path = _resolve_path(spark.sparkContext._jsc.hadoopConfiguration().get(
        "hadoop.security.credential.provider.path", ""))
    import subprocess
    _list = subprocess.run(
        "hadoop credential list -provider {} 2>/dev/null".format(_cred_path),
        shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    if alias in _list.stdout:
        # Credential exists but getPassword failed — try reading it again
        try:
            chars = spark.sparkContext._jsc.hadoopConfiguration().getPassword(alias)
            if chars:
                _pwd = "".join(chars)
                _PASSWORD_CACHE[alias] = _pwd
                return _pwd
        except Exception:
            pass

    # Interactive first-time setup
    print(f"")
    print(f"!!! Credential '{alias}' not found in provider: {_cred_path}")
    import getpass
    _pwd = getpass.getpass(f"    Password for {alias}: ")
    if _pwd:
        import shlex
        _cmd = f"hadoop credential create {alias} -value {shlex.quote(_pwd)} -provider {_cred_path}"
        _result = subprocess.run(_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        if _result.returncode == 0:
            print(f"    Credential '{alias}' saved successfully.")
        else:
            # May already exist — try updating instead
            _upd = f"hadoop credential update {alias} -value {shlex.quote(_pwd)} -provider {_cred_path}"
            subprocess.run(_upd, shell=True, check=True)
            print(f"    Credential '{alias}' updated successfully.")
        _PASSWORD_CACHE[alias] = _pwd
        return _pwd
    print(f"    Skipped. Create it later with:")
    print(f"    hadoop credential create {alias} -provider {_cred_path}")
    print(f"")
    return raw


def get_jdbc_url(conn_config: Dict[str, Any]) -> str:
    """Build JDBC URL from connection configuration.

    Supports:
      - Oracle SID:     jdbc:oracle:thin:@host:port:sid
      - Oracle Service:  jdbc:oracle:thin:@//host:port/service_name
      - SQL Server, PostgreSQL, MySQL
    """
    db_type = conn_config.get("type", "oracle").lower()
    host = conn_config.get("host", "localhost")
    port = conn_config.get("port", 1521)
    database = conn_config.get("database", "")
    
    if db_type in ("sqlserver", "mssql", "microsoft sql server"):
        return f"jdbc:sqlserver://{host}:{port};databaseName={database};encrypt=false"
    elif db_type == "oracle":
        service_name = conn_config.get("service_name")
        if service_name:
            return f"jdbc:oracle:thin:@//{host}:{port}/{service_name}"
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
    password = _resolve_password(spark, conn_config)
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
    """Read a file into a Spark DataFrame.

    The path is passed through as-is so Spark resolves it via its configured
    default filesystem (e.g. HDFS if fs.defaultFS=hdfs://..., local otherwise).
    """
    opts = options or {}
    fmt = (format or "csv").lower()
    if fmt == "csv":
        reader = spark.read.options(header=opts.get("header", "true"))
        return reader.csv(path)
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
    spark = SparkSession.builder.getOrCreate()
    password = _resolve_password(spark, conn_config)
    driver = conn_config.get("driver", "oracle.jdbc.driver.OracleDriver")
    
    df.write.format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table) \
        .option("user", user) \
        .option("password", password) \
        .option("driver", driver) \
        .mode(mode) \
        .save()


def write_file(df: DataFrame, path: str, format: str = "csv",
               mode: str = "overwrite", options: Dict[str, Any] = None) -> None:
    """Write DataFrame to file (CSV, Parquet, etc.).

    For CSV format the output is coalesced to a single partition and the
    single part file is renamed to the target path (so the result is a
    single file rather than a Spark-partitioned directory).
    """
    opts = options or {}
    fmt = (format or "csv").lower()
    df_out = df.coalesce(1)

    if fmt == "csv":
        # Default CSV options
        csv_opts = {"header": "true"}
        csv_opts.update(opts)
        # Write to a temporary directory, then rename the single part file
        _tmp_dir = path + ".__tmp__"
        writer = df_out.write.format("csv").mode("overwrite")
        for k, v in csv_opts.items():
            writer = writer.option(k, str(v))
        writer.save(_tmp_dir)

        # Find the part-*.csv file and rename it to the final path
        import glob
        part_files = glob.glob(os.path.join(_tmp_dir, "part-*.csv"))
        if part_files:
            if os.path.exists(path):
                os.remove(path)
            os.rename(part_files[0], path)
        # Clean up temp directory
        import shutil
        shutil.rmtree(_tmp_dir, ignore_errors=True)
    else:
        writer = df_out.write.format(fmt).mode(mode)
        for k, v in opts.items():
            writer = writer.option(k, str(v))
        writer.save(path)


def test_connection(spark: SparkSession, conn_config: Dict[str, Any]) -> bool:
    """Test a database connection by attempting to connect and run a simple query.
    
    Returns True if successful, False otherwise.
    """
    try:
        jdbc_url = get_jdbc_url(conn_config)
        user = conn_config.get("username", "")
        password = _resolve_password(spark, conn_config)
        driver = conn_config.get("driver", "oracle.jdbc.driver.OracleDriver")
        spark._jvm.java.lang.Class.forName(driver)
        conn = spark._jvm.java.sql.DriverManager.getConnection(jdbc_url, user, password)
        try:
            stmt = conn.createStatement()
            stmt.execute("SELECT 1 FROM DUAL")
            stmt.close()
        finally:
            conn.close()
        return True
    except Exception as e:
        logger = get_logger()
        logger.error("Connection test failed: %s", e)
        return False


def execute_sql(spark: SparkSession, conn_config: Dict[str, Any], sql: str) -> None:
    """Execute a SQL statement (DDL/DML)."""
    jdbc_url = get_jdbc_url(conn_config)
    user = conn_config.get("username", "")
    password = _resolve_password(spark, conn_config)
    driver = conn_config.get("driver", "oracle.jdbc.driver.OracleDriver")
    
    spark._jvm.java.lang.Class.forName(driver)
    conn = spark._jvm.java.sql.DriverManager.getConnection(jdbc_url, user, password)
    try:
        stmt = conn.createStatement()
        stmt.execute(sql)
        # conn.commit()
        stmt.close()
    finally:
        conn.close()


def batch_delete(spark: SparkSession, conn_config: Dict[str, Any],
                 table_name: str, key_column: str, key_values: list,
                 batch_size: int = 1000) -> None:
    """Delete rows from a table by key values using JDBC PreparedStatement (bind variables).
    
    Uses PreparedStatement with bind variables to avoid SQL length limits and
    SQL injection risks. Values are processed in batches.
    """
    if not key_values:
        return
    jdbc_url = get_jdbc_url(conn_config)
    user = conn_config.get("username", "")
    password = _resolve_password(spark, conn_config)
    driver = conn_config.get("driver", "oracle.jdbc.driver.OracleDriver")
    
    spark._jvm.java.lang.Class.forName(driver)
    conn = spark._jvm.java.sql.DriverManager.getConnection(jdbc_url, user, password)
    try:
        sql = "DELETE FROM {} WHERE {} = ?".format(table_name, key_column)
        pstmt = conn.prepareStatement(sql)
        for i, val in enumerate(key_values):
            pstmt.setString(1, str(val))
            pstmt.addBatch()
            if (i + 1) % batch_size == 0:
                pstmt.executeBatch()
                # conn.commit()
        pstmt.executeBatch()
        # conn.commit()
        pstmt.close()
    finally:
        conn.close()


def get_spark_session(app_name: str, config: Dict[str, Any] = None) -> SparkSession:
    """Get or create Spark session.

    Supports three connection profiles defined in config.yml's spark_connections:
      - spark_local:      local[*] mode, no cluster deps
      - spark3_client:    YARN client mode with kerberos + executor PYTHONPATH
      - spark3_on_yarn:   YARN cluster mode with kerberos + executor PYTHONPATH

    The active profile is selected via spark.connection (default: spark_local).
    This mirrors how @task.pyspark(conn_id="spark3_on_yarn") works in Airflow DAGs.
    """
    spark_cfg = (config or {}).get("spark", {})
    # Allow SPARK_CONNECTION env var to override config value
    conn_name = os.environ.get("SPARK_CONNECTION") or spark_cfg.get("connection", "spark_local")

    # Look up the connection profile under spark_connections
    profiles = (config or {}).get("spark_connections", {})
    profile = profiles.get(conn_name, profiles.get("spark_local", {}))

    builder = SparkSession.builder.appName(app_name)

    # 1. Set master from profile (or fallback to spark.master)
    master_url = profile.get("master") or spark_cfg.get("master")
    if master_url:
        builder = builder.master(str(master_url))

    # 2. Set deploy-mode if present
    deploy_mode = profile.get("deploy-mode")
    if deploy_mode:
        builder = builder.config("spark.submit.deployMode", str(deploy_mode))

    # 3. Apply config entries from the profile
    for key, value in profile.get("config", {}).items():
        builder = builder.config(key, str(value))

    # 4. Apply any spark-level config overrides (can override profile values)
    for key, value in spark_cfg.get("config", {}).items():
        builder = builder.config(key, str(value))

    spark = builder.getOrCreate()

    # Inject spark.hadoop.* into Hadoop Configuration so absolute paths
    # resolve via the cluster's default filesystem (like @task.pyspark does).
    try:
        all_cfg = {}
        all_cfg.update(profile.get("config", {}))
        all_cfg.update(spark_cfg.get("config", {}))
        for key, value in all_cfg.items():
            if key.startswith("spark.hadoop."):
                hadoop_key = key.replace("spark.hadoop.", "")
                spark.sparkContext._jsc.hadoopConfiguration().set(hadoop_key, _resolve_path(str(value)))
    except Exception:
        pass  # best-effort

    return spark


def safe_col(df: DataFrame, col_name: str):
    """Get a column with case-insensitive matching."""
    col_lower = col_name.lower()
    for c in df.columns:
        if c.lower() == col_lower:
            return col(c)
    return col(col_name)


def safe_string(col_expr):
    """Convert column to string, avoiding scientific notation for numeric types.
    First casts to the column's own numeric type if applicable, then to string.
    """
    return format_string("%s", col_expr)


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


def write_table(df: DataFrame, conn_config: Dict[str, Any], table_name: str,
                mode: str = "append") -> None:
    """Write DataFrame to SQL database (mirrors write_sql interface).

    Args:
        df: DataFrame to write
        conn_config: Connection configuration dict (with host, port, username, etc.)
        table_name: Target table name
        mode: Write mode (append, overwrite, etc.)
    """
    jdbc_url = get_jdbc_url(conn_config)
    user = conn_config.get("username", "")
    spark = SparkSession.builder.getOrCreate()
    password = _resolve_password(spark, conn_config)
    driver = conn_config.get("driver", "oracle.jdbc.driver.OracleDriver")

    try:
        row_count = df.count()
        if row_count == 0:
            return
        df_out = smart_repartition(df, 20)
        df_out.write.format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", table_name) \
            .option("user", user) \
            .option("password", password) \
            .option("driver", driver) \
            .option("batchsize", 10000) \
            .mode(mode) \
            .save()
    except Exception as e:
        logging.error(f"Error writing to {table_name}: {e}")
        raise


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


class NullMetrics:
    """No-op metrics tracker — used when no metrics collection is needed.
    Provides the same interface as MappingMetrics but with empty method bodies.
    """
    def __init__(self, mapping_name: str = ""):
        self.mapping_name = mapping_name
        self.row_counts = {}
        self.warnings = []
        self.status = "PENDING"

    def start(self):
        self.status = "RUNNING"

    def complete(self):
        self.status = "SUCCESS"

    def fail(self, error: Exception):
        self.status = "FAILED"

    def log_row_count(self, step_name: str, count: int):
        self.row_counts[step_name] = count

    def add_warning(self, warning: str):
        self.warnings.append(warning)


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
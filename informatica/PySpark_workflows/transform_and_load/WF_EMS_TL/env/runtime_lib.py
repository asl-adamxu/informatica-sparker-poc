"""
Runtime library for PySpark jobs.
Provides helper functions for database connections, file reading, transformations, and metrics.
"""
import logging
import os
import sys
import time
import yaml
import inspect
from datetime import datetime
from typing import Dict, Any, Optional, List
from logging.handlers import TimedRotatingFileHandler

# Save builtins before pyspark.sql.functions shadows max/min with column versions
_builtin_max = max
_builtin_min = min

# Bootstrap CDP pyspark from env/config.yml (spark.home) before any pyspark import.
# Lets `python m_xxx.py` run directly without setting PYTHONPATH/SPARK_HOME.
# NOTE: must also inject the matching py4j zip (SPARK_HOME/python/lib/py4j-*.zip)
# — otherwise `import py4j` falls back to an older system py4j, whose gateway
# protocol is incompatible with pyspark 3.5.4 (executor heartbeat fails with
# RpcEndpointNotFoundException). Works with the system python3 (3.6 on RHEL8)
# and python3.11 alike — keep driver and executors on the same interpreter
# (get_spark_session sets spark.pyspark.python = sys.executable for that).
try:
    with open('env/config.yml', 'r') as _bf:
        _b_cfg = yaml.safe_load(_bf) or {}
    _spark_home = os.environ.get('SPARK_HOME') or (_b_cfg.get('spark', {}) or {}).get('home', '')
    if _spark_home and os.path.isdir(_spark_home):
        os.environ.setdefault('SPARK_HOME', _spark_home)
        _py_dir = os.path.join(_spark_home, 'python')
        if os.path.isdir(_py_dir) and _py_dir not in sys.path:
            sys.path.insert(0, _py_dir)
        _lib_dir = os.path.join(_py_dir, 'lib')
        if os.path.isdir(_lib_dir):
            for _entry in sorted(os.listdir(_lib_dir)):
                if _entry.startswith('py4j-') and _entry.endswith('.zip'):
                    _p4 = os.path.join(_lib_dir, _entry)
                    if _p4 not in sys.path:
                        sys.path.insert(0, _p4)
except Exception:
    pass  # best-effort: fall back to system pyspark

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
import re as _re

# Pre-compiled regex for ${VAR:default} resolution in _resolve_path
_RESOLVE_PATH_DEFAULT_RE = _re.compile(r'\$\{(\w+):([^}]*)\}')


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

    # Root logger: console handler (once) + file handler (first caller only)
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    has_console = any(
        isinstance(h, logging.StreamHandler) and not isinstance(h, logging.FileHandler)
        for h in root_logger.handlers
    )
    if not has_console:
        console_handler = logging.StreamHandler(sys.stderr)
        console_handler.setLevel(log_level)
        console_handler.setFormatter(log_formatter)
        root_logger.addHandler(console_handler)

    # Named logger: separate log file per module
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
        # Also give root the same file handler once, so loggers like
        # env.runtime_lib (used by shared workflow functions) have their
        # messages written to the first caller's log file.
        if not any(isinstance(h, logging.FileHandler) for h in root_logger.handlers):
            root_logger.addHandler(file_handler)

    # Quiet py4j's own chatty INFO logs (e.g. "Closing down clientserver
    # connection" on every gateway connection close) — keep WARNING+ only.
    logging.getLogger("py4j").setLevel(logging.WARNING)

    return logger


def get_logger(name: str = None) -> logging.Logger:
    """Get a logger instance for the calling module."""
    if not name:
        caller_frame = inspect.stack()[1]
        caller_file = os.path.basename(caller_frame.filename)
        name = os.path.splitext(caller_file)[0]
    return logging.getLogger(name)


def _resolve_path(value: str) -> str:
    """Resolve shell variables ($VAR, ${VAR}, ${VAR:default}) and $(pwd) in config values."""
    if not isinstance(value, str):
        return value
    # Handle ${VAR:default} syntax (os.path.expandvars doesn't support defaults)
    def _replace_default(_m):
        _var = _m.group(1)
        _default = _m.group(2)
        return os.environ.get(_var, _default)
    value = _RESOLVE_PATH_DEFAULT_RE.sub(_replace_default, value)
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
_PASSWORD_PENDING = {}  # alias → password deferred for credential provider save


def _flush_pending_passwords():
    """Save interactively-entered passwords to Hadoop CredentialProvider.

    Call this only after the connection has been verified successfully,
    so we don't persist a wrong password.
    """
    if not _PASSWORD_PENDING:
        return
    import shlex as _shlex
    import subprocess as _subprocess
    for _alias, _pwd in _PASSWORD_PENDING.items():
        _cred_path = ""
        try:
            from pyspark.sql import SparkSession
            _sp = SparkSession.builder.getOrCreate()
            _cred_path = _resolve_path(_sp.sparkContext._jsc.hadoopConfiguration().get(
                "hadoop.security.credential.provider.path", ""))
        except Exception:
            _cred_path = "jceks://file/$(pwd)/env/passwords.jceks"
        _cmd = f"hadoop credential create {_alias} -value {_shlex.quote(_pwd)} -provider {_cred_path}"
        _result = _subprocess.run(_cmd, shell=True, stdout=_subprocess.PIPE, stderr=_subprocess.PIPE, universal_newlines=True)
        if _result.returncode == 0:
            print(f"    Credential '{_alias}' saved successfully.")
        else:
            _upd = f"hadoop credential update {_alias} -value {_shlex.quote(_pwd)} -provider {_cred_path}"
            _subprocess.run(_upd, shell=True, stdout=_subprocess.PIPE, stderr=_subprocess.PIPE, universal_newlines=True, check=True)
            print(f"    Credential '{_alias}' updated successfully.")
    _PASSWORD_PENDING.clear()


def _resolve_password(spark: SparkSession, conn_config: Dict[str, Any]) -> str:
    """Resolve password from connection config, using Hadoop CredentialProvider if applicable.

    If the password starts with 'credential:', the remainder is treated as a
    credential alias looked up via the Hadoop CredentialProvider.
    Otherwise the plaintext value is returned as-is.
    Results are cached in memory so the first caller's input is reused by all subsequent callers.
    Interactive passwords are NOT saved to the credential provider until
    _flush_pending_passwords() is called (after connection verification).
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

    # Don't search credential provider if we already have a pending (unverified) entry
    if alias in _PASSWORD_PENDING:
        return _PASSWORD_PENDING[alias]

    try:
        _hconf = spark.sparkContext._jsc.hadoopConfiguration()
        # Force-set the resolved provider path before getPassword. The config
        # value may contain $(pwd)/${VAR} literals that Spark auto-injected at
        # SparkContext init without resolution, or the runtime injection may
        # have been skipped by an exception — getPassword would then look up a
        # literal path and fail. Setting it here guarantees resolution.
        _raw_path = _hconf.get("hadoop.security.credential.provider.path", "")
        if _raw_path:
            _hconf.set("hadoop.security.credential.provider.path", _resolve_path(_raw_path))
        chars = _hconf.getPassword(alias)
        if chars:
            _pwd = "".join(chars)
            _PASSWORD_CACHE[alias] = _pwd
            return _pwd
    except Exception:
        pass

    # Interactive first-time setup — cache in memory only; save deferred
    _cred_path = _resolve_path(spark.sparkContext._jsc.hadoopConfiguration().get(
        "hadoop.security.credential.provider.path", ""))
    print(f"")
    print(f"!!! Credential '{alias}' not found in provider: {_cred_path}")
    import getpass
    _pwd = getpass.getpass(f"    Password for {alias}: ")
    if _pwd:
        _PASSWORD_PENDING[alias] = _pwd
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
        .option("driver", driver) \
        .option("isolationLevel", "READ_COMMITTED")

    # Oracle rejects boolean literals (TRUE/FALSE) in WHERE clauses. Spark's
    # JDBC predicate pushdown emits them when a pushed filter contains a
    # non-table column (e.g. computed NewLookupRow / _update_flag), which fails
    # with ORA-00920. Keep predicate pushdown disabled for Oracle so those
    # filters are evaluated by Spark after the read.
    if str(conn_config.get("type", "oracle")).lower() == "oracle":
        reader = reader.option("pushDownPredicate", "false")

    if query:
        reader = reader.option("query", query)
    elif table:
        reader = reader.option("dbtable", table)
    else:
        raise ValueError("Either table or query must be provided")
    
    return reader.load()


def _dynamic_lookup_is_null(value):
    if value is None:
        return True
    try:
        return bool(value != value)
    except Exception:
        return False


def _dynamic_lookup_normalize(value):
    if _dynamic_lookup_is_null(value):
        return None
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except Exception:
            return value
    return value


def _dynamic_lookup_equal(a, b, case_sensitive=False):
    a = _dynamic_lookup_normalize(a)
    b = _dynamic_lookup_normalize(b)
    if a is None or b is None:
        return a is None and b is None
    if isinstance(a, str) or isinstance(b, str):
        if not case_sensitive:
            return str(a).lower() == str(b).lower()
        return str(a) == str(b)
    try:
        return bool(a == b)
    except Exception:
        return str(a) == str(b)


def _dynamic_lookup_candidate(row, out_field, cfg):
    ref = out_field.get("ref_field") or ""
    if ref.upper() == "SEQUENCE-ID":
        return row.get("__dyn_seq_key")
    if ref and ref in row:
        return _dynamic_lookup_normalize(row.get(ref))
    if out_field.get("name") in row:
        return _dynamic_lookup_normalize(row.get(out_field["name"]))
    return None


def _dynamic_lookup_condition_holds(row, cache_state, cfg):
    expr = str(cfg.get("update_condition") or "TRUE").strip()
    if not expr or expr.upper() == "TRUE":
        return True
    if expr.upper() == "FALSE":
        return False
    # Minimal evaluator for the simple boolean expressions Informatica stores
    # in "Update Dynamic Cache Condition". Current NHS/EMS mappings all use
    # TRUE; anything more complex is evaluated best-effort and falls back to
    # TRUE with a warning.
    try:
        _e = _re.sub(r"\bIS\s+NOT\s+NULL\b", " is not None ", expr, flags=_re.IGNORECASE)
        _e = _re.sub(r"\bIS\s+NULL\b", " is None ", _e, flags=_re.IGNORECASE)
        _e = _re.sub(r"(?<![=!<>])=(?!=)", "==", _e)
        _e = _e.replace("'", '"')
        def _sub(m):
            _name = m.group(1)
            _val = cache_state.get(_name, row.get(_name))
            if _val is None:
                return "None"
            if isinstance(_val, str):
                return '"' + _val.replace('"', '\\"') + '"'
            return repr(_val)
        _e = _re.sub(r"\b([A-Za-z_][A-Za-z0-9_]*)\b", _sub, _e)
        return bool(eval(_e, {"__builtins__": {}}, {}))
    except Exception as _exc:
        logging.warning(
            "Update Dynamic Cache Condition '%s' could not be evaluated (%s); "
            "treating as TRUE", expr, _exc)
        return True


def _process_dynamic_lookup_rows(rows, cfg):
    """Run the dynamic-cache state machine over one join-key group.

    rows are dicts carrying the input columns, prefixed base columns
    (__lkp_<output>), __base_exists, __dyn_seq and __dyn_seq_key. The same
    key's rows are processed in __dyn_seq order, so each row sees the cache
    state left by the previous row (Informatica row-by-row semantics).
    """
    input_columns = list(cfg.get("_input_columns") or [])
    out_fields = cfg.get("lookup_output_fields") or []
    new_lookup_row_col = cfg.get("new_lookup_row_col") or "NewLookupRow"
    insert_else_update = bool(cfg.get("insert_else_update"))
    output_old_value = bool(cfg.get("output_old_value_on_update"))
    case_sensitive = bool(cfg.get("case_sensitive_string_comparison"))

    normalized = [
        {k: _dynamic_lookup_normalize(v) for k, v in row.items()}
        for row in rows
    ]
    normalized.sort(key=lambda r: (r.get("__dyn_seq") is None, r.get("__dyn_seq")))

    results = []
    cache_state = None
    cache_present = False
    for row in normalized:
        if not cache_present:
            cache_state = {}
            if row.get("__base_exists"):
                for f in out_fields:
                    cache_state[f["name"]] = row.get("__lkp_" + f["name"])
                new_lookup_row = 0
            else:
                for f in out_fields:
                    ref = f.get("ref_field") or ""
                    if ref.upper() == "SEQUENCE-ID":
                        cache_state[f["name"]] = row.get("__dyn_seq_key")
                    else:
                        cache_state[f["name"]] = _dynamic_lookup_candidate(row, f, cfg)
                new_lookup_row = 1
            cache_present = True
            output_vals = dict(cache_state)
        else:
            candidates = {}
            changed = False
            for f in out_fields:
                ref = f.get("ref_field") or ""
                if ref.upper() == "SEQUENCE-ID":
                    # Sequence-Id surrogate keys are immutable once inserted:
                    # a hit always keeps the cache value and never triggers an
                    # update comparison.
                    cand = cache_state.get(f["name"])
                else:
                    cand = _dynamic_lookup_candidate(row, f, cfg)
                if (f.get("ignore_null_inputs")
                        and ref and ref.upper() != "SEQUENCE-ID"
                        and _dynamic_lookup_is_null(row.get(ref))):
                    cand = cache_state.get(f["name"])
                candidates[f["name"]] = cand
                if (not f.get("ignore_in_compare")
                        and not _dynamic_lookup_equal(
                            cand, cache_state.get(f["name"]), case_sensitive)):
                    changed = True
            if (insert_else_update and changed
                    and _dynamic_lookup_condition_holds(row, cache_state, cfg)):
                new_lookup_row = 2
                old_vals = dict(cache_state)
                for f in out_fields:
                    ref = f.get("ref_field") or ""
                    if ref.upper() == "SEQUENCE-ID":
                        continue
                    if (f.get("ignore_null_inputs") and ref
                            and _dynamic_lookup_is_null(row.get(ref))):
                        continue
                    cache_state[f["name"]] = candidates[f["name"]]
                output_vals = old_vals if output_old_value else dict(cache_state)
            else:
                new_lookup_row = 0
                output_vals = dict(cache_state)

        out = {name: row.get(name) for name in input_columns}
        for f in out_fields:
            out[f["name"]] = output_vals.get(f["name"])
        out[new_lookup_row_col] = new_lookup_row
        results.append(out)
    return results


def _dynamic_lookup_spark_type(datatype):
    t = str(datatype or "").lower()
    if "bigint" in t or "long" in t:
        return LongType()
    if "integer" in t or "int" in t:
        return IntegerType()
    if "smallint" in t or "short" in t:
        return ShortType()
    if "decimal" in t or "number" in t:
        return DecimalType(38, 10)
    if "date/time" in t or "timestamp" in t:
        return TimestampType()
    if "date" in t:
        return DateType()
    if "float" in t or "double" in t:
        return DoubleType()
    if "binary" in t:
        return BinaryType()
    return StringType()


def _dynamic_lookup_output_schema(input_df, lookup_df, cfg):
    out_fields = cfg.get("lookup_output_fields") or []
    new_col = cfg.get("new_lookup_row_col") or "NewLookupRow"
    lookup_fields = {f.name.lower(): f for f in lookup_df.schema.fields}
    fields = []
    seen = set()
    for f in input_df.schema.fields:
        lower = f.name.lower()
        if lower == new_col.lower():
            continue
        if isinstance(f.dataType, NullType):
            # Arrow cannot carry Spark NullType; a typed NULL string is
            # semantically equivalent and safe for downstream expressions.
            fields.append(StructField(f.name, StringType(), True))
        else:
            fields.append(f)
        seen.add(lower)
    for of in out_fields:
        lower = of["name"].lower()
        ref = str(of.get("ref_field") or "").upper()
        lf = lookup_fields.get(lower)
        if ref == "SEQUENCE-ID":
            # Sequence-Id values come from the pre-allocated bigint key
            # (__dyn_seq_key), never from the lookup source column type
            # (often Oracle NUMBER → Decimal(38,0)); declare long so the
            # pandas UDF round-trips as int64 without an Arrow cast
            # (int64 → decimal128(38,0) raises PySparkValueError).
            sf = StructField(of["name"], LongType(), True)
        elif lf is not None and not isinstance(lf.dataType, NullType):
            sf = lf
        else:
            sf = StructField(
                of["name"], _dynamic_lookup_spark_type(of.get("datatype")), True)
        if lower in seen:
            fields = [sf if f.name.lower() == lower else f for f in fields]
        else:
            fields.append(sf)
            seen.add(lower)
    fields.append(StructField(new_col, IntegerType(), True))
    return StructType(fields)


def _dynamic_lookup_pyarrow_ok():
    try:
        import pandas  # noqa: F401
        import pyarrow  # noqa: F401
        from pyspark.sql.pandas.utils import require_minimum_pyarrow_version
        require_minimum_pyarrow_version()
        return True
    except Exception:
        return False


def _dynamic_lookup_apply_in_pandas(spark, joined, cfg, output_schema):
    import pandas as pd
    output_columns = [f.name for f in output_schema.fields]

    def _process_group(pdf):
        rows = pdf.to_dict("records")
        return pd.DataFrame(
            _process_dynamic_lookup_rows(rows, cfg),
            columns=output_columns,
        )

    return joined.groupBy("__dyn_key").applyInPandas(
        _process_group, schema=output_schema)


def _dynamic_lookup_rdd(spark, joined, cfg, output_schema):
    from pyspark.sql import Row

    def _key(row):
        k = row["__dyn_key"]
        if isinstance(k, Row):
            return tuple(k)
        return (k,)

    def _flat(kv):
        return _process_dynamic_lookup_rows(
            [r.asDict() for r in kv[1]], cfg)

    results = joined.rdd.map(lambda r: (_key(r), r)).groupByKey().flatMap(_flat)
    return spark.createDataFrame(results, schema=output_schema)


def dynamic_lookup(spark: SparkSession, input_df: DataFrame,
                   lookup_df: DataFrame, cfg: Dict[str, Any] = None,
                   config: Dict[str, Any] = None,
                   **dl_kwargs: Any) -> DataFrame:
    """Convert one Informatica dynamic lookup into a precise PySpark step.

    The implementation is distributed but deterministic:
      1. The base lookup source is checked for duplicate condition keys and
         fails with Report Error / CMN_1650 semantics.
      2. Input rows are left-joined to the base cache and grouped by the join
         key (null keys get a unique per-row key).
      3. applyInPandas runs the row-by-row dynamic-cache state machine inside
         each group (fallback: equivalent RDD cogroup when pyarrow is missing
         or below the Spark minimum).
      4. Sequence-Id surrogate keys are pre-allocated outside the UDF, so the
         same key is never generated twice across executors.

    The lookup configuration may be passed either as a positional dict (legacy
    generated code, tests) or as individual keyword arguments (current
    generated code) — the two are merged and equivalent.
    """
    cfg = dict(cfg or {})
    cfg.update(dl_kwargs)
    cfg["_input_columns"] = list(input_df.columns)
    name = cfg.get("name") or "dynamic_lookup"
    join_predicates = cfg.get("join_predicates") or []
    if not join_predicates:
        raise ValueError(
            "dynamic_lookup requires at least one join predicate")
    source_cols = [jp.get("source_col") for jp in join_predicates]
    lookup_cols = [jp.get("lookup_col") for jp in join_predicates]
    for sc in source_cols:
        if sc not in input_df.columns:
            raise ValueError(
                "Lookup %s: input column '%s' not found" % (name, sc))
    for lc in lookup_cols:
        if lc not in lookup_df.columns:
            raise ValueError(
                "Lookup %s: lookup column '%s' not found" % (name, lc))

    # Report Error: duplicate condition keys in the base cache must fail the
    # session (CMN_1650). Duplicate keys in the *input* stream are legal and
    # are handled by the state machine below (1/2/0 classification).
    dup_cnt = lookup_df.groupBy(*[col(c) for c in lookup_cols]) \
        .count().filter(col("count") > 1).count()
    if dup_cnt:
        raise RuntimeError(
            "Lookup %s: %d duplicate keys found in lookup source — dynamic "
            "cache only supports unique condition keys (Report Error / CMN_1650)"
            % (name, dup_cnt))

    joined = input_df
    order_by = cfg.get("order_by_columns") or []
    if order_by:
        joined = joined.orderBy(*[col(c) for c in order_by])
    joined = joined.withColumn("__dyn_seq", monotonically_increasing_id())

    base_aliases = {c: "__lkp_" + c for c in lookup_df.columns}
    base_df = lookup_df.select(*[
        col(c).alias(base_aliases[c]) for c in lookup_df.columns])
    on_cond = None
    for sc, lc in zip(source_cols, lookup_cols):
        part = joined[sc] == base_df[base_aliases[lc]]
        on_cond = part if on_cond is None else (on_cond & part)
    joined = joined.join(base_df, on=on_cond, how="left")
    joined = joined.withColumn(
        "__base_exists",
        when(base_df[base_aliases[lookup_cols[0]]].isNotNull(), lit(1))
        .otherwise(lit(0)),
    )

    # Group key: a same-typed struct of stringified join keys. SQL '=' never
    # matches NULL, so each null-key input row gets a unique __dyn_seq-derived
    # value and is treated as its own miss; non-null rows with the same key
    # share one group and advance the cache state row by row.
    key_exprs = []
    for _i, sc in enumerate(source_cols):
        key_exprs.append(
            when(col(sc).isNull(),
                 concat(lit("__NULL__:"),
                        col("__dyn_seq").cast("string")))
            .otherwise(concat(lit("V:"), col(sc).cast("string")))
            .alias("k%d" % _i)
        )
    joined = joined.withColumn("__dyn_key", struct(*key_exprs))

    # Sequence-Id pre-allocation: only the first input row of a key that misses
    # the base cache can insert, so exactly those rows get a globally unique
    # surrogate key before applyInPandas runs.
    seq_cfg = cfg.get("sequence_config")
    if seq_cfg and seq_cfg.get("output_col"):
        seq_col = seq_cfg["output_col"]
        base_seq_col = "__lkp_" + seq_col
        if base_seq_col not in joined.columns:
            raise ValueError(
                "Lookup %s: sequence output column '%s' not found in lookup "
                "source" % (name, seq_col))
        # The base cache's sequence column is Oracle NUMBER → Decimal(38,0) in
        # Spark, while the pre-allocated key is a long. Normalize to bigint so
        # the pandas UDF sees one int64 dtype for both base hits and inserts
        # (a mixed decimal/int64 Series breaks the Arrow conversion too).
        joined = joined.withColumn(
            base_seq_col, col(base_seq_col).cast("bigint"))
        seq_start = joined.agg(max(col(base_seq_col))).collect()[0][0]
        if seq_start is None:
            seq_start = 0
        seq_start = cfg.get("sequence_start", seq_start)
        from pyspark.sql.window import Window as DynamicWindow
        w_first = DynamicWindow.partitionBy("__dyn_key").orderBy("__dyn_seq")
        w_all = DynamicWindow.orderBy("__dyn_seq")
        prealloc = joined.withColumn("__rn", row_number().over(w_first))
        prealloc = prealloc.filter(
            (col("__base_exists") == 0) & (col("__rn") == 1))
        prealloc = prealloc.withColumn("__seq_idx", row_number().over(w_all))
        prealloc = prealloc.withColumn(
            "__dyn_seq_key", lit(seq_start) + col("__seq_idx"))
        joined = joined.join(
            prealloc.select("__dyn_seq", "__dyn_seq_key"),
            on="__dyn_seq", how="left")
    else:
        joined = joined.withColumn("__dyn_seq_key", lit(None).cast("long"))

    output_schema = _dynamic_lookup_output_schema(input_df, lookup_df, cfg)
    executor = "apply_in_pandas"
    if config:
        dl_config = config.get("dynamic_lookup") or {}
        executor = dl_config.get("executor", "apply_in_pandas")

    if executor == "apply_in_pandas" and _dynamic_lookup_pyarrow_ok():
        return _dynamic_lookup_apply_in_pandas(spark, joined, cfg, output_schema)

    logging.warning(
        "dynamic_lookup %s: pyarrow missing/too old or executor=rdd — using "
        "the equivalent RDD fallback", name)
    return _dynamic_lookup_rdd(spark, joined, cfg, output_schema)


def _read_local_csv(spark: SparkSession, local_path: str, opts: Dict[str, Any]) -> DataFrame:
    """Read a small local CSV file on the driver and return a DataFrame.

    Used for control files (table lists, session lists, job params) in YARN
    mode: distributing via addFile() places the file under the EXECUTOR's
    SparkFiles root, while SparkFiles.get() on the DRIVER returns the driver's
    own temp path — so executors receive a path that only exists on the driver
    (SparkFileNotFoundException). Reading on the driver avoids executors
    entirely; these files are small configuration inputs.
    """
    import csv as _csv
    _delim = str(opts.get("delimiter", ","))
    _header = str(opts.get("header", "true")).lower() in ("true", "1", "yes")
    _cols = []
    _rows = []
    with open(local_path, "r") as _fh:
        _reader = _csv.reader(_fh, delimiter=_delim)
        if _header:
            try:
                _cols = next(_reader)
            except StopIteration:
                pass
        for _row in _reader:
            _rows.append([None if _c == "" else _c for _c in _row])
    if not _cols:
        _ncols = _builtin_max([len(_r) for _r in _rows] or [0])
        _cols = [f"_c{i}" for i in range(_ncols)]
    _schema = StructType([StructField(str(_c), StringType(), True) for _c in _cols])
    if _rows:
        return spark.createDataFrame(_rows, schema=_schema)
    return spark.createDataFrame([], _schema)


def read_file(spark: SparkSession, path: str, format: str = "csv", options: Dict[str, Any] = None) -> DataFrame:
    """Read a file into a Spark DataFrame.

    Shell variables ($VAR, ${VAR}) are resolved via _resolve_path.

    Local file support (YARN-safe):
      - Small CSV control files (≤64 MB) are read on the driver and built into
        a DataFrame via createDataFrame — no executors involved, so the file is
        readable regardless of where executors run.
      - Larger local files are staged onto the default filesystem (HDFS) first,
        then read from there by every executor.
      - Fallback: addFile() + SparkFiles.get() (only works when driver and
        executors share the filesystem, e.g. local mode).
    Other paths (hdfs://, s3://, etc.) are passed through as-is.
    """
    path = _resolve_path(path)
    opts = options or {}
    fmt = (format or "csv").lower()

    _local_path = path
    if _local_path.startswith("file://"):
        _local_path = _local_path[len("file://"):]
    if os.path.exists(_local_path) and os.path.isfile(_local_path):
        if fmt == "csv" and os.path.getsize(_local_path) <= 64 * 1024 * 1024:
            return _read_local_csv(spark, _local_path, opts)
        # Larger local files: stage a copy on the default filesystem so every
        # executor can read it via the same path.
        try:
            _fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
                spark._jsc.hadoopConfiguration())
            _dst = spark._jvm.org.apache.hadoop.fs.Path(
                "/tmp/pcis01_input/" + os.path.basename(_local_path))
            _fs.copyFromLocalFile(
                False, True,
                spark._jvm.org.apache.hadoop.fs.Path("file://" + _local_path),
                _dst)
            path = "/tmp/pcis01_input/" + os.path.basename(_local_path)
        except Exception:
            try:
                from pyspark import SparkFiles
                spark.sparkContext.addFile(_local_path)
                # Driver-side SparkFiles path — only usable when executors run
                # in the same JVM/filesystem (local mode).
                path = "file://" + SparkFiles.get(os.path.basename(_local_path))
            except Exception:
                pass  # best-effort: fall back to Spark's own path resolution
    elif not _re.match(r'^[a-zA-Z][a-zA-Z0-9+.\-]*://', path):
        # Local-style path (bare absolute / file://) that does NOT exist on the
        # driver — do NOT pass it to Spark: it would be resolved against the
        # default filesystem (HDFS in YARN mode) and fail with a confusing
        # [PATH_NOT_FOUND] hdfs://... message. Control files (table lists,
        # session lists) may legitimately be absent — return an empty DataFrame
        # so the mapping runs empty (callers fall back to their "empty or has
        # no columns" path).
        logger.warning("Local file '%s' not found — returning empty DataFrame", path)
        return spark.createDataFrame([], StructType([]))

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
        .option("isolationLevel", "READ_COMMITTED") \
        .mode(mode) \
        .save()


def _write_local_csv(df: DataFrame, local_path: str, opts: Dict[str, Any]) -> None:
    """Write a DataFrame to a local CSV file on the driver.

    Used for local targets in YARN mode: Spark executors cannot create
    directories on the driver node's local filesystem (Mkdirs failed ...).
    Collects the rows on the driver and writes the single file with Python's
    csv module — mirrors _read_local_csv for the write side.
    """
    import csv as _csv
    _delim = str(opts.get("delimiter", ","))
    _header = str(opts.get("header", "true")).lower() in ("true", "1", "yes")
    _cols = list(df.columns)
    _dir = os.path.dirname(local_path)
    if _dir and not os.path.isdir(_dir):
        os.makedirs(_dir, exist_ok=True)
    with open(local_path, "w") as _fh:
        _writer = _csv.writer(_fh, delimiter=_delim, lineterminator="\n")
        if _header:
            _writer.writerow(_cols)
        for _r in df.collect():
            _writer.writerow(["" if _r[_c] is None else str(_r[_c]) for _c in _cols])


def write_file(df: DataFrame, path: str, format: str = "csv",
               mode: str = "overwrite", options: Dict[str, Any] = None) -> None:
    """Write DataFrame to file (CSV, Parquet, etc.).

    For CSV format the output is coalesced to a single partition and the
    single part file is renamed to the target path (so the result is a
    single file rather than a Spark-partitioned directory).
    Shell variables ($VAR, ${VAR}) are resolved via _resolve_path.

    Local targets (bare absolute paths without a URI scheme) are written on
    the DRIVER, never by Spark executors:
      - Spark resolves bare paths against the default filesystem (HDFS in
        YARN mode) → data would land on HDFS while the driver-side rename
        finds nothing locally (the "file silently never appears" bug), and
      - forcing file:// makes every executor try to mkdir the target dir on
        its OWN node, which does not exist / is not writable (Mkdirs failed).
      - CSV: rows are collected on the driver and written with Python's csv
        module (single file, header handling like Spark).
      - Other formats: the single part file is staged on the default
        filesystem (HDFS), then copied back to the local path on the driver.
    Explicit URIs (hdfs://, s3://, ...) are written with Spark as-is.
    """
    path = _resolve_path(path)
    opts = options or {}
    fmt = (format or "csv").lower()

    _is_local = not _re.match(r'^[a-zA-Z][a-zA-Z0-9+.\-]*://', path)

    if _is_local and fmt == "csv":
        return _write_local_csv(df, path, opts)

    df_out = df.coalesce(1)

    if _is_local:
        # Non-CSV local target: stage the single part file on the default
        # filesystem (HDFS), then copy it back to the local path on the driver.
        _spark = SparkSession.builder.getOrCreate()
        _stage = "/tmp/pcis01_output/" + os.path.basename(path)
        _writer = df_out.write.format(fmt).mode("overwrite")
        for k, v in opts.items():
            _writer = _writer.option(k, str(v))
        _writer.save(_stage)
        try:
            _fs = _spark._jvm.org.apache.hadoop.fs.FileSystem.get(
                _spark._jsc.hadoopConfiguration())
            _parts = _fs.globStatus(
                _spark._jvm.org.apache.hadoop.fs.Path(_stage + "/part-*"))
            if _parts and len(_parts) > 0:
                _dir = os.path.dirname(path)
                if _dir and not os.path.isdir(_dir):
                    os.makedirs(_dir, exist_ok=True)
                if os.path.exists(path):
                    os.remove(path)
                _fs.copyToLocalFile(
                    False, True, _parts[0].getPath(),
                    _spark._jvm.org.apache.hadoop.fs.Path(path))
            _fs.delete(_spark._jvm.org.apache.hadoop.fs.Path(_stage), True)
        except Exception:
            pass  # best-effort cleanup
        return

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


# =============================================================================
# Stored procedure invocation with runtime signature resolution
# =============================================================================
# Informatica XML only carries the transformation's input ports; the real
# Oracle procedure may have extra/mismatched parameters (e.g. the UAT wrapper
# PYSPARK.SP_DELETE_DDS_FACT has an OUT parameter RET_MSG that the metadata
# does not list). Calling with the metadata-only arg list then fails with
# ORA-06550/PLS-00306 "wrong number or types of arguments". These helpers
# resolve the actual signature from the data dictionary at runtime and bind
# OUT / IN OUT parameters to declared variables (literals are rejected for
# them by PL/SQL).

_SP_SIG_CACHE = {}


def _get_sp_signature(spark: SparkSession, conn_config: Dict[str, Any], sp_name: str) -> list:
    """Resolve the parameter list (position, name, in_out) of a stored procedure.

    Queries USER_ARGUMENTS (objects in the session schema) first, then
    ALL_ARGUMENTS, and caches the result per call name. sp_name may be a bare
    procedure (SP_DELETE_DDS_FACT), a package member (PKG_CDI_UTIL.SP_TRUNCATE)
    or schema-qualified (pyspark.PKG_CDI_UTIL.SP_TRUNCATE) — only the last two
    parts are used for the lookup. Returns a list of
    {"position", "name", "data_type", "in_out"} dicts; empty if unresolvable.
    """
    if sp_name in _SP_SIG_CACHE:
        return _SP_SIG_CACHE[sp_name]
    _parts = sp_name.split('.')
    _proc = _parts[-1]
    _pkg = _parts[-2] if len(_parts) > 1 else ''
    _where = "OBJECT_NAME = '{}' AND DATA_LEVEL = 0".format(_proc)
    if _pkg:
        _where += " AND PACKAGE_NAME = '{}'".format(_pkg)
    _sig = []
    for _view in ("USER_ARGUMENTS", "ALL_ARGUMENTS"):
        _sql = ("SELECT POSITION, ARGUMENT_NAME, DATA_TYPE, IN_OUT FROM {} "
                "WHERE {} ORDER BY POSITION").format(_view, _where)
        try:
            _df = spark.read.format("jdbc") \
                .option("url", get_jdbc_url(conn_config)) \
                .option("user", conn_config.get("username", "")) \
                .option("password", _resolve_password(spark, conn_config)) \
                .option("driver", conn_config.get("driver", "oracle.jdbc.driver.OracleDriver")) \
                .option("query", _sql).load()
            _rows = _df.collect()
        except Exception:
            _rows = None
        if _rows:
            for _r in _rows:
                try:
                    _sig.append({
                        "position": int(_r["POSITION"]),
                        "name": str(_r["ARGUMENT_NAME"]),
                        "data_type": str(_r["DATA_TYPE"]),
                        "in_out": str(_r["IN_OUT"]).upper(),
                    })
                except Exception:
                    pass
            break
    _SP_SIG_CACHE[sp_name] = _sig
    return _sig


def call_stored_procedure(spark: SparkSession, conn_config: Dict[str, Any],
                          sp_name: str, arg_values: list) -> None:
    """Call a stored procedure by name with positional argument values.

    The actual signature is probed at runtime (see _get_sp_signature):
      - IN parameters receive the values as quoted literals (Oracle converts
        them to the target type; None becomes NULL).
      - OUT / IN OUT parameters are bound to DECLARE'd VARCHAR2 variables —
        passing a literal to them raises PLS-00306.
      - If the signature cannot be resolved, falls back to the legacy
        all-literals call so the previous behavior is preserved.
    """
    _sig = _get_sp_signature(spark, conn_config, sp_name)
    _vals = list(arg_values)
    if not _sig:
        # Fallback: legacy behavior — call with all values as string literals
        _arg_txt = ", ".join(
            "'" + str(v).replace("'", "''") + "'" if v is not None else "NULL"
            for v in _vals)
        _sql = "BEGIN {}({}); END;".format(sp_name, _arg_txt)
        execute_sql(spark, conn_config, _sql)
        return
    _args = []
    _decls = []
    for _p in _sig:
        _pos = int(_p["position"])
        _io = (_p["in_out"] or "").replace("_", " ").strip()
        if _io in ("OUT", "IN OUT"):
            # PL/SQL identifiers must start with a letter — "_v4" is a syntax
            # error (PLS-00103), hence the "v_out_" prefix.
            _vname = "v_out_{}".format(_pos)
            _decls.append("{} VARCHAR2(4000)".format(_vname))
            _args.append(_vname)
        else:
            _val = _vals.pop(0) if _vals else None
            if _val is None:
                _args.append("NULL")
            else:
                _args.append("'" + str(_val).replace("'", "''") + "'")
    _call = "{}({})".format(sp_name, ", ".join(_args))
    if _decls:
        _sql = "DECLARE {}; BEGIN {}; END;".format(", ".join(_decls), _call)
    else:
        _sql = "BEGIN {}; END;".format(_call)
    logger = get_logger()
    logger.debug("SP call: %s", _sql)
    execute_sql(spark, conn_config, _sql)


def execute_stored_procedure(spark: SparkSession, conn_config: Dict[str, Any], sp_call: str) -> None:
    """Execute an Oracle stored procedure (BEGIN ... END;) and raise on any error.

    Captures:
      - SP not found (ORA-06550 / PLS-00201)
      - SP execution error (ORA-xxxxx)
      - Connection / JDBC errors
    """
    jdbc_url = get_jdbc_url(conn_config)
    user = conn_config.get("username", "")
    password = _resolve_password(spark, conn_config)
    driver = conn_config.get("driver", "oracle.jdbc.driver.OracleDriver")

    spark._jvm.java.lang.Class.forName(driver)
    conn = spark._jvm.java.sql.DriverManager.getConnection(jdbc_url, user, password)
    cs = None
    try:
        cs = conn.prepareCall(sp_call)
        cs.execute()
    except Exception as e:
        err_msg = str(e)
        # Py4JJavaError wraps the Java exception in java_exception
        if hasattr(e, 'java_exception'):
            try:
                je = e.java_exception
                err_msg = je.getMessage()
                # Also include SQLState / error code if available
                if hasattr(je, 'getSQLState') and je.getSQLState():
                    err_msg += f" (SQLState: {je.getSQLState()})"
                if hasattr(je, 'getErrorCode') and je.getErrorCode():
                    err_msg += f" (ErrorCode: {je.getErrorCode()})"
            except Exception:
                pass
        raise RuntimeError(f"Stored procedure execution failed: {err_msg[:1000]}")
    finally:
        if cs:
            try:
                cs.close()
            except Exception:
                pass
        try:
            conn.close()
        except Exception:
            pass


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


def batch_update(spark: SparkSession, conn_config: Dict[str, Any],
                 table_name: str, set_columns: list, key_columns: list,
                 rows: list, batch_size: int = 1000) -> None:
    """Update rows by composite key using JDBC PreparedStatement (bind variables).

    Generates: UPDATE table SET col1=?, col2=? WHERE key1=? AND key2=?
    Each row in `rows` is a tuple of (set_val1, set_val2, ..., key_val1, key_val2, ...)
    in the order of set_columns followed by key_columns.
    """
    if not rows:
        return
    jdbc_url = get_jdbc_url(conn_config)
    user = conn_config.get("username", "")
    password = _resolve_password(spark, conn_config)
    driver = conn_config.get("driver", "oracle.jdbc.driver.OracleDriver")

    set_clause = ", ".join("{} = ?".format(c) for c in set_columns)
    where_clause = " AND ".join("{} = ?".format(c) for c in key_columns)
    sql = "UPDATE {} SET {} WHERE {}".format(table_name, set_clause, where_clause)

    spark._jvm.java.lang.Class.forName(driver)
    conn = spark._jvm.java.sql.DriverManager.getConnection(jdbc_url, user, password)
    try:
        pstmt = conn.prepareStatement(sql)
        for i, row in enumerate(rows):
            for j, val in enumerate(row):
                pstmt.setString(j + 1, str(val) if val is not None else None)
            pstmt.addBatch()
            if (i + 1) % batch_size == 0:
                pstmt.executeBatch()
        pstmt.executeBatch()
        pstmt.close()
    finally:
        conn.close()


def batch_delete_composite(spark: SparkSession, conn_config: Dict[str, Any],
                           table_name: str, key_columns: list,
                           key_rows: list, batch_size: int = 1000) -> None:
    """Delete rows by composite key using JDBC PreparedStatement.

    WHERE clause uses AND of all key columns: key1=? AND key2=?
    Each row in key_rows is a tuple of (key_val1, key_val2, ...).
    """
    if not key_rows or not key_columns:
        return
    jdbc_url = get_jdbc_url(conn_config)
    user = conn_config.get("username", "")
    password = _resolve_password(spark, conn_config)
    driver = conn_config.get("driver", "oracle.jdbc.driver.OracleDriver")
    where_clause = " AND ".join("{} = ?".format(c) for c in key_columns)
    sql = "DELETE FROM {} WHERE {}".format(table_name, where_clause)
    spark._jvm.java.lang.Class.forName(driver)
    conn = spark._jvm.java.sql.DriverManager.getConnection(jdbc_url, user, password)
    try:
        pstmt = conn.prepareStatement(sql)
        for i, row in enumerate(key_rows):
            for j, val in enumerate(row):
                pstmt.setString(j + 1, str(val) if val is not None else None)
            pstmt.addBatch()
            if (i + 1) % batch_size == 0:
                pstmt.executeBatch()
        pstmt.executeBatch()
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
    # Allow SPARK_CONNECTION env var to override config value. Resolve ${VAR:default}
    # syntax (yaml keeps it as a literal string) before comparing profile names.
    conn_name = os.environ.get("SPARK_CONNECTION") or _resolve_path(
        str(spark_cfg.get("connection", "spark_local")))

    # Look up the connection profile under spark_connections
    profiles = (config or {}).get("spark_connections", {})
    profile = profiles.get(conn_name, profiles.get("spark_local", {}))

    # Auto-kinit: when a kerberos profile (spark3_client / spark3_on_yarn) is
    # selected and no valid ticket exists, kinit automatically using the
    # configured keytab + principal — no manual kinit required.
    if conn_name != "spark_local":
        _kconf = profile.get("config", {})
        _keytab = _kconf.get("spark.kerberos.keytab", "")
        _principal = _kconf.get("spark.kerberos.principal", "")
        if _keytab and _principal:
            import subprocess as _sp
            import shlex as _sh
            _ticket_ok = _sp.run("klist -s", shell=True).returncode == 0
            if not _ticket_ok:
                _kr = _sp.run(
                    "kinit -kt {0} {1}".format(_sh.quote(_keytab), _sh.quote(_principal)),
                    shell=True, stdout=_sp.PIPE, stderr=_sp.PIPE, universal_newlines=True
                )
                if _kr.returncode != 0:
                    print("WARN: kinit failed: " + (_kr.stderr or "").strip())
                else:
                    print("kinit OK: {0} (keytab {1})".format(_principal, _keytab))

    builder = SparkSession.builder.appName(app_name)

    # 1. Set master from profile (or fallback to spark.master) — resolve ${VAR:default}
    master_url = profile.get("master") or _resolve_path(str(spark_cfg.get("master", "")))
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

    # 4b. Keep worker interpreters in lockstep with the driver. Without this,
    #     executors fall back to PATH's default `python`, which may differ from
    #     the interpreter running this script → [PYTHON_VERSION_MISMATCH] Python
    #     in worker has different version than that in driver. sys.executable is
    #     the driver's own interpreter, so `python m_xxx.py` and
    #     `python3.11 m_xxx.py` both work as long as the same interpreter exists
    #     on the executor nodes.
    builder = builder.config("spark.pyspark.python", sys.executable)

    # 4c. Make this workflow's env package importable on executor Python workers.
    #     Worker-side closures in env.runtime_lib (e.g. the dynamic lookup state
    #     machine) are cloudpickled BY REFERENCE (env.runtime_lib.<function>), so
    #     executors must be able to `import env`. YARN executors do not share the
    #     driver's cwd/sys.path — prepend the workflow root directory (parent of
    #     this env/ package) to spark.executorEnv.PYTHONPATH, alongside Spark's
    #     own python + py4j paths already configured in config.yml.
    _wf_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    _epp_key = "spark.executorEnv.PYTHONPATH"
    _epp = ""
    for _src in (profile.get("config", {}), spark_cfg.get("config", {})):
        if str(_src.get(_epp_key) or ""):
            _epp = str(_src[_epp_key])
            break
    if _wf_root not in _epp.split(os.pathsep):
        _epp = _wf_root + os.pathsep + _epp if _epp else _wf_root
    builder = builder.config(_epp_key, _epp)

    # 5. Spark log level — spark.log.level is read at SparkContext construction
    #    (before setLogLevel below), suppressing init-time WARN noise.
    try:
        _sll = _resolve_path(str(spark_cfg.get("log_level", "ERROR"))).upper()
        if _sll in ("ALL", "DEBUG", "INFO", "WARN", "ERROR", "FATAL", "OFF"):
            builder = builder.config("spark.log.level", _sll)
    except Exception:
        pass

    spark = builder.getOrCreate()

    # 4d. Ship this workflow's env package to executors. Worker-side closures
    #     in env.runtime_lib (e.g. the dynamic lookup state machine) are
    #     cloudpickled BY REFERENCE (env.runtime_lib.<function>), so the Python
    #     workers must be able to `import env`. YARN executors run on other
    #     nodes that do NOT have this workflow's directory on disk — the
    #     PYTHONPATH entry from 4c alone is not enough. addPyFile ships a small
    #     zip of env/ (regular package: __init__.py + all *.py) to every
    #     executor; local mode already has the files, so this is best-effort.
    try:
        import io as _io  # noqa: F401
        import zipfile as _zf
        import tempfile as _tf
        _env_dir = os.path.join(_wf_root, "env")
        _zip_path = os.path.join(
            _tf.gettempdir(),
            "pcis_env_%s.zip" % os.path.basename(_wf_root))
        with _zf.ZipFile(_zip_path, "w", _zf.ZIP_DEFLATED) as _z:
            _z.writestr("env/__init__.py", "")
            for _f in sorted(os.listdir(_env_dir)):
                if _f.endswith(".py"):
                    _z.write(os.path.join(_env_dir, _f), "env/" + _f)
        spark.sparkContext.addPyFile(_zip_path)
    except Exception as _exc:
        print("WARN: could not ship env/ to executors: %s" % _exc)

    # Apply Spark log level from config (spark.log_level, e.g. ERROR) to suppress
    # the default WARN noise and Java log4j chatter.
    try:
        _sll = _resolve_path(str(spark_cfg.get("log_level", "ERROR"))).upper()
        if _sll in ("ALL", "DEBUG", "INFO", "WARN", "ERROR", "FATAL", "OFF"):
            spark.sparkContext.setLogLevel(_sll)
    except Exception:
        pass  # best-effort

    all_cfg = {}
    all_cfg.update(profile.get("config", {}))
    all_cfg.update(spark_cfg.get("config", {}))

    # Runtime driver-jar loading. spark.driver.extraClassPath only takes effect at
    # JVM startup (spark-submit --conf); when the session is created in-process
    # (direct python, YARN cluster driver), addJar() explicitly loads each jar
    # into the driver classloader so JDBC drivers (e.g. ojdbc8.jar) resolve.
    try:
        for key, value in all_cfg.items():
            _cp = str(value) if key in ("spark.driver.extraClassPath", "spark.jars") else None
            if _cp:
                for _jar in [p.strip() for p in _cp.split(",") if p.strip()]:
                    if os.path.exists(_jar):
                        spark.sparkContext.addJar(_jar)
    except Exception:
        pass  # best-effort

    # Inject spark.hadoop.* into Hadoop Configuration so absolute paths
    # resolve via the cluster's default filesystem (like @task.pyspark does).
    # Kept separate from addJar so a jar-loading failure cannot skip this.
    try:
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
        optimal = _builtin_max([1, _builtin_min([target_partitions, row_count // min_rows_per_partition])])
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
            .option("isolationLevel", "READ_COMMITTED") \
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


# =============================================================================
# Reusable Workflow Runner
# =============================================================================
# These functions are imported by generated workflow orchestrators.
# Every workflow only needs to define EXECUTION_PLAN, TASK_INFO, and
# MAPPING_FUNCTIONS; the run_workflow() logic below is shared.

logger = logging.getLogger(__name__)


# =============================================================================
# Shared helper functions used by every generated workflow orchestrator
# =============================================================================

def discover_mappings(directory: str = None) -> dict:
    """Auto-discover mapping modules (m_*.py) and return {module_name: run_mapping}.

    Args:
        directory: Directory to scan (default: same directory as caller's file).

    Returns:
        Dict mapping lowercase module names to their run_mapping functions.
    """
    import glob as _glob
    import importlib as _importlib
    import os as _os

    if directory is None:
        caller = inspect.stack()[1]
        directory = _os.path.dirname(caller.filename) or "."
    functions = {}
    for file_path in sorted(_glob.glob(_os.path.join(directory, "m_*.py"))):
        module_name = _os.path.splitext(_os.path.basename(file_path))[0]
        try:
            module = _importlib.import_module(module_name)
            if not hasattr(module, "run_mapping"):
                raise AttributeError(
                    f"Module '{module_name}' missing 'run_mapping' function"
                )
            functions[module_name] = module.run_mapping
            logger.debug("Registered mapping: %s", module_name)
        except Exception as e:
            logger.error("Failed to load mapping '%s': %s", module_name, e)
            raise
    return functions


def format_infa_template(template: str, context: dict) -> str:
    """Replace Informatica-style placeholders with actual values."""
    replacements = {
        "%s": context.get("session_name", ""),
        "%n": context.get("folder_name", ""),
        "%e": context.get("error_code", ""),
        "%b": context.get("error_msg", ""),
        "%c": context.get("session_status", ""),
        "%i": context.get("run_id", ""),
        "%g": context.get("log_file", ""),
    }
    for placeholder, value in replacements.items():
        template = template.replace(placeholder, value)
    return template


def send_email(subject: str, body: str, to_address: str = None):
    """Send email notification via SMTP."""
    import smtplib as _smtplib
    from email.mime.text import MIMEText as _MIMEText

    smtp_host = os.environ.get("SMTP_HOST", "localhost")
    smtp_port = int(os.environ.get("SMTP_PORT", "25"))
    from_address = os.environ.get("SMTP_FROM", "noreply@example.com")
    message = _MIMEText(body, _charset="utf-8")
    message["Subject"] = subject
    message["From"] = from_address
    message["To"] = to_address or from_address
    try:
        with _smtplib.SMTP(smtp_host, smtp_port) as server:
            server.sendmail(from_address, [message["To"]], message.as_string())
        logger.info("Email sent: %s", subject)
    except Exception as e:
        logger.warning("Failed to send email: %s", e)


# =============================================================================
# Reusable Workflow Runner
# =============================================================================

def _resolve_mapping_from_plan(plan_step, mapping_functions):
    """Recursively resolve a mapping name from a plan step for validation."""
    name = plan_step.get("mapping_name", "")
    if name and name.lower() in mapping_functions:
        return
    if not name:
        name = plan_step.get("name", "")
    if name:
        for s in plan_step.get("sessions", []):
            mn = s.get("mapping_name", "")
            if mn and mn.lower() in mapping_functions:
                return
    for s in plan_step.get("steps", []):
        _resolve_mapping_from_plan(s, mapping_functions)
    for s in plan_step.get("plan", []):
        _resolve_mapping_from_plan(s, mapping_functions)


def validate_execution_plan(execution_plan, mapping_functions, task_info=None):
    """Validate every mapping referenced in the plan is registered."""
    valid_types = {"session", "parallel_group", "worklet", "task", "sequential_chain"}
    task_info = task_info or {}

    def _validate_step(step, path=""):
        stype = step.get("type", "")
        if stype not in valid_types:
            raise ValueError(
                f"{path}: unknown step type '{stype}', valid: {valid_types}"
            )
        if stype == "parallel_group":
            for i, s in enumerate(step.get("steps", [])):
                _validate_step(s, f"{path}[{i}]")
        elif stype == "worklet":
            for i, s in enumerate(step.get("plan", [])):
                _validate_step(s, f"{path}/{step.get('name','worklet')}[{i}]")
        elif stype in ("session", "sequential_chain"):
            sessions = step.get("sessions", [step])
            for s in sessions:
                mn = s.get("mapping_name", "")
                if mn and mn.lower() not in mapping_functions:
                    available = list(mapping_functions.keys())
                    raise ValueError(
                        f"{path}: mapping '{mn}' not found. Available: {available}"
                    )
        elif stype == "task":
            name = step.get("name", "")
            if name and task_info and name not in task_info:
                # No special handler for this task (e.g. a Decision with no
                # condition) — it is a pass-through; do not alarm users.
                logger.debug("Task '%s' has no special handler, pass-through", name)

    for i, step in enumerate(execution_plan):
        _validate_step(step, f"Step[{i}]")

    logger.info("Execution plan validation passed: %d top-level steps", len(execution_plan))


def run_sessions_sequential(sessions_list, mapping_functions, ctx, metrics_cls,
                            fail_fast=True, job_params=None, session_sqls=None):
    """Execute a list of sessions one after another (respects DAG order)."""
    completed = set()
    failed = set()
    results = {}
    for session_info in sessions_list:
        session_name = session_info["session_name"]
        mapping_name = session_info.get("mapping_name", session_name)
        metrics = metrics_cls(mapping_name) if metrics_cls else NullMetrics()
        try:
            fn = mapping_functions[mapping_name.lower()]
            logger.info("Start to run mapping %s", mapping_name)
            # Pass the session's Target Pre/Post SQL from the workflow's
            # SESSION_SQLS — the real logic of carrier mappings lives there
            _ok = fn(ctx, metrics, job_params,
                     session_sqls=(session_sqls or {}).get(session_name))
            if not _ok:
                raise RuntimeError("Mapping returned failure status")
            logger.info("Mapping %s completed: SUCCESS", mapping_name)
            completed.add(session_name)
            results[session_name] = {"status": "SUCCESS"}
        except Exception as e:
            logger.error("Session '%s' failed: %s", session_name, e)
            failed.add(session_name)
            results[session_name] = {"status": "FAILED", "error": str(e)}
            if fail_fast:
                break
    return completed, failed, results


def run_sessions_parallel(sessions_list, mapping_functions, ctx, metrics_cls,
                          fail_fast=True, job_params=None, session_sqls=None):
    """Execute a list of sessions concurrently (independent tasks at same DAG level)."""
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from threading import Lock

    completed = set()
    failed = set()
    results = {}
    lock = Lock()
    should_cancel = False

    with ThreadPoolExecutor(max_workers=_builtin_max([1, len(sessions_list)])) as executor:
        future_map = {}
        for session_info in sessions_list:
            session_name = session_info["session_name"]
            mapping_name = session_info.get("mapping_name", session_name)
            metrics = metrics_cls(mapping_name) if metrics_cls else NullMetrics()
            fn = mapping_functions[mapping_name.lower()]
            logger.info("Start to run mapping %s", mapping_name)
            future_map[executor.submit(fn, ctx, metrics, job_params,
                                       session_sqls=(session_sqls or {}).get(session_name))] = (session_name, mapping_name)

        for future in as_completed(future_map):
            if should_cancel:
                future.cancel()
                continue
            session_name, mapping_name = future_map[future]
            try:
                _ok = future.result()
                if not _ok:
                    raise RuntimeError("Mapping returned failure status")
                with lock:
                    completed.add(session_name)
                    results[session_name] = {"status": "SUCCESS"}
                logger.info("Mapping %s completed: SUCCESS", mapping_name)
            except Exception as e:
                logger.error("Session '%s' failed: %s", session_name, e)
                with lock:
                    failed.add(session_name)
                    results[session_name] = {"status": "FAILED", "error": str(e)}
                if fail_fast:
                    should_cancel = True
                    for f, (sname, _) in future_map.items():
                        if f != future and not f.done():
                            f.cancel()
                            logger.warning("Cancelled session '%s' due to fail_fast", sname)

    return completed, failed, results


def _apply_task_timer(tcfg, task_name):
    """Apply Informatica Timer task semantics: a Timer task starts counting
    when its previous task completes (START_RELATIVE_TO_PREVIOUSTASK) and
    fires after the RECURRING interval — wait that interval before the
    downstream tasks are allowed to run."""
    if not tcfg or tcfg.get("type") != "timer":
        return
    _tmr = tcfg.get("timer", {}) or {}
    _days = int(_tmr.get("days", 0) or 0)
    _hours = int(_tmr.get("hours", 0) or 0)
    _mins = int(_tmr.get("minutes", 0) or 0)
    _secs = _days * 86400 + _hours * 3600 + _mins * 60
    if _secs > 0:
        logger.info("Task '%s' timer: waiting %d seconds (%dd %dh %dm)", task_name, _secs, _days, _hours, _mins)
        time.sleep(_secs)


def execute_plan_step(step, mapping_functions, ctx, metrics_cls,
                      fail_fast, job_params, workflow_name="",
                      task_info=None, send_email_fn=None,
                      format_infa_fn=None, session_sqls=None):
    """Execute a single plan step, recursing into worklets and parallel groups.

    Returns (completed: set, failed: set, results: dict).
    Raises RuntimeError if fail_fast triggers.
    """
    completed = set()
    failed = set()
    results = {}
    stype = step.get("type", "")

    if stype == "session":
        sessions_list = [{
            "session_name": step.get("name", ""),
            "mapping_name": step.get("mapping_name", ""),
        }]
        c, f, r = run_sessions_parallel(sessions_list, mapping_functions, ctx,
                                        metrics_cls, fail_fast, job_params,
                                        session_sqls)
        completed.update(c)
        failed.update(f)
        results.update(r)

    elif stype == "parallel_group":
        sub_steps = step.get("steps", [])
        logger.info("Executing parallel group: %d steps", len(sub_steps))
        c, f, r = run_sessions_parallel(
            [{"session_name": s.get("name", s.get("mapping_name", "")),
              "mapping_name": s.get("mapping_name", "")}
             for s in sub_steps if s.get("type") == "session"],
            mapping_functions, ctx, metrics_cls, fail_fast, job_params,
            session_sqls
        )
        completed.update(c)
        failed.update(f)
        results.update(r)
        for s in sub_steps:
            if s.get("type") == "task":
                task_name = s.get("name", "")
                logger.info("Processing task: %s", task_name)
                try:
                    tcfg = task_info.get(task_name, {}) if task_info else {}
                    _apply_task_timer(tcfg, task_name)
                    if tcfg.get("type") == "email" and send_email_fn:
                        ctx_data = {
                            "session_name": task_name,
                            "folder_name": workflow_name,
                        }
                        send_email_fn(
                            subject=format_infa_fn(tcfg.get("subject", ""), ctx_data) if format_infa_fn else tcfg.get("subject", ""),
                            body=format_infa_fn(tcfg.get("text", ""), ctx_data) if format_infa_fn else tcfg.get("text", ""),
                            to_address=tcfg.get("user"),
                        )
                    elif tcfg.get("type") == "command":
                        _cmds = tcfg.get("commands", [])
                        if _cmds:
                            logger.info("Task '%s' commands: %s", task_name, "; ".join([_c.get("value", "") for _c in _cmds]))
                        else:
                            logger.info("Task '%s' has no commands defined", task_name)
                    logger.info("Task '%s' completed: SUCCESS", task_name)
                    completed.add(task_name)
                    results[task_name] = {"status": "SUCCESS"}
                except Exception as e:
                    logger.error("Task '%s' failed: %s", task_name, e)
                    results[task_name] = {"status": "FAILED"}
                    failed.add(task_name)
            elif s.get("type") == "worklet":
                c2, f2, r2 = execute_plan_step(
                    s, mapping_functions, ctx, metrics_cls,
                    fail_fast, job_params, workflow_name,
                    task_info, send_email_fn, format_infa_fn, session_sqls
                )
                completed.update(c2)
                failed.update(f2)
                results.update(r2)

    elif stype == "sequential_chain":
        sessions_list = step.get("sessions", [])
        logger.info("Executing sequential chain: %d sessions", len(sessions_list))
        c, f, r = run_sessions_sequential(sessions_list, mapping_functions, ctx,
                                          metrics_cls, fail_fast, job_params,
                                          session_sqls)
        completed.update(c)
        failed.update(f)
        results.update(r)

    elif stype == "worklet":
        wkl_name = step.get("name", "worklet")
        wkl_plan = step.get("plan", [])
        logger.info("Entering worklet [%s]: %d steps", wkl_name, len(wkl_plan))
        for sub_step in wkl_plan:
            c, f, r = execute_plan_step(
                sub_step, mapping_functions, ctx, metrics_cls,
                fail_fast, job_params, workflow_name,
                task_info, send_email_fn, format_infa_fn, session_sqls
            )
            completed.update(c)
            failed.update(f)
            results.update(r)
            if failed and fail_fast:
                break

    elif stype == "task":
        task_name = step.get("name", "")
        logger.info("Processing task: %s", task_name)
        try:
            tcfg = task_info.get(task_name, {}) if task_info else {}
            _apply_task_timer(tcfg, task_name)
            if tcfg.get("type") == "email" and send_email_fn:
                ctx_data = {
                    "session_name": task_name,
                    "folder_name": workflow_name,
                }
                send_email_fn(
                    subject=format_infa_fn(tcfg.get("subject", ""), ctx_data) if format_infa_fn else tcfg.get("subject", ""),
                    body=format_infa_fn(tcfg.get("text", ""), ctx_data) if format_infa_fn else tcfg.get("text", ""),
                    to_address=tcfg.get("user"),
                )
            elif tcfg.get("type") == "command":
                _cmds = tcfg.get("commands", [])
                if _cmds:
                    logger.info("Task '%s' commands: %s", task_name, "; ".join([_c.get("value", "") for _c in _cmds]))
                else:
                    logger.info("Task '%s' has no commands defined (type=%s)", task_name, tcfg.get("type"))
            logger.info("Task '%s' completed: SUCCESS", task_name)
            completed.add(task_name)
            results[task_name] = {"status": "SUCCESS"}
        except Exception as e:
            logger.error("Task '%s' failed: %s", task_name, e)
            results[task_name] = {"status": "FAILED"}
            failed.add(task_name)

    if failed and fail_fast:
        raise RuntimeError(f"Step failed: {failed}")

    return completed, failed, results


def run_workflow(execution_plan, mapping_functions, workflow_name,
                 task_info=None, send_email_fn=None, format_infa_fn=None,
                 config=None, fail_fast=True, metrics_cls=None,
                 session_sqls=None):
    """Execute a workflow from its execution plan (reusable across workflows).

    Args:
        execution_plan: list of plan-step dicts (top-level, sequential order)
        mapping_functions: dict of {module_name_lower: callable}
        workflow_name: str used for logging and Spark app name
        task_info: optional dict of task_name → config for email tasks
        send_email_fn: optional callable(subject, body, to_address);
                       defaults to this module's send_email
        format_infa_fn: optional callable(template, context) → str;
                        defaults to this module's format_infa_template
        config: optional config dict (loaded from config.yml if None)
        fail_fast: stop at first failure when True
        metrics_cls: optional metrics class (defaults to NullMetrics)

    Returns:
        dict with workflow_name, status, completed, failed, results
    """
    logger.info("=" * 60)
    logger.info("Workflow [%s] START", workflow_name)
    logger.info("=" * 60)

    if send_email_fn is None:
        send_email_fn = send_email
    if format_infa_fn is None:
        format_infa_fn = format_infa_template
    metrics_cls = metrics_cls or NullMetrics
    validate_execution_plan(execution_plan, mapping_functions, task_info)

    if config is None:
        config = load_config("env/config.yml")

    # Inject email config from config.yml into task_info so email recipients
    # can be configured per-environment without regenerating the workflow.
    _email_cfg = config.get("email", {})
    if _email_cfg:
        # Set SMTP env vars from config so send_email() picks them up
        os.environ.setdefault("SMTP_HOST", _email_cfg.get("smtp_host", "localhost"))
        os.environ.setdefault("SMTP_PORT", str(_email_cfg.get("smtp_port", 25)))
        os.environ.setdefault("SMTP_FROM", _email_cfg.get("mail_from", "noreply@example.com"))
    if task_info and _email_cfg.get("mail_to"):
        mail_to = _email_cfg["mail_to"]
        for _tname, _tcfg in task_info.items():
            if isinstance(_tcfg, dict) and _tcfg.get("type") == "email":
                _tcfg["user"] = mail_to

    job_params = {}
    param_file = _resolve_path(config.get("objects", {}).get("UTL_JOB_PARAM", {}).get("path", "/tmp/UTL_JOB_PARAM"))
    if os.path.exists(param_file):
        with open(param_file, "r") as f:
            for line in f:
                line = line.strip()
                if "=" in line:
                    key, value = line.split("=", 1)
                    job_params[key.strip()] = value.strip()
        logger.info("Loaded job params from %s: %s", param_file, job_params)
    else:
        logger.warning("Job param file not found: %s", param_file)

    spark = get_spark_session(workflow_name, config)
    ctx = SparkContext(spark)

    connections = config.get("connections", {})
    logger.info("Validating %d database connections ...", len(connections))
    conn_ok = True
    for cname, ccfg in connections.items():
        if ccfg.get("type", "").lower() in ("oracle", "sqlserver", "postgresql", "mysql"):
            if not test_connection(spark, ccfg):
                logger.error("Connection '%s' is not reachable", cname)
                conn_ok = False
    if not conn_ok:
        spark.stop()
        raise RuntimeError("One or more database connections are unreachable. Aborting workflow.")
    # All connections verified — now persist any interactively-entered passwords
    _flush_pending_passwords()

    completed = set()
    failed = set()
    results = {}

    try:
        # Local helper to send T_MAIL_FAIL — shared by normal and exception paths
        def _send_fail(failed_set, results_dict):
            if not (failed_set and task_info and send_email_fn):
                return
            _fc = task_info.get("T_MAIL_FAIL")
            if _fc and _fc.get("type") == "email":
                _first = next(iter(failed_set), "")
                _ctx = {
                    "session_name": ", ".join(sorted(failed_set)),
                    "folder_name": workflow_name,
                    "error_code": "FAILED",
                    "session_status": "FAILED",
                    "error_msg": str(results_dict.get(_first, {}).get("error", "")),
                    "log_file": "",
                }
                send_email_fn(
                    subject=format_infa_fn(_fc.get("subject", ""), _ctx) if format_infa_fn else _fc.get("subject", ""),
                    body=format_infa_fn(_fc.get("text", ""), _ctx) if format_infa_fn else _fc.get("text", ""),
                    to_address=_fc.get("user"),
                )

        for plan_step in execution_plan:
            try:
                c, f, r = execute_plan_step(
                    plan_step, mapping_functions, ctx, metrics_cls,
                    fail_fast, job_params, workflow_name,
                    task_info, send_email_fn, format_infa_fn, session_sqls
                )
                completed.update(c)
                failed.update(f)
                results.update(r)
            except RuntimeError as _wf_err:
                # fail_fast raised this — capture failure info before propagating
                _wf_msg = str(_wf_err)
                if "Step failed:" in _wf_msg:
                    import re as _re
                    for _m in _re.finditer(r"'([^']+)'", _wf_msg.split("Step failed:")[1]):
                        _name = _m.group(1)
                        if _name:
                            failed.add(_name)
                            if _name not in results:
                                results[_name] = {"status": "FAILED", "error": _wf_msg}
                if not failed:
                    failed.add("UNKNOWN")
                    results["UNKNOWN"] = {"status": "FAILED", "error": _wf_msg}
                _send_fail(failed, results)
                raise
            if failed and fail_fast:
                break

        _send_fail(failed, results)

        workflow_status = "FAILED" if failed else "SUCCESS"
        logger.info("Workflow %s: completed=%d, failed=%d", workflow_status, len(completed), len(failed))
        logger.info("=" * 60)
        logger.info("Workflow [%s] END - %s", workflow_name, workflow_status)
        logger.info("=" * 60)
        return {"workflow_name": workflow_name, "status": workflow_status,
                "completed": list(completed), "failed": list(failed), "results": results}
    finally:
        spark.stop()
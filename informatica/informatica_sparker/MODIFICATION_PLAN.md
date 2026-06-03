# Informatica-Sparker 修改方案（参考 informatica-python）

## 参考架构对比

| 特性 | informatica-python ✅ | informatica-sparker ❌ |
|------|---------------------|----------------------|
| **共享运行时库** | `helper_functions.py` — 所有通用代码在一个文件 | 每个 mapping 文件内联 ~500 行重复代码 |
| **数据库类型检测** | 从 XML 的 `database_type` 字段正确获取 Oracle/MSSQL | 解析阶段正确检测，但模板硬编码 MSSQL |
| **Config 生成** | 基于 XML 源/目标定义动态生成 `config.yml`，包含正确的 `type: oracle` | 硬编码 `connections` 为 MSSQL（msscdm_dev） |
| **SQL 方言翻译** | `sql_dialect.py` 提供 Oracle→ANSI/MSSQL→ANSI 翻译 | 无 SQL 翻译，直接推送 Oracle SQL 到 MSSQL |
| **Mapping 变量支持** | 定义 `v_snsh_date = ''`，SQL 中使用 `$$v_snsh_date` 占位 | `$$v_snsh_date` 未被识别和处理 |
| **更新策略** | 直接使用 `pd.merge` 实现 upsert | 仅添加注释，未实际实现 |
| **数据库连接函数** | `read_from_db()` / `write_to_db()` — 从 config 动态获取连接参数 | 硬编码 `read_mssql_query()` / `write_mssql_target()` |
| **工作流编排** | 从 XML WORKFLOW 解析任务依赖，正确生成串行/并行逻辑 | 固定顺序执行全部 mapping |

---

## 修改方案

### 修改 1: 提取共享运行时库（参考 helper_functions.py）

**目标**: 消除每个 mapping 文件中的 ~500 行重复代码

**参考**: `informatica-python` 将所有通用函数提取到 `helper_functions.py`，mapping 文件仅 `from helper_functions import *`

**具体改动**:

#### 1a. 创建 `runtime_lib.py.j2` 模板

参考 `informatica_python/generators/helper_gen.py`，生成包含以下内容的共享库：
- 配置加载（`load_config`, `resolve_env`）
- 数据库连接管理（`get_db_connection`, `read_from_db`, `write_to_db`）
- 文件读写函数（`read_file`, `write_file`）
- 表达式辅助函数（`iif`, `decode`, `nvl`）
- 列名处理函数（`normalize_column_names`）
- Delta Lake 操作函数
- Mapping 变量解析（`get_param`, `resolve_builtin_variable`）

**数据库连接函数**应支持多数据库类型（参考 `helper_functions.py` 的 `get_db_connection`）：
```python
def get_db_connection(config, connection_name='default'):
    conn_config = config.get('connections', {}).get(connection_name, {})
    db_type = conn_config.get('type', 'oracle')
    # 根据 db_type 自动选择 JDBC 驱动和 URL 格式
    if db_type == 'oracle':
        url = f"jdbc:oracle:thin:@{host}:{port}/{database}"
        driver = "oracle.jdbc.driver.OracleDriver"
    elif db_type == 'sqlserver':
        url = f"jdbc:sqlserver://{host}:{port};databaseName={database}"
        driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    ...
```

#### 1b. 修改 `mapping.py.j2` 模板

去除所有内联的函数定义，改为引用 `runtime_lib`：
```jinja2
from runtime_lib import (
    load_config, get_db_connection, read_from_db, write_to_db,
    normalize_column_names, infa_iif, infa_decode, infa_nvl,
    get_param, resolve_builtin_variable, MappingMetrics, SparkContext
)

# Load configuration
config = load_config()

# =============================================================================
# MAPPING LOGIC
# =============================================================================

def run_mapping(config, ctx: SparkContext = None):
    spark = ctx.spark if ctx else get_spark_session("{{ mapping_name }}")
    mapping_vars = config.get('variables', {})
    
    {% for step in steps %}
    {{ step.code }}
    {% endfor %}
```

---

### 修改 2: 基于 XML 元数据动态生成 Config（参考 config_gen.py）

**目标**: Config 中的数据库类型、连接信息来自 XML 而非硬编码

**参考**: `informatica_python/generators/config_gen.py`

**具体改动**:

#### 2a. 修改 `service.py` 中的 `_generate_config_file` 方法

遍历 `folder.sources` 和 `folder.targets`，从 XML 元数据获取：
```python
for src in folder.sources:
    conn_info = {
        "type": get_db_type(src.database_type),  # "Oracle" → "oracle"
        "host": "${DB_HOST}",
        "port": _get_default_port(src.database_type),  # "Oracle" → 1521
        "database": src.db_name,    # "DDS" / "DPA"
        "schema": src.owner_name,   # "PDDS" / "PDPA"
    }
```

端口映射（参考 `config_gen.py` 的 `_get_default_port`）：
```python
port_map = {"Microsoft SQL Server": 1433, "Oracle": 1521, ...}
```

#### 2b. 修改 `config.yml.j2` 模板

不再硬编码 `CDM_PRE_LANDING` / `msscdm_dev`，而是根据 mapping 的源/目标动态渲染。

---

### 修改 3: 添加 SQL 方言翻译引擎（参考 sql_dialect.py）

**目标**: 解决 Oracle SQL pushdown 到 MSSQL 不兼容的问题

**参考**: `informatica_python/utils/sql_dialect.py`

**具体改动**:

#### 3a. 新建 `informatica_sparker/utils/sql_dialect.py`

复制 `informatica_python/utils/sql_dialect.py` 的核心翻译逻辑：

```python
# Oracle → Spark SQL 翻译规则
ORACLE_TO_SPARK = [
    (r"\bNVL\(([^,]+),([^)]+)\)", r"COALESCE(\1, \2)"),
    (r"\bSYSDATE\b", "CURRENT_DATE"),
    (r"\bto_date\(([^,]+),'([^']+)'\)", r"TO_DATE(\1, '\2')"),
    (r"\badd_months\(([^,]+),([^)]+)\)", r"ADD_MONTHS(\1, \2)"),
    (r"\btrunc\(([^)]+)\)", r"CAST(\1 AS DATE)"),
    (r"\bto_char\(([^,]+),'([^']+)'\)", r"DATE_FORMAT(\1, '\2')"),
    (r"\bsysdate\b", "CURRENT_DATE"),
    (r"\bnvl\(([^,]+),([^)]+)\)", r"COALESCE(\1, \2)"),
]

# Oracle outer join (+) → LEFT/RIGHT JOIN 转换
def convert_oracle_outer_join(sql):
    # 处理 t1.col1 = t2.col1 (+) → LEFT JOIN
    # 处理 t1.col1 (+) = t2.col1 → RIGHT JOIN
```

#### 3b. 修改 `handlers.py` 的 `_handle_source_qualifier`

在设置 SQL pushdown 前添加翻译步骤：
```python
if use_sql_pushdown:
    from .utils.sql_dialect import translate_sql, detect_sql_dialect
    
    source_db_type = self._get_source_db_type(instance)
    target_db_type = self._get_target_db_type()
    
    # 翻译 Oracle SQL 为目标数据库/Spark SQL
    if source_db_type == 'oracle' and target_db_type != 'oracle':
        translated_sql = translate_sql(final_sql, 
                                        source_dialect='oracle', 
                                        target_dialect='spark')
        step.params["sql_query"] = translated_sql
    else:
        step.params["sql_query"] = final_sql
```

---

### 修改 4: 代码生成器支持多数据库（参考 lib_adapters.py 模式）

**目标**: `codegen.py` 根据数据库类型生成正确的读/写代码

**参考**: `informatica_python/utils/lib_adapters.py` (adapter pattern)

**具体改动**:

#### 4a. 修改 `codegen.py` 的 `generate()` 方法

接收 `source_db_type` 和 `target_db_type` 参数，传递给模板：
```python
def generate(self, plan, user_config, source_db_type='oracle', target_db_type='oracle'):
    template = self.env.get_template("mapping.py.j2")
    return template.render(
        source_db_type=source_db_type,
        target_db_type=target_db_type,
        ...
    )
```

#### 4b. 修改 `codegen.py` 的 `_generate_step_code_v2`

`READ_SQL` 分支根据数据库类型选择函数名：
```python
if step.step_type == IRStepType.READ_SQL:
    db_type = step.params.get("db_type", "oracle")  # 从解析结果传入
    read_func = f"read_{db_type}_query"  # → read_oracle_query / read_mssql_query
```

---

### 修改 5: 处理 Mapping 变量（`$$v_snsh_date`）

**目标**: `$$` 参数能正确传递到运行时

**参考**: `informatica-python` 的 `config_gen.py` 和 `mapping_gen.py`

**具体改动**:

#### 5a. 修改 `expr_translator.py` 的 `_replace_pm_variables`

添加 `$$` 变量处理：
```python
# 处理 $$ 变量 → 变为配置参数引用
remaining_global = re.findall(r'\$\$[A-Za-z_][A-Za-z0-9_]*', result)
for var_name in remaining_global:
    clean_name = var_name.replace("$$", "")
    # 替换为 config 中的变量引用
    result = result.replace(var_name, f"${{{{{clean_name}}}}}")
```

#### 5b. 在生成的 mapping 代码中添加变量定义

```python
# Mapping Variables
v_snsh_date = config.get('variables', {}).get('v_snsh_date', '')
```

---

### 修改 6: 实现 Update Strategy

**目标**: DD_DELETE/DD_UPDATE/DD_INSERT 正确生成代码

**参考**: `informatica-python` 使用 merge/upsert 实现

**具体改动**:

#### 6a. 修改 `codegen.py`，添加 `APPLY_UPDATE_STRATEGY` 处理

```python
elif step.step_type == IRStepType.APPLY_UPDATE_STRATEGY:
    strategy = step.params.get("strategy_expression", "DD_INSERT")
    has_delete = step.params.get("has_delete", False)
    has_update = step.params.get("has_update", False)
    
    if has_delete:
        lines.append(f'{step.df_output} = {step.df_input}.withColumn("_op", lit("delete"))')
    elif has_update:
        lines.append(f'{step.df_output} = {step.df_input}.withColumn("_op", lit("update"))')
    else:
        lines.append(f'{step.df_output} = {step.df_input}.withColumn("_op", lit("insert"))')
    
    # 目标写入前根据 _op 执行不同操作
    if has_delete:
        lines.append(f'# Execute delete: DELETE FROM {table_name} WHERE ...')
        lines.append(f'execute_sql(config, "DELETE FROM {table_name} WHERE ...", "{conn_alias}")')
```

---

## 修改优先级和实施路线

### Phase 1 (P0 — 阻断性问题) — 2-3天

| # | 修改 | 文件 | 预期效果 |
|---|------|------|---------|
| 1 | 提取 `runtime_lib.py` | 新建 + `mapping.py.j2` | 消除重复代码，集中管理 DB 连接逻辑 |
| 2 | Config 动态生成 | `service.py` + `config.yml.j2` | config.yml 包含正确的 Oracle 连接信息 |
| 3 | 添加 SQL 方言翻译 | 新建 `utils/sql_dialect.py` + `handlers.py` | Oracle SQL 自动翻译为 Spark SQL |
| 4 | 处理 `$$` 变量 | `expr_translator.py` + `mapping.py.j2` | `$$v_snsh_date` 参数可传递 |

### Phase 2 (P1 — 功能完整性) — 1-2天

| # | 修改 | 文件 | 预期效果 |
|---|------|------|---------|
| 5 | 代码生成器多 DB 适配 | `codegen.py` | 根据源/目标 DB 类型生成不同的 JDBC 代码 |
| 6 | Update Strategy 实现 | `codegen.py` | DD_DELETE/DD_INSERT 正确执行 |
| 7 | 工作流并行执行 | `workflow_orchestration.py.j2` | 并行执行独立 mapping |

### Phase 3 (P2 — 优化) — 1天

| # | 修改 | 文件 | 预期效果 |
|---|------|------|---------|
| 8 | Schema/Owner 映射 | `handlers.py` | `PDDS.TABLE_NAME` 格式正确 |
| 9 | CLI `--source-db` 参数 | `cli.py` | 用户可指定源数据库类型 |

---

## 示例：修改后的代码结构

```
WF_GMS_DDS_APLY_DLY_SPARK_fixed/
├── runtime_lib.py              # 共享运行时库（所有通用代码）
├── config.yml                  # 动态生成，包含 Oracle 连接
├── m_dds_apl_fact_gms_dly_msd_smry.py  # ~80行（仅业务逻辑）
├── m_dds_apl_fact_gms_dly_dog_rgstr.py # ~80行
├── m_dds_apl_fact_gms_dly_msd_incdt.py # ~80行
├── m_dpa_sum_fact_gms_dly_msd_smry.py  # ~80行
├── ...
├── workflow.py                 # 正确的工作流编排
└── all_sql_queries.sql
```

对比修改前：每个 mapping 文件 ~800 行，其中 ~720 行为重复样板代码。

---

## 核心差异对照

| 功能 | informatica-python 做法 | informatica-sparker 应如何改 |
|------|------------------------|---------------------------|
| **DB 连接** | `config.yml` 有 `type: oracle` → `get_db_connection()` 动态选择驱动 | `config.yml` 应包含 `type` 字段 → `runtime_lib` 根据 `type` 选 JDBC 驱动 |
| **SQL 翻译** | `sql_dialect.py` 的 `translate_sql()` | 在 `handlers.py` 的 SQL pushdown 前调用 `translate_sql()` |
| **共享库** | 每个 mapping `from helper_functions import *` | 改为 `from runtime_lib import *` |
| **Config 来源** | 遍历 `folder.sources/targets` 动态生成 | 同样遍历 XML 解析结果，而不是硬编码 |
| **变量处理** | `config['variables']` 含 `v_snsh_date` | mapping 代码从 `config['variables']` 读取 |

# PySpark 速查与知识点

## ⚡ 核心 API 速查

### 数据读取
```python
# Oracle（JDBC）
spark.read.format('jdbc') \
    .option('url', jdbc_url) \
    .option('dbtable', 'table_name') \
    .option('user', user).option('password', pwd) \
    .option('driver', 'oracle.jdbc.driver.OracleDriver') \
    .load()

# Parquet
df = spark.read.parquet('/path/file')
```

### 数据转换
```python
df.withColumn('new_col', expression)
when(condition, value).otherwise(value2)
df.filter(condition)
```

### 聚合与排序
```python
df.groupBy('dim1', 'dim2').agg(
    count('*').alias('cnt'),
    sum('value').alias('total'),
    avg('value').alias('avg_val'),
    countDistinct('id').alias('distinct_ids')
)
df.orderBy(col('col').desc()).limit(10)
```

### 窗口函数（移动平均）
```python
from pyspark.sql.window import Window
w = Window.orderBy('date_col').rangeBetween(-6, 0)
df.withColumn('ma7', avg('value').over(w))
```

### 数据输出
```python
df.coalesce(1).write.mode('overwrite').parquet('/path')
df.coalesce(1).write.mode('overwrite').csv('/path', header=True)
```

---

## 🔍 调试技巧

| 问题 | 解决 |
|------|------|
| 数据量大执行慢 | 先用 `df.limit(1000)` 测试逻辑 |
| 聚合结果全是 null | `df.select(count(when(col('c').isNull(), 1)))` 检查空值 |
| 内存不足 | `.option('numPartitions', '4')` 控制分区数 |
| `disp_date_id` 数字 YYYYMMDD | `to_date(col('disp_date_id').cast('string'), 'yyyyMMdd')` |
| 结果显示不全 | 用 `.show(truncate=False)` 完整显示 |

---

## 一、题目核心知识点覆盖

### 1. 数据读取与连接管理
```python
spark.read.format('jdbc') \
    .option('url', jdbc_url) \
    .option('dbtable', 'table_name') \
    .load()
```

### 2. 数据清洗与转换 — withColumn、when/otherwise
```python
df.withColumn('total_cost', 
    when(col('itemcost').isNull() | col('disp_qty').isNull(), 0)
    .otherwise(col('itemcost') * col('disp_qty'))
)
```

### 3. 聚合分析 — groupBy、agg、countDistinct
- 按机构、患者维度的多维聚合统计

### 4. 排序与Top N — orderBy、limit
- 获取配送成本 Top 10 的机构

### 5. 窗口函数 — Window.orderBy()、rangeBetween
```python
window_spec = Window.orderBy('disp_date_id').rangeBetween(-6, 0)
df.withColumn('ma7_qty', avg('daily_total_qty').over(window_spec))
```

### 6. 条件过滤 — filter、where
- 多条件异常值检测

### 7. 数据输出 — parquet、CSV、coalesce
- 批量保存多个分析结果

---

## 二、常见问题解答

**Q1: 如何处理null值？**
```python
# 用0填充
.withColumn('col', when(col('col').isNull(), 0).otherwise(col('col')))
# 删除null行
.na.drop(subset=['col1', 'col2'])
```

**Q2: 如何计算比率/百分比？**
```python
count(when(condition, 1)) / df.count() * 100
```

**Q3: 窗口函数什么时候用？**
- 排名：`dense_rank()` / `row_number()` over (partition by ... order by ...)
- 移动平均：`rangeBetween(-n, 0)`

**Q4: 如何优化性能？**
- 及时 filter 减少数据量
- 合理设置 shuffle 分区数
- 使用 parquet 格式存储
- 避免多次 action（使用 cache()）

---

## 三、函数参考表

| 函数 | 用途 | 示例 |
|-----|------|------|
| `count(*)` | 计数 | `count('*')` |
| `countDistinct()` | 去重计数 | `countDistinct('patient_id')` |
| `sum()` | 求和 | `spark_sum('disp_qty')` |
| `avg()` | 平均值 | `avg('unitprice')` |
| `when/otherwise` | 条件赋值 | `when(col('status')=='Y', 1).otherwise(0)` |
| `coalesce()` | 返回首个非null | `coalesce(col('A'), col('B'), 0)` |
| `to_date()` | 字符串转日期 | `to_date('20240101', 'yyyyMMdd')` |

---

## 四、代码模板

```python
# 模板1：基础聚合
result = df.groupBy('dimension').agg(
    count('*').alias('count'),
    spark_sum('value').alias('total'),
    avg('value').alias('average')
).orderBy(col('total').desc()).limit(10)

# 模板2：条件统计
result = df.select(
    count(when(condition1, 1)).alias('metric1'),
    count(when(condition2, 1)).alias('metric2'),
)

# 模板3：窗口函数
w = Window.partitionBy('group').orderBy('date')
result = df.withColumn('rank', row_number().over(w)).filter(col('rank') <= 3)

# 模板4：数据保存
df.coalesce(1).write.mode('overwrite').parquet('/path/to/output')
```

---

## 五、进阶挑战

1. **性能优化** — 使用 cache() 缓存中间结果，分析执行计划
2. **复杂分析** — 实现患者 RFM 分析（Recency/Frequency/Monetary）
3. **机器学习** — 用 MLlib 预测下月配送量

## 六、参考资源

- [Apache Spark 官方文档](https://spark.apache.org/docs/latest/)
- [PySpark API 文档](https://spark.apache.org/docs/latest/api/python/)

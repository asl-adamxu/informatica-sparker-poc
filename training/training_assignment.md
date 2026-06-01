# PySpark 实战训练题

## 数据背景
`airflow.bda_study_source_table` 是一个医疗药物配送系统的数据表，包含患者、处方、药品、机构等多维度信息。

## 题目要求

### 实战题：药品配送分析报告

基于 `airflow.bda_study_source_table` 表，完成以下分析任务：

#### 任务1：基础数据清洗与探索（必做）
- 读取Oracle中的数据
- 统计总行数
- 显示数据schema
- 检查是否存在空值的关键列（patient_id, disp_qty, unitprice, itemcost）

#### 任务2：单机构配送分析（必做）
- 按 `phs_inst_cd`（药房机构代码）统计：
  - 配送交易数量
  - 总配送数量（disp_qty）
  - 总配送成本（itemcost * disp_qty）
  - 平均单价
- 按配送成本降序排列，显示Top 10

#### 任务3：患者用药模式分析（必做）
- 按 `patient_id` 分组统计：
  - 患者配送次数
  - 患者总用药数量
  - 患者关联的处方数
  - 患者关联的不同药品种类数
- 筛选配送次数>=3的患者，按配送次数降序显示Top 5

#### 任务4：时间维度分析（进阶）
- 按 `disp_date_id`（配送日期ID）统计每日的：
  - 日均配送量
  - 日均配送成本
  - 日活跃患者数
  - 日活跃机构数
- 计算近7天（按日期ID）的移动平均配送量

#### 任务5：数据质量检查（进阶）
- 检查 `multi_dose_ind` 和 `refill_ind` 字段的数据分布
- 分析重复配送情况（同一患者同一项目多次配送的比例）
- 识别异常值：
  - 配送数量异常（> 5000）
  - 单价异常（> 1000）
- 生成异常项目汇总报告

#### 任务6：数据输出（进阶）
- 将分析结果保存为Parquet格式
- 同时保存一份CSV汇总报告

---

## 数据连接信息
- 数据库连接：oracle_107
- Schema：airflow
- 表名：bda_study_source_table

## 关键列说明
| 列名 | 说明 | 数据类型 |
|-----|------|--------|
| patient_id | 患者ID | NUMBER |
| phs_inst_cd | 药房机构代码 | VARCHAR2 |
| disp_qty | 配送数量 | NUMBER |
| disp_qty_abs | 配送数量（绝对值） | NUMBER |
| unitprice | 单价 | NUMBER |
| itemcost | 项目成本 | NUMBER |
| disp_date | 配送日期 | TIMESTAMP |
| disp_date_id | 配送日期ID（YYYYMMDD格式） | NUMBER |
| presc_no | 处方号 | NUMBER |
| item_no | 项目号 | NUMBER |
| multi_dose_ind | 多剂次标志 | VARCHAR2 |
| refill_ind | 重复配送标志 | VARCHAR2 |
| action_type | 操作类型 (I/U) | VARCHAR2 |

---

## 完成要求
1. 完整脚本文件
2. 在 Airflow 上运行成功

## 评分标准
- 功能完成度：60% (每个任务10%)
- 第一次成功运行时间：30%
- 代码规范性和可读性：10%

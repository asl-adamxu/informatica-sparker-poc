# WF_EMS_DDS_APLY_MTH — Informatica PowerCenter Workflow 总结文档

> 创建日期: 2026-06-02 | 仓库: `rep_svc_petl` | 文件夹: `PCIS01` | 数据库: Oracle

---

## 1. 概述

该 PowerCenter 工作流（Workflow）属于 **DDS（Data Distribution System，数据分发系统）** 模块，名为 **WF_EMS_DDS_APLY_MTH**，用于按月汇总 EMS（物业管理）系统中的申请（Application）相关交易数据。

主要功能是从 **SOR（Source of Record，源记录系统）** 和 **DPA（Data Processing Area，数据处理区）** 多个表中抽取、清洗、汇总数据，最终写回 **DPA** 层的多个事实表。

---

## 2. 数据源 (Sources)

| 源表 | 所属 Schema | 说明 |
|---|---|---|
| `DPA_FACT_EMS_SMS_APLY_TYPE_TXN` | pdpa (DPA) | 按申请类型分类的交易事实表 |
| `DPA_FACT_EMS_SMS_RENT_TXN` | pdpa (DPA) | 租金交易事实表 |
| `DPA_FACT_EMS_SMS_DSTR_TXN` | pdpa (DPA) | 区域交易事实表 |
| `DPA_FACT_EMS_SMS_PGS` | pdpa (DPA) | PGS 数据事实表 |
| `SOR_EMS_TPS_APLY_STS` | psor (SOR) | TPS 申请状态表 |
| `SOR_EMS_SMS_CEP_APLY_STS` | psor (SOR) | CEP 申请状态表 |
| `SOR_EMS_SMS_CEP_APLY` | psor (SOR) | CEP 申请表 |
| `SOR_EMS_SMS_CEP_CERT_STS` | psor (SOR) | CEP 证书状态表 |
| `SOR_EMS_SMS_CEP_CERT` | psor (SOR) | CEP 证书表 |
| `SOR_EMS_SMS_CAS_APLY_STS` | psor (SOR) | CAS 申请状态表 |
| `SOR_EMS_SMS_CAS_APLY` | psor (SOR) | CAS 申请表 |
| `SOR_EMS_SMS_CAS_CERT_STS` | psor (SOR) | CAS 证书状态表 |
| `SOR_EMS_SMS_CAS_CERT` | psor (SOR) | CAS 证书表 |
| `SOR_EMS_SMS_LN` | psor (SOR) | 贷款表 |
| `SOR_EMS_SMS_LN_STS` | psor (SOR) | 贷款状态表 |
| `SOR_EMS_SMS_LN_APLY` | psor (SOR) | 贷款申请表 |
| `SOR_EMS_SMS_LN_APLY_STS` | psor (SOR) | 贷款申请状态表 |

---

## 3. 目标表 (Targets)

| 目标表 | 说明 |
|---|---|
| `DPA_FACT_EMS_SMS_APLY_TYPE_TXN` | 按申请类型汇总的交易事实表 |
| `DPA_FACT_EMS_SMS_RENT_TXN` | 租金交易事实表 |
| `DPA_FACT_EMS_SMS_FLAT_SIZE_TXN` | 按单位面积分组的交易事实表 |
| `DPA_FACT_EMS_SMS_PREM_PYMT` | 保费支付事实表 |
| `DPA_FACT_EMS_SMS_DSTR_TXN` | 区域交易事实表 |
| `DPA_FACT_EMS_SMS_PGS` | PGS 数据事实表 |
| `DPA_FACT_EMS_SMS_TXN` | 交易汇总事实表 |
| `DPA_FACT_EMS_SMS_CRT_HGST_SALE` | 法院最高拍卖价事实表 |
| `DPA_FACT_EMS_SMS_FLAT_PRC_TXN` | 单位价格交易事实表 |

---

## 4. 映射 (Mappings) 详解

该工作流包含多个 Mapping，以下是核心 Mapping 及其 **Expression 转化逻辑** 的详细分析。

---

### 4.1 `M_S5_DPA_SUMMARIZE_FACT_EMS_SMS_APLY_TYPE_TXN`

**功能**: 按申请类型（APLY_TYPE_CODE）汇总交易数据，包括内部转让（PRH/INT）、联权共有（COT/COT）、遗产继承（THA/HSO）等类型。

#### 数据流

```
SOR (LN/LN_STS/CEP等) --SQ--> EXPTRANS1/Union --> AGGTRANS3 --> EXPTRANS --> LKP --> Target
DPA (历史事实表) --SQ--> LKP --> EXPTRANS3 --> Target
DPA (历史总和) --SQ--> LKP --> EXPTRANS2 --> Target
```

#### Expression 转化逻辑详解

##### (1) **EXPTRANS1** — 单位地址码转申请类型

| 输出字段 | 表达式 | 说明 |
|---|---|---|
| `APLY_TYPE_CODE` | `DECODE(UNIT_CODE_ADDR, 'E', 'PRH', 'C', 'COT', 'R', 'HSO', 'L', 'THA')` | 根据单位地址码首字符映射为申请类型编码 |

**业务逻辑**:
- `E` (Estate  estate) → `PRH` (Private Housing)
- `C` (Commercial) → `COT` (Commercial Other)
- `R` (Rural) → `HSO` (Home Ownership)
- `L` (Lease) → `THA` (Tertiary Housing)

##### (2) **EXPTRANS** — 核心 Expression（申请类型明细映射）

| 输出字段 | 表达式 | 说明 |
|---|---|---|
| `DESP_DTL_SCHM_CODE` | `decode(SCHM_CODE, 'T', 'no_of_tran_tps', 'no_of_tran_hos')` | 根据 Schema 编码区分 TPS（私人）和 HOS（公营） |
| `APLY_TYPE_CODE_OUT` | `RTRIM(LTRIM(APLY_TYPE_CODE))` | 去除申请类型码两端空格 |
| `TIME` | `to_date(concat($$v_rpt_mth, '01'), 'yyyymmdd')` | 将报表月份参数转为日期（当月1日） |
| `LAST_REC_TXN_DATE` | `SYSDATE` | 当前系统时间 |
| `SMS_SCHM_CODE` | `'SMS'` | 固定为 SMS Schema |

##### (3) **EXPTRANS2** — 历史数据汇总 + 总数计算

| 输出字段 | 表达式 | 说明 |
|---|---|---|
| `CNT1_V` | `IIF(ISNULL(CNT1), 0, CNT1)` | 处理 NULL，历史 no_of_tran_hos 计数 |
| `CNT2_V` | `IIF(ISNULL(CNT2), 0, CNT2)` | 处理 NULL，历史 no_of_tran_tps 计数 |
| `TOT_CNT` | `CNT1_V + CNT2_V` | 两路数据求和 = 总数 |
| `DESP_DTL_CODE` | `'no_of_tran_total'` | 标记为"交易总数" |
| `DESP_DTL_SCHM_CDOE` | `'SMS'` | 固定 SMS Schema |

##### (4) **EXPTRANS3** — 五种申请类型的汇总求和

| 局部变量 | 表达式 | 说明 |
|---|---|---|
| `TXN_CNT_V` | `iif(ISNULL(TXN_CNT), 0, TXN_CNT)` | PRH 计数，NULL 转 0 |
| `TXN_CNT1_V` | `iif(ISNULL(TXN_CNT1), 0, TXN_CNT1)` | 原始数据源计数，NULL 转 0 |
| `TXN_CNT2_V` | `iif(ISNULL(TXN_CNT2), 0, TXN_CNT2)` | THA 计数，NULL 转 0 |
| `TXN_CNT3_V` | `iif(ISNULL(TXN_CNT3), 0, TXN_CNT3)` | COT 计数，NULL 转 0 |
| `TXN_CNT4_V` | `iif(ISNULL(TXN_CNT4), 0, TXN_CNT4)` | HSO 计数，NULL 转 0 |
| `SUM` | `TXN_CNT_V + TXN_CNT1_V + TXN_CNT2_V + TXN_CNT3_V + TXN_CNT4_V` | **五种申请类型计数汇总求和** |
| `APLY_TYPE_CODE` | `'ST'` | 固定类型码 ST (Sub-Total) |
| `SCHM_CODE` | `'SMS'` | 固定 Schema |

#### 查询逻辑（Source Qualifier）

**SQ_SOR_1** — 非 E9 地址（按地址首字母分类 E/C/R/L）:
```sql
SELECT aply_sts.SCHM_CODE, substr(UNIT_CODE_ADDR, 1, 1)
FROM SOR_EMS_SMS_LN ln, SOR_EMS_SMS_LN_STS ln_sts, ...
WHERE ln_sts.LN_STS_CODE IN ('V','S')
  AND UNIT_CODE_ADDR IS NOT NULL
  AND substr(UNIT_CODE_ADDR, 1, 2) <> 'E9'
  AND substr(UNIT_CODE_ADDR, 1, 1) IN ('E','C','R','L')
  AND trunc(ln_sts.LN_ISS_DATE, 'MM') = to_date($$v_rpt_mth,'YYYYMM')
  AND TO_DATE($$V_SNSH_DATE, 'YYYYMMDD') BETWEEN ln_sts.BGN_DATE AND ln_sts.END_DATE
  -- ... 其他时间范围条件
```

**SQ_SOR_2** — E9 地址（INT 类型）:
```sql
SELECT aply_sts.SCHM_CODE, 'INT'
FROM ...
WHERE ln_sts.LN_STS_CODE IN ('V','S')
  AND UNIT_CODE_ADDR IS NOT NULL
  AND substr(UNIT_CODE_ADDR, 1, 2) = 'E9'
  AND trunc(ln_sts.LN_ISS_DATE, 'MM') = to_date($$v_rpt_mth,'YYYYMM')
```

**SQ_SOR_3** — GF_CERT_TYPE_CODE 不为空的记录:
```sql
SELECT aply_sts.SCHM_CODE, cep_sts.GF_CERT_TYPE_CODE
FROM ...
WHERE ln_sts.LN_STS_CODE IN ('V','S')
  AND cep_sts.GF_CERT_TYPE_CODE IS NOT NULL
  AND trunc(ln_sts.ln_iss_date, 'MM') = to_date($$v_rpt_mth,'YYYYMM')
```

---

### 4.2 `M_S5_DPA_SUMMARIZE_FACT_EMS_SMS_RENT_TXN`

**功能**: 按租金因子率（RENT_FCTR_RATE）和 Schema 汇总租金交易数据。

#### Expression 转化逻辑

##### EXPTRANS — 核心表达式

| 输出字段 | 表达式 | 说明 |
|---|---|---|
| `RENT_FCTR_RATE_OUT` | `TO_CHAR(RENT_FCTR_RATE)` | 将数值型租金因子率转为字符串 |
| `SCHM_CODE_OUT` | `decode(SCHM_CODE, 'T', 'no_of_tran_tps', 'no_of_tran_hos')` | Schema 转描述码 |
| `TIME` | `to_date(concat($$v_rpt_mth, '01'), 'yyyymmdd')` | 报表月份 |
| `LAST_REC_TXN_DATE` | `SYSDATE` | 系统时间 |
| `SCHM_CODE1` | `'SMS'` | SMS Schema |
| `SCHM_CODE2` | `'SMS2'` | SMS2 Schema（用于区分租金索引维度） |

##### EXPTRANS1 — 历史+当前汇总

| 输出字段 | 表达式 | 说明 |
|---|---|---|
| `TXN_CNT_V` | `iif(ISNULL(TXN_CNT), 0, TXN_CNT)` | 当前数据，NULL 处理 |
| `TXN_CNT1_V` | `iif(ISNULL(TXN_CNT1), 0, TXN_CNT1)` | 历史数据，NULL 处理 |
| `SUM` | `TXN_CNT_V + TXN_CNT1_V` | 当前 + 历史求和 |
| `DESP_DTL_CODE` | `'no_of_tran_total'` | 总数标记 |
| `SCHM_CODE` | `'SMS'` | SMS Schema |

---

### 4.3 `M_S5_DPA_SUMMARIZE_FACT_EMS_SMS_FLAT_SIZE_TXN`

**功能**: 按单位面积（UNIT_SFA_AREA）分组统计各类面积区间的房屋数量。

#### 核心逻辑 — Router + Expression 组合

##### RTRTRANS / RTRTRANS1 — 路由分组条件

| 分组名 | 条件 | 面积范围 | 生成面积编码 |
|---|---|---|---|
| `Group_Over_60` | `UNIT_SFA_AREA > 60` | > 60 | `'1'` |
| `Group_55_to_60` | `> 55 and <= 60` | 55~60 | `'2'` |
| `Group_50_to_55` | `> 50 and <= 55` | 50~55 | `'3'` |
| `Group_45_to_50` | `> 45 and <= 50` | 45~50 | `'4'` |
| `Group_40_to_45` | `> 40 and <= 45` | 40~45 | `'5'` |
| `Group_35_to_40` | `> 35 and <= 40` | 35~40 | `'6'` |
| `Group_30_to_35` | `> 30 and <= 35` | 30~35 | `'7'` |
| `Group_22_to_30` | `> 22 and <= 30` | 22~30 | `'8'` |
| `Group_Under_22` | `<= 22` | <= 22 | `'9'` |

##### EXPTRANS1~EXPTRANS9 — 每组对应的面积编码

每个 Expression 转换逻辑一致，例如：
```
EXPTRANS1:  UNIT_SIZE_CODE = '1'   (面积 > 60)
EXPTRANS2:  UNIT_SIZE_CODE = '2'   (面积 55~60)
...
EXPTRANS9:  UNIT_SIZE_CODE = '9'   (面积 <= 22)
```

##### EXPTRANS — 汇总后的 Expression

| 输出字段 | 表达式 | 说明 |
|---|---|---|
| `DESP_DTL` | `decode(SCHM_CODE, 'T', 'no_of_tran_tps', 'no_of_tran_hos')` | Schema 转描述 |
| `LAST_REC_TXN_DATE` | `SYSDATE` | 系统时间 |
| `TIME` | `to_date(concat($$v_rpt_mth, '01'), 'yyyymmdd')` | 报表月份 |
| `SCHM_CODE1` | `'SMS'` | SMS Schema |

##### EXPTRANS10 — 总数 Expression

| 输出字段 | 表达式 | 说明 |
|---|---|---|
| `DESP_DTL` | `'no_of_tran_total'` | 总数标记 |
| `LAST_REC_TXN_DATE` | `SYSDATE` | 系统时间 |
| `TIME` | `to_date(concat($$v_rpt_mth, '01'), 'yyyymmdd')` | 报表月份 |
| `SCHM_CODE1` | `'SMS'` | SMS Schema |

---

### 4.4 `M_S5_DPA_SUMMARIZE_FACT_EMS_SMS_PREM_PYMT_A`

**功能**: 计算保费支付相关数据。

#### 关键 SQ 查询逻辑

**SQ_SOR_EMS_SMS_CAS_APLY_STS1** — 已支付保费的 TPS 单元数:
```sql
SELECT COUNT(1)
FROM sor_ems_hsm_tps_unit_sts
WHERE TRUNC(unit_prem_pay_date, 'MM') <= TO_DATE($$v_rpt_mth, 'YYYYMM')
  AND TO_DATE($$V_SNSH_DATE, 'YYYYMMDD') BETWEEN BGN_DATE AND END_DATE
```

**SQ_SOR_EMS_SMS_CAS_APLY_STS2** — TPS 协议但没有外部业主的单元 - 已支付保费的 TPS 单元:
```sql
SELECT SMS - TPS
FROM (
  -- SMS: TPS 协议且首次分配日在24个月前 ~ 报表月之间的记录
  SELECT COUNT(1) AS SMS FROM SOR_EMS_TOW_TPS_AGRMT ...
  -- TPS: 已支付保费的单元数
  SELECT COUNT(1) AS TPS FROM sor_ems_hsm_tps_unit_sts ...
) A, B
```

---

## 5. 可重用查询转换 (Reusable Lookups)

| 转换名称 | 查询表 | 关联条件 |
|---|---|---|
| `LKP_DDS_DMNS_EMS_APLY_TYPE` | `DDS_DMNS_EMS_APLY_TYPE` | `APLY_TYPE_CODE = IN_APLY_TYPE_CODE AND APLY_TYPE_SCHM_CODE = IN_APLY_TYPE_SCHM_CODE` |
| `LKP_DDS_DMNS_EMS_UNIT_SIZE` | `DDS_DMNS_EMS_UNIT_SIZE` | `UNIT_SIZE_CODE = IN_UNIT_SIZE_CODE AND UNIT_SIZE_SCHM_CODE = IN_UNIT_SIZE_SCHM_CODE` |
| `LKP_DDS_DMNS_EMS_DSTR` | `DDS_DMNS_DSTR_DTL` | `DSTR_CODE = IN_DSTR_CODE AND DSTR_SCHM_CODE = IN_DSTR_SCHM_CODE` |
| `LKP_DDS_DMNS_EMS_CRT` | `DDS_DMNS_EMS_CRT` | `CRT_CODE = IN_CRT_CODE AND CRT_SCHM_CODE = IN_CRT_SCHM_CODE` |
| `LKP_DDS_DMNS_EMS_GNRL_STAT` | `DDS_DMNS_EMS_GNRL_STAT` | `GNRL_STAT_CODE = IN_GNRL_STAT_CODE AND GNRL_STAT_SCHM_CODE = IN_GNRL_STAT_SCHM_CODE` |
| `LKP_DDS_DMNS_EMS_ASGN_YEAR` | `DDS_DMNS_EMS_ASGN_YEAR` | `ASGN_YEAR_CODE = IN_ASGN_YEAR_CODE AND ASGN_YEAR_SCHM_CODE = IN_ASGN_YEAR_SCHM_CODE` |
| `LKP_DDS_DMNS_EMS_DESP_DTL` | `DDS_DMNS_EMS_DESP_DTL` | `DESP_DTL_CODE = IN_DESP_DTL_CODE AND DESP_DTL_SCHM_CODE = IN_DESP_DTL_SCHM_CODE` |
| `LKP_DDS_DMNS_EMS_UNIT_PRC` | `DDS_DMNS_EMS_UNIT_PRC` | `UNIT_PRC_CODE = IN_UNIT_PRC_CODE AND UNIT_PRC_SCHM_CODE = IN_UNIT_PRC_SCHM_CODE` |
| `LKP_DDS_DMNS_EMS_UNIT_TYPE` | `DDS_DMNS_EMS_UNIT_TYPE` | `UNIT_TYPE_CODE = IN_UNIT_TYPE_CODE AND UNIT_TYPE_SCHM_CODE = IN_UNIT_TYPE_SCHM_CODE` |
| `LKP_DDS_DMNS_TIME_1` | `DDS_DMNS_TIME` | `TIME_VAL_DATE = IN_TIME_VAL_DATE`（带过滤条件 `TIME_DMNS_KEY like '2%'`） |

---

## 6. 映射参数

| 参数名 | 数据类型 | 说明 |
|---|---|---|
| `$$v_snsh_date` | string (19) | 快照日期，格式 `YYYYMMDD` |
| `$$v_rpt_mth` | string (6) | 报表月份，格式 `YYYYMM` |

这两个参数贯穿整个工作流，用于控制数据的时间范围。

---

## 7. 总结

### 整体业务逻辑

1. **数据抽取**：从 SOR 层多表（LN、LN_STS、LN_APLY、LN_APLY_STS、CEP_CERT、CEP_CERT_STS、CEP_APLY、CEP_APLY_STS、CAS_CERT、CAS_CERT_STS、CAS_APLY、CAS_APLY_STS 等）通过多表 JOIN 抽取原始数据
2. **数据分类**：
   - 按地址编码首字母将申请类型分为 PRH/E、COT/C、HSO/R、THA/L、INT/E9
   - 按 Schema 区分 TPS（私人房屋）和 HOS（公营房屋）
   - 按面积区间将房屋分为 9 个面积段
3. **Expression 核心转换模式**：
   - **`DECODE` / `IIF`**：条件映射与 NULL 处理
   - **`RTRIM(LTRIM())`**：字符串清洗
   - **`to_date(concat($$v_rpt_mth, '01'), 'yyyymmdd')`**：报表月份参数转日期
   - **`SYSDATE`**：记录当前处理时间
   - **NULL 安全求和**：`iif(ISNULL(X), 0, X)` 模式确保 NULL 不破坏总和
4. **汇总计算**：通过 Aggregator（`COUNT(*)` / `SUM(TXN_CNT)`）和 Expression 计算各维度汇总值
5. **维度关联**：通过 Lookup 将业务编码转换为对应的维度键（DMNS_KEY）
6. **目标写入**：最终数据写入 DPA 层的多个事实表

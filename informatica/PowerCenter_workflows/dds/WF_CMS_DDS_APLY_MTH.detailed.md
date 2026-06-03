# WF_CMS_DDS_APLY_MTH 详细数据逻辑

说明：本文件基于 PowerCenter 导出 XML (`WF_CMS_DDS_APLY_MTH.XML`) 提取，列出每个 Mapping 与 Session 的字段级映射、Source/Target、以及会话（Session）相关数据逻辑与配置。

## 总览
- Worklet: `WL_CMS_DDS_FACT_MTH_APLY`
- Sessions: `S_DDS_APLY_FACT_CMS_CASE_SMRY`, `S_DDS_APLY_FACT_CMS_CASE_OSTD_SMRY`, `S_DDS_APLY_FACT_CMS_ORD_SMRY`, `S_DDS_APLY_FACT_CMS_CASE_PRNT_SMRY`
- 连接（ConnectionReference）: `DB_PCIS`（用于所有 Reader/Writer）
- 参数文件（每个 session）: `$PMSourceFileDir\\PCIS01\\cms_mth_job_param.txt`

---

## Mapping: `M_DPA_APLY_FACT_CMS_CASE_OSTD_SMRY`
- Source: `DPA_FACT_CMS_CASE_OSTD_SMRY`
- Target: `DDS_FACT_CMS_CASE_OSTD_SMRY`
- Source Qualifier: `SQ_DPA_FACT_CMS_CASE_OSTD_SMRY`

### 字段映射（来自 <CONNECTOR>）
- `CASE_CATG_SCD_KEY` -> `CASE_CATG_SCD_KEY`
- `BLK_SCD_KEY` -> `BLK_SCD_KEY`
- `CASE_OSTD_PRD_DMNS_KEY` -> `CASE_OSTD_PRD_DMNS_KEY`
- `CMS_CASE_OSTD_CNT` -> `CMS_CASE_OSTD_CNT`
- `LAST_REC_TXN_DATE` -> `LAST_REC_TXN_DATE`
- `TIME_DMNS_KEY` -> `TIME_DMNS_KEY`
- `CASE_TYPE_SCD_KEY` -> `CASE_TYPE_SCD_KEY`
- `BLK_AGE_DMNS_KEY` -> `BLK_AGE_DMNS_KEY`
- `EST_OFFC_SCD_KEY` -> `EST_OFFC_SCD_KEY`
- `LAST_REC_TXN_TYPE_CODE` -> `LAST_REC_TXN_TYPE_CODE`
- `CMS_BLK_SCD_KEY` -> `CMS_BLK_SCD_KEY`
- `EST_SCD_KEY` -> `EST_SCD_KEY`
- `CMS_EST_SCD_KEY` -> `CMS_EST_SCD_KEY`
- `CMS_CASE_ITEM_OSTD_CNT` -> `CMS_CASE_ITEM_OSTD_CNT`

### Mapping 配置/属性
- Source Qualifier TableAttributes: `Tracing Level=Normal`, `Select Distinct=NO`, `Is Partitionable=NO`（未定义 SQL Query/Source Filter）
- TargetLoadOrder: `DDS_FACT_CMS_CASE_OSTD_SMRY` (ORDER=1)

---

## Mapping: `M_DDS_APLY_FACT_CMS_CASE_SMRY`
- Source: `DPA_FACT_CMS_CASE_SMRY`
- Target: `DDS_FACT_CMS_CASE_SMRY`
- Source Qualifier: `SQ_DPA_FACT_CMS_CASE_SMRY`

### 字段映射
- `TIME_DMNS_KEY` -> `TIME_DMNS_KEY`
- `RQS_CHNL_DMNS_KEY` -> `RQS_CHNL_DMNS_KEY`
- `CASE_TYPE_SCD_KEY` -> `CASE_TYPE_SCD_KEY`
- `BLK_AGE_DMNS_KEY` -> `BLK_AGE_DMNS_KEY`
- `EST_OFFC_SCD_KEY` -> `EST_OFFC_SCD_KEY`
- `HSHLD_SIZE_DMNS_KEY` -> `HSHLD_SIZE_DMNS_KEY`
- `UNIT_SIZE_DMNS_KEY` -> `UNIT_SIZE_DMNS_KEY`
- `CASE_CATG_SCD_KEY` -> `CASE_CATG_SCD_KEY`
- `RSDN_LNG_DMNS_KEY` -> `RSDN_LNG_DMNS_KEY`
- `BLK_SCD_KEY` -> `BLK_SCD_KEY`
- `HSHLD_AEM_IND` -> `HSHLD_AEM_IND`
- `HSHLD_ELDR_IND` -> `HSHLD_ELDR_IND`
- `HSHLD_DSBL_IND` -> `HSHLD_DSBL_IND`
- `CMS_CASE_CNT` -> `CMS_CASE_CNT`
- `CMS_CASE_CMPLT_CNT` -> `CMS_CASE_CMPLT_CNT`
- `CMS_CASE_RPET_CNT` -> `CMS_CASE_RPET_CNT`
- `CMS_CASE_NEW_CNT` -> `CMS_CASE_NEW_CNT`
- `LAST_REC_TXN_DATE` -> `LAST_REC_TXN_DATE`
- `LAST_REC_TXN_TYPE_CODE` -> `LAST_REC_TXN_TYPE_CODE`
- `EST_SCD_KEY` -> `EST_SCD_KEY`
- `CMS_BLK_SCD_KEY` -> `CMS_BLK_SCD_KEY`
- `CMS_EST_SCD_KEY` -> `CMS_EST_SCD_KEY`
- `CMS_CASE_ITEM_CNT` -> `CMS_CASE_ITEM_CNT`
- `CMS_CASE_ITEM_CMPLT_CNT` -> `CMS_CASE_ITEM_CMPLT_CNT`
- `CMS_CASE_ITEM_RPET_CNT` -> `CMS_CASE_ITEM_RPET_CNT`
- `CMS_CASE_ITEM_NEW_CNT` -> `CMS_CASE_ITEM_NEW_CNT`
- `CMS_RCPT_PRN_CNT` -> `CMS_RCPT_PRN_CNT`

### Mapping 配置
- Source Qualifier TableAttributes: `Tracing Level=Normal`, `Select Distinct=NO`, `Is Partitionable=NO`
- TargetLoadOrder: `DDS_FACT_CMS_CASE_SMRY` (ORDER=1)

---

## Mapping: `M_DDS_APLY_FACT_CMS_ORD_SMRY`
- Source: `DPA_FACT_CMS_ORD_SMRY`
- Target: `DDS_FACT_CMS_ORD_SMRY`
- Source Qualifier: `SQ_DPA_FACT_CMS_ORD_SMRY`

### 字段映射
- `BLK_SCD_KEY` -> `BLK_SCD_KEY`
- `BLK_AGE_DMNS_KEY` -> `BLK_AGE_DMNS_KEY`
- `ERP_WO_CNT` -> `ERP_WO_CNT`
- `ERP_WO_CMPLT_CNT` -> `ERP_WO_CMPLT_CNT`
- `ARTSN_ORD_CNT` -> `ARTSN_ORD_CNT`
- `ARTSN_ORD_CMPLT_CNT` -> `ARTSN_ORD_CMPLT_CNT`
- `LAST_REC_TXN_DATE` -> `LAST_REC_TXN_DATE`
- `LAST_REC_TXN_TYPE_CODE` -> `LAST_REC_TXN_TYPE_CODE`
- `CMS_BLK_SCD_KEY` -> `CMS_BLK_SCD_KEY`
- `EST_SCD_KEY` -> `EST_SCD_KEY`
- `CMS_EST_SCD_KEY` -> `CMS_EST_SCD_KEY`
- `ERP_WO_ITEM_CNT` -> `ERP_WO_ITEM_CNT`
- `ERP_WO_ITEM_CMPLT_CNT` -> `ERP_WO_ITEM_CMPLT_CNT`
- `TIME_DMNS_KEY` -> `TIME_DMNS_KEY`
- `EST_OFFC_SCD_KEY` -> `EST_OFFC_SCD_KEY`
- `UNIT_SIZE_DMNS_KEY` -> `UNIT_SIZE_DMNS_KEY`

### Mapping 配置
- Source Qualifier TableAttributes: `Tracing Level=Normal`, `Select Distinct=NO`, `Is Partitionable=NO`
- TargetLoadOrder: `DDS_FACT_CMS_ORD_SMRY` (ORDER=1)

---

## Mapping: `M_DDS_APLY_FACT_CMS_CASE_PRNT_SMRY`
- Source: `DPA_FACT_CMS_CASE_PRNT_SMRY`
- Target: `DDS_FACT_CMS_CASE_PRNT_SMRY`
- Source Qualifier: `SQ_DPA_FACT_CMS_CASE_PRNT_SMRY`

### 字段映射
- `TIME_DMNS_KEY` -> `TIME_DMNS_KEY`
- `RQS_CHNL_DMNS_KEY` -> `RQS_CHNL_DMNS_KEY`
- `CASE_CATG_SCD_KEY` -> `CASE_CATG_SCD_KEY`
- `EST_SCD_KEY` -> `EST_SCD_KEY`
- `CMS_CASE_CNT` -> `CMS_CASE_CNT`
- `CMS_RCPT_PRN_CNT` -> `CMS_RCPT_PRN_CNT`
- `LAST_REC_TXN_DATE` -> `LAST_REC_TXN_DATE`
- `LAST_REC_TXN_TYPE_CODE` -> `LAST_REC_TXN_TYPE_CODE`

### Mapping / Target 实例属性
- 在 Target Definition (`DDS_FACT_CMS_CASE_PRNT_SMRY`) 的 <INSTANCE> 上定义了 Pre SQL：
  - `delete from dds_fact_cms_case_prnt_smry where time_dmns_key in (select distinct time_dmns_key from dpa_fact_cms_case_prnt_smry)`
- TargetLoadOrder: `DDS_FACT_CMS_CASE_PRNT_SMRY` (ORDER=1)

---

## Sessions（会话）详细配置

### `S_DDS_APLY_FACT_CMS_CASE_OSTD_SMRY`
- Mapping: `M_DPA_APLY_FACT_CMS_CASE_OSTD_SMRY`
- Session Log File: `S_DDS_APLY_FACT_CMS_CASE_OSTD_SMRY.log`
- Parameter Filename: `$PMSourceFileDir\\PCIS01\\cms_mth_job_param.txt`
- Connection 值: `Relational:DB_PCIS`（Reader/Writer 均使用 `DB_PCIS`）
- Treat source rows as: `Insert`
- Commit Interval: `10000`; `Commit On End Of File = YES`
- Recovery Strategy: `Fail task and continue workflow`
- Writer (`Relational Writer`) 属性：
  - `Target load type = Normal`
  - `Insert = YES`
  - `Update as Update = YES`
  - `Update as Insert = NO`
  - `Update else Insert = NO`
  - `Delete = YES`
  - `Truncate target table option = NO`
  - `Reject file directory = $PMBadFileDir\\`，`Reject filename = dds_fact_cms_case_ostd_smry1.bad`

### `S_DDS_APLY_FACT_CMS_CASE_SMRY`
- Mapping: `M_DDS_APLY_FACT_CMS_CASE_SMRY`
- Session Log File: `S_DDS_APLY_FACT_CMS_CASE_SMRY.log`
- Parameter Filename: `$PMSourceFileDir\\PCIS01\\cms_mth_job_param.txt`
- Connection: `DB_PCIS`
- Treat source rows as: `Insert`
- Commit Interval: `10000`; `Commit On End Of File = YES`
- Recovery Strategy: `Fail task and continue workflow`
- Writer 属性：
  - `Insert = YES`
  - `Update as Update = NO`
  - `Delete = NO`
  - `Reject filename = dds_fact_cms_case_smry1.bad`

### `S_DDS_APLY_FACT_CMS_ORD_SMRY`
- Mapping: `M_DDS_APLY_FACT_CMS_ORD_SMRY`
- Session Log File: `S_DDS_APLY_FACT_CMS_ORD_SMRY.log`
- Parameter Filename: `$PMSourceFileDir\\PCIS01\\cms_mth_job_param.txt`
- Connection: `DB_PCIS`
- Commit Interval: `10000`; `Commit On End Of File = YES`
- Writer 属性：
  - `Insert = YES`
  - `Update as Update = NO`
  - `Delete = NO`
  - `Reject filename = dds_fact_cms_ord_smry1.bad`

### `S_DDS_APLY_FACT_CMS_CASE_PRNT_SMRY`
- Mapping: `M_DDS_APLY_FACT_CMS_CASE_PRNT_SMRY`
- Session Log File: `S_DDS_APLY_FACT_CMS_CASE_PRNT_SMRY.log`
- Parameter Filename: `$PMSourceFileDir\\PCIS01\\cms_mth_job_param.txt`
- Connection: `DB_PCIS`
- Commit Interval: `10000`; `Commit On End Of File = YES`
- Writer 属性：
  - `Insert = YES`
  - `Update as Update = YES`
  - `Delete = YES`
  - `Reject filename = dds_fact_cms_case_prnt_smry1.bad`
- Target Instance Pre SQL: 同 Mapping 部分，预先 delete 对应 time_dmns_key 的历史数据
- 注意：在 Worklet 中该 TaskInstance 配置 `FAIL_PARENT_IF_INSTANCE_FAILS="NO"`（失败不影响父流程）

---

## Worklet 节点与流程控制
- Worklet: `WL_CMS_DDS_FACT_MTH_APLY` 包含 TaskInstances：
  - `Start` -> `S_DDS_APLY_FACT_CMS_CASE_SMRY` -> `S_DDS_APLY_FACT_CMS_CASE_OSTD_SMRY` -> `S_DDS_APLY_FACT_CMS_ORD_SMRY`
  - 并行分支：`Start` -> `S_DDS_APLY_FACT_CMS_CASE_PRNT_SMRY`（并行；且失败不影响主链）
- TaskInstance 上 `FAIL_PARENT_IF_INSTANCE_DID_NOT_RUN` 与 `FAIL_PARENT_IF_INSTANCE_FAILS` 多为 `YES`，但 `S_DDS_APLY_FACT_CMS_CASE_PRNT_SMRY` 标记为 `NO/NO`（容错）

## 排查与扩展建议
- 若需确认增量逻辑：检查 `cms_mth_job_param.txt` 参数文件中是否传入时间窗（time_dmns_key 或起止日期）以及 Session 的 Source Filter 或 Pre SQL（在 XML 中 Source Filter 为空，意味着参数控制或全量读取）。
- 若需控制并发/性能：查看 Repository 中的 Session Config `default_session_config`（已设置 `Maximum Memory Allowed For Auto Memory Attributes=512MB` 等）或在运行时覆盖。
- 若需确保数据一致性：关注 `Commit Interval`、`Delete` 操作（尤其是 `CASE_PRNT_SMRY` 的 Pre SQL），并在目标表上确认相应索引/外键是否存在以避免锁争用。

---
_文件生成自 PowerCenter XML 导出：WF_CMS_DDS_APLY_MTH.XML_
